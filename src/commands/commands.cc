/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include <memory>

#include "fanout.h"
#include "ft_create_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

// Delay in milliseconds between retry attempts when waiting for in-flight
// mutations to complete for pure full-text queries.
constexpr mstime_t kTextQueryRetryDelayMs = 1;

namespace async {

struct Result {
  cancel::Token cancellation_token;
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
  std::unique_ptr<QueryCommand> parameters;
};

int Timeout(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
            [[maybe_unused]] int argc) {
  return ValkeyModule_ReplyWithError(
      ctx, "Search operation cancelled due to timeout");
}

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *res =
      static_cast<Result *>(ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(res != nullptr);

  // Check if operation was cancelled and partial results are disabled
  if (!options::GetEnablePartialResults().GetValue() &&
      res->parameters->cancellation_token->IsCancelled()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, "Search operation cancelled due to timeout");
  }

  if (!res->neighbors.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, res->neighbors.status().message().data());
  }
  res->parameters->SendReply(ctx, res->neighbors.value());
  return VALKEYMODULE_OK;
}

void Free([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *result = static_cast<Result *>(privdata);
  delete result;
}

}  // namespace async

// State for text query retry when in-flight mutations are detected.
// Holds all state needed to retry processing and eventually unblock the client.
struct TextQueryRetryState {
  vmsdk::BlockedClient blocked_client;
  std::deque<indexes::Neighbor> neighbors;
  std::unique_ptr<QueryCommand> parameters;

  TextQueryRetryState(vmsdk::BlockedClient bc,
                      std::deque<indexes::Neighbor> n,
                      std::unique_ptr<QueryCommand> p)
      : blocked_client(std::move(bc)),
        neighbors(std::move(n)),
        parameters(std::move(p)) {}
};

// Forward declaration
void ProcessTextQueryWithRetry(TextQueryRetryState *state);

// Timer callback for text query retry
void TextQueryRetryTimerCallback(ValkeyModuleCtx * /*ctx*/, void *data) {
  auto *state = static_cast<TextQueryRetryState *>(data);
  ProcessTextQueryWithRetry(state);
}

// Process a pure full-text query with retry logic for in-flight mutations.
// If conflicts are detected and timeout hasn't occurred, schedules a retry.
// Otherwise, completes processing and unblocks the client.
void ProcessTextQueryWithRetry(TextQueryRetryState *state) {
  auto &neighbors = state->neighbors;
  auto *parameters = state->parameters.get();

  // Check if we've timed out
  bool timed_out = parameters->cancellation_token->IsCancelled();
  if (timed_out && !options::GetEnablePartialResults().GetValue()) {
    // Timed out and partial results disabled - return error
    auto result = std::make_unique<async::Result>(async::Result{
        .neighbors = absl::DeadlineExceededError(
            "Search operation cancelled due to timeout"),
        .parameters = std::move(state->parameters),
    });
    state->blocked_client.SetReplyPrivateData(result.release());
    delete state;
    return;
  }

  // Create a thread-safe context for processing
  auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);

  // Try to process neighbors - check for conflicts
  auto status = query::ProcessNonVectorNeighborsForReply(
      ctx.get(), parameters->index_schema->GetAttributeDataType(), neighbors,
      *parameters);

  if (status == query::ProcessingStatus::kConflictsDetected && !timed_out) {
    // Conflicts detected and not timed out - schedule retry
    // Use StartTimerFromBackgroundThread since we might not be on main thread
    vmsdk::StartTimerFromBackgroundThread(ctx.get(), kTextQueryRetryDelayMs,
                                          TextQueryRetryTimerCallback, state);
    return;
  }

  // Processing complete - prepare result and unblock client
  auto result = std::make_unique<async::Result>(async::Result{
      .neighbors = std::move(neighbors),
      .parameters = std::move(state->parameters),
  });
  state->blocked_client.SetReplyPrivateData(result.release());
  delete state;
}

CONTROLLED_BOOLEAN(ForceReplicasOnly, false);

//
// Common Class for FT.SEARCH and FT.AGGREGATE command
//
absl::Status QueryCommand::Execute(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString **argv, int argc,
                                   std::unique_ptr<QueryCommand> parameters) {
  auto status = [&]() -> absl::Status {
    auto &schema_manager = SchemaManager::Instance();
    vmsdk::ArgsIterator itr{argv + 1, argc - 1};
    parameters->timeout_ms = options::GetDefaultTimeoutMs().GetValue();
    VMSDK_RETURN_IF_ERROR(
        vmsdk::ParseParamValue(itr, parameters->index_schema_name));
    VMSDK_ASSIGN_OR_RETURN(
        parameters->index_schema,
        SchemaManager::Instance().GetIndexSchema(
            ValkeyModule_GetSelectedDb(ctx), parameters->index_schema_name));
    VMSDK_RETURN_IF_ERROR(
        vmsdk::ParseParamValue(itr, parameters->parse_vars.query_string));
    VMSDK_RETURN_IF_ERROR(parameters->ParseCommand(itr));
    parameters->parse_vars.ClearAtEndOfParse();
    parameters->cancellation_token =
        cancel::Make(parameters->timeout_ms, nullptr);
    static const auto permissions =
        PrefixACLPermissions(kSearchCmdPermissions, kSearchCommand);
    VMSDK_RETURN_IF_ERROR(AclPrefixCheck(
        ctx, permissions, parameters->index_schema->GetKeyPrefixes()));

    parameters->index_schema->ProcessMultiQueue();

    const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParallelQueries() ||
                           inside_multi_exec)) {
      VMSDK_ASSIGN_OR_RETURN(
          auto neighbors,
          query::Search(*parameters, query::SearchMode::kLocal));
      if (!options::GetEnablePartialResults().GetValue() &&
          parameters->cancellation_token->IsCancelled()) {
        ValkeyModule_ReplyWithError(
            ctx, "Search operation cancelled due to timeout");
        ++Metrics::GetStats().query_failed_requests_cnt;
        return absl::OkStatus();
      }
      parameters->SendReply(ctx, neighbors);
      return absl::OkStatus();
    }

    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, parameters->timeout_ms);
    blocked_client.MeasureTimeStart();

    // Check if this is a pure full-text query that needs special retry handling
    bool needs_text_query_retry =
        query::ShouldBlockOnInFlightMutations(*parameters);

    auto on_done_callback = [blocked_client = std::move(blocked_client),
                             needs_text_query_retry](
                                auto &neighbors, auto parameters) mutable {
      std::unique_ptr<QueryCommand> upcast_parameters(
          dynamic_cast<QueryCommand *>(parameters.release()));
      CHECK(upcast_parameters != nullptr);

      // For pure full-text queries, use retry mechanism to wait for in-flight
      // mutations
      if (needs_text_query_retry && neighbors.ok()) {
        auto *state = new TextQueryRetryState(std::move(blocked_client),
                                              std::move(neighbors.value()),
                                              std::move(upcast_parameters));
        // Run on main thread to check conflicts and potentially retry
        vmsdk::RunByMain(
            [state]() { ProcessTextQueryWithRetry(state); }, true);
        return;
      }

      // Standard path for vector queries and error cases
      auto result = std::make_unique<async::Result>(async::Result{
          .neighbors = std::move(neighbors),
          .parameters = std::move(upcast_parameters),
      });
      blocked_client.SetReplyPrivateData(result.release());
    };

    if (ValkeySearch::Instance().UsingCoordinator() &&
        ValkeySearch::Instance().IsCluster() && !parameters->local_only) {
      auto mode = /* !vmsdk::IsReadOnly(ctx) ? query::fanout::kPrimaries ? */
          ForceReplicasOnly.GetValue()
              ? vmsdk::cluster_map::FanoutTargetMode::kOneReplicaPerShard
              : vmsdk::cluster_map::FanoutTargetMode::kRandom;
      // refresh cluster map if needed
      auto search_targets =
          ValkeySearch::Instance().GetOrRefreshClusterMap(ctx)->GetTargets(
              mode);
      return query::fanout::PerformSearchFanoutAsync(
          ctx, search_targets,
          ValkeySearch::Instance().GetCoordinatorClientPool(),
          std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
          std::move(on_done_callback));
    }
    return query::SearchAsync(
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done_callback), query::SearchMode::kLocal);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

}  // namespace valkey_search
