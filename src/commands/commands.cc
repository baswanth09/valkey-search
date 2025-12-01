/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include "fanout.h"
#include "ft_create_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/query/predicate.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
namespace async {

// Retry interval in milliseconds for checking in-flight keys
constexpr mstime_t kInFlightKeyRetryIntervalMs = 10;

struct Result {
  cancel::Token cancellation_token;
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
  std::unique_ptr<QueryCommand> parameters;
};

// Context for retrying non-vector queries when keys are in-flight
struct NonVectorRetryContext {
  vmsdk::BlockedClient blocked_client;
  std::deque<indexes::Neighbor> neighbors;
  std::unique_ptr<QueryCommand> parameters;
  ValkeyModuleTimerID timer_id{0};
};

// Forward declaration
void TryUnblockNonVectorQuery(NonVectorRetryContext *retry_ctx);

// Timer callback for retrying non-vector query processing
void NonVectorQueryRetryTimerCallback(ValkeyModuleCtx *ctx, void *data) {
  auto *retry_ctx = static_cast<NonVectorRetryContext *>(data);
  retry_ctx->timer_id = 0;
  TryUnblockNonVectorQuery(retry_ctx);
}

// Check if any neighbor keys are in-flight for a non-vector query with text
// predicates. If so, schedule a retry timer. Otherwise, unblock the client.
void TryUnblockNonVectorQuery(NonVectorRetryContext *retry_ctx) {
  // Check if the query has been cancelled/timed out
  if (retry_ctx->parameters->cancellation_token->IsCancelled()) {
    // Timeout reached, unblock with the current state
    auto result = std::make_unique<Result>(Result{
        .neighbors = std::move(retry_ctx->neighbors),
        .parameters = std::move(retry_ctx->parameters),
    });
    retry_ctx->blocked_client.SetReplyPrivateData(result.release());
    delete retry_ctx;
    return;
  }

  // Check if query has text predicates and if any keys are in-flight
  bool has_text_predicates =
      retry_ctx->parameters->index_schema &&
      retry_ctx->parameters->index_schema->GetTextIndexSchema() &&
      query::ContainsTextPredicate(
          retry_ctx->parameters->filter_parse_results.root_predicate.get());

  bool keys_in_flight = false;
  if (has_text_predicates) {
    for (const auto &neighbor : retry_ctx->neighbors) {
      if (retry_ctx->parameters->index_schema->IsKeyInFlight(
              neighbor.external_id)) {
        keys_in_flight = true;
        VMSDK_LOG(DEBUG, nullptr)
            << "Key " << neighbor.external_id->Str()
            << " is still in-flight, scheduling retry";
        break;
      }
    }
  }

  if (keys_in_flight) {
    // Schedule another retry timer
    // We need a context to create timers from
    auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
    retry_ctx->timer_id = ValkeyModule_CreateTimer(
        ctx.get(), kInFlightKeyRetryIntervalMs,
        NonVectorQueryRetryTimerCallback, retry_ctx);
    if (retry_ctx->timer_id == 0) {
      // Timer creation failed, proceed with unblocking anyway
      VMSDK_LOG(WARNING, nullptr)
          << "Failed to create retry timer, proceeding with in-flight keys";
      auto result = std::make_unique<Result>(Result{
          .neighbors = std::move(retry_ctx->neighbors),
          .parameters = std::move(retry_ctx->parameters),
      });
      retry_ctx->blocked_client.SetReplyPrivateData(result.release());
      delete retry_ctx;
    }
    return;
  }

  // No in-flight keys (or no text predicates), proceed with unblocking
  auto result = std::make_unique<Result>(Result{
      .neighbors = std::move(retry_ctx->neighbors),
      .parameters = std::move(retry_ctx->parameters),
  });
  retry_ctx->blocked_client.SetReplyPrivateData(result.release());
  delete retry_ctx;
}

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

    // Check if this is a non-vector query with text predicates that might need
    // in-flight key handling
    bool is_non_vector_with_text =
        parameters->IsNonVectorQuery() && parameters->index_schema &&
        parameters->index_schema->GetTextIndexSchema() &&
        query::ContainsTextPredicate(
            parameters->filter_parse_results.root_predicate.get());

    auto on_done_callback = [blocked_client = std::move(blocked_client),
                             is_non_vector_with_text](
                                auto &neighbors, auto parameters) mutable {
      std::unique_ptr<QueryCommand> upcast_parameters(
          dynamic_cast<QueryCommand *>(parameters.release()));
      CHECK(upcast_parameters != nullptr);

      // For non-vector queries with text predicates, check for in-flight keys
      // before unblocking to ensure text predicate evaluation uses correctly
      // indexed data.
      if (is_non_vector_with_text && neighbors.ok()) {
        // Use RunByMain to check for in-flight keys and potentially schedule
        // retries. This ensures the check happens on the main thread where
        // keyspace access is safe.
        auto *retry_ctx = new async::NonVectorRetryContext{
            .blocked_client = std::move(blocked_client),
            .neighbors = std::move(neighbors.value()),
            .parameters = std::move(upcast_parameters),
        };
        vmsdk::RunByMain([retry_ctx]() {
          async::TryUnblockNonVectorQuery(retry_ctx);
        });
        return;
      }

      // Standard path: immediately unblock the client
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
