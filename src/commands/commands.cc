/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include <optional>

#include "fanout.h"
#include "ft_create_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/query/search.h"
#include "src/query/text_ingestion_awaiter.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"

namespace valkey_search {
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

namespace {

struct PendingSearchReply {
  std::optional<vmsdk::BlockedClient> blocked_client;
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
  std::unique_ptr<QueryCommand> parameters;
  std::shared_ptr<query::TextMutationAwaiter> waiter;

  PendingSearchReply(vmsdk::BlockedClient blocked_client_in,
                     absl::StatusOr<std::deque<indexes::Neighbor>> neighbors_in,
                     std::unique_ptr<QueryCommand> parameters_in)
      : blocked_client(std::move(blocked_client_in)),
        neighbors(std::move(neighbors_in)),
        parameters(std::move(parameters_in)) {}

  void Reply() {
    if (!blocked_client.has_value()) {
      return;
    }
    auto result = std::make_unique<async::Result>(async::Result{
        .neighbors = std::move(neighbors),
        .parameters = std::move(parameters),
    });
    blocked_client->SetReplyPrivateData(result.release());
    blocked_client.reset();
  }

  void ReplyWithStatus(absl::Status status) {
    neighbors = std::move(status);
    Reply();
  }
};

}  // namespace

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
    auto on_done_callback =
        [blocked_client = std::move(blocked_client)](
            auto &neighbors, auto parameters) mutable {
          std::unique_ptr<QueryCommand> upcast_parameters(
              dynamic_cast<QueryCommand *>(parameters.release()));
          CHECK(upcast_parameters != nullptr);
          auto pending = std::make_shared<PendingSearchReply>(
              std::move(blocked_client), std::move(neighbors),
              std::move(upcast_parameters));
          if (!pending->neighbors.ok()) {
            pending->Reply();
            return;
          }
          if (!query::ShouldBlockOnTextPredicates(*pending->parameters) ||
              !pending->parameters->index_schema) {
            pending->Reply();
            return;
          }
          auto conflicts = query::CollectConflictingKeys(
              *pending->parameters, pending->neighbors.value());
          if (conflicts.empty()) {
            pending->Reply();
            return;
          }
          auto scheduler = ValkeySearch::Instance().GetReaderThreadPool();
          CHECK(scheduler != nullptr);
          auto weak_schema = pending->parameters->index_schema;
          query::ConflictSupplier conflict_supplier =
              [pending]() -> std::vector<InternedStringPtr> {
            return query::CollectConflictingKeys(*pending->parameters,
                                                 pending->neighbors.value());
          };
          query::RegisterWaiterFn register_waiter =
              [weak_schema](
                  const InternedStringPtr &key,
                  const std::shared_ptr<query::TextIngestionWaiter> &waiter) {
                if (auto schema = weak_schema.lock()) {
                  schema->RegisterTextIngestionWaiter(key, waiter);
                } else {
                  waiter->OnKeySettled(key);
                }
              };
          query::CompletionCallback on_ready =
              [pending]() { pending->Reply(); };
          query::FailureCallback on_failure =
              [pending](absl::Status status) {
                pending->ReplyWithStatus(std::move(status));
              };
          pending->waiter = std::make_shared<query::TextMutationAwaiter>(
              std::move(conflict_supplier), std::move(register_waiter),
              pending->parameters->cancellation_token, scheduler,
              std::move(on_ready), std::move(on_failure));
          pending->waiter->Start();
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
