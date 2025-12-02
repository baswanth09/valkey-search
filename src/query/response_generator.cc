/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/response_generator.h"

#include <algorithm>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

constexpr absl::string_view kMaxSearchResultRecordSizeConfig{
    "max-search-result-record-size"};
constexpr int kMaxSearchResultRecordSize{10 * 1024 * 1024};  // 10MB
constexpr int kMinSearchResultRecordSize{100};
constexpr absl::string_view kMaxSearchResultFieldsCountConfig{
    "max-search-result-record-size"};
constexpr int kMaxSearchResultFieldsCount{1000};

/// Register the "--max-search-result-record-size" flag. Controls the max
/// content size for a record in the search response
static auto max_search_result_record_size =
    vmsdk::config::NumberBuilder(
        kMaxSearchResultRecordSizeConfig,  // name
        kMaxSearchResultRecordSize / 2,    // default size
        kMinSearchResultRecordSize,        // min size
        kMaxSearchResultRecordSize)        // max size
        .WithValidationCallback(CHECK_RANGE(kMinSearchResultRecordSize,
                                            kMaxSearchResultRecordSize,
                                            kMaxSearchResultRecordSizeConfig))
        .Build();

/// Register the "--max-search-result-fields-count" flag. Controls the max
/// number of fields in the content of the search response
static auto max_search_result_fields_count =
    vmsdk::config::NumberBuilder(
        kMaxSearchResultFieldsCountConfig,  // name
        kMaxSearchResultFieldsCount / 2,    // default size
        1,                                  // min size
        kMaxSearchResultFieldsCount)        // max size
        .WithValidationCallback(CHECK_RANGE(1, kMaxSearchResultFieldsCount,
                                            kMaxSearchResultFieldsCountConfig))
        .Build();

vmsdk::config::Number &GetMaxSearchResultRecordSize() {
  return dynamic_cast<vmsdk::config::Number &>(*max_search_result_record_size);
}
vmsdk::config::Number &GetMaxSearchResultFieldsCount() {
  return dynamic_cast<vmsdk::config::Number &>(*max_search_result_fields_count);
}

/// Configuration for text query in-flight mutation wait timeout (milliseconds).
/// This is the maximum time the client can be blocked waiting for in-flight
/// mutations before timing out.
constexpr absl::string_view kTextQueryInFlightWaitTimeoutMsConfig{
    "text-query-inflight-wait-timeout-ms"};
constexpr int kDefaultTextQueryInFlightWaitTimeoutMs{5000};  // 5 seconds
constexpr int kMinTextQueryInFlightWaitTimeoutMs{0};
constexpr int kMaxTextQueryInFlightWaitTimeoutMs{60000};  // 60 seconds

static auto text_query_inflight_wait_timeout_ms =
    vmsdk::config::NumberBuilder(
        kTextQueryInFlightWaitTimeoutMsConfig,
        kDefaultTextQueryInFlightWaitTimeoutMs,
        kMinTextQueryInFlightWaitTimeoutMs,
        kMaxTextQueryInFlightWaitTimeoutMs)
        .WithValidationCallback(
            CHECK_RANGE(kMinTextQueryInFlightWaitTimeoutMs,
                        kMaxTextQueryInFlightWaitTimeoutMs,
                        kTextQueryInFlightWaitTimeoutMsConfig))
        .Build();

vmsdk::config::Number &GetTextQueryInFlightWaitTimeoutMs() {
  return dynamic_cast<vmsdk::config::Number &>(
      *text_query_inflight_wait_timeout_ms);
}

}  // namespace valkey_search::options

namespace valkey_search::query {

// Forward declarations
ProcessNeighborsStatus ProcessNeighborsForReplyImpl(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters, const std::string &identifier,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response);

// State stored as private data for the blocked client reply callback.
// This contains all the information needed to continue processing after
// the client is unblocked.
struct InFlightWaitState {
  std::deque<indexes::Neighbor> neighbors;
  // We store a copy of relevant parameters rather than the full SearchParameters
  // since we can't guarantee the lifetime of the original.
  std::shared_ptr<IndexSchema> index_schema;
  std::unique_ptr<AttributeDataType> attribute_data_type;
  std::string identifier;
  // The callback to invoke when processing completes.
  std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
      send_response;
  // Copy of filter parse results for re-evaluation
  query::FilterParseResults filter_parse_results;
  // Other search parameters needed for processing
  std::vector<query::SearchParameters::ReturnAttribute> return_attributes;
};

// Reply callback invoked when the blocked client is unblocked
// (i.e., when the in-flight mutation completes).
int InFlightReplyCallback(ValkeyModuleCtx *ctx,
                          [[maybe_unused]] ValkeyModuleString **argv,
                          [[maybe_unused]] int argc) {
  auto *state = static_cast<InFlightWaitState *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(state != nullptr);

  // Reconstruct SearchParameters from the stored state
  query::SearchParameters parameters;
  parameters.index_schema = state->index_schema;
  parameters.filter_parse_results = std::move(state->filter_parse_results);
  parameters.return_attributes = std::move(state->return_attributes);

  // Re-invoke ProcessNeighborsForReply to check for remaining conflicts
  // and complete processing.
  ProcessNeighborsStatus status = ProcessNeighborsForReplyImpl(
      ctx, *state->attribute_data_type, state->neighbors, parameters,
      state->identifier, std::move(state->send_response));

  // If still blocked, the callback will be invoked again later.
  // If complete, the send_response callback has already been called.
  if (status == ProcessNeighborsStatus::kComplete) {
    // Processing completed, state will be freed by InFlightFreeCallback
  }

  return VALKEYMODULE_OK;
}

// Timeout callback for blocked client waiting for in-flight mutations.
int InFlightTimeoutCallback(ValkeyModuleCtx *ctx,
                            [[maybe_unused]] ValkeyModuleString **argv,
                            [[maybe_unused]] int argc) {
  ++Metrics::GetStats().text_query_inflight_timeout_cnt;

  auto *state = static_cast<InFlightWaitState *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(state != nullptr);

  VMSDK_LOG(WARNING, ctx)
      << "Text query timed out waiting for in-flight mutations";

  // On timeout, we filter out in-flight keys and respond with partial results
  if (state->index_schema) {
    std::vector<InternedStringPtr> neighbor_keys;
    neighbor_keys.reserve(state->neighbors.size());
    for (const auto &neighbor : state->neighbors) {
      if (!neighbor.attribute_contents.has_value()) {
        neighbor_keys.push_back(neighbor.external_id);
      }
    }

    std::vector<InternedStringPtr> still_in_flight =
        state->index_schema->FilterInFlightKeys(neighbor_keys);

    if (!still_in_flight.empty()) {
      absl::flat_hash_set<const InternedString *> in_flight_set;
      for (const auto &key : still_in_flight) {
        in_flight_set.insert(key.get());
      }

      // Remove neighbors that are still in-flight
      state->neighbors.erase(
          std::remove_if(
              state->neighbors.begin(), state->neighbors.end(),
              [&in_flight_set](const indexes::Neighbor &neighbor) {
                return in_flight_set.contains(neighbor.external_id.get());
              }),
          state->neighbors.end());

      VMSDK_LOG(DEBUG, ctx) << "Filtered out " << still_in_flight.size()
                            << " in-flight keys from text query results";
    }
  }

  // Send partial response
  if (state->send_response) {
    state->send_response(ctx, state->neighbors);
  }

  return VALKEYMODULE_OK;
}

// Free callback for cleaning up InFlightWaitState.
void InFlightFreeCallback([[maybe_unused]] ValkeyModuleCtx *ctx,
                          void *privdata) {
  auto *state = static_cast<InFlightWaitState *>(privdata);
  delete state;
}

class PredicateEvaluator : public query::Evaluator {
 public:
  explicit PredicateEvaluator(const RecordsMap &records)
      : records_(records), per_key_indexes_(nullptr) {}

  PredicateEvaluator(
      const RecordsMap &records,
      const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
          *per_key_indexes,
      InternedStringPtr target_key)
      : records_(records),
        per_key_indexes_(per_key_indexes),
        target_key_(target_key) {}

  const InternedStringPtr &GetTargetKey() const override { return target_key_; }

  EvaluationResult EvaluateTags(const query::TagPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return EvaluationResult(false);
      ;
    }
    auto index = predicate.GetIndex();
    auto tags = indexes::Tag::ParseSearchTags(
        vmsdk::ToStringView(it->second.value.get()), index->GetSeparator());
    if (!tags.ok()) {
      return EvaluationResult(false);
      ;
    }
    return predicate.Evaluate(&tags.value(), index->IsCaseSensitive());
  }

  EvaluationResult EvaluateNumeric(
      const query::NumericPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return EvaluationResult(false);
    }
    auto out_numeric =
        vmsdk::To<double>(vmsdk::ToStringView(it->second.value.get()));
    if (!out_numeric.ok()) {
      return EvaluationResult(false);
    }
    return predicate.Evaluate(&out_numeric.value());
  }

  EvaluationResult EvaluateText(const query::TextPredicate &predicate,
                                bool require_positions) override {
    auto it = per_key_indexes_->find(target_key_);
    if (it == per_key_indexes_->end()) {
      VMSDK_LOG(WARNING, nullptr)
          << "Target key not found in index for predicate evaluation";
      return EvaluationResult(false);
    }
    return predicate.Evaluate(it->second, target_key_, require_positions);
  }

 private:
  const RecordsMap &records_;
  const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
      *per_key_indexes_;
  InternedStringPtr target_key_;
};

bool VerifyFilter(const query::Predicate *predicate, const RecordsMap &records,
                  const query::SearchParameters &parameters,
                  InternedStringPtr target_key) {
  if (predicate == nullptr) {
    return true;
  }
  // For text predicates, evaluate using the text index instead of raw data.
  // Note: In-flight key waiting is now handled in ProcessNeighborsForReply
  // before this function is called, ensuring the text index is up-to-date.
  if (parameters.index_schema &&
      parameters.index_schema->GetTextIndexSchema()) {
    return parameters.index_schema->GetTextIndexSchema()->WithPerKeyTextIndexes(
        [&](auto &per_key_indexes) {
          PredicateEvaluator evaluator(records, &per_key_indexes, target_key);
          EvaluationResult result = predicate->Evaluate(evaluator);
          return result.matches;
        });
  }
  PredicateEvaluator evaluator(records);
  EvaluationResult result = predicate->Evaluate(evaluator);
  return result.matches;
}

absl::StatusOr<RecordsMap> GetContentNoReturnJson(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::SearchParameters &parameters, InternedStringPtr key_ptr,
    const std::string &vector_identifier) {
  absl::flat_hash_set<absl::string_view> identifiers;
  identifiers.insert(kJsonRootElementQuery);
  for (const auto &filter_identifier :
       parameters.filter_parse_results.filter_identifiers) {
    identifiers.insert(filter_identifier);
  }
  auto key = key_ptr->Str();
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key_str.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  VMSDK_ASSIGN_OR_RETURN(auto content, attribute_data_type.FetchAllRecords(
                                           ctx, vector_identifier,
                                           key_obj.get(), key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content, parameters, key_ptr)) {
    return absl::NotFoundError("Verify filter failed");
  }
  RecordsMap return_content;
  static const vmsdk::UniqueValkeyString kJsonRootElementQueryPtr =
      vmsdk::MakeUniqueValkeyString(kJsonRootElementQuery);
  return_content.emplace(
      kJsonRootElementQuery,
      RecordsMapValue(
          kJsonRootElementQueryPtr.get(),
          std::move(content.find(kJsonRootElementQuery)->second.value)));
  return return_content;
}

absl::StatusOr<RecordsMap> GetContent(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::SearchParameters &parameters, InternedStringPtr key_ptr,
    const std::string &vector_identifier) {
  if (attribute_data_type.ToProto() ==
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON &&
      parameters.return_attributes.empty()) {
    return GetContentNoReturnJson(ctx, attribute_data_type, parameters, key_ptr,
                                  vector_identifier);
  }
  absl::flat_hash_set<absl::string_view> identifiers;
  for (const auto &return_attribute : parameters.return_attributes) {
    identifiers.insert(vmsdk::ToStringView(return_attribute.identifier.get()));
  }
  if (!parameters.return_attributes.empty()) {
    for (const auto &filter_identifier :
         parameters.filter_parse_results.filter_identifiers) {
      identifiers.insert(filter_identifier);
    }
  }
  auto key = key_ptr->Str();
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key_str.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  VMSDK_ASSIGN_OR_RETURN(auto content, attribute_data_type.FetchAllRecords(
                                           ctx, vector_identifier,
                                           key_obj.get(), key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content, parameters, key_ptr)) {
    return absl::NotFoundError("Verify filter failed");
  }
  if (parameters.return_attributes.empty()) {
    return content;
  }
  RecordsMap return_content;
  for (auto &return_attribute : parameters.return_attributes) {
    auto itr =
        content.find(vmsdk::ToStringView(return_attribute.identifier.get()));
    if (itr == content.end()) {
      continue;
    }
    return_content.emplace(
        vmsdk::ToStringView(return_attribute.identifier.get()),
        RecordsMapValue(
            return_attribute.identifier.get(),
            vmsdk::RetainUniqueValkeyString(itr->second.value.get())));
  }
  return return_content;
}

// Helper to check if blocking for in-flight mutations is required.
// Returns true for non-vector queries that contain text predicates and
// when the client supports blocking (i.e., is a real user client, not a
// thread-safe context).
bool ShouldBlockForInFlightMutations(
    ValkeyModuleCtx *ctx, const query::SearchParameters &parameters) {
  // Only block for real user clients (not thread-safe contexts used by gRPC)
  // Thread-safe contexts don't support ValkeyModule_BlockClient properly.
  if (!vmsdk::IsRealUserClient(ctx)) {
    return false;
  }

  // Only block for non-vector queries (pure text or text+tag/numeric)
  if (!parameters.IsNonVectorQuery()) {
    return false;
  }

  // Only block if the query has text predicates
  return parameters.filter_parse_results.root_predicate != nullptr &&
         ContainsTextPredicate(
             parameters.filter_parse_results.root_predicate.get());
}

// Core implementation of ProcessNeighborsForReply that handles in-flight
// blocking.
ProcessNeighborsStatus ProcessNeighborsForReplyImpl(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters, const std::string &identifier,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response) {
  // For text queries, check for in-flight mutations before processing.
  if (ShouldBlockForInFlightMutations(ctx, parameters) && parameters.index_schema) {
    // Extract all neighbor keys that need to be checked
    std::vector<InternedStringPtr> neighbor_keys;
    neighbor_keys.reserve(neighbors.size());
    for (const auto &neighbor : neighbors) {
      // Only check keys that don't already have content (local keys)
      if (!neighbor.attribute_contents.has_value()) {
        neighbor_keys.push_back(neighbor.external_id);
      }
    }

    if (!neighbor_keys.empty()) {
      // Check which keys are in-flight
      std::vector<InternedStringPtr> in_flight_keys =
          parameters.index_schema->FilterInFlightKeys(neighbor_keys);

      if (!in_flight_keys.empty()) {
        VMSDK_LOG(DEBUG, ctx)
            << "Text query blocking for " << in_flight_keys.size()
            << " in-flight keys";

        // Create state for the blocked client callback
        auto *state = new InFlightWaitState();
        state->neighbors = std::move(neighbors);
        state->index_schema = parameters.index_schema;
        state->attribute_data_type = attribute_data_type.Clone();
        state->identifier = identifier;
        state->send_response = std::move(send_response);
        state->filter_parse_results = parameters.filter_parse_results;
        state->return_attributes = parameters.return_attributes;

        // Create blocked client with callback
        int64_t timeout_ms =
            options::GetTextQueryInFlightWaitTimeoutMs().GetValue();
        vmsdk::BlockedClient blocked_client(
            ctx, InFlightReplyCallback, InFlightTimeoutCallback,
            InFlightFreeCallback, timeout_ms);
        blocked_client.SetReplyPrivateData(state);
        blocked_client.MeasureTimeStart();

        // Attach to the first in-flight key's DocumentMutation.
        // When that mutation completes, our client will be unblocked.
        // The callback will re-check all keys in case there are still conflicts.
        bool attached = parameters.index_schema->AttachBlockedClientToInFlightKey(
            in_flight_keys[0], std::move(blocked_client));

        if (attached) {
          ++Metrics::GetStats().text_query_inflight_wait_success_cnt;
          return ProcessNeighborsStatus::kBlocked;
        }

        // If attach failed (key completed between check and attach), fall
        // through and process normally. The state will be cleaned up when
        // blocked_client goes out of scope and unblocks, which will trigger
        // InFlightFreeCallback.
        VMSDK_LOG(DEBUG, ctx) << "In-flight key completed during attach, "
                                 "proceeding with processing";

        // Recover the state since we're not blocking
        neighbors = std::move(state->neighbors);
        send_response = std::move(state->send_response);
        // Note: state will be freed by InFlightFreeCallback when blocked_client
        // is destroyed
      }
    }
  }

  // No in-flight conflicts (or not a text query), process normally
  const auto max_content_size =
      options::GetMaxSearchResultRecordSize().GetValue();
  const auto max_content_fields =
      options::GetMaxSearchResultFieldsCount().GetValue();
  for (auto &neighbor : neighbors) {
    // neighbors which were added from remote nodes already have attribute
    // content
    if (neighbor.attribute_contents.has_value()) {
      continue;
    }
    auto content = GetContent(ctx, attribute_data_type, parameters,
                              neighbor.external_id, identifier);
    if (!content.ok()) {
      continue;
    }

    // Check content size before assigning
    size_t total_size = 0;
    bool size_exceeded = false;
    if (content.value().size() > max_content_fields) {
      ++Metrics::GetStats().query_result_record_dropped_cnt;
      VMSDK_LOG(WARNING, ctx)
          << "Content field number exceeds configured limit of "
          << max_content_fields
          << " for neighbor with ID: " << (*neighbor.external_id).Str();
      continue;
    }

    for (const auto &item : content.value()) {
      total_size += item.first.size();
      total_size += vmsdk::ToStringView(item.second.value.get()).size();
      if (total_size > max_content_size) {
        ++Metrics::GetStats().query_result_record_dropped_cnt;
        VMSDK_LOG(WARNING, ctx)
            << "Content size exceeds configured limit of " << max_content_size
            << " bytes for neighbor with ID: " << (*neighbor.external_id).Str();
        size_exceeded = true;
        break;
      }
    }

    // Only assign content if it's within size limit
    if (!size_exceeded) {
      neighbor.attribute_contents = std::move(content.value());
    }
  }

  // Remove all entries that don't have content now.
  neighbors.erase(
      std::remove_if(neighbors.begin(), neighbors.end(),
                     [](const indexes::Neighbor &neighbor) {
                       return !neighbor.attribute_contents.has_value();
                     }),
      neighbors.end());

  // Call the send_response callback to serialize and send the response
  if (send_response) {
    send_response(ctx, neighbors);
  }

  return ProcessNeighborsStatus::kComplete;
}

ProcessNeighborsStatus ProcessNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters, const std::string &identifier,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response) {
  return ProcessNeighborsForReplyImpl(ctx, attribute_data_type, neighbors,
                                      parameters, identifier,
                                      std::move(send_response));
}

ProcessNeighborsStatus ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response) {
  return ProcessNeighborsForReply(ctx, attribute_data_type, neighbors,
                                  parameters, "", std::move(send_response));
}

}  // namespace valkey_search::query
