/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/response_generator.h"

#include <unistd.h>

#include <algorithm>
#include <deque>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
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

}  // namespace valkey_search::options

namespace valkey_search::query {

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
  if (parameters.index_schema &&
      parameters.index_schema->GetTextIndexSchema()) {
    // Evaluate against the per-key text indexes created by background ingestion.
    // This assumes the key is no longer in-flight and has been fully indexed.
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

// Adds all local content for neighbors to the list of neighbors.
// This function is meant to be used for non-vector queries.
absl::Status ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters) {
  return ProcessNeighborsForReply(ctx, attribute_data_type, neighbors, parameters, "");
}

// Adds all local content for neighbors to the list of neighbors.
//
// Any neighbors already contained in the attribute content map will be skipped.
// Any data not found locally will be skipped.
//
// For queries with text predicates, this function checks if any neighbor keys
// are in-flight (being mutated/indexed). If so, it keeps the client blocked
// by yielding and retrying periodically until the background ingestion completes,
// ensuring we can safely evaluate text predicates against the per-key text indexes.
absl::Status ProcessNeighborsForReply(ValkeyModuleCtx *ctx,
                              const AttributeDataType &attribute_data_type,
                              std::deque<indexes::Neighbor> &neighbors,
                              const query::SearchParameters &parameters,
                              const std::string &identifier) {
  // Check if we need to evaluate text predicates (schema has text fields and filter exists)
  const bool has_text_filter = 
      parameters.filter_parse_results.root_predicate &&
      parameters.index_schema &&
      parameters.index_schema->GetTextIndexSchema() != nullptr;
  
  // If we have text filters, wait for any in-flight keys to finish indexing
  // This ensures we can safely evaluate text predicates against the per-key
  // text indexes created by background ingestion.
  if (has_text_filter) {
    constexpr int kMaxRetries = 1000;  // Prevent infinite loops (livelock protection)
    constexpr int kRetryDelayUs = 1000;  // 1ms between retries
    
    for (int retry = 0; retry < kMaxRetries; ++retry) {
      absl::flat_hash_set<InternedStringPtr> in_flight_keys;
      for (const auto &neighbor : neighbors) {
        if (neighbor.attribute_contents.has_value()) {
          // Already has content, skip
          continue;
        }
        if (parameters.index_schema->IsKeyInFlight(neighbor.external_id)) {
          in_flight_keys.insert(neighbor.external_id);
        }
      }
      
      // If no keys are in-flight, proceed with processing
      if (in_flight_keys.empty()) {
        break;
      }
      
      // Keys are still in-flight, yield and retry
      if (retry == 0) {
        VMSDK_LOG(DEBUG, ctx) << "Found " << in_flight_keys.size() 
                              << " in-flight keys with text filter. "
                              << "Waiting for background ingestion to complete.";
      }
      
      // Check for cancellation/timeout
      if (parameters.cancellation_token->IsCancelled()) {
        return absl::DeadlineExceededError(
            "Timed out waiting for in-flight keys to complete indexing");
      }
      
      // Yield briefly to allow background indexing to progress
      usleep(kRetryDelayUs);
      
      // Log if we're retrying for too long (potential livelock)
      if (retry > 0 && retry % 100 == 0) {
        VMSDK_LOG(WARNING, ctx) << "Still waiting for " << in_flight_keys.size()
                                << " in-flight keys after " << retry << " retries";
      }
    }
  }
  
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
  // TODO: incorporate a retry in case of removal.
  neighbors.erase(
      std::remove_if(neighbors.begin(), neighbors.end(),
                     [](const indexes::Neighbor &neighbor) {
                       return !neighbor.attribute_contents.has_value();
                     }),
      neighbors.end());
  
  return absl::OkStatus();
}

}  // namespace valkey_search::query
