/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
#define VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_

#include <deque>
#include <string>
#include <vector>

#include "src/attribute_data_type.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

/// Return the configuration entry that allows the caller to control the
/// maximum content size for a record in the search response
vmsdk::config::Number &GetMaxSearchResultRecordSize();

/// Return the configuration entry that allows the caller to control the
/// maximum number of fields in the content of the search response
vmsdk::config::Number &GetMaxSearchResultFieldsCount();

/// Return the configuration entry that allows the caller to control the
/// maximum wait time (in milliseconds) for in-flight mutations to complete
/// before processing text query results.
vmsdk::config::Number &GetTextQueryInFlightWaitTimeoutMs();

/// Return the configuration entry that allows the caller to control the
/// poll interval (in milliseconds) when waiting for in-flight mutations.
vmsdk::config::Number &GetTextQueryInFlightPollIntervalMs();

}  // namespace valkey_search::options
namespace valkey_search::query {

// Result of waiting for in-flight keys to complete.
struct InFlightWaitResult {
  // True if all in-flight keys completed before timeout.
  bool all_completed{true};
  // Number of keys that were in-flight at the start of waiting.
  size_t initial_in_flight_count{0};
  // Number of keys still in-flight after waiting (if timed out).
  size_t remaining_in_flight_count{0};
  // Total time spent waiting.
  int64_t wait_time_ms{0};
};

// Waits for in-flight mutations to complete for the specified keys.
// This is used by text queries to ensure that the text index is up-to-date
// before evaluating predicates.
//
// Returns InFlightWaitResult indicating whether all keys completed or timed
// out.
InFlightWaitResult WaitForInFlightKeys(
    ValkeyModuleCtx *ctx, const IndexSchema &index_schema,
    const std::vector<InternedStringPtr> &keys);

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
void ProcessNeighborsForReply(ValkeyModuleCtx *ctx,
                              const AttributeDataType &attribute_data_type,
                              std::deque<indexes::Neighbor> &neighbors,
                              const query::SearchParameters &parameters,
                              const std::string &identifier);

void ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
