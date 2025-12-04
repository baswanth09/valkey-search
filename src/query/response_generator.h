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

#include "src/attribute_data_type.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

/// Return the configuration entry that allows the caller to control the
/// maximum content size for a record in the search response
vmsdk::config::Number &GetMaxSearchResultRecordSize();

/// Return the configuration entry that allows the caller to control the
/// maximum number of fields in the content of the search response
vmsdk::config::Number &GetMaxSearchResultFieldsCount();

}  // namespace valkey_search::options
namespace valkey_search::query {

// Status returned by ProcessNeighborsForReply indicating whether processing
// completed or if a retry is needed due to in-flight mutations.
enum class ProcessingStatus {
  kComplete,           // Processing finished successfully
  kConflictsDetected   // In-flight mutations detected, caller should retry
};

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
//
// For pure full-text queries (non-vector queries with text predicates),
// this function checks for in-flight mutations on result keys. If any
// result key has pending mutations, returns kConflictsDetected to signal
// that the caller should retry after a brief delay.
//
// Returns:
//   kComplete - Processing finished, neighbors are ready
//   kConflictsDetected - In-flight mutations detected, retry needed
ProcessingStatus ProcessNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters, const std::string &identifier);

ProcessingStatus ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters);

// Helper function to check if a predicate tree contains any text predicates.
// Used to determine if a query is a "pure full-text query" that requires
// blocking on in-flight mutations.
bool ContainsTextPredicate(const Predicate *predicate);

// Helper function to determine if a query should block on in-flight mutations.
// Returns true for pure full-text queries (non-vector queries with text
// predicates).
bool ShouldBlockOnInFlightMutations(const SearchParameters &parameters);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
