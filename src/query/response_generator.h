/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
#define VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_

#include <deque>
#include <functional>
#include <memory>
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
/// before blocking the client.
vmsdk::config::Number &GetTextQueryInFlightWaitTimeoutMs();

}  // namespace valkey_search::options
namespace valkey_search::query {

// Result status from ProcessNeighborsForReply functions.
enum class ProcessNeighborsStatus {
  // Processing completed successfully. Caller should send response.
  kComplete,
  // Client was blocked waiting for in-flight mutations. Caller should NOT
  // send response - the blocked client callback will handle it.
  kBlocked,
};

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
//
// For text queries with in-flight mutations, this may block the client
// and return kBlocked. In that case, the caller should NOT send a response -
// the blocked client callback will handle response generation when the
// mutations complete.
//
// The send_response callback is called when processing completes (either
// immediately or after blocking). It should serialize and send the response.
ProcessNeighborsStatus ProcessNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters, const std::string &identifier,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response);

ProcessNeighborsStatus ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters,
    std::function<void(ValkeyModuleCtx *, std::deque<indexes::Neighbor> &)>
        send_response);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
