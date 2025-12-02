/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_WAITER_H_
#define VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_WAITER_H_

#include <memory>

#include "src/utils/string_interning.h"

namespace valkey_search::query {

// Interface used by IndexSchema to notify waiters when a key finishes
// ingestion. Implementations should assume OnKeySettled can be invoked from
// arbitrary threads.
class TextIngestionWaiter
    : public std::enable_shared_from_this<TextIngestionWaiter> {
 public:
  virtual ~TextIngestionWaiter() = default;
  virtual void OnKeySettled(const InternedStringPtr &key) = 0;
};

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_WAITER_H_
