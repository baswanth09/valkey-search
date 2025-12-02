/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_AWAITER_H_
#define VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_AWAITER_H_

#include <functional>
#include <memory>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/query/text_ingestion_waiter.h"
#include "src/utils/cancel.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {

using ConflictSupplier =
    absl::AnyInvocable<std::vector<InternedStringPtr>()>;
using RegisterWaiterFn = absl::AnyInvocable<void(
    const InternedStringPtr &, const std::shared_ptr<TextIngestionWaiter> &)>;
using CompletionCallback = absl::AnyInvocable<void()>;
using FailureCallback = absl::AnyInvocable<void(absl::Status)>;

class TextMutationAwaiter : public TextIngestionWaiter,
                            public std::enable_shared_from_this<
                                TextMutationAwaiter> {
 public:
  TextMutationAwaiter(ConflictSupplier conflict_supplier,
                      RegisterWaiterFn register_waiter,
                      cancel::Token cancellation_token,
                      vmsdk::ThreadPool *scheduler,
                      CompletionCallback on_ready,
                      FailureCallback on_failure,
                      absl::Duration poll_interval = absl::Milliseconds(2));
  void Start();
  void OnKeySettled(const InternedStringPtr &key) override;

 private:
  void Evaluate();
  void Register(const std::vector<InternedStringPtr> &conflicts);
  void Complete();
  void Fail(absl::Status status);
  void ScheduleTimeoutPoll();
  void CheckTimeout();
  bool IsCancelled() const;

  ConflictSupplier conflict_supplier_;
  RegisterWaiterFn register_waiter_;
  cancel::Token cancellation_token_;
  vmsdk::ThreadPool *scheduler_;
  CompletionCallback on_ready_;
  FailureCallback on_failure_;
  absl::Duration poll_interval_;

  std::atomic<bool> completed_{false};
  absl::Mutex mutex_;
  size_t pending_keys_ ABSL_GUARDED_BY(mutex_){0};
  bool timeout_poll_scheduled_ ABSL_GUARDED_BY(mutex_){false};
};

bool ShouldBlockOnTextPredicates(const SearchParameters &parameters);

std::vector<InternedStringPtr> CollectConflictingKeys(
    const SearchParameters &parameters,
    const std::deque<indexes::Neighbor> &neighbors);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_TEXT_INGESTION_AWAITER_H_
