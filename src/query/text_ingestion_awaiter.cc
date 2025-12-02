/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/text_ingestion_awaiter.h"

#include <optional>
#include <utility>

#include "absl/log/log.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "src/index_schema.h"
#include "src/query/predicate.h"

namespace valkey_search::query {

namespace {

constexpr absl::Duration kDefaultPollInterval = absl::Milliseconds(5);

}  // namespace

TextMutationAwaiter::TextMutationAwaiter(ConflictSupplier conflict_supplier,
                                         RegisterWaiterFn register_waiter,
                                         cancel::Token cancellation_token,
                                         vmsdk::ThreadPool *scheduler,
                                         CompletionCallback on_ready,
                                         FailureCallback on_failure,
                                         absl::Duration poll_interval)
    : conflict_supplier_(std::move(conflict_supplier)),
      register_waiter_(std::move(register_waiter)),
      cancellation_token_(std::move(cancellation_token)),
      scheduler_(scheduler),
      on_ready_(std::move(on_ready)),
      on_failure_(std::move(on_failure)),
      poll_interval_(poll_interval <= absl::ZeroDuration()
                         ? kDefaultPollInterval
                         : poll_interval) {
  CHECK(scheduler_ != nullptr);
  CHECK(on_ready_);
  CHECK(on_failure_);
}

void TextMutationAwaiter::Start() { Evaluate(); }

void TextMutationAwaiter::OnKeySettled(const InternedStringPtr & /*key*/) {
  size_t remaining = 0;
  {
    absl::MutexLock lock(&mutex_);
    if (pending_keys_ == 0) {
      return;
    }
    remaining = --pending_keys_;
    if (remaining > 0) {
      return;
    }
    timeout_poll_scheduled_ = false;
  }
  auto weak_self = weak_from_this();
  scheduler_->Schedule(
      [weak_self]() {
        if (auto self = weak_self.lock()) {
          self->Evaluate();
        }
      },
      vmsdk::ThreadPool::Priority::kLow);
}

void TextMutationAwaiter::Evaluate() {
  if (completed_.load(std::memory_order_acquire)) {
    return;
  }
  auto conflicts = conflict_supplier_();
  if (conflicts.empty()) {
    Complete();
    return;
  }
  if (IsCancelled()) {
    Fail(absl::DeadlineExceededError(
        "Timed out waiting for text ingestion to finish"));
    return;
  }
  Register(conflicts);
}

void TextMutationAwaiter::Register(
    const std::vector<InternedStringPtr> &conflicts) {
  {
    absl::MutexLock lock(&mutex_);
    pending_keys_ = conflicts.size();
    timeout_poll_scheduled_ = false;
  }
  auto waiter = std::static_pointer_cast<TextIngestionWaiter>(shared_from_this());
  for (const auto &key : conflicts) {
    register_waiter_(key, waiter);
  }
  ScheduleTimeoutPoll();
}

void TextMutationAwaiter::Complete() {
  bool expected = false;
  if (!completed_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
    return;
  }
  on_ready_();
}

void TextMutationAwaiter::Fail(absl::Status status) {
  bool expected = false;
  if (!completed_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
    return;
  }
  on_failure_(std::move(status));
}

void TextMutationAwaiter::ScheduleTimeoutPoll() {
  bool should_schedule = false;
  {
    absl::MutexLock lock(&mutex_);
    if (pending_keys_ == 0) {
      return;
    }
    if (!timeout_poll_scheduled_) {
      timeout_poll_scheduled_ = true;
      should_schedule = true;
    }
  }
  if (!should_schedule) {
    return;
  }
  auto weak_self = weak_from_this();
  scheduler_->Schedule(
      [weak_self]() {
        if (auto self = weak_self.lock()) {
          absl::SleepFor(self->poll_interval_);
          self->CheckTimeout();
        }
      },
      vmsdk::ThreadPool::Priority::kLow);
}

void TextMutationAwaiter::CheckTimeout() {
  if (completed_.load(std::memory_order_acquire)) {
    return;
  }
  {
    absl::MutexLock lock(&mutex_);
    timeout_poll_scheduled_ = false;
    if (pending_keys_ == 0) {
      return;
    }
  }
  if (IsCancelled()) {
    Fail(absl::DeadlineExceededError(
        "Timed out waiting for text ingestion to finish"));
    return;
  }
  ScheduleTimeoutPoll();
}

bool TextMutationAwaiter::IsCancelled() const {
  return cancellation_token_ && cancellation_token_->IsCancelled();
}

bool ShouldBlockOnTextPredicates(const SearchParameters &parameters) {
  if (parameters.no_content || !parameters.IsNonVectorQuery()) {
    return false;
  }
  if (!parameters.filter_parse_results.root_predicate) {
    return false;
  }
  return ContainsTextPredicate(
      parameters.filter_parse_results.root_predicate.get());
}

std::vector<InternedStringPtr> CollectConflictingKeys(
    const SearchParameters &parameters,
    const std::deque<indexes::Neighbor> &neighbors) {
  std::vector<InternedStringPtr> conflicts;
  if (!parameters.index_schema) {
    return conflicts;
  }
  for (const auto &neighbor : neighbors) {
    if (!neighbor.external_id) {
      continue;
    }
    if (parameters.index_schema->IsKeyInFlight(neighbor.external_id)) {
      conflicts.push_back(neighbor.external_id);
    }
  }
  return conflicts;
}

}  // namespace valkey_search::query
