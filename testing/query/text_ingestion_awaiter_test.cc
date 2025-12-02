#include "src/query/text_ingestion_awaiter.h"

#include <atomic>
#include <memory>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {
namespace {

class TextMutationAwaiterTest : public ::testing::Test {
 protected:
  TextMutationAwaiterTest() : thread_pool_("text-awaiter-test", 1) {
    thread_pool_.StartWorkers();
  }
  vmsdk::ThreadPool thread_pool_;
};

TEST_F(TextMutationAwaiterTest, CompletesImmediatelyWithoutConflicts) {
  absl::Notification done;
  auto cancellation = cancel::Make(1000, nullptr);
  auto waiter = std::make_shared<TextMutationAwaiter>(
      []() { return std::vector<InternedStringPtr>(); },
      [](const InternedStringPtr&,
         const std::shared_ptr<TextIngestionWaiter>&) {},
      cancellation, &thread_pool_, [&]() { done.Notify(); },
      [&](absl::Status status) { ADD_FAILURE() << status; });
  waiter->Start();
  EXPECT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST_F(TextMutationAwaiterTest, WaitsUntilKeySettled) {
  absl::Notification done;
  auto key = StringInternStore::Intern("blocking-key");
  std::atomic<int> attempts = 0;
  auto cancellation = cancel::Make(1000, nullptr);
  std::shared_ptr<TextIngestionWaiter> registered_waiter;
  absl::Mutex mu;
  ConflictSupplier supplier = [&]() {
    if (attempts.fetch_add(1) == 0) {
      return std::vector<InternedStringPtr>{key};
    }
    return std::vector<InternedStringPtr>();
  };
  RegisterWaiterFn register_waiter =
      [&](const InternedStringPtr&,
          const std::shared_ptr<TextIngestionWaiter>& waiter) {
        absl::MutexLock lock(&mu);
        registered_waiter = waiter;
      };
  auto waiter = std::make_shared<TextMutationAwaiter>(
      std::move(supplier), std::move(register_waiter), cancellation,
      &thread_pool_, [&]() { done.Notify(); },
      [&](absl::Status status) { ADD_FAILURE() << status; });
  waiter->Start();
  // Wait until the waiter is registered before notifying completion.
  bool notified = false;
  for (int i = 0; i < 100; ++i) {
    {
      absl::MutexLock lock(&mu);
      if (registered_waiter) {
        registered_waiter->OnKeySettled(key);
        notified = true;
        break;
      }
    }
    absl::SleepFor(absl::Milliseconds(5));
  }
  ASSERT_TRUE(notified) << "Waiter was never registered";
  EXPECT_TRUE(done.WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST_F(TextMutationAwaiterTest, FailsWhenCancelled) {
  absl::Notification failed;
  auto cancellation = cancel::Make(0, nullptr);
  cancellation->Cancel();
  auto key = StringInternStore::Intern("timeout-key");
  auto waiter = std::make_shared<TextMutationAwaiter>(
      [key]() { return std::vector<InternedStringPtr>{key}; },
      [](const InternedStringPtr&,
         const std::shared_ptr<TextIngestionWaiter>&) {},
      cancellation, &thread_pool_,
      []() { ADD_FAILURE() << "Should not complete successfully"; },
      [&](absl::Status status) {
        EXPECT_TRUE(absl::IsDeadlineExceeded(status));
        failed.Notify();
      });
  waiter->Start();
  EXPECT_TRUE(failed.WaitForNotificationWithTimeout(absl::Seconds(1)));
}

}  // namespace
}  // namespace valkey_search::query
