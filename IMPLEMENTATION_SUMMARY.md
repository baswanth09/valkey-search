# Implementation Summary: Blocking Search Response for In-Flight Keys with Text Predicates

## Overview
This implementation addresses the issue where text predicate evaluation needs to wait for background ingestion to complete when response keys are in-flight (being mutated/indexed). Instead of creating temporary text indexes for evaluation, we leverage the per-key text indexes populated by background ingestion.

## Changes Made

### 1. Added Helper Function to Detect Text Predicates (`src/query/predicate.h` & `src/query/predicate.cc`)

**File: `src/query/predicate.h`**
- Added `bool HasTextPredicate() const;` method to the `Predicate` class
- This method recursively checks if a predicate tree contains any text predicates

**File: `src/query/predicate.cc`**
- Implemented `Predicate::HasTextPredicate()` which:
  - Returns `true` for `PredicateType::kText`
  - Recursively checks child predicates for composed predicates (AND/OR) and negated predicates
  - Returns `false` for numeric and tag predicates

### 2. Implemented Blocking Mechanism in ProcessNeighborsForReply (`src/query/response_generator.cc`)

**Key Changes:**
- Modified `ProcessNeighborsForReply` signature to return `absl::Status` instead of `void`
- Added in-flight key detection logic when text predicates are present:
  - Checks each neighbor key using `IndexSchema::IsKeyInFlight()`
  - If keys are in-flight, enters a retry loop with the following characteristics:
    - **Max retries**: 1000 (preventing infinite loops/livelocks)
    - **Retry delay**: 1ms (`usleep(1000)`)
    - **Timeout check**: Respects query cancellation token
    - **Logging**: Warns after every 100 retries if keys are still in-flight
- Removed the TODO comment about waiting for in-flight operations (line 155)
- Added comment explaining that evaluation now happens against per-key text indexes after background ingestion completes

**Rationale:**
The blocking approach keeps the client blocked on the main thread while waiting for background ingestion to complete. This ensures:
1. We don't create expensive temporary text indexes
2. We evaluate against accurate, fully-indexed data
3. The implementation is simple and doesn't require complex callback mechanisms

### 3. Updated Function Signature (`src/query/response_generator.h`)

**Changes:**
- Updated `ProcessNeighborsForReply` and `ProcessNonVectorNeighborsForReply` to return `absl::Status`
- Added documentation explaining that `absl::UnavailableError` is returned when processing needs retry

### 4. Updated All Callers to Handle Status Return

**File: `src/commands/ft_search.cc`**
- Modified `SearchCommand::SendReply()` to check status from `ProcessNeighborsForReply`
- Returns error reply if status is not OK
- Increments failure metrics on error

**File: `src/coordinator/server.cc`**
- Updated gRPC search request handler to check status
- Returns gRPC error status if processing fails
- Records failure metrics appropriately

**File: `src/commands/ft_aggregate.cc`**
- Updated `SendReplyInner()` to propagate status from `ProcessNeighborsForReply`
- Returns early if status indicates failure

**File: `testing/query/response_generator_test.cc`**
- Updated test calls to ignore status (`.IgnoreError()`) since tests don't exercise the in-flight key scenario

### 5. Added Required Include

**File: `src/query/response_generator.cc`**
- Added `#include <unistd.h>` for `usleep()` function used in retry loop

## Behavior

### Normal Case (No In-Flight Keys)
1. Search completes and returns neighbors
2. `ProcessNeighborsForReply` checks for text predicates
3. If present, checks if any neighbor keys are in-flight
4. If no keys are in-flight, proceeds normally with content fetching and verification

### In-Flight Keys Case (Text Predicates Present)
1. Search completes and returns neighbors
2. `ProcessNeighborsForReply` detects text predicates and checks for in-flight keys
3. Finds one or more keys are in-flight
4. Enters retry loop:
   - Yields for 1ms to allow background threads to progress
   - Checks again if keys are still in-flight
   - Repeats until either:
     - All keys are no longer in-flight → proceeds with verification
     - Query timeout is reached → returns `absl::DeadlineExceededError`
     - Max retries reached (livelock protection)
5. Once keys are no longer in-flight, evaluates text predicates against the per-key text indexes

### Livelock Protection
- Maximum 1000 retries (approximately 1 second of waiting)
- Respects query cancellation token
- Logs warnings if stuck waiting for too long

## Thread Safety
- `IndexSchema::IsKeyInFlight()` is thread-safe (uses mutex internally)
- The retry loop runs on the main thread
- Background indexing threads progress independently

## Performance Considerations
- **Best case**: No in-flight keys → no blocking, immediate evaluation
- **Typical case**: Keys in-flight for short period (a few ms) → minimal blocking
- **Worst case**: Livelock scenario → timeout after configured query timeout or max retries

## Testing
- Existing tests updated to handle new status return
- Livelock scenario is expected to be rare (as mentioned in task description)
- Integration tests should verify behavior with concurrent mutations

## Compatibility
- This is a prerequisite-aware implementation that builds on commit 8723c7b52f2af3aac9f2c5f1ffecbe0268b4d649
- Leverages the existing `IsKeyInFlight()` utility added in that commit
- No breaking changes to external API
