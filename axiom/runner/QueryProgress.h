/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "axiom/common/Enums.h"

namespace facebook::axiom::runner {

/// Execution state of a query or a stage. Models successful progress only;
/// query failure and cancellation are reported through the completion callback
/// (QueryCompletionInfo), not here, so a failed query's final progress report
/// reflects its last observed state.
enum class ExecutionState {
  /// No split or driver has started yet.
  kPlanned,
  /// At least one split or driver is executing.
  kRunning,
  /// All splits and drivers have completed.
  kFinished,
};

AXIOM_DECLARE_ENUM_NAME(ExecutionState);

/// Execution counters for a query or a single stage.
struct ExecutionStats {
  /// Overall completion of the query or stage. Already accounts for the
  /// runner's finished flag and driver completion, not just the split counts
  /// below.
  ExecutionState state{ExecutionState::kPlanned};

  /// Rows/bytes read from storage by table scans.
  int64_t scanRows{0};
  int64_t scanBytes{0};
  /// Rows/bytes received from exchanges (shuffled in from producer stages).
  int64_t shuffleRows{0};
  int64_t shuffleBytes{0};
  /// Operator CPU time, in microseconds.
  int64_t cpuTimeMicros{0};

  /// Splits seen so far. The runner adds splits during execution, so this grows
  /// over time and is not a fixed denominator; it only bounds its substates
  /// (>= queued + running + completed), which aggregate across concurrently
  /// updating tasks.
  int32_t totalSplits{0};
  int32_t queuedSplits{0};
  int32_t runningSplits{0};
  int32_t completedSplits{0};
};

/// Execution counters for one stage, with its place in the stage tree.
struct StageProgress {
  /// Indexes of the stages feeding this one over an exchange, by position in
  /// `QueryProgress::stages`. The stages form a tree; these are its producer
  /// edges. Empty for a connector-only leaf. Entries must not be reordered or
  /// filtered, or these references break.
  std::vector<int32_t> producers;

  ExecutionStats stats;
};

/// Live progress of a query during execution.
struct QueryProgress {
  /// Unique identifier for this query execution.
  std::string queryId;

  /// Query-level counters: each numeric field is the sum across stages. Read
  /// `stats.state` directly for overall completion.
  ExecutionStats stats;

  /// Wall-clock microseconds since query start.
  int64_t wallTimeMicros{0};

  /// Per-stage detail, one entry per task in the runner's stats() and
  /// positionally 1:1 with it.
  std::vector<StageProgress> stages;
};

/// Reports progress while the query runs. Periodic reports come from a
/// scheduler (background) thread; the final report on teardown is invoked
/// synchronously on the thread that destroys the ProgressReporter, so a caller
/// may see it on its own thread. Keep it cheap and thread-safe; it must not
/// block the query. It must not deregister its own query from within the
/// callback -- that waits on the poll it is running inside and deadlocks.
using QueryProgressCallback = std::function<void(const QueryProgress&)>;

} // namespace facebook::axiom::runner
