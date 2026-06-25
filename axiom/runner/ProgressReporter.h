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

#include <chrono>
#include <string>
#include <vector>

#include "axiom/runner/QueryProgress.h"
#include "axiom/runner/QueryProgressBuilder.h"

namespace folly {
class FunctionScheduler;
} // namespace folly

namespace facebook::axiom::runner {

class Runner;

/// RAII periodic reporter of one query's progress.
///
/// While alive it polls `runner` on the supplied `scheduler` every `intervalMs`
/// (clamped up to kMinProgressReportIntervalMs) and invokes `onProgress` from a
/// scheduler thread; the destructor stops polling -- waiting out any in-flight
/// poll -- then emits one final report from the calling thread, which by then
/// is single-threaded with respect to this query. The user callback never
/// throws into the query: an exception from it is logged and skipped.
///
/// @code
///   folly::FunctionScheduler scheduler;
///   scheduler.start();
///   {
///     ProgressReporter reporter(runner, scheduler, queryId, onProgress, 500);
///     fetchResults(runner); // reporter polls while the query runs
///   } // ~reporter stops polling and emits the final report
/// @endcode
///
/// Invariants the caller must uphold:
/// - `scheduler` is started before construction and outlives this object.
/// - `runner` outlives this object; the final report reads it during teardown.
/// - `onProgress` must not deregister this query -- that would deadlock the
/// poll.
///
/// When `onProgress` is empty nothing is registered and the scheduler is
/// untouched.
class ProgressReporter {
 public:
  /// Default target interval between progress reports, in milliseconds.
  static constexpr int32_t kDefaultProgressReportIntervalMs = 500;

  /// Minimum interval between progress reports, in milliseconds. Caller-set
  /// intervals are clamped up to this to bound stats() aggregation overhead.
  static constexpr int32_t kMinProgressReportIntervalMs = 50;

  ProgressReporter(
      Runner& runner,
      folly::FunctionScheduler& scheduler,
      std::string queryId,
      QueryProgressCallback onProgress,
      int32_t intervalMs);

  ~ProgressReporter();

  ProgressReporter(const ProgressReporter&) = delete;
  ProgressReporter& operator=(const ProgressReporter&) = delete;
  ProgressReporter(ProgressReporter&&) = delete;
  ProgressReporter& operator=(ProgressReporter&&) = delete;

 private:
  // Invokes onProgress_ with a caller-built snapshot, logging and swallowing
  // any exception the user callback throws. Building the snapshot stays with
  // the caller, so this guards only the callback. poll() builds outside this
  // guard so a builder invariant violation surfaces during execution; the
  // destructor builds inside its own catch since it must not throw while a
  // failing query unwinds.
  void reportProgress(const QueryProgress& progress);

  // Builds a snapshot and reports it. Does nothing until the runner is running.
  void poll();

  Runner& runner_;
  folly::FunctionScheduler& scheduler_;
  std::string queryId_;
  QueryProgressCallback onProgress_;
  std::chrono::steady_clock::time_point startTime_;
  // Stage topology, derived once from the plan and reused by every poll and the
  // final report.
  std::vector<QueryProgressBuilder::StageTopology> stages_;
  // Scheduler function handle; empty when nothing is registered.
  std::string name_;
};

} // namespace facebook::axiom::runner
