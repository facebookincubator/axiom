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

#include <optional>

#include <folly/coro/AsyncScope.h>

#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/connectors/ConnectorSplitManager.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/runner/Runner.h"
#include "axiom/runner/RunnerSession.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/Cursor.h"

namespace facebook::axiom::runner {

/// A factory for getting a SplitSource for each TableScan. The splits produced
/// may depend on partition keys, buckets etc mentioned by each tableScan.
class SplitSourceFactory {
 public:
  virtual ~SplitSourceFactory() = default;

  /// Returns a splitSource for one TableScan across all Tasks of
  /// the fragment. The source will be invoked to produce splits for
  /// each individual worker running the scan. When 'partitionType' is
  /// non-null, emitted Splits are tagged with a groupId. When
  /// 'samplePercentage' is set (TABLESAMPLE SYSTEM), the source emits each
  /// split with that probability.
  virtual std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const connector::ConnectorSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage) = 0;
};

class SimpleSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit SimpleSplitSourceFactory(
      folly::F14FastMap<
          velox::core::PlanNodeId,
          std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
          nodeSplitMap)
      : nodeSplitMap_(std::move(nodeSplitMap)) {}

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const connector::ConnectorSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage) override;

 private:
  folly::F14FastMap<
      velox::core::PlanNodeId,
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
      nodeSplitMap_;
};

/// Generic SplitSourceFactory that delegates the work to ConnectorSplitManager.
class ConnectorSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit ConnectorSplitSourceFactory(QueryRuntimeStats& runtimeStats)
      : runtimeStats_(runtimeStats) {}

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const connector::ConnectorSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage) override;

 protected:
  QueryRuntimeStats& runtimeStats_;
};

/// Runner for in-process execution of a distributed plan.
class LocalRunner : public Runner,
                    public std::enable_shared_from_this<LocalRunner> {
 public:
  /// @param outputPool Optional memory pool to use for allocating memory for
  /// query results. Required if 'finishWrite' is set.
  /// @param baseSpillDirectory Base directory for spill files. If non-empty,
  /// each task gets a unique subdirectory under this path. Empty disables
  /// spilling at the task level.
  /// @param runtimeStats Optional recorder for split enumeration metrics.
  LocalRunner(
      RunnerSessionPtr session,
      optimizer::MultiFragmentPlanPtr plan,
      optimizer::FinishWrite finishWrite,
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      std::shared_ptr<SplitSourceFactory> splitSourceFactory,
      std::shared_ptr<velox::memory::MemoryPool> outputPool,
      std::string baseSpillDirectory,
      QueryRuntimeStats& runtimeStats);

  ~LocalRunner() override;

  LocalRunner(LocalRunner&&) = delete;
  LocalRunner& operator=(LocalRunner&&) = delete;

  /// Execution starts lazily on the first pull of the returned generator, not
  /// when execute() is called.
  folly::coro::AsyncGenerator<velox::RowVectorPtr> execute() override;

  /// Terminal wind-down: cancels any still-running work and reaps it (split
  /// scope joined, tasks completed, final stats captured, pools released).
  /// Awaited (never blocks the awaiting thread), so it is safe on a Velox
  /// executor thread. Idempotent, and safe when execute() was never pulled
  /// (state() == kInitialized): it just joins the empty split scope.
  folly::coro::Task<void> co_close() override;

  /// Returns a list of fragments from the 'plan' specified in constructor
  /// sorted in topological order.
  ///
  /// Note: Topological sort is a linear ordering of nodes in a directed acyclic
  /// graph (DAG), where for every directed edge from node A to node B, A
  /// appears before B in the sequence. It's essentially a way to arrange tasks
  /// or items with dependencies so that all prerequisites are completed before
  /// the dependent tasks.
  const std::vector<optimizer::ExecutableFragment>& fragments() const override {
    return fragments_;
  }

  /// Returns aggregated runtime stats for each fragment in 'fragments()'.
  /// Corresponds 1:1 to 'fragments()'. For multi-task fragments, stats from all
  /// tasks are aggregated together. While running this is a live snapshot; once
  /// execution has been reaped and the final stats captured, it returns those.
  std::vector<velox::exec::TaskStats> stats() const override;

  /// Prints the distributed plan annotated with runtime stats. Similar to
  /// velox::exec::printPlanWithStats and velox::exec::Task::printPlanWithStats
  /// APIs.
  /// @param includeCustomStats If true, prints operator-specific counters.
  /// @param addContext Optional lambda to add context to plan nodes. Receives
  /// plan node ID, indentation and std::ostream where to append the context.
  /// Start each line of context with 'indentation' and end with a new-line
  /// character.
  std::string printPlanWithStats(
      bool includeCustomStats = false,
      const std::function<void(
          const velox::core::PlanNodeId& nodeId,
          std::string_view indentation,
          std::ostream& out)>& addContext = nullptr) const;

  State state() const override {
    return state_;
  }

 private:
  // Fixed timeout for co_reap()'s task waits (stop running, then release
  // resources).
  static constexpr int32_t kReapTimeoutMicros = 1'000'000;

  // Best-effort attempt to cancel the execution. May be invoked from any
  // thread (via the cancellation callback), without serialization, and is
  // idempotent. A no-op once execution has finished (state() == kFinished).
  // Otherwise state() becomes kCancelled (without fabricating an error_) and
  // the in-flight or next pull surfaces cooperative cancellation.
  void cancelTasks();

  // Awaited reap shared by co_close() and the co_runWrite() error path. Joins
  // the split scope, then waits for all tasks to complete in two phases (stop
  // running, then release resources), each bounded by kReapTimeoutMicros.
  // Captures a final stats snapshot once every task has stopped running, so a
  // subsequent stats() returns final stats rather than the in-progress
  // snapshot. Idempotent (re-entry is a no-op once the scope is joined and
  // stages_ cleared) and safe when state() == kInitialized (execute() created
  // but never pulled): in that case it just joins the empty split scope.
  folly::coro::Task<void> co_reap();

  // Cancelable single-batch pull; starts execution on the first call. Returns
  // nullptr at end of output.
  folly::coro::Task<velox::RowVectorPtr> co_pull();

  // Reads all results and calls commit(...) on them if successful. Catches
  // exceptions, reaps and rethrows on error. Returns the number of rows
  // written. Drives the cancelable pull, so it honors the awaiting scope's
  // cancellation during the produce/drain phase (the commit itself is a
  // point of no return and runs to completion).
  [[nodiscard]] folly::coro::Task<int64_t> co_runWrite();

  // Builds a single-row vector with 'rows' in a "rows" BIGINT column.
  velox::RowVectorPtr makeWriteResult(int64_t rows);

  void start();

  void makeStages(const std::shared_ptr<velox::exec::Task>& lastStageTask);

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const connector::ConnectorSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage);

  // Aggregates the live per-fragment task stats. This is the in-progress
  // snapshot returned by stats() before the run completes. Caller must hold
  // mutex_.
  std::vector<velox::exec::TaskStats> aggregatedStats() const;

  // Serializes 'cursor_', 'error_', and mutation of 'stages_' (which
  // cancelTasks()/reap() read from a task thread while makeStages() is still
  // building it).
  mutable std::mutex mutex_;

  const RunnerSessionPtr session_;
  const optimizer::MultiFragmentPlanPtr plan_;
  const std::vector<optimizer::ExecutableFragment> fragments_;
  optimizer::FinishWrite finishWrite_;

  velox::exec::CursorParameters params_;

  std::atomic<State> state_{State::kInitialized};

  std::unique_ptr<velox::exec::TaskCursor> cursor_;
  std::vector<std::vector<std::shared_ptr<velox::exec::Task>>> stages_;
  // Final stats captured by co_reap() once all tasks stopped running.
  // Once set, stats() returns this instead of the live snapshot, which lets
  // stats() report final stats after the tasks have been torn down.
  std::optional<std::vector<velox::exec::TaskStats>> finalStats_;
  std::exception_ptr error_;
  std::shared_ptr<SplitSourceFactory> splitSourceFactory_;
  // Base directory for task spill files. Empty disables spilling.
  std::string baseSpillDirectory_;
  QueryRuntimeStats& runtimeStats_;
  // Cancellable so teardown can stop split enumeration promptly rather than
  // draining each source to noMoreSplits. co_reap() cancelAndJoinAsync()s it.
  folly::coro::CancellableAsyncScope splitScope_{/*throwOnJoin=*/true};
  bool splitScopeJoined_{false};
  // Set once co_close() has reaped. The destructor asserts this (or that
  // execution never started) rather than reaping itself, so no drop blocks a
  // thread. See co_close().
  bool closed_{false};
};

} // namespace facebook::axiom::runner
