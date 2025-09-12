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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/runner/MultiFragmentPlan.h"
#include "axiom/runner/Runner.h"
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
  /// each individual worker running the scan.
  virtual std::shared_ptr<velox::connector::SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan) = 0;
};

class SimpleSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit SimpleSplitSourceFactory(
      std::unordered_map<
          velox::core::PlanNodeId,
          std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
          nodeSplitMap)
      : nodeSplitMap_(std::move(nodeSplitMap)) {}

  std::shared_ptr<velox::connector::SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan) override;

 private:
  std::unordered_map<
      velox::core::PlanNodeId,
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
      nodeSplitMap_;
};

/// Generic SplitSourceFactory that delegates the work to ConnectorSplitManager.
class ConnectorSplitSourceFactory : public SplitSourceFactory {
 public:
  ConnectorSplitSourceFactory(velox::connector::SplitOptions options = {})
      : options_(std::move(options)) {}

  std::shared_ptr<velox::connector::SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan) override;

 protected:
  const velox::connector::SplitOptions options_;
};

/// Runner for in-process execution of a distributed plan.
class LocalRunner : public Runner,
                    public std::enable_shared_from_this<LocalRunner> {
 public:
  LocalRunner(
      const MultiFragmentPlanPtr& plan,
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      std::shared_ptr<SplitSourceFactory> splitSourceFactory,
      std::shared_ptr<velox::memory::MemoryPool> outputPool = nullptr);

  LocalRunner(
      const MultiFragmentPlanPtr& plan,
      std::shared_ptr<velox::core::QueryCtx> queryCtx)
      : LocalRunner(
            plan,
            queryCtx,
            std::make_shared<ConnectorSplitSourceFactory>()) {}

  /// First call starts execution.
  velox::RowVectorPtr next() override;

  /// Returns a list of fragments from the 'plan' specified in constructor
  /// sorted in topological order.
  ///
  /// Note: Topological sort is a linear ordering of nodes in a directed acyclic
  /// graph (DAG), where for every directed edge from node A to node B, A
  /// appears before B in the sequence. It's essentially a way to arrange tasks
  /// or items with dependencies so that all prerequisites are completed before
  /// the dependent tasks.
  const std::vector<ExecutableFragment>& fragments() const {
    return fragments_;
  }

  /// Returns aggregated runtime stats for each fragment in 'fragments()'.
  /// Corresponds 1:1 to 'fragments()'. For multi-task fragments, stats from all
  /// tasks are aggregated together.
  std::vector<velox::exec::TaskStats> stats() const override;

  /// Prints the distributed plan annotated with runtime stats. Similar to
  /// velox::exec::printPlanWithStats and velox::exec::Task::printPlanWithStats
  /// APIs.
  /// @param addContext Optional lambda to add context to plan nodes. Receives
  /// plan node ID, indentation and std::ostream where to append the context.
  /// Start each line of context with 'indentation' and end with a new-line
  /// character.
  std::string printPlanWithStats(
      const std::function<void(
          const velox::core::PlanNodeId& nodeId,
          const std::string& indentation,
          std::ostream& out)>& addContext = nullptr) const;

  /// Best-effort attempt to cancel the execution.
  void abort() override;

  void waitForCompletion(int32_t maxWaitMicros) override;

  State state() const override {
    return state_;
  }

 private:
  void start();

  void makeStages(const std::shared_ptr<velox::exec::Task>& lastStageTask);

  std::shared_ptr<velox::connector::SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan);

  // Serializes 'cursor_' and 'error_'.
  mutable std::mutex mutex_;

  const MultiFragmentPlanPtr plan_;
  const std::vector<ExecutableFragment> fragments_;

  velox::exec::CursorParameters params_;

  velox::tsan_atomic<State> state_{State::kInitialized};

  std::unique_ptr<velox::exec::TaskCursor> cursor_;
  std::vector<std::vector<std::shared_ptr<velox::exec::Task>>> stages_;
  std::exception_ptr error_;
  std::shared_ptr<SplitSourceFactory> splitSourceFactory_;
};

} // namespace facebook::axiom::runner
