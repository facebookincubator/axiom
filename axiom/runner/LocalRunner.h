/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "axiom/runner/MultiFragmentPlan.h"
#include "axiom/runner/Runner.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/Cursor.h"

namespace facebook::axiom::runner {

/// Testing proxy for a split source managed by a system with full metadata
/// access.
class SimpleSplitSource : public SplitSource {
 public:
  explicit SimpleSplitSource(
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits)
      : splits_(std::move(splits)) {}

  virtual std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) override;

 private:
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits_;
  int32_t splitIdx_{0};
};

/// Testing proxy for a split source factory that uses connector metadata to
/// enumerate splits. This takes a precomputed split list for each scan.
class SimpleSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit SimpleSplitSourceFactory(
      std::unordered_map<
          velox::core::PlanNodeId,
          std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
          nodeSplitMap)
      : nodeSplitMap_(std::move(nodeSplitMap)) {}

  std::shared_ptr<SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan) override;

 private:
  std::unordered_map<
      velox::core::PlanNodeId,
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
      nodeSplitMap_;
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

  velox::RowVectorPtr next() override;

  std::vector<velox::exec::TaskStats> stats() const override;

  void abort() override;

  void waitForCompletion(int32_t maxWaitMicros) override;

  State state() const override {
    return state_;
  }

 private:
  void start();

  void makeStages(const std::shared_ptr<velox::exec::Task>& lastStageTask);

  std::shared_ptr<SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& scan);

  // Serializes 'cursor_' and 'error_'.
  mutable std::mutex mutex_;

  const std::vector<ExecutableFragment> fragments_;
  const MultiFragmentPlan::Options options_;

  velox::exec::CursorParameters params_;

  velox::tsan_atomic<State> state_{State::kInitialized};

  std::unique_ptr<velox::exec::TaskCursor> cursor_;
  std::vector<std::vector<std::shared_ptr<velox::exec::Task>>> stages_;
  std::exception_ptr error_;
  std::shared_ptr<SplitSourceFactory> splitSourceFactory_;
};

} // namespace facebook::axiom::runner
