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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include "axiom/runner/LocalRunner.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "gtest/gtest.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/type/Type.h"

namespace facebook::axiom::runner {
namespace {

class TestSplitSourceFactory : public SplitSourceFactory {
 public:
  explicit TestSplitSourceFactory(
      std::shared_ptr<connector::SplitSource> splitSource)
      : splitSource_(std::move(splitSource)) {}

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const velox::core::TableScanNode& /*scan*/) override {
    return splitSource_;
  }

 private:
  std::shared_ptr<connector::SplitSource> splitSource_;
};

class LocalSplitGeneratorTest : public test::LocalRunnerTestBase {
 protected:
  void SetUp() override {
    LocalRunnerTestBase::SetUp();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(2);
    auto rowType = velox::ROW({"c0"}, {velox::BIGINT()});
    auto tableHandle =
        std::make_shared<velox::connector::hive::HiveTableHandle>(
            "hive",
            "test_table",
            false,
            velox::common::SubfieldFilters{},
            nullptr,
            rowType);
    velox::connector::ColumnHandleMap assignments;
    tableScanNode_ = std::make_shared<velox::core::TableScanNode>(
        "scan_1", rowType, tableHandle, assignments);
  }

  std::shared_ptr<velox::connector::ConnectorSplit> makeSplit(int32_t id) {
    return std::static_pointer_cast<velox::connector::ConnectorSplit>(
        std::make_shared<velox::connector::hive::HiveConnectorSplit>(
            "test",
            fmt::format("split{}", id),
            velox::dwio::common::FileFormat::DWRF));
  }

  std::shared_ptr<SimpleSplitSourceFactory> makeSplitSourceFactory(
      const std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>&
          splits) {
    folly::F14FastMap<
        velox::core::PlanNodeId,
        std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
        nodeSplitMap;
    nodeSplitMap[tableScanNode_->id()] = splits;
    return std::make_shared<SimpleSplitSourceFactory>(std::move(nodeSplitMap));
  }

  std::shared_ptr<velox::exec::Task> makeTask(const std::string& taskId) {
    auto queryCtx = makeQueryCtx(taskId);

    return velox::exec::Task::create(
        taskId,
        velox::core::PlanFragment{tableScanNode_},
        0,
        queryCtx,
        velox::exec::Task::ExecutionMode::kParallel);
  }

  velox::core::TableScanNodePtr tableScanNode_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

TEST_F(LocalSplitGeneratorTest, basicSplitGeneration) {
  auto splits = std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>{
      makeSplit(1), makeSplit(2), makeSplit(3)};
  auto factory = makeSplitSourceFactory(splits);

  LocalSplitGenerator generator(factory, executor_.get());

  auto task = makeTask("task1");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task};

  auto future = generator.generateSplits(stage, tableScanNode_);
  std::move(future).get();
}

TEST_F(LocalSplitGeneratorTest, multipleTasks) {
  auto splits = std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>{
      makeSplit(1), makeSplit(2), makeSplit(3), makeSplit(4), makeSplit(5)};
  auto factory = makeSplitSourceFactory(splits);

  LocalSplitGenerator generator(factory, executor_.get());

  auto task1 = makeTask("task1");
  auto task2 = makeTask("task2");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task1, task2};

  auto future = generator.generateSplits(stage, tableScanNode_);
  std::move(future).get();
}

TEST_F(LocalSplitGeneratorTest, emptySplitSource) {
  auto splits =
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>{};
  auto factory = makeSplitSourceFactory(splits);

  LocalSplitGenerator generator(factory, executor_.get());

  auto task = makeTask("task1");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task};

  auto future = generator.generateSplits(stage, tableScanNode_);
  std::move(future).get();
}

TEST_F(LocalSplitGeneratorTest, interrupt) {
  class BlockedSplitSource : public connector::SplitSource {
   public:
    explicit BlockedSplitSource(
        std::shared_ptr<velox::connector::ConnectorSplit> split,
        std::shared_ptr<std::condition_variable> cv,
        std::shared_ptr<std::mutex> mutex,
        std::shared_ptr<bool> unblocked)
        : split_(split), cv_(cv), mutex_(mutex), unblocked_(unblocked) {}

    std::vector<SplitAndGroup> getSplits(uint64_t /* targetBytes */) override {
      std::unique_lock<std::mutex> lock(*mutex_);
      cv_->wait(lock, [this] { return *unblocked_; });
      return {{std::move(split_), 0}};
    }

   private:
    std::shared_ptr<velox::connector::ConnectorSplit> split_;
    std::shared_ptr<std::condition_variable> cv_;
    std::shared_ptr<std::mutex> mutex_;
    std::shared_ptr<bool> unblocked_;
  };

  auto cv = std::make_shared<std::condition_variable>();
  auto mutex = std::make_shared<std::mutex>();
  auto unblocked = std::make_shared<bool>(false);
  auto factory = std::make_shared<TestSplitSourceFactory>(
      std::make_shared<BlockedSplitSource>(makeSplit(1), cv, mutex, unblocked));

  LocalSplitGenerator generator(factory, executor_.get());
  auto task = makeTask("task1");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task};
  auto future = generator.generateSplits(stage, tableScanNode_);
  generator.interrupt();
  {
    std::lock_guard<std::mutex> lock(*mutex);
    *unblocked = true;
  }
  cv->notify_all();
  EXPECT_THROW(std::move(future).get(), std::runtime_error);
}

TEST_F(LocalSplitGeneratorTest, exceptionInSplitSource) {
  class FailingSplitSource : public connector::SplitSource {
   public:
    std::vector<SplitAndGroup> getSplits(uint64_t /* targetBytes */) override {
      throw std::runtime_error("Split generation failed");
    }
  };
  auto factory = std::make_shared<TestSplitSourceFactory>(
      std::make_shared<FailingSplitSource>());

  LocalSplitGenerator generator(factory, executor_.get());
  auto task = makeTask("task1");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task};

  auto future = generator.generateSplits(stage, tableScanNode_);
  EXPECT_THROW(std::move(future).get(), std::runtime_error);
}

TEST_F(LocalSplitGeneratorTest, roundRobin) {
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  splits.reserve(1000);
  for (int i = 0; i < 1000; ++i) {
    splits.push_back(makeSplit(i));
  }

  auto factory = makeSplitSourceFactory(splits);

  LocalSplitGenerator generator(factory, executor_.get());

  auto task1 = makeTask("task1");
  auto task2 = makeTask("task2");
  auto task3 = makeTask("task3");
  std::vector<std::shared_ptr<velox::exec::Task>> stage = {task1, task2, task3};

  auto future = generator.generateSplits(stage, tableScanNode_);
  std::move(future).get();
}

} // namespace
} // namespace facebook::axiom::runner
