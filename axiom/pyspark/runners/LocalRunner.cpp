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

#include "axiom/pyspark/runners/LocalRunner.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "axiom/pyspark/Exception.h"
#include "axiom/pyspark/runners/Runner.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/core/QueryCtx.h"

DEFINE_int32(num_workers, 4, "Number of in-process workers");
DEFINE_int32(num_drivers, 4, "Number of drivers per worker");
DEFINE_int64(split_target_bytes, 16 << 20, "Approx bytes covered by one split");

using namespace ::facebook;

namespace axiom::collagen::runner {
namespace {

void waitForCompletion(
    const std::shared_ptr<facebook::axiom::runner::LocalRunner>& runner) {
  if (runner) {
    try {
      runner->waitForCompletion(500000);
    } catch (const std::exception& /*ignore*/) {
      // Ignore exceptions during wait for completion
    }
  }
}

folly::CPUThreadPoolExecutor* getCPUExecutor() {
  static auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      FLAGS_num_workers * FLAGS_num_drivers);
  return executor.get();
}

/// A runner implementation that executes plans using Velox LocalRunner.
class LocalRunner : public Runner {
 public:
  LocalRunner(
      const std::string& runId,
      ::facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
      ::facebook::axiom::optimizer::FinishWrite finishWrite,
      std::shared_ptr<::facebook::velox::memory::MemoryPool> pool)
      : Runner(
            runId,
            std::move(plan),
            std::move(finishWrite),
            std::move(pool)) {
    aggregatePool_ = rootPool_->addAggregateChild("TestRunner");
    COLLAGEN_CHECK_NOT_NULL(aggregatePool_, "Failed to create aggregate pool");

    leafPool_ = aggregatePool_->addLeafChild("LocalRunner");
    COLLAGEN_CHECK_NOT_NULL(leafPool_, "Failed to create leaf pool");
  }

  ~LocalRunner() override = default;

  std::vector<::facebook::velox::RowVectorPtr> execute(
      const std::unordered_map<std::string, std::string>& queryConfigs = {})
      override;

 private:
  std::shared_ptr<::facebook::velox::memory::MemoryPool> aggregatePool_;
  std::shared_ptr<::facebook::velox::memory::MemoryPool> leafPool_;
};

std::vector<velox::RowVectorPtr> LocalRunner::execute(
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  std::vector<velox::RowVectorPtr> results;

  auto queryCtx = velox::core::QueryCtx::create(
      getCPUExecutor(),
      velox::core::QueryConfig{queryConfigs},
      {}, // Connector configs
      velox::cache::AsyncDataCache::getInstance(),
      aggregatePool_);

  auto splitSourceFactory =
      std::make_shared<facebook::axiom::runner::ConnectorSplitSourceFactory>();

  auto runner = std::make_shared<facebook::axiom::runner::LocalRunner>(
      plan_, std::move(finishWrite_), queryCtx, splitSourceFactory, leafPool_);
  try {
    while (auto rows = runner->next()) {
      results.push_back(rows);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Query terminated with: " << e.what();
    waitForCompletion(runner);
    throw;
  }

  waitForCompletion(runner);
  return results;
}

} // namespace

void registerLocalRunnerFactory(const std::string& runnerId) {
  registerRunnerFactory(
      runnerId,
      [](const std::string& runId,
         ::facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
         ::facebook::axiom::optimizer::FinishWrite finishWrite,
         std::shared_ptr<::facebook::velox::memory::MemoryPool> pool) {
        return std::make_unique<LocalRunner>(
            runId, std::move(plan), std::move(finishWrite), std::move(pool));
      });
}

} // namespace axiom::collagen::runner
