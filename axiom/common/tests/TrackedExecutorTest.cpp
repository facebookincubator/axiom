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

#include "axiom/common/TrackedExecutor.h"
#include <fmt/core.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/SerialExecutor.h>
#include <gtest/gtest.h>
#include "axiom/common/QueryRuntimeStats.h"

namespace facebook::axiom {

class TrackedExecutorTest : public ::testing::Test {
 protected:
  folly::CPUThreadPoolExecutor cpuExecutor_{1};
};

TEST_F(TrackedExecutorTest, tracksExecutionMetrics) {
  auto serialExecutor =
      folly::SerialExecutor::create(folly::getKeepAliveToken(&cpuExecutor_));
  TrackedExecutor tracked(std::move(serialExecutor));
  folly::Baton<> done;
  tracked.add([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    done.post();
  });
  done.wait();

  QueryRuntimeStats stats;
  tracked.reportTo(stats, "test");

  auto map = stats.toMap();
  ASSERT_EQ(map.size(), 3);

  auto waitKey = fmt::format("test-{}", TrackedExecutor::kExecutorWaitNanos);
  ASSERT_EQ(map.count(waitKey), 1);
  auto& waitMetric = map.at(waitKey);
  EXPECT_EQ(waitMetric.count, 1);
  EXPECT_GE(waitMetric.sum, 0);

  auto wallKey =
      fmt::format("test-{}", TrackedExecutor::kExecutorExecutionWallNanos);
  ASSERT_EQ(map.count(wallKey), 1);
  auto& wallMetric = map.at(wallKey);
  EXPECT_EQ(wallMetric.count, 1);
  EXPECT_GE(wallMetric.sum, 1'000'000);

  auto cpuKey =
      fmt::format("test-{}", TrackedExecutor::kExecutorExecutionCpuNanos);
  ASSERT_EQ(map.count(cpuKey), 1);
  auto& cpuMetric = map.at(cpuKey);
  EXPECT_EQ(cpuMetric.count, 1);
  EXPECT_GE(cpuMetric.sum, 0);
}

TEST_F(TrackedExecutorTest, tracksMultipleCallbacks) {
  auto serialExecutor =
      folly::SerialExecutor::create(folly::getKeepAliveToken(&cpuExecutor_));
  TrackedExecutor tracked(std::move(serialExecutor));
  folly::Baton<> done;
  tracked.add([]() {});
  tracked.add([]() {});
  tracked.add([&]() { done.post(); });
  done.wait();

  QueryRuntimeStats stats;
  tracked.reportTo(stats, "multi");

  auto map = stats.toMap();
  auto wallKey =
      fmt::format("multi-{}", TrackedExecutor::kExecutorExecutionWallNanos);
  ASSERT_EQ(map.count(wallKey), 1);
  auto& wallMetric = map.at(wallKey);
  EXPECT_EQ(wallMetric.count, 3);
}

TEST_F(TrackedExecutorTest, reportToWithNoCallbacks) {
  auto serialExecutor =
      folly::SerialExecutor::create(folly::getKeepAliveToken(&cpuExecutor_));
  TrackedExecutor tracked(std::move(serialExecutor));

  QueryRuntimeStats stats;
  tracked.reportTo(stats, "empty");

  auto map = stats.toMap();
  for (const auto& [name, metric] : map) {
    EXPECT_EQ(metric.count, 0);
  }
}

} // namespace facebook::axiom
