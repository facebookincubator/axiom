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

#include "axiom/cli/common/QueryRuntimeStats.h"

#include <chrono>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom {
namespace {

using testing::Field;
using testing::Pair;
using testing::UnorderedElementsAre;

auto sumIs(int64_t sum) {
  return Field(&velox::RuntimeMetric::sum, sum);
}

TEST(QueryRuntimeStatsTest, snapshotKeysComponentAndConnector) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("getSplitsCount", 42);
  stats.writerForConnector("hive").addCount("listPartitionsCount", 3);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(
          Pair("runner/getSplitsCount", sumIs(42)),
          Pair("connector/hive/listPartitionsCount", sumIs(3))));
}

TEST(QueryRuntimeStatsTest, toMapEmptyBeforeAnyRecording) {
  QueryRuntimeStats stats;
  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash), testing::IsEmpty());
}

TEST(QueryRuntimeStatsTest, sameIdReturnsSameHandle) {
  QueryRuntimeStats stats;
  EXPECT_EQ(&stats.writerFor("runner"), &stats.writerFor("runner"));
  EXPECT_NE(&stats.writerFor("runner"), &stats.writerFor("parser"));
}

TEST(QueryRuntimeStatsTest, concurrentRecordingAndRegistration) {
  QueryRuntimeStats stats;
  constexpr int kThreads = 8;
  constexpr int kIterations = 1'000;

  // Taken before the threads start, so recording races the insertions that
  // grow the bucket map underneath it.
  auto& runnerWriter = stats.writerFor("runner");

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&stats, &runnerWriter, i]() {
      for (int j = 0; j < kIterations; ++j) {
        runnerWriter.addTiming("executeWallNanos", std::chrono::nanoseconds(1));
        stats.writerForConnector(fmt::format("connector{}", i))
            .addCount("listPartitionsCount", 1);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  const auto shared = stats.find("runner", "executeWallNanos");
  ASSERT_TRUE(shared.has_value());
  EXPECT_EQ(shared->sum, kThreads * kIterations);
  EXPECT_EQ(shared->count, kThreads * kIterations);

  for (int i = 0; i < kThreads; ++i) {
    const auto perThread = stats.find(
        fmt::format("connector/connector{}", i), "listPartitionsCount");
    ASSERT_TRUE(perThread.has_value());
    EXPECT_EQ(perThread->sum, kIterations);
  }
}

TEST(QueryRuntimeStatsTest, sameNameUnderTwoIds) {
  QueryRuntimeStats stats;
  stats.writerFor("optimizer")
      .addTiming("sharedWallNanos", std::chrono::nanoseconds(5'000));
  stats.writerForConnector("hive").addTiming(
      "sharedWallNanos", std::chrono::nanoseconds(700));

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(
          Pair("optimizer/sharedWallNanos", sumIs(5'000)),
          Pair("connector/hive/sharedWallNanos", sumIs(700))));

  // find() takes an id of either kind.
  const auto connectorMetric = stats.find("connector/hive", "sharedWallNanos");
  ASSERT_TRUE(connectorMetric.has_value());
  EXPECT_EQ(connectorMetric->sum, 700);
}

// A handle stays valid once later ids have grown the bucket map, so a holder
// may hold on to the reference it took at the start of a query.
TEST(QueryRuntimeStatsTest, writerOutlivesBucketMapGrowth) {
  QueryRuntimeStats stats;
  auto& writer = stats.writerFor("runner");

  // Enough ids to grow the map past its initial capacity several times.
  for (int i = 0; i < 64; ++i) {
    stats.writerForConnector(fmt::format("connector{}", i));
  }
  writer.addCount("getSplitsCount", 42);

  const auto metric = stats.find("runner", "getSplitsCount");
  ASSERT_TRUE(metric.has_value());
  EXPECT_EQ(metric->sum, 42);
}

TEST(QueryRuntimeStatsTest, findMissing) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("getSplitsCount", 1);

  EXPECT_FALSE(stats.find("parser", "getSplitsCount").has_value());
  EXPECT_FALSE(stats.find("runner", "listPartitionsCount").has_value());
}

TEST(QueryRuntimeStatsTest, idMustBeNonEmptyAndSeparatorFree) {
  QueryRuntimeStats stats;

  VELOX_ASSERT_THROW(stats.writerFor(""), "Stats id must not be empty");
  VELOX_ASSERT_THROW(
      stats.writerForConnector("hive/di"), "Stats id must not contain '/'");
}

// A catalog may share a component's name: the two live in separate
// namespaces, so neither can reach the other's bucket.
TEST(QueryRuntimeStatsTest, catalogMayShareAComponentName) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("getSplitsCount", 1);
  stats.writerForConnector("runner").addCount("listPartitionsCount", 2);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(
          Pair("runner/getSplitsCount", sumIs(1)),
          Pair("connector/runner/listPartitionsCount", sumIs(2))));
}

// A component cannot take the connector namespace, which would put it in
// reach of every catalog's bucket.
TEST(QueryRuntimeStatsTest, componentMayNotBeNamedConnector) {
  QueryRuntimeStats stats;

  VELOX_ASSERT_THROW(
      stats.writerFor("connector"), "Component id must not be 'connector'");
}

// The published form flattens the id but never the metric name.
TEST(QueryRuntimeStatsTest, dashSeparatorFlattensOnlyTheId) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("splits/perNode", 3);
  stats.writerForConnector("hive").addCount("listPartitionsCount", 4);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kDash),
      UnorderedElementsAre(
          Pair("runner-splits/perNode", sumIs(3)),
          Pair("connector-hive-listPartitionsCount", sumIs(4))));
}

// A '/' inside the metric name is unambiguous: the key splits on the first
// separator, which the component id cannot contain.
TEST(QueryRuntimeStatsTest, separatorInMetricName) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("splits/perNode", 3);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(Pair("runner/splits/perNode", sumIs(3))));

  const auto metric = stats.find("runner", "splits/perNode");
  ASSERT_TRUE(metric.has_value());
  EXPECT_EQ(metric->sum, 3);
}

TEST(QueryRuntimeStatsTest, unitSurvivesTheSnapshot) {
  QueryRuntimeStats stats;
  stats.writerFor("runner").addCount("getSplitsCount", 1);
  stats.writerFor("parser").addTiming(
      "parseWallNanos", std::chrono::nanoseconds(1));

  const auto count = stats.find("runner", "getSplitsCount");
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(count->unit, velox::RuntimeCounter::Unit::kNone);

  const auto timing = stats.find("parser", "parseWallNanos");
  ASSERT_TRUE(timing.has_value());
  EXPECT_EQ(timing->unit, velox::RuntimeCounter::Unit::kNanos);
}

} // namespace
} // namespace facebook::axiom
