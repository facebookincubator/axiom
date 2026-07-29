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

#include "axiom/common/QueryRuntimeStats.h"

#include "axiom/connectors/StatsComponents.h"
#include "axiom/optimizer/OptimizerMetrics.h"
#include "axiom/runner/RunnerMetrics.h"
#include "axiom/sql/presto/ParserMetrics.h"
#include "velox/common/base/tests/GTestUtils.h"

#include <thread>

#include <folly/BenchmarkUtil.h>
#include <folly/json.h>
#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

// Repeated timing recordings of the same name within one component aggregate.
TEST(QueryRuntimeStatsTest, timingsAggregateWithinComponent) {
  QueryRuntimeStats stats;
  auto sink = stats.sinkFor(optimizer::kStatsComponent);
  sink.recordTiming("axiom-optimizeWallNanos", std::chrono::nanoseconds(100));
  sink.recordTiming("axiom-optimizeWallNanos", std::chrono::nanoseconds(200));

  auto map = stats.toMap();
  ASSERT_EQ(map.count("optimizer/axiom-optimizeWallNanos"), 1);
  const auto& metric = map.at("optimizer/axiom-optimizeWallNanos");
  EXPECT_EQ(metric.sum, 300);
  EXPECT_EQ(metric.count, 2);
  EXPECT_EQ(metric.min, 100);
  EXPECT_EQ(metric.max, 200);
  EXPECT_EQ(metric.unit, velox::RuntimeCounter::Unit::kNanos);
}

// Repeated count recordings of the same name within one component aggregate.
TEST(QueryRuntimeStatsTest, countsAggregateWithinComponent) {
  QueryRuntimeStats stats;
  auto sink = stats.sinkFor(optimizer::kStatsComponent);
  sink.recordCount("axiom-candidatesPruned", 10);
  sink.recordCount("axiom-candidatesPruned", 5);

  auto map = stats.toMap();
  ASSERT_EQ(map.count("optimizer/axiom-candidatesPruned"), 1);
  const auto& metric = map.at("optimizer/axiom-candidatesPruned");
  EXPECT_EQ(metric.sum, 15);
  EXPECT_EQ(metric.count, 2);
  EXPECT_EQ(metric.min, 5);
  EXPECT_EQ(metric.max, 10);
  EXPECT_EQ(metric.unit, velox::RuntimeCounter::Unit::kNone);
}

// RuntimeStatsSink::merge folds a pre-aggregated metric into an existing entry.
TEST(QueryRuntimeStatsTest, mergePreAggregatedMetric) {
  QueryRuntimeStats stats;
  auto sink = stats.sinkFor(optimizer::kStatsComponent);
  sink.recordCount("axiom-candidatesPruned", 4);
  velox::RuntimeMetric metric(velox::RuntimeCounter::Unit::kNone);
  metric.addValue(10);
  metric.addValue(2);
  metric.addValue(6);
  sink.merge("axiom-candidatesPruned", metric);

  auto map = stats.toMap();
  ASSERT_EQ(map.count("optimizer/axiom-candidatesPruned"), 1);
  const auto& merged = map.at("optimizer/axiom-candidatesPruned");
  EXPECT_EQ(merged.sum, 22);
  EXPECT_EQ(merged.count, 4);
  EXPECT_EQ(merged.min, 2);
  EXPECT_EQ(merged.max, 10);
  EXPECT_EQ(merged.unit, velox::RuntimeCounter::Unit::kNone);
}

// A metric name containing '/' collides with the component-id separator used by
// toMap(), so recording it is rejected.
TEST(QueryRuntimeStatsTest, rejectsSlashInMetricName) {
  QueryRuntimeStats stats;
  auto sink = stats.sinkFor(optimizer::kStatsComponent);
  VELOX_ASSERT_THROW(sink.recordCount("a/b", 1), "must not contain");
}

// The same metric name from two components stays distinct in the flattened
// view, qualified by component id.
TEST(QueryRuntimeStatsTest, sameNameAcrossComponentsStaysDistinct) {
  QueryRuntimeStats stats;
  auto optimizer = stats.sinkFor(optimizer::kStatsComponent);
  auto splits = stats.sinkFor(connector::connectorSplits("hive"));
  optimizer.recordCount("wallNanos", 100);
  splits.recordCount("wallNanos", 7);

  auto map = stats.toMap();
  EXPECT_EQ(map.count("wallNanos"), 0);
  EXPECT_EQ(map.at("optimizer/wallNanos").sum, 100);
  EXPECT_EQ(map.at("hive/splits/wallNanos").sum, 7);
}

// totalsByName sums each metric across components into a per-query total keyed
// by the bare name.
TEST(QueryRuntimeStatsTest, totalsByNameSumsAcrossComponents) {
  QueryRuntimeStats stats;
  stats.sinkFor(optimizer::kStatsComponent).recordCount("wallNanos", 100);
  stats.sinkFor(connector::connectorSplits("hive")).recordCount("wallNanos", 7);
  stats.sinkFor(connector::connectorSplits("prism"))
      .recordCount("wallNanos", 3);

  auto totals = stats.totalsByName();
  ASSERT_EQ(totals.count("wallNanos"), 1);
  const auto& total = totals.at("wallNanos");
  EXPECT_EQ(total.sum, 110);
  EXPECT_EQ(total.count, 3);
  EXPECT_EQ(total.min, 3);
  EXPECT_EQ(total.max, 100);
}

// Distinct names from different components each appear under their qualified
// key.
TEST(QueryRuntimeStatsTest, distinctNamesAcrossComponentsQualified) {
  QueryRuntimeStats stats;
  stats.sinkFor(::axiom::sql::presto::kStatsComponent)
      .recordCount("axiom-parseCount", 1);
  stats.sinkFor(optimizer::kStatsComponent)
      .recordCount("axiom-optimizeCount", 2);

  auto map = stats.toMap();
  ASSERT_EQ(map.count("parser/axiom-parseCount"), 1);
  ASSERT_EQ(map.count("optimizer/axiom-optimizeCount"), 1);
  EXPECT_EQ(map.at("parser/axiom-parseCount").sum, 1);
  EXPECT_EQ(map.at("optimizer/axiom-optimizeCount").sum, 2);
}

// A snapshot reflects everything recorded so far, including metrics added after
// an earlier snapshot.
TEST(QueryRuntimeStatsTest, liveSnapshotComplete) {
  QueryRuntimeStats stats;
  stats.sinkFor(::axiom::sql::presto::kStatsComponent)
      .recordCount("axiom-parseCount", 1);

  auto first = stats.toMap();
  EXPECT_EQ(first.count("parser/axiom-parseCount"), 1);
  EXPECT_EQ(first.count("optimizer/axiom-optimizeCount"), 0);

  stats.sinkFor(optimizer::kStatsComponent)
      .recordCount("axiom-optimizeCount", 2);

  auto second = stats.toMap();
  EXPECT_EQ(second.count("parser/axiom-parseCount"), 1);
  EXPECT_EQ(second.count("optimizer/axiom-optimizeCount"), 1);
}

TEST(QueryRuntimeStatsTest, emptyStats) {
  QueryRuntimeStats stats;
  EXPECT_TRUE(stats.toMap().empty());
  EXPECT_TRUE(stats.totalsByName().empty());

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());
  EXPECT_TRUE(dynamic.empty());
}

// Concurrent sinks for the same component share one map, so recordings across
// threads aggregate without loss.
TEST(QueryRuntimeStatsTest, concurrentRecording) {
  QueryRuntimeStats stats;
  constexpr int kThreads = 8;
  constexpr int kIterations = 1000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&stats]() {
      auto sink = stats.sinkFor(runner::kStatsComponent);
      for (int j = 0; j < kIterations; ++j) {
        sink.recordTiming(
            "axiom-executeWallNanos", std::chrono::nanoseconds(1));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto map = stats.toMap();
  ASSERT_EQ(map.count("runner/axiom-executeWallNanos"), 1);
  const auto& metric = map.at("runner/axiom-executeWallNanos");
  EXPECT_EQ(metric.sum, kThreads * kIterations);
  EXPECT_EQ(metric.count, kThreads * kIterations);
}

// toDynamic emits one object per bare metric name with correct sum and unit.
TEST(QueryRuntimeStatsTest, toDynamicFormat) {
  QueryRuntimeStats stats;
  stats.sinkFor(::axiom::sql::presto::kStatsComponent)
      .recordTiming("axiom-parseWallNanos", std::chrono::nanoseconds(1000));
  stats.sinkFor(connector::connectorSplits("hive"))
      .recordCount("axiom-getSplitsCount", 42);

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());

  ASSERT_TRUE(dynamic.count("axiom-parseWallNanos"));
  EXPECT_EQ(
      dynamic["axiom-parseWallNanos"]["name"].asString(),
      "axiom-parseWallNanos");
  EXPECT_EQ(dynamic["axiom-parseWallNanos"]["sum"].asInt(), 1000);
  EXPECT_EQ(dynamic["axiom-parseWallNanos"]["count"].asInt(), 1);
  EXPECT_EQ(dynamic["axiom-parseWallNanos"]["unit"].asString(), "NANO");

  ASSERT_TRUE(dynamic.count("axiom-getSplitsCount"));
  EXPECT_EQ(
      dynamic["axiom-getSplitsCount"]["name"].asString(),
      "axiom-getSplitsCount");
  EXPECT_EQ(dynamic["axiom-getSplitsCount"]["sum"].asInt(), 42);
  EXPECT_EQ(dynamic["axiom-getSplitsCount"]["count"].asInt(), 1);
  EXPECT_EQ(dynamic["axiom-getSplitsCount"]["unit"].asString(), "NONE");
}

TEST(QueryRuntimeStatsTest, scopedCpuWallStatsTimer) {
  QueryRuntimeStats stats;
  {
    ScopedCpuWallStatsTimer timer(
        stats.sinkFor(::axiom::sql::presto::kStatsComponent),
        "axiom-parseWallNanos",
        "axiom-parseCpuNanos");
    int64_t sum = 0;
    for (int64_t i = 0; i < 1'000'000; ++i) {
      sum += i;
    }
    folly::doNotOptimizeAway(sum);
  }
  auto map = stats.toMap();
  ASSERT_EQ(map.count("parser/axiom-parseWallNanos"), 1);
  ASSERT_EQ(map.count("parser/axiom-parseCpuNanos"), 1);

  auto wallNanos = map.at("parser/axiom-parseWallNanos").sum;
  auto cpuNanos = map.at("parser/axiom-parseCpuNanos").sum;
  EXPECT_GT(wallNanos, 0);
  EXPECT_GT(cpuNanos, 0);
  EXPECT_LE(cpuNanos, wallNanos);
}

} // namespace
} // namespace facebook::axiom
