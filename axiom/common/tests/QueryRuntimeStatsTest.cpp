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

#include <thread>

#include <folly/json.h>
#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

TEST(QueryRuntimeStatsTest, recordTiming) {
  QueryRuntimeStats stats;
  stats.recordTiming(
      QueryRuntimeStats::kParseWallNanos, std::chrono::nanoseconds(100));
  stats.recordTiming(
      QueryRuntimeStats::kParseWallNanos, std::chrono::nanoseconds(200));

  auto map = stats.toMap();
  ASSERT_EQ(map.count(std::string(QueryRuntimeStats::kParseWallNanos)), 1);
  auto& metric = map.at(std::string(QueryRuntimeStats::kParseWallNanos));
  EXPECT_EQ(metric.sum, 300);
  EXPECT_EQ(metric.count, 2);
  EXPECT_EQ(metric.min, 100);
  EXPECT_EQ(metric.max, 200);
  EXPECT_EQ(metric.unit, velox::RuntimeCounter::Unit::kNanos);
}

TEST(QueryRuntimeStatsTest, recordCount) {
  QueryRuntimeStats stats;
  stats.recordCount(QueryRuntimeStats::kListPartitionsCount, 10);
  stats.recordCount(QueryRuntimeStats::kListPartitionsCount, 5);

  auto map = stats.toMap();
  ASSERT_EQ(map.count(std::string(QueryRuntimeStats::kListPartitionsCount)), 1);
  auto& metric = map.at(std::string(QueryRuntimeStats::kListPartitionsCount));
  EXPECT_EQ(metric.sum, 15);
  EXPECT_EQ(metric.count, 2);
  EXPECT_EQ(metric.min, 5);
  EXPECT_EQ(metric.max, 10);
  EXPECT_EQ(metric.unit, velox::RuntimeCounter::Unit::kNone);
}

TEST(QueryRuntimeStatsTest, merge) {
  QueryRuntimeStats stats;

  velox::RuntimeMetric existing(500, velox::RuntimeCounter::Unit::kNanos);
  existing.addValue(300);
  stats.merge(QueryRuntimeStats::kOptimizeWallNanos, existing);

  auto map = stats.toMap();
  auto& metric = map.at(std::string(QueryRuntimeStats::kOptimizeWallNanos));
  EXPECT_EQ(metric.sum, 800);
  EXPECT_EQ(metric.count, 2);
  EXPECT_EQ(metric.min, 300);
  EXPECT_EQ(metric.max, 500);
}

TEST(QueryRuntimeStatsTest, toDynamic) {
  QueryRuntimeStats stats;
  stats.recordTiming(
      QueryRuntimeStats::kParseWallNanos, std::chrono::nanoseconds(1000));
  stats.recordCount(QueryRuntimeStats::kGetSplitsCount, 42);

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());

  auto parseKey = std::string(QueryRuntimeStats::kParseWallNanos);
  ASSERT_TRUE(dynamic.count(parseKey));
  EXPECT_EQ(dynamic[parseKey]["sum"].asInt(), 1000);
  EXPECT_EQ(dynamic[parseKey]["count"].asInt(), 1);
  EXPECT_EQ(dynamic[parseKey]["unit"].asString(), "NANO");

  auto splitsKey = std::string(QueryRuntimeStats::kGetSplitsCount);
  ASSERT_TRUE(dynamic.count(splitsKey));
  EXPECT_EQ(dynamic[splitsKey]["sum"].asInt(), 42);
  EXPECT_EQ(dynamic[splitsKey]["unit"].asString(), "NONE");
}

TEST(QueryRuntimeStatsTest, emptyStats) {
  QueryRuntimeStats stats;
  EXPECT_TRUE(stats.toMap().empty());

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());
  EXPECT_TRUE(dynamic.empty());
}

TEST(QueryRuntimeStatsTest, concurrentRecording) {
  QueryRuntimeStats stats;
  constexpr int kThreads = 8;
  constexpr int kIterations = 1000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&stats]() {
      for (int j = 0; j < kIterations; ++j) {
        stats.recordTiming(
            QueryRuntimeStats::kExecuteWallNanos, std::chrono::nanoseconds(1));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto map = stats.toMap();
  auto& metric = map.at(std::string(QueryRuntimeStats::kExecuteWallNanos));
  EXPECT_EQ(metric.sum, kThreads * kIterations);
  EXPECT_EQ(metric.count, kThreads * kIterations);
}

} // namespace
} // namespace facebook::axiom
