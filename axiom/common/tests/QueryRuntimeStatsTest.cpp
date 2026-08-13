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

#include <folly/BenchmarkUtil.h>
#include <folly/dynamic.h>
#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

TEST(QueryRuntimeStatsTest, toDynamic) {
  QueryRuntimeStats stats;
  stats.addTiming(
      QueryRuntimeStats::kParseWallNanos, std::chrono::nanoseconds(1000));
  stats.addCount(QueryRuntimeStats::kGetSplitsCount, 42);

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());

  auto parseKey = std::string(QueryRuntimeStats::kParseWallNanos);
  ASSERT_TRUE(dynamic.count(parseKey));
  EXPECT_EQ(dynamic[parseKey]["name"].asString(), parseKey);
  EXPECT_EQ(dynamic[parseKey]["sum"].asInt(), 1000);
  EXPECT_EQ(dynamic[parseKey]["count"].asInt(), 1);
  EXPECT_EQ(dynamic[parseKey]["min"].asInt(), 1000);
  EXPECT_EQ(dynamic[parseKey]["max"].asInt(), 1000);
  EXPECT_EQ(dynamic[parseKey]["unit"].asString(), "NANO");

  auto splitsKey = std::string(QueryRuntimeStats::kGetSplitsCount);
  ASSERT_TRUE(dynamic.count(splitsKey));
  EXPECT_EQ(dynamic[splitsKey]["name"].asString(), splitsKey);
  EXPECT_EQ(dynamic[splitsKey]["sum"].asInt(), 42);
  EXPECT_EQ(dynamic[splitsKey]["unit"].asString(), "NONE");
}

TEST(QueryRuntimeStatsTest, emptyStats) {
  QueryRuntimeStats stats;
  EXPECT_TRUE(stats.runtimeStats().empty());

  auto dynamic = stats.toDynamic();
  ASSERT_TRUE(dynamic.isObject());
  EXPECT_TRUE(dynamic.empty());
}

TEST(QueryRuntimeStatsTest, scopedCpuWallStatsTimer) {
  QueryRuntimeStats stats;
  {
    ScopedCpuWallStatsTimer timer(
        stats,
        QueryRuntimeStats::kParseWallNanos,
        QueryRuntimeStats::kParseCpuNanos);
    int64_t sum = 0;
    for (int64_t i = 0; i < 1'000'000; ++i) {
      sum += i;
    }
    folly::doNotOptimizeAway(sum);
  }
  auto map = stats.runtimeStats();
  ASSERT_EQ(map.count(std::string(QueryRuntimeStats::kParseWallNanos)), 1);
  ASSERT_EQ(map.count(std::string(QueryRuntimeStats::kParseCpuNanos)), 1);

  auto wallNanos = map.at(std::string(QueryRuntimeStats::kParseWallNanos)).sum;
  auto cpuNanos = map.at(std::string(QueryRuntimeStats::kParseCpuNanos)).sum;
  EXPECT_GT(wallNanos, 0);
  EXPECT_GT(cpuNanos, 0);
  EXPECT_LE(cpuNanos, wallNanos);
}

TEST(QueryRuntimeStatsTest, cpuMetricNaming) {
  QueryRuntimeStats stats;
  stats.addTiming(
      QueryRuntimeStats::kFindTableCpuNanos, std::chrono::nanoseconds(5000));

  auto dynamic = stats.toDynamic();
  auto key = std::string(QueryRuntimeStats::kFindTableCpuNanos);
  ASSERT_TRUE(dynamic.count(key));
  EXPECT_EQ(dynamic[key]["unit"].asString(), "NANO");
  EXPECT_EQ(dynamic[key]["sum"].asInt(), 5000);
}

} // namespace
} // namespace facebook::axiom
