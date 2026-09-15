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

using testing::Contains;
using testing::ElementsAre;
using testing::Field;
using testing::Pair;
using testing::UnorderedElementsAre;

auto sumIs(int64_t sum) {
  return Field(&velox::RuntimeMetric::sum, sum);
}

TEST(QueryRuntimeStatsTest, snapshotKeysComponentAndConnector) {
  QueryRuntimeStats stats;
  stats.writerFor("runner")->addCount("getSplitsCount", 42);
  stats.writerForConnector("hive")->addCount("listPartitionsCount", 3);

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
  EXPECT_EQ(stats.writerFor("runner").get(), stats.writerFor("runner").get());
  EXPECT_NE(stats.writerFor("runner").get(), stats.writerFor("parser").get());
}

// A session still holding a handle records into a live writer, not a freed one.
TEST(QueryRuntimeStatsTest, aHandleOutlivesTheStats) {
  auto stats = std::make_unique<QueryRuntimeStats>();
  auto writer = stats->writerFor("runner");
  EXPECT_EQ(writer.use_count(), 2);

  stats.reset();

  EXPECT_EQ(writer.use_count(), 1);
  writer->addCount("getSplitsCount", 1);
}

TEST(QueryRuntimeStatsTest, concurrentRecordingAndRegistration) {
  QueryRuntimeStats stats;
  constexpr int kThreads = 8;
  constexpr int kIterations = 1'000;

  // Taken before the threads start, so recording races the insertions that
  // grow the bucket map underneath it.
  auto runnerWriter = stats.writerFor("runner");

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&stats, &runnerWriter, i]() {
      for (int j = 0; j < kIterations; ++j) {
        runnerWriter->addTiming(
            "executeWallNanos", std::chrono::nanoseconds(1));
        stats.writerForConnector(fmt::format("connector{}", i))
            ->addCount("listPartitionsCount", 1);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  const auto metrics = stats.toMap(QueryRuntimeStats::KeySeparator::kSlash);

  const auto shared = metrics.find("runner/executeWallNanos");
  ASSERT_NE(shared, metrics.end());
  EXPECT_EQ(shared->second.sum, kThreads * kIterations);
  EXPECT_EQ(shared->second.count, kThreads * kIterations);

  for (int i = 0; i < kThreads; ++i) {
    const auto perThread = metrics.find(
        fmt::format("connector/connector{}/listPartitionsCount", i));
    ASSERT_NE(perThread, metrics.end());
    EXPECT_EQ(perThread->second.sum, kIterations);
  }
}

TEST(QueryRuntimeStatsTest, sameNameUnderTwoIds) {
  QueryRuntimeStats stats;
  stats.writerFor("optimizer")
      ->addTiming("sharedWallNanos", std::chrono::nanoseconds(5'000));
  stats.writerForConnector("hive")->addTiming(
      "sharedWallNanos", std::chrono::nanoseconds(700));

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(
          Pair("optimizer/sharedWallNanos", sumIs(5'000)),
          Pair("connector/hive/sharedWallNanos", sumIs(700))));
}

// A handle stays valid once later ids have grown the bucket map, so a holder
// may keep the one it took at the start of a query.
TEST(QueryRuntimeStatsTest, writerOutlivesBucketMapGrowth) {
  QueryRuntimeStats stats;
  auto writer = stats.writerFor("runner");

  // Enough ids to grow the map past its initial capacity several times.
  for (int i = 0; i < 64; ++i) {
    stats.writerForConnector(fmt::format("connector{}", i));
  }
  writer->addCount("getSplitsCount", 42);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      Contains(Pair("runner/getSplitsCount", sumIs(42))));
}

TEST(QueryRuntimeStatsTest, idMustBeNonEmpty) {
  QueryRuntimeStats stats;

  VELOX_ASSERT_THROW(stats.writerFor(""), "Stats id must not be empty");
}

TEST(QueryRuntimeStatsTest, idMustBeSeparatorFree) {
  QueryRuntimeStats stats;

  VELOX_ASSERT_THROW(
      stats.writerForConnector("hive/di"), "Stats id must not contain '/'");
}

// A catalog may share a component's name: the two live in separate
// namespaces, so neither can reach the other's bucket.
TEST(QueryRuntimeStatsTest, catalogMayShareAComponentName) {
  QueryRuntimeStats stats;
  stats.writerFor("runner")->addCount("getSplitsCount", 1);
  stats.writerForConnector("runner")->addCount("listPartitionsCount", 2);

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

// Under kDash a connector's 'connector/' becomes 'connector-', so a component
// carrying a '-' could spell a catalog's published key.
TEST(QueryRuntimeStatsTest, componentMayNotSpellAConnectorKey) {
  QueryRuntimeStats stats;

  VELOX_ASSERT_THROW(
      stats.writerFor("connector-prism"),
      "Component id must not contain '-': connector-prism");
}

// A catalog may carry a '-'; only the component side is restricted.
TEST(QueryRuntimeStatsTest, catalogMayContainADash) {
  QueryRuntimeStats stats;
  stats.writerForConnector("prism-di")->addCount("listPartitionsCount", 1);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kDash),
      UnorderedElementsAre(
          Pair("connector-prism-di-listPartitionsCount", sumIs(1))));
}

// The published form flattens the id but never the metric name.
TEST(QueryRuntimeStatsTest, dashSeparatorFlattensOnlyTheId) {
  QueryRuntimeStats stats;
  stats.writerFor("runner")->addCount("splits/perNode", 3);
  stats.writerForConnector("hive")->addCount("listPartitionsCount", 4);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kDash),
      UnorderedElementsAre(
          Pair("runner-splits/perNode", sumIs(3)),
          Pair("connector-hive-listPartitionsCount", sumIs(4))));
}

// Two connector ids can flatten onto one published key, since catalogs carry
// a '-'. A sink cannot tell the two apart.
TEST(QueryRuntimeStatsTest, duplicatePublishedKeyKeepsOne) {
  QueryRuntimeStats stats;
  stats.writerForConnector("prism")->addCount("a-b", 1);
  stats.writerForConnector("prism-a")->addCount("b", 2);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kDash),
      ElementsAre(Pair("connector-prism-a-b", testing::_)));
}

// A '/' inside the metric name is unambiguous: the key splits on the first
// separator, which the component id cannot contain.
TEST(QueryRuntimeStatsTest, separatorInMetricName) {
  QueryRuntimeStats stats;
  stats.writerFor("runner")->addCount("splits/perNode", 3);

  EXPECT_THAT(
      stats.toMap(QueryRuntimeStats::KeySeparator::kSlash),
      UnorderedElementsAre(Pair("runner/splits/perNode", sumIs(3))));
}

TEST(QueryRuntimeStatsTest, unitSurvivesTheSnapshot) {
  QueryRuntimeStats stats;
  stats.writerFor("runner")->addCount("getSplitsCount", 1);
  stats.writerFor("parser")->addTiming(
      "parseWallNanos", std::chrono::nanoseconds(1));

  const auto metrics = stats.toMap(QueryRuntimeStats::KeySeparator::kSlash);

  const auto count = metrics.find("runner/getSplitsCount");
  ASSERT_NE(count, metrics.end());
  EXPECT_EQ(count->second.unit, velox::RuntimeCounter::Unit::kNone);

  const auto timing = metrics.find("parser/parseWallNanos");
  ASSERT_NE(timing, metrics.end());
  EXPECT_EQ(timing->second.unit, velox::RuntimeCounter::Unit::kNanos);
}

} // namespace
} // namespace facebook::axiom
