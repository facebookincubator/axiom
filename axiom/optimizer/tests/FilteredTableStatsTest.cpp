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

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/OptimizerMetrics.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

constexpr double kCardinalityTolerance = 1;

class FilteredTableStatsTest : public test::HiveQueriesTestBase,
                               public ::testing::WithParamInterface<bool> {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  void SetUp() override {
    useV2_ = GetParam();
    test::HiveQueriesTestBase::SetUp();

    runCtas(
        "CREATE TABLE t WITH (partitioned_by = ARRAY['k']) AS "
        "SELECT n_nationkey as a, CAST(n_nationkey % 3 AS INTEGER) as k "
        "FROM nation");
  }

  void TearDown() override {
    hiveMetadata().dropTableIfExists("t");
    test::HiveQueriesTestBase::TearDown();
  }

  // Returns the cardinality the optimizer estimates for 'sql'. Sampling is off
  // so the estimate comes from co_estimateStats.
  float estimatedCardinality(const std::string& sql) {
    auto logicalPlan = parseSelect(sql, velox::exec::test::kHiveConnectorId);

    OptimizerOptions optimizerOptions;
    optimizerOptions.sampleJoins = false;
    optimizerOptions.sampleFilters = false;

    auto planAndStats = planVelox(
        logicalPlan,
        {.numWorkers = 1, .numDrivers = 1},
        std::move(optimizerOptions));

    const auto& root = planAndStats.plan->fragments().back().fragment.planNode;
    auto it = planAndStats.prediction.find(root->id());
    VELOX_CHECK(
        it != planAndStats.prediction.end(),
        "No estimate for the plan's root node");
    return it->second.cardinality;
  }
};

// Verifies that co_estimateStats provides accurate row counts for an
// unpartitioned TPC-H table scan. The nation table has 25 rows.
TEST_P(FilteredTableStatsTest, noFilter) {
  EXPECT_NEAR(
      estimatedCardinality("SELECT n_nationkey, n_name FROM nation"),
      25,
      kCardinalityTolerance);
}

// Verifies that a filter on a data column reduces cardinality, using
// min/max from per-file stats for range-based selectivity estimation.
TEST_P(FilteredTableStatsTest, dataFilter) {
  // n_nationkey in [0, 24]. n_nationkey > 10 should give ~14/24 of rows,
  // whether or not the query selects the column the filter reads.
  EXPECT_NEAR(
      estimatedCardinality(
          "SELECT n_nationkey FROM nation WHERE n_nationkey > 10"),
      25.0 * 14 / 24,
      kCardinalityTolerance);

  EXPECT_NEAR(
      estimatedCardinality("SELECT n_name FROM nation WHERE n_nationkey > 10"),
      25.0 * 14 / 24,
      kCardinalityTolerance);
}

// Verifies that a partition key filter prunes partitions, resulting in
// reduced estimated row count from co_estimateStats.
TEST_P(FilteredTableStatsTest, partitionFilter) {
  // Partition k=0 has 9 rows (nationkeys 0,3,6,9,12,15,18,21,24).
  EXPECT_NEAR(
      estimatedCardinality("SELECT a FROM t WHERE k = 0"),
      9,
      kCardinalityTolerance);

  // Partitions k=1 and k=2 have 8 rows each (16 total).
  EXPECT_NEAR(
      estimatedCardinality("SELECT a FROM t WHERE k IN (1, 2)"),
      16,
      kCardinalityTolerance);
}

// Verifies that a partition key filter combined with a data column filter
// first prunes by partition, then applies data filter selectivity.
TEST_P(FilteredTableStatsTest, partitionAndDataFilter) {
  // Partition k=0 has 9 rows with a in [0, 24]. a > 10 gives selectivity of
  // 14/24, so ~9 * 14/24 = 5.25 rows.
  EXPECT_NEAR(
      estimatedCardinality("SELECT a FROM t WHERE k = 0 AND a > 10"),
      9.0 * 14 / 24,
      kCardinalityTolerance);
}

// Verifies that a join key column with all NULLs (numDistinct = 0 from
// connector stats) does not produce non-finite cardinality via division by
// zero in estimateFanout.
TEST_P(FilteredTableStatsTest, joinKeyWithZeroDistinctValues) {
  runCtas(
      "CREATE TABLE u AS SELECT n_nationkey AS a, CAST(NULL AS INTEGER) AS b FROM nation");
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("u");
  };

  // TODO: Improve estimate for all-NULL join keys. The correct result is 25
  // (left join preserves all left-side rows, no matches on NULLs), but NDV is
  // clamped from 0 to 1, producing fanout = 25 and cardinality = 625.
  EXPECT_NEAR(
      estimatedCardinality(
          "SELECT u1.a + u2.b FROM u AS u1 LEFT JOIN u AS u2 ON u1.b = u2.b"),
      625,
      kCardinalityTolerance);
}

AXIOM_INSTANTIATE_V1_V2(FilteredTableStatsTest);
// Each optimizer drives its own estimate fan-out, so both are checked.
class EstimateStatsTimingTest : public test::HiveQueriesTestBase,
                                public testing::WithParamInterface<bool> {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_NATION, velox::tpch::Table::TBL_REGION});
  }

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    useV2_ = GetParam();
  }
};

TEST_P(EstimateStatsTimingTest, optimizerTimesTheEstimateFanOut) {
  // Both tables are estimated in one fan-out, so a per-call timer records two
  // samples where a fan-out timer records one.
  planVelox(parseSelect(
      "SELECT n_name FROM nation, region WHERE n_regionkey = r_regionkey"));

  const auto componentStats = statsWriter_.runtimeStats();
  const auto wallNanos = componentStats.find(
      std::string(OptimizerMetrics::kEstimateStatsWallNanos));
  ASSERT_NE(wallNanos, componentStats.end());
  EXPECT_EQ(wallNanos->second.count, 1);

  // The connector reports its own internals, never the caller's observation.
  EXPECT_FALSE(connectorStatsWriter_.runtimeStats().contains(
      std::string(OptimizerMetrics::kEstimateStatsWallNanos)));
}

TEST_P(EstimateStatsTimingTest, optimizerTimesTableLookups) {
  // The repeat reference is served from the Schema's cache and not sampled.
  planVelox(parseSelect(
      "SELECT a.n_name FROM nation a, nation b WHERE a.n_regionkey = b.n_regionkey"));

  const auto componentStats = statsWriter_.runtimeStats();
  const auto wallNanos =
      componentStats.find(std::string(OptimizerMetrics::kFindTableWallNanos));
  ASSERT_NE(wallNanos, componentStats.end());
  EXPECT_EQ(wallNanos->second.count, 1);
}

AXIOM_INSTANTIATE_V1_V2(EstimateStatsTimingTest);

} // namespace
} // namespace facebook::axiom::optimizer
