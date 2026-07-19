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
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

constexpr double kCardinalityTolerance = 1;

class FilteredTableStatsTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_NATION});
  }

  void SetUp() override {
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

  // Parses SQL, optimizes, and invokes the callback with the best plan.
  // Uses sampleFilters=false to ensure the optimizer relies on
  // co_estimateStats rather than sampling.
  void verifyPlan(
      const std::string& sql,
      const std::function<void(const Plan&)>& callback) {
    auto logicalPlan = parseSelect(sql, velox::exec::test::kHiveConnectorId);

    verifyOptimization(
        *logicalPlan,
        [&](Optimization& optimization) {
          auto* plan = optimization.bestPlan();
          ASSERT_NE(plan, nullptr) << "Best plan should not be null";

          callback(*plan);
        },
        [] {
          OptimizerOptions options;
          options.sampleJoins = false;
          options.sampleFilters = false;
          return options;
        }());
  }
};

// Verifies that co_estimateStats provides accurate row counts for an
// unpartitioned TPC-H table scan. The nation table has 25 rows.
TEST_F(FilteredTableStatsTest, noFilter) {
  verifyPlan("SELECT n_nationkey, n_name FROM nation", [](const Plan& plan) {
    const auto& op = *plan.op;
    EXPECT_NEAR(op.resultCardinality().value(), 25, kCardinalityTolerance);
  });
}

// Verifies that a filter on a data column reduces cardinality, using
// min/max from per-file stats for range-based selectivity estimation.
TEST_F(FilteredTableStatsTest, dataFilter) {
  verifyPlan(
      "SELECT n_nationkey FROM nation WHERE n_nationkey > 10",
      [](const Plan& plan) {
        const auto& op = *plan.op;
        // n_nationkey in [0, 24]. n_nationkey > 10 should give ~14/24 of rows.
        EXPECT_NEAR(
            op.resultCardinality().value(),
            25.0 * 14 / 24,
            kCardinalityTolerance);
      });
}

// Verifies that a partition key filter prunes partitions, resulting in
// reduced estimated row count from co_estimateStats.
TEST_F(FilteredTableStatsTest, partitionFilter) {
  verifyPlan("SELECT a FROM t WHERE k = 0", [](const Plan& plan) {
    const auto& op = *plan.op;
    // Partition k=0 has 9 rows (nationkeys 0,3,6,9,12,15,18,21,24).
    EXPECT_NEAR(op.resultCardinality().value(), 9, kCardinalityTolerance);
  });

  verifyPlan("SELECT a FROM t WHERE k IN (1, 2)", [](const Plan& plan) {
    const auto& op = *plan.op;
    // Partitions k=1 and k=2 have 8 rows each (16 total).
    EXPECT_NEAR(op.resultCardinality().value(), 16, kCardinalityTolerance);
  });
}

// Verifies that a partition key filter combined with a data column filter
// first prunes by partition, then applies data filter selectivity.
TEST_F(FilteredTableStatsTest, partitionAndDataFilter) {
  verifyPlan("SELECT a FROM t WHERE k = 0 AND a > 10", [](const Plan& plan) {
    const auto& op = *plan.op;
    // Partition k=0 has 9 rows with a in [0, 24].
    // a > 10 gives selectivity of 14/24, so ~9 * 14/24 = 5.25 rows.
    EXPECT_NEAR(
        op.resultCardinality().value(), 9.0 * 14 / 24, kCardinalityTolerance);
  });
}

// The optimizer owns its runtime metrics: findTable (recorded while building
// the query graph) and estimateStats (recorded while planning) are visible via
// runtimeStats() after optimizing.
TEST_F(FilteredTableStatsTest, optimizerOwnsRuntimeStats) {
  auto logicalPlan = parseSelect(
      "SELECT n_nationkey FROM nation WHERE n_nationkey > 10",
      velox::exec::test::kHiveConnectorId);

  verifyOptimization(
      *logicalPlan,
      [](Optimization& optimization) {
        ASSERT_NE(optimization.bestPlan(), nullptr);
        // estimateStats records CPU only on-thread, so assert the wall keys.
        const auto stats = optimization.runtimeStats();
        EXPECT_GT(
            stats.at(std::string(QueryRuntimeStats::kFindTableWallNanos)).count,
            0);
        EXPECT_GT(
            stats.at(std::string(QueryRuntimeStats::kEstimateStatsWallNanos))
                .count,
            0);
      },
      [] {
        // sampleFilters=false so co_estimateStats runs and records the metric.
        OptimizerOptions options;
        options.sampleFilters = false;
        return options;
      }());
}

// Verifies that a join key column with all NULLs (numDistinct = 0 from
// connector stats) does not produce non-finite cardinality via division by
// zero in estimateFanout.
TEST_F(FilteredTableStatsTest, joinKeyWithZeroDistinctValues) {
  runCtas(
      "CREATE TABLE u AS SELECT n_nationkey AS a, CAST(NULL AS INTEGER) AS b FROM nation");
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("u");
  };

  verifyPlan(
      "SELECT u1.a + u2.b FROM u AS u1 LEFT JOIN u AS u2 ON u1.b = u2.b",
      [](const Plan& plan) {
        // TODO: Improve estimate for all-NULL join keys. The correct result
        // is 25 (left join preserves all left-side rows, no matches on NULLs),
        // but NDV is clamped from 0 to 1, producing fanout = 25 and
        // resultCardinality = 625.
        EXPECT_NEAR(
            plan.op->resultCardinality().value(), 625, kCardinalityTolerance);
      });
}

} // namespace
} // namespace facebook::axiom::optimizer
