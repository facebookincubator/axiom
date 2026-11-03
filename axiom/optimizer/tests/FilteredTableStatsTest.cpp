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

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <cmath>
#include <tuple>
#include "axiom/optimizer/OptimizerMetrics.h"
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
    createTpchTables(
        {velox::tpch::Table::TBL_NATION, velox::tpch::Table::TBL_REGION});
  }

  void SetUp() override {
    useV2_ = GetParam();
    test::HiveQueriesTestBase::SetUp();

    runCtas(
        "CREATE TABLE t WITH (partitioned_by = ARRAY['k']) AS "
        "SELECT n_nationkey as a, CAST(n_nationkey % 3 AS INTEGER) as k "
        "FROM nation");
    // Exclude setup planning from the per-query timing assertions.
    statsWriter_.clear();
    connectorStatsWriter_.clear();
  }

  void TearDown() override {
    hiveMetadata().dropTableIfExists("t");
    test::HiveQueriesTestBase::TearDown();
  }

  // Plans 'sql' on one node. Sampling is off so estimates come from
  // co_estimateStats.
  PlanAndStats plan(const std::string& sql) {
    auto logicalPlan = parseSelect(sql, velox::exec::test::kHiveConnectorId);

    OptimizerOptions optimizerOptions;
    optimizerOptions.sampleJoins = false;
    optimizerOptions.sampleFilters = false;

    return planVelox(
        logicalPlan,
        {.maxRemotePartitions = 1, .maxLocalPartitions = 1},
        std::move(optimizerOptions));
  }

  // Returns the cardinality the optimizer estimates for 'sql'.
  float estimatedCardinality(const std::string& sql) {
    auto planAndStats = plan(sql);
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

// The estimated size of a scan's output row counts fixed-width columns by type
// and variable-width columns by the average size their table's writes
// recorded.
TEST_P(FilteredTableStatsTest, rowSize) {
  if (!useV2_) {
    return;
  }
  auto scanRowSize = [&](const std::string& sql) {
    auto planAndStats = plan(sql);
    for (const auto& [id, prediction] : planAndStats.prediction) {
      if (prediction.numRawInputRows.has_value()) {
        return prediction.numOutputBytesPerRow;
      }
    }
    VELOX_FAIL("No scan estimate: {}", sql);
  };

  EXPECT_EQ(scanRowSize("SELECT n_nationkey, n_regionkey FROM nation"), 16);

  // Each name counts its characters plus a 4-byte offset: the 25 names have
  // 177 characters, so (177 + 25 * 4) / 25 rounds to 11 bytes.
  EXPECT_EQ(scanRowSize("SELECT n_nationkey, n_name FROM nation"), 8 + 11);
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

TEST_P(FilteredTableStatsTest, bucketFilter) {
  if (!useV2_) {
    return;
  }

  {
    // Each of the 25 distinct a values appears 25 times.
    runCtas(
        "CREATE TABLE u WITH (bucket_count = 8, bucketed_by = ARRAY['a']) AS "
        "SELECT n_nationkey AS a, r.r_regionkey AS b "
        "FROM nation, region r, region s");
    SCOPE_EXIT {
      hiveMetadata().dropTableIfExists("u");
    };

    // Scale the distinct key count by the fraction of selected buckets.
    for (const auto& [condition, bucketFraction, rowFraction] :
         std::vector<std::tuple<std::string, double, double>>{
             {"BETWEEN 0 AND 7", 1, 1},
             {"BETWEEN 2 AND 5", 0.5, 0.5},
             {"= 2", 0.125, 0.125},
             {"IN (1, 3)", 0.25, 0.25},
             {"BETWEEN 2 AND 5 AND b = 1", 0.5, 0.1},
         }) {
      {
        const auto sql =
            fmt::format("SELECT a FROM u WHERE \"$bucket\" {}", condition);
        SCOPED_TRACE(sql);
        EXPECT_NEAR(
            estimatedCardinality(sql),
            625 * rowFraction,
            kCardinalityTolerance);
      }
      {
        const auto sql = fmt::format(
            "SELECT DISTINCT a FROM u WHERE \"$bucket\" {}", condition);
        SCOPED_TRACE(sql);
        EXPECT_EQ(estimatedCardinality(sql), std::ceil(25 * bucketFraction));
      }
      {
        const auto sql = fmt::format(
            "SELECT \"$bucket\", min(a) FROM u WHERE \"$bucket\" {} GROUP BY 1",
            condition);
        SCOPED_TRACE(sql);
        EXPECT_EQ(estimatedCardinality(sql), 8 * bucketFraction);
      }
    }

    // Estimate key predicates within the selected buckets.
    {
      const auto sql =
          "SELECT a FROM u WHERE \"$bucket\" BETWEEN 0 AND 3 AND a = 2";
      SCOPED_TRACE(sql);
      EXPECT_NEAR(estimatedCardinality(sql), 25, kCardinalityTolerance);
    }
    {
      const auto sql =
          "SELECT DISTINCT a FROM u WHERE \"$bucket\" = 1 AND a IN (1, 9)";
      SCOPED_TRACE(sql);
      EXPECT_EQ(estimatedCardinality(sql), 2);
    }
  }

  // Estimate rows and distinct keys when most buckets are empty.
  runCtas(
      "CREATE TABLE u WITH (bucket_count = 8, bucketed_by = ARRAY['a']) AS "
      "SELECT * FROM (VALUES (0), (1)) AS t(a)");
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("u");
  };
  for (const auto& [condition, fraction] :
       std::vector<std::pair<std::string, double>>{
           {"= 1", 0.125},
           {"IN (1, 3, 5, 7)", 0.5},
       }) {
    {
      const auto sql =
          fmt::format("SELECT a FROM u WHERE \"$bucket\" {}", condition);
      SCOPED_TRACE(sql);
      // Keep uncertain scan estimates at least one row.
      EXPECT_EQ(
          estimatedCardinality(sql),
          std::max<int64_t>(1, std::llround(2 * fraction)));
    }
    {
      const auto sql = fmt::format(
          "SELECT DISTINCT a FROM u WHERE \"$bucket\" {}", condition);
      SCOPED_TRACE(sql);
      EXPECT_EQ(estimatedCardinality(sql), 1);
    }
  }
}

// Bucket selection preserves each individual key's NDV for composite keys.
TEST_P(FilteredTableStatsTest, multiKeyBucketFilter) {
  if (!useV2_) {
    return;
  }

  runCtas(
      "CREATE TABLE u WITH (bucket_count = 8, bucketed_by = ARRAY['a', 'b']) AS "
      "SELECT n_nationkey AS a, r.r_regionkey AS b "
      "FROM nation, region r, region s");
  SCOPE_EXIT {
    hiveMetadata().dropTableIfExists("u");
  };

  for (const auto& [condition, fraction] :
       std::vector<std::pair<std::string, double>>{
           {"BETWEEN 2 AND 5", 0.5},
           {"BETWEEN 2 AND 5 AND b = 1", 0.1},
       }) {
    {
      const auto sql =
          fmt::format("SELECT a FROM u WHERE \"$bucket\" {}", condition);
      SCOPED_TRACE(sql);
      EXPECT_NEAR(
          estimatedCardinality(sql), 625 * fraction, kCardinalityTolerance);
    }
    {
      const auto sql = fmt::format(
          "SELECT DISTINCT a FROM u WHERE \"$bucket\" {}", condition);
      SCOPED_TRACE(sql);
      EXPECT_EQ(estimatedCardinality(sql), 25);
    }
  }
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

// A partition filter that matches no partitions eliminates scans while
// preserving each parent operator's empty-input semantics.
TEST_P(FilteredTableStatsTest, emptyPartition) {
  for (
      const auto& [query, matcher] : std::vector<
          std::pair<std::string_view, std::shared_ptr<core::PlanMatcher>>>{
          {
              "SELECT n_name FROM nation JOIN t ON n_nationkey = a WHERE k = 7",
              matchValues(makeRowVector({makeFlatVector<std::string>({})}))
                  .build(),
          },
          {
              "SELECT count(*) AS c FROM t WHERE k = 7",
              matchValues(makeRowVector(ROW({}, {}), 0))
                  .singleAggregation({}, {"count(*) as c"})
                  .build(),
          },
          {
              "SELECT max(a) AS m FROM t WHERE k = 7",
              matchValues<int64_t>({"a"}, {})
                  .singleAggregation({}, {"max(a) as m"})
                  .build(),
          },
          {
              "SELECT (SELECT a FROM t WHERE k = 7)",
              matchValues<int64_t>({{std::nullopt}}).build(),
          },
          {
              "SELECT a AS x FROM t WHERE k = 7 "
              "UNION ALL SELECT r_regionkey AS x FROM region",
              matchHiveScan("region").project({"r_regionkey as x"}).build(),
          },
          {
              "SELECT v.a, r_regionkey "
              "FROM (SELECT a FROM t WHERE k = 7) v "
              "RIGHT JOIN region ON v.a = r_regionkey",
              matchHiveScan("region").project({"null", "r_regionkey"}).build(),
          },
          {
              "SELECT v.a, p.r "
              "FROM (SELECT a FROM t WHERE k = 7) v "
              "RIGHT JOIN (SELECT r_regionkey + 1 AS r FROM region) p "
              "ON v.a = p.r",
              matchHiveScan("region")
                  .project({"null", "r_regionkey + 1 as r"})
                  .build(),
          },
          {
              "SELECT r_regionkey, "
              "r_regionkey IN (SELECT a FROM t WHERE k = 7) AS present "
              "FROM region",
              matchHiveScan("region")
                  .project({"r_regionkey", "false as present"})
                  .build(),
          },
          {
              "SELECT r_regionkey FROM region "
              "WHERE r_regionkey NOT IN (SELECT a FROM t WHERE k = 7)",
              matchHiveScan("region").build(),
          },
      }) {
    SCOPED_TRACE(query);
    AXIOM_ASSERT_PLAN_V2(toSingleNodePlan(query), matcher);
  }
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

TEST_P(FilteredTableStatsTest, estimateTiming) {
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

TEST_P(FilteredTableStatsTest, tableLookupTiming) {
  // The repeat reference is served from the Schema's cache and not sampled.
  planVelox(parseSelect(
      "SELECT a.n_name FROM nation a, nation b WHERE a.n_regionkey = b.n_regionkey"));

  const auto componentStats = statsWriter_.runtimeStats();
  const auto wallNanos =
      componentStats.find(std::string(OptimizerMetrics::kFindTableWallNanos));
  ASSERT_NE(wallNanos, componentStats.end());
  EXPECT_EQ(wallNanos->second.count, 1);
}

AXIOM_INSTANTIATE_V1_V2(FilteredTableStatsTest);

} // namespace
} // namespace facebook::axiom::optimizer
