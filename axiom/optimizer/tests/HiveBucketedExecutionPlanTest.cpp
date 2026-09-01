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

#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class HiveBucketedExecutionTest : public test::HiveQueriesTestBase,
                                  public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    useV2_ = GetParam();
  }

  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_CUSTOMER});
  }

  void TearDown() override {
    for (const auto& name : tablesToDrop_) {
      hiveMetadata().dropTableIfExists({kDefaultSchema, name});
    }
    tablesToDrop_.clear();
    HiveQueriesTestBase::TearDown();
  }

  // Creates a table bucketed on 'bucketedBy' into 'bucketCount' buckets,
  // populated by 'selectSql', and verifies the metadata reports 'bucketCount'.
  void createBucketedTable(
      std::string_view name,
      int32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      std::string_view selectSql) {
    tablesToDrop_.emplace_back(name);
    runCtas(
        fmt::format(
            "CREATE TABLE {} WITH (bucket_count = {}, bucketed_by = ARRAY['{}']) AS {}",
            name,
            bucketCount,
            fmt::join(bucketedBy, "', '"),
            selectSql));

    auto table = hiveMetadata().findTable({kDefaultSchema, std::string{name}});
    ASSERT_NE(table, nullptr);
    ASSERT_FALSE(table->layouts().empty());
    const auto* layout =
        table->layouts().at(0)->as<connector::hive::HiveTableLayout>();
    ASSERT_NE(layout, nullptr);
    const auto partitionType = layout->partitionType();
    ASSERT_NE(partitionType, nullptr);
    EXPECT_EQ(partitionType->numPartitions(), bucketCount);
  }

  // Creates an unbucketed table populated by 'selectSql'.
  void createRegularTable(std::string_view name, std::string_view selectSql) {
    tablesToDrop_.emplace_back(name);
    runCtas(fmt::format("CREATE TABLE {} AS {}", name, selectSql));
  }

  // Plans 'logicalPlan' for distributed execution across 'numWorkers' workers.
  // 'numWorkers' must exceed 1, else the plan is single-node and unbucketed.
  PlanAndStats planDistributed(
      const lp::LogicalPlanNodePtr& logicalPlan,
      int32_t numWorkers = 4) {
    VELOX_CHECK_GT(numWorkers, 1);
    return planVelox(
        logicalPlan,
        {.numWorkers = numWorkers, .numDrivers = 4},
        optimizerOptions_);
  }

  // Runs 'sql' distributed with grouped execution on and off, checks the
  // results match, and returns the number of completed split groups reported
  // for the grouped run (per-fragment, so groups a single task processed).
  // Zero means grouped execution did not run; the ungrouped run must report no
  // split groups at all.
  size_t runGroupedVsUngrouped(const std::string& sql) {
    const auto logicalPlan = parseSelect(sql);
    auto grouped = runVelox(
        logicalPlan,
        {.numWorkers = 4, .numDrivers = 4, .groupedExecution = true});
    auto ungrouped = runVelox(
        logicalPlan,
        {.numWorkers = 4, .numDrivers = 4, .groupedExecution = false});
    velox::exec::test::assertEqualResults(grouped.results, ungrouped.results);

    size_t splitGroups = 0;
    for (const auto& stageStats : grouped.stats) {
      splitGroups += stageStats.completedSplitGroups.size();
    }
    for (const auto& stageStats : ungrouped.stats) {
      EXPECT_TRUE(stageStats.completedSplitGroups.empty());
    }
    return splitGroups;
  }

  std::vector<std::string> tablesToDrop_;
};

TEST_P(HiveBucketedExecutionTest, join) {
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "u",
      16,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");

  {
    auto logicalPlan =
        parseSelect("SELECT * FROM t, u WHERE t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinInner(matchHiveScan("u"))
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
  }

  // RIGHT join: v1 repartitions the build side into the bucketed fragment; v2
  // co-buckets both sides instead (both are bucketed on the join key). Results
  // are checked below.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t RIGHT JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("t")
              .hashJoinRight(matchHiveScan("u")
                                 .aliases({"u_nationkey"})
                                 .shuffle({"u_nationkey"}))
              .project()
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .project()
              .build());
    }
  }

  // FULL join: both sides stay co-bucketed in one fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t FULL OUTER JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinFull(matchHiveScan("u"))
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
  }

  // LEFT join: both sides stay co-bucketed in one fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t LEFT JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinLeft(matchHiveScan("u"))
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
  }

  // Unbucketed right side is repartitioned by bucket into the bucketed join.
  createRegularTable(
      "w",
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");
  auto logicalPlan =
      parseSelect("SELECT * FROM t, w WHERE t.c_nationkey = w.c_nationkey");
  auto plan = planDistributed(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchHiveScan("t")
          .hashJoinInner(matchHiveScan("w")
                             .aliases({"w_nationkey"})
                             .shuffle({"w_nationkey"}))
          .bucketed()
          .fragmentWidth(4)
          .gather()
          .project()
          .build());
}

TEST_P(HiveBucketedExecutionTest, semijoin) {
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "u",
      16,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey FROM customer WHERE c_nationkey < 5");

  {
    auto logicalPlan = parseSelect(
        "SELECT c_custkey, c_nationkey FROM t "
        "WHERE c_nationkey IN (SELECT c_nationkey FROM u)");
    auto plan = planDistributed(logicalPlan);
    // v2 plans IN as a null-aware kLeftSemiProject and shuffles+replicates
    // rather than co-bucketing; its shape is asserted in
    // BucketedExecutionPlanTest, and its results are checked below.
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("t")
              .hashJoinLeftSemiFilter(matchHiveScan("u"))
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .build());
    }
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT c_custkey, c_nationkey FROM t "
        "WHERE c_nationkey NOT IN (SELECT c_nationkey FROM u)");
    auto plan = planDistributed(logicalPlan);
    // v1 co-buckets the null-aware anti join in one fragment; v2 shuffles+
    // replicates the existence side instead (a bucketed side confines a NULL
    // key to one bucket). Results are checked below.
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("t")
              .hashJoinAnti(matchHiveScan("u"))
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .build());
    }
  }
}

TEST_P(HiveBucketedExecutionTest, aggregation) {
  createBucketedTable("t", 16, {"c_nationkey"}, "SELECT * FROM customer");

  {
    auto logicalPlan =
        parseSelect("SELECT c_nationkey, count(*) FROM t GROUP BY 1");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation({"c_nationkey"}, {"count(*) as cnt"})
            .localPartition({"c_nationkey"})
            .finalAggregation({"c_nationkey"}, {"count(cnt) as cnt"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  // Composite grouping key that is a superset of the single bucket key.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, c_mktsegment, count(*) FROM t GROUP BY 1, 2");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation(
                {"c_nationkey", "c_mktsegment"}, {"count(*) as cnt"})
            .localPartition({"c_nationkey", "c_mktsegment"})
            .finalAggregation(
                {"c_nationkey", "c_mktsegment"}, {"count(cnt) as cnt"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  // Multiple aggregates over the bucket key.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*), min(c_acctbal), max(c_acctbal) FROM t GROUP BY 1");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation(
                {"c_nationkey"},
                {"count(*) as cnt",
                 "min(c_acctbal) as mn",
                 "max(c_acctbal) as mx"})
            .localPartition({"c_nationkey"})
            .finalAggregation(
                {"c_nationkey"},
                {"count(cnt) as cnt", "min(mn) as mn", "max(mx) as mx"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  // DISTINCT aggregate lowers to stacked partial/final aggregations.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(DISTINCT c_mktsegment) FROM t "
        "GROUP BY c_nationkey");
    auto plan = planDistributed(logicalPlan);
    // v2 computes the distinct in a single co-located aggregation (the input is
    // already bucketed on the grouping key), not the stacked partial/final
    // form. Results are checked below.
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("t")
              .partialAggregation()
              .localPartition({"c_nationkey", "c_mktsegment"})
              .finalAggregation()
              .partialAggregation()
              .localPartition({"c_nationkey"})
              .finalAggregation()
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .build());
    }
  }

  // HAVING becomes a Filter above the aggregation.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(*) AS cnt FROM t GROUP BY 1 HAVING count(*) > 100");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation({"c_nationkey"}, {"count(*) as cnt"})
            .localPartition({"c_nationkey"})
            .finalAggregation({"c_nationkey"}, {"count(cnt) as cnt"})
            .filter("cnt > 100")
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  // Non-bucket grouping key falls through to partial (bucketed) + final
  // (repartitioned) aggregation across two fragments.
  {
    auto logicalPlan =
        parseSelect("SELECT c_mktsegment, count(*) FROM t GROUP BY 1");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation()
            // The grouping keys are not the table's bucket keys, so nothing
            // reads it grouped; v1 groups it regardless.
            .bucketed(!useV2_)
            .fragmentWidthIf(!useV2_, 4)
            .shuffle({"c_mktsegment"})
            .localPartition({"c_mktsegment"})
            .finalAggregation()
            .gather()
            .build());
  }
}

TEST_P(HiveBucketedExecutionTest, singleBucket) {
  createBucketedTable("t", 1, {"c_nationkey"}, "SELECT * FROM customer");

  auto logicalPlan =
      parseSelect("SELECT c_nationkey, count(*) FROM t GROUP BY 1");
  auto plan = planDistributed(logicalPlan);
  // One bucket holds every row, so pairing rows by it co-locates nothing. The
  // table is read plainly and the aggregation shuffles, spreading it over the
  // workers rather than running it on one task.
  AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
      plan.plan,
      matchHiveScan("t")
          .partialAggregation({"c_nationkey"}, {"count(*) as cnt"})
          .notBucketed()
          .shuffle({"c_nationkey"})
          .localPartition({"c_nationkey"})
          .finalAggregation({"c_nationkey"}, {"count(cnt) as cnt"})
          .fragmentWidth(4)
          .gather()
          .build());
}

TEST_P(HiveBucketedExecutionTest, joinDifferentBucketCounts) {
  // 8 divides 16, so the 16-bucket and 8-bucket sides share a compatible
  // bucketing and co-fragment without a shuffle.
  createBucketedTable(
      "t16",
      16,
      {"c_nationkey"},
      "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "t8",
      8,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");

  auto logicalPlan = parseSelect(
      "SELECT * FROM t16, t8 WHERE t16.c_nationkey = t8.c_nationkey");
  auto plan = planDistributed(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchHiveScan("t16")
          .hashJoinInner(matchHiveScan("t8"))
          .bucketed()
          .fragmentWidth(4)
          .gather()
          .project()
          .build());
}

TEST_P(HiveBucketedExecutionTest, compositeBucketKeys) {
  createBucketedTable(
      "t", 16, {"c_nationkey", "c_mktsegment"}, "SELECT * FROM customer");

  // Grouping by all bucket keys co-fragments.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, c_mktsegment, count(*) FROM t GROUP BY 1, 2");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation(
                {"c_nationkey", "c_mktsegment"}, {"count(*) as cnt"})
            .localPartition({"c_nationkey", "c_mktsegment"})
            .finalAggregation(
                {"c_nationkey", "c_mktsegment"}, {"count(cnt) as cnt"})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  // Grouping by a strict subset of the bucket keys needs a reshuffle: partial
  // aggregation in the bucketed fragment, final aggregation after.
  {
    auto logicalPlan =
        parseSelect("SELECT c_nationkey, count(*) FROM t GROUP BY 1");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation()
            // The grouping keys are not the table's bucket keys, so nothing
            // reads it grouped; v1 groups it regardless.
            .bucketed(!useV2_)
            .fragmentWidthIf(!useV2_, 4)
            .shuffle({"c_nationkey"})
            .localPartition({"c_nationkey"})
            .finalAggregation()
            .gather()
            .build());
  }
}

TEST_P(HiveBucketedExecutionTest, widthClampsToNumWorkers) {
  // 8 buckets, 5 workers: fragment width clamps to 5.
  createBucketedTable(
      "t", 8, {"c_nationkey"}, "SELECT c_custkey, c_nationkey FROM customer");

  {
    auto logicalPlan =
        parseSelect("SELECT c_nationkey, count(*) FROM t GROUP BY 1");
    auto plan = planDistributed(logicalPlan, 5);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation({"c_nationkey"}, {"count(*) as cnt"})
            .localPartition({"c_nationkey"})
            .finalAggregation({"c_nationkey"}, {"count(cnt) as cnt"})
            .bucketed()
            .fragmentWidth(5)
            .gather()
            .build());
  }

  createRegularTable(
      "u",
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");

  {
    auto logicalPlan =
        parseSelect("SELECT * FROM t, u WHERE t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan, 5);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinInner(matchHiveScan("u")
                               .aliases({"u_nationkey"})
                               .shuffle({"u_nationkey"}))
            .bucketed()
            .fragmentWidth(5)
            .gather()
            .project()
            .build());
  }
}

TEST_P(HiveBucketedExecutionTest, copartitionedJoinIndivisibleWorkers) {
  // 3 workers divide neither bucket count (16, 8); the co-bucketed join must
  // still return all matching rows.
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "u",
      8,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey AS varchar) AS label FROM customer");

  auto logicalPlan =
      parseSelect("SELECT * FROM t, u WHERE t.c_nationkey = u.c_nationkey");
  auto plan = planDistributed(logicalPlan, 3);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchHiveScan("t")
          .hashJoinInner(matchHiveScan("u"))
          .bucketed()
          .fragmentWidth(3)
          .gather()
          .project()
          .build());
}

TEST_P(HiveBucketedExecutionTest, unionall) {
  // The two inputs bucket on different keys, so they cannot co-partition; the
  // union reshuffles on the grouping key before aggregating.
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_nationkey, c_custkey FROM customer");
  createBucketedTable(
      "u", 16, {"c_custkey"}, "SELECT c_nationkey, c_custkey FROM customer");

  auto logicalPlan = parseSelect(
      "SELECT c_nationkey, count(*) FROM ("
      "  SELECT c_nationkey FROM t"
      "  UNION ALL"
      "  SELECT c_custkey AS c_nationkey FROM u"
      ") GROUP BY 1");
  auto plan = planDistributed(logicalPlan);
  // Each leg contributes its own bucket column as the union key (t:
  // c_nationkey, u: c_custkey), and they hash identically, so the legs
  // co-bucket and the aggregation needs no shuffle.
  //
  // Wrong plan under v1: it reads both legs by their buckets, which already
  // co-locates every group, and then shuffles on the grouping key anyway, so
  // only v2's shape is pinned.
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .localPartition(matchHiveScan("u").project())
            .partialAggregation()
            .localPartition({"c_nationkey"})
            .finalAggregation()
            .fragment({.width = 4, .bucketedScans = 2})
            .gather()
            .build());
  }
}

// A join between tables whose bucket counts coarsen to different widths runs
// on one side's buckets; everything else joining it is shuffled onto them.
TEST_P(HiveBucketedExecutionTest, bucketCountsThatScaleDifferently) {
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "u",
      2,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey AS varchar) AS label FROM customer");
  createRegularTable("v", "SELECT c_custkey, c_nationkey FROM customer");

  // The unbucketed table is shuffled onto the wider table's buckets.
  {
    auto plan = planDistributed(parseSelect(
        "SELECT t.c_custkey, u.label, count(*) FROM t, u, v "
        "WHERE t.c_nationkey = u.c_nationkey AND t.c_nationkey = v.c_nationkey "
        "GROUP BY t.c_custkey, u.label"));
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("t")
              .hashJoinInner(matchHiveScan("u").aliases({"uk"}).shuffle({"uk"}))
              .hashJoinInner(matchHiveScan("v").aliases({"vk"}).shuffle({"vk"}))
              .partialAggregation()
              .fragment(
                  {.width = 4, .bucketedScans = 1, .bucketedExchanges = 2})
              .shuffle({"c_custkey", "label"})
              .localPartition({"c_custkey", "label"})
              .finalAggregation()
              .gather()
              .build());
    }
  }

  // Aggregating the wider table first keeps its width; the result is then
  // shuffled onto the narrower table's buckets.
  {
    auto plan = planDistributed(parseSelect(
        "SELECT x.c_nationkey, x.cnt, u.label FROM "
        "(SELECT c_nationkey, count(*) AS cnt FROM t GROUP BY c_nationkey) x, u "
        "WHERE x.c_nationkey = u.c_nationkey"));
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchHiveScan("u")
              .hashJoinInner(matchHiveScan("t")
                                 .partialAggregation()
                                 .localPartition({"c_nationkey"})
                                 .finalAggregation()
                                 .fragment({.width = 4, .bucketedScans = 1})
                                 .shuffle({"c_nationkey"}))
              .fragment(
                  {.width = 2, .bucketedScans = 1, .bucketedExchanges = 1})
              .gather()
              .build());
    }
  }
}

// A union of a bucketed and an unbucketed leg cannot be read by buckets: the
// unbucketed leg has none, so the union has none either.
TEST_P(HiveBucketedExecutionTest, unionAllWithUnbucketedLeg) {
  createBucketedTable(
      "t", 16, {"c_nationkey"}, "SELECT c_nationkey, c_custkey FROM customer");
  createRegularTable("u", "SELECT c_nationkey, c_custkey FROM customer");

  auto plan = planDistributed(parseSelect(
      "SELECT c_nationkey, count(*) FROM ("
      "  SELECT c_nationkey FROM t"
      "  UNION ALL"
      "  SELECT c_nationkey FROM u"
      ") GROUP BY 1"));
  // Wrong plan under v1: it reads t by its buckets even though the union's
  // other leg has none, so the grouping cannot be used and is shuffled away.
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .localPartition(matchHiveScan("u").project())
            .partialAggregation()
            .notBucketed()
            .shuffle({"c_nationkey"})
            .localPartition({"c_nationkey"})
            .finalAggregation()
            .gather()
            .build());
  }
}

TEST_P(HiveBucketedExecutionTest, groupedAggregationRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  createBucketedTable("ge_agg", 16, {"c_nationkey"}, "SELECT * FROM customer");
  const auto splitGroups = runGroupedVsUngrouped(
      "SELECT c_nationkey, count(*) FROM ge_agg GROUP BY 1");
  EXPECT_GT(splitGroups, 0)
      << "grouped aggregation must complete at least one split group";
}

TEST_P(HiveBucketedExecutionTest, groupedJoinRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  createBucketedTable(
      "ge_t",
      16,
      {"c_nationkey"},
      "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "ge_u",
      16,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");
  const auto splitGroups = runGroupedVsUngrouped(
      "SELECT * FROM ge_t, ge_u WHERE ge_t.c_nationkey = ge_u.c_nationkey");
  EXPECT_GT(splitGroups, 0)
      << "co-located grouped join must complete at least one split group";
}

TEST_P(
    HiveBucketedExecutionTest,
    groupedJoinDifferentBucketCountsRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  // 8 divides 16, so the sides co-partition on a common 8-group space; a key in
  // the 16-bucket table's bucket b co-locates with the 8-bucket table's bucket
  // b % 8.
  createBucketedTable(
      "ge_t16",
      16,
      {"c_nationkey"},
      "SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "ge_u8",
      8,
      {"c_nationkey"},
      "SELECT DISTINCT c_nationkey, cast(c_nationkey as varchar) as label FROM customer");
  const auto splitGroups = runGroupedVsUngrouped(
      "SELECT * FROM ge_t16, ge_u8 WHERE ge_t16.c_nationkey = ge_u8.c_nationkey");
  EXPECT_GT(splitGroups, 0)
      << "grouped join across differing bucket counts must complete at least one split group";
}

AXIOM_INSTANTIATE_V1_V2(HiveBucketedExecutionTest);

} // namespace
} // namespace facebook::axiom::optimizer
