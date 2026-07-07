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

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class HiveBucketedExecutionTest : public test::HiveQueriesTestBase {
 protected:
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

  // Checks that the already-planned distributed 'plan' returns the same rows as
  // a single-node (unbucketed) run of 'logicalPlan'. Single-node execution is
  // the oracle; 'plan' is the bucketed run under test.
  void assertSameAsSingleNode(
      const lp::LogicalPlanNodePtr& logicalPlan,
      PlanAndStats& plan) {
    auto reference = checkSame(plan, toSingleNodePlan(logicalPlan));
    ASSERT_GT(reference.results.size(), 0);
  }

  std::vector<std::string> tablesToDrop_;
};

TEST_F(HiveBucketedExecutionTest, join) {
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
            .hashJoinInner(matchHiveScan("u").build())
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }

  // RIGHT join: the build side is repartitioned into the bucketed fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t RIGHT JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinRight(matchHiveScan("u")
                               .aliases({"u_nationkey"})
                               .shuffle({"u_nationkey"})
                               .build())
            .project()
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }

  // FULL join: both sides stay co-bucketed in one fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t FULL OUTER JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinFull(matchHiveScan("u").build())
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }

  // LEFT join: both sides stay co-bucketed in one fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t LEFT JOIN u ON t.c_nationkey = u.c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinLeft(matchHiveScan("u").build())
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
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
                             .shuffle({"w_nationkey"})
                             .build())
          .bucketed()
          .fragmentWidth(4)
          .gather()
          .project()
          .build());
  assertSameAsSingleNode(logicalPlan, plan);
}

TEST_F(HiveBucketedExecutionTest, semijoin) {
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinLeftSemiFilter(matchHiveScan("u").build())
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }

  // NOT IN lowers to a null-aware anti join, co-bucketed in one fragment.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_custkey, c_nationkey FROM t "
        "WHERE c_nationkey NOT IN (SELECT c_nationkey FROM u)");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .hashJoinAnti(matchHiveScan("u").build())
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }
}

TEST_F(HiveBucketedExecutionTest, aggregation) {
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
    assertSameAsSingleNode(logicalPlan, plan);
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
    assertSameAsSingleNode(logicalPlan, plan);
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
    assertSameAsSingleNode(logicalPlan, plan);
  }

  // DISTINCT aggregate lowers to stacked partial/final aggregations.
  {
    auto logicalPlan = parseSelect(
        "SELECT c_nationkey, count(DISTINCT c_mktsegment) FROM t "
        "GROUP BY c_nationkey");
    auto plan = planDistributed(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("t")
            .partialAggregation()
            .localPartition({"c_nationkey", "c_mktsegment"})
            .finalAggregation()
            .localPartition({"c_nationkey"})
            .singleAggregation()
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
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
    assertSameAsSingleNode(logicalPlan, plan);
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
            .localPartition({"c_mktsegment"})
            .intermediateAggregation()
            .bucketed()
            .fragmentWidth(4)
            .shuffle({"c_mktsegment"})
            .localPartition({"c_mktsegment"})
            .finalAggregation()
            .gather()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }
}

TEST_F(HiveBucketedExecutionTest, singleBucket) {
  createBucketedTable("t", 1, {"c_nationkey"}, "SELECT * FROM customer");

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
          .fragmentWidth(1)
          .gather()
          .build());
  assertSameAsSingleNode(logicalPlan, plan);
}

TEST_F(HiveBucketedExecutionTest, joinDifferentBucketCounts) {
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
          .hashJoinInner(matchHiveScan("t8").build())
          .bucketed()
          .fragmentWidth(4)
          .gather()
          .project()
          .build());
  assertSameAsSingleNode(logicalPlan, plan);
}

TEST_F(HiveBucketedExecutionTest, compositeBucketKeys) {
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
    assertSameAsSingleNode(logicalPlan, plan);
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
            .localPartition({"c_nationkey"})
            .intermediateAggregation()
            .bucketed()
            .fragmentWidth(4)
            .shuffle({"c_nationkey"})
            .localPartition({"c_nationkey"})
            .finalAggregation()
            .gather()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }
}

TEST_F(HiveBucketedExecutionTest, widthClampsToNumWorkers) {
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
    assertSameAsSingleNode(logicalPlan, plan);
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
                               .shuffle({"u_nationkey"})
                               .build())
            .bucketed()
            .fragmentWidth(5)
            .gather()
            .project()
            .build());
    assertSameAsSingleNode(logicalPlan, plan);
  }
}

TEST_F(HiveBucketedExecutionTest, copartitionedJoinIndivisibleWorkers) {
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
          .hashJoinInner(matchHiveScan("u").build())
          .bucketed()
          .fragmentWidth(3)
          .gather()
          .project()
          .build());
  assertSameAsSingleNode(logicalPlan, plan);
}

TEST_F(HiveBucketedExecutionTest, unionall) {
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
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchHiveScan("t")
          .localPartition(matchHiveScan("u").project().build())
          .bucketed()
          .fragmentWidth(4)
          .shuffle({"c_nationkey"})
          .localPartition({"c_nationkey"})
          .singleAggregation({"c_nationkey"}, {"count(*)"})
          .gather()
          .build());
  assertSameAsSingleNode(logicalPlan, plan);
}

} // namespace
} // namespace facebook::axiom::optimizer
