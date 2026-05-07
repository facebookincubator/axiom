// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

void assertBucketedPlan(
    const MultiFragmentPlan& plan,
    int32_t fragmentIndex = 0) {
  ASSERT_LT(fragmentIndex, plan.fragments().size());
  ASSERT_FALSE(plan.fragments()[fragmentIndex].splitToWorkerFns.empty());
}

void assertNotBucketed(
    const MultiFragmentPlan& plan,
    int32_t fragmentIndex = 0) {
  ASSERT_LT(fragmentIndex, plan.fragments().size());
  ASSERT_TRUE(plan.fragments()[fragmentIndex].splitToWorkerFns.empty());
}

class HiveBucketedExecutionTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables({velox::tpch::Table::TBL_CUSTOMER});
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    optimizerOptions_.enableBucketedExecution = true;
  }

  void TearDown() override {
    for (const auto& name : tablesToDrop_) {
      hiveMetadata().dropTableIfExists({kDefaultSchema, name});
    }
    tablesToDrop_.clear();
    HiveQueriesTestBase::TearDown();
  }

  void createBucketedTable(const std::string& name, const std::string& sql) {
    tablesToDrop_.push_back(name);
    runCtas(sql);
  }

  PlanAndStats planQuery(std::string_view sql) {
    return planVelox(
        parseSelect(sql),
        {.numWorkers = 4, .numDrivers = 4},
        optimizerOptions_);
  }

  std::vector<std::string> tablesToDrop_;
};

TEST_F(HiveBucketedExecutionTest, join) {
  createBucketedTable(
      "j_left",
      "CREATE TABLE j_left "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "j_right",
      "CREATE TABLE j_right "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT DISTINCT c_nationkey, "
      "cast(c_nationkey as varchar) as label FROM customer");

  {
    auto plan = planQuery(
        "SELECT * FROM j_left JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("j_left")
            .hashJoin(matchHiveScan("j_right").build(), core::JoinType::kInner)
            .gather()
            .project()
            .build());
  }

  {
    auto plan = planQuery(
        "SELECT * FROM j_left RIGHT JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planQuery(
        "SELECT * FROM j_left FULL OUTER JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  // LEFT currently emits a sub-optimal multi-fragment shape.
  {
    auto plan = planQuery(
        "SELECT * FROM j_left LEFT JOIN j_right "
        "ON j_left.c_nationkey = j_right.c_nationkey");
    bool foundBucketed = false;
    for (const auto& fragment : plan.plan->fragments()) {
      if (!fragment.splitToWorkerFns.empty()) {
        foundBucketed = true;
        break;
      }
    }
    EXPECT_TRUE(foundBucketed);
  }

  tablesToDrop_.emplace_back("j_unbucketed");
  runCtas(
      "CREATE TABLE j_unbucketed AS "
      "SELECT DISTINCT c_nationkey, "
      "cast(c_nationkey as varchar) as label FROM customer");
  auto plan = planQuery(
      "SELECT * FROM j_left JOIN j_unbucketed "
      "ON j_left.c_nationkey = j_unbucketed.c_nationkey");
  bool foundBucketed = false;
  bool foundNotBucketed = false;
  for (const auto& fragment : plan.plan->fragments()) {
    (fragment.splitToWorkerFns.empty() ? foundNotBucketed : foundBucketed) =
        true;
  }
  EXPECT_TRUE(foundBucketed);
  EXPECT_TRUE(foundNotBucketed);
}

TEST_F(HiveBucketedExecutionTest, semijoin) {
  createBucketedTable(
      "sj_left",
      "CREATE TABLE sj_left "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_custkey, c_nationkey FROM customer");
  createBucketedTable(
      "sj_right",
      "CREATE TABLE sj_right "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT DISTINCT c_nationkey FROM customer WHERE c_nationkey < 5");

  {
    auto plan = planQuery(
        "SELECT c_custkey, c_nationkey FROM sj_left "
        "WHERE c_nationkey IN (SELECT c_nationkey FROM sj_right)");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("sj_left")
            .hashJoin(
                matchHiveScan("sj_right").project().build(),
                core::JoinType::kLeftSemiFilter)
            .gather()
            .build());
  }

  {
    auto plan = planQuery(
        "SELECT c_custkey, c_nationkey FROM sj_left "
        "WHERE c_nationkey NOT IN (SELECT c_nationkey FROM sj_right)");
    assertBucketedPlan(*plan.plan);
  }
}

TEST_F(HiveBucketedExecutionTest, aggregation) {
  createBucketedTable(
      "a_customers",
      "CREATE TABLE a_customers "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT * FROM customer");

  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) FROM a_customers GROUP BY c_nationkey");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("a_customers")
            .localPartition({"c_nationkey"})
            .singleAggregation({"c_nationkey"}, {"count(*)"})
            .gather()
            .build());
  }

  {
    auto plan = planQuery(
        "SELECT c_nationkey, c_mktsegment, count(*) FROM a_customers "
        "GROUP BY c_nationkey, c_mktsegment");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("a_customers")
            .localPartition({"c_nationkey", "c_mktsegment"})
            .singleAggregation({"c_nationkey", "c_mktsegment"}, {"count(*)"})
            .gather()
            .build());
  }

  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*), min(c_acctbal), max(c_acctbal) "
        "FROM a_customers GROUP BY c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(DISTINCT c_mktsegment) FROM a_customers "
        "GROUP BY c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) AS cnt FROM a_customers "
        "GROUP BY c_nationkey HAVING count(*) > 100");
    assertBucketedPlan(*plan.plan);
  }

  // Non-bucket grouping key forces partial+final.
  {
    auto plan = planQuery(
        "SELECT c_mktsegment, count(*) FROM a_customers GROUP BY c_mktsegment");
    assertBucketedPlan(*plan.plan);
    ASSERT_GT(plan.plan->fragments().size(), 1);
    for (size_t i = 1; i < plan.plan->fragments().size(); ++i) {
      assertNotBucketed(*plan.plan, /*fragmentIndex=*/static_cast<int32_t>(i));
    }
  }
}

TEST_F(HiveBucketedExecutionTest, select) {
  {
    createBucketedTable(
        "s_one",
        "CREATE TABLE s_one "
        "WITH (bucket_count = 1, bucketed_by = ARRAY['c_nationkey']) "
        "AS SELECT * FROM customer");
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) FROM s_one GROUP BY c_nationkey");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("s_one")
            .localPartition({"c_nationkey"})
            .singleAggregation({"c_nationkey"}, {"count(*)"})
            .gather()
            .build());
  }

  // gcd(16, 8) = 8.
  {
    createBucketedTable(
        "s_match",
        "CREATE TABLE s_match "
        "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
        "AS SELECT c_custkey, c_nationkey FROM customer");
    createBucketedTable(
        "s_down",
        "CREATE TABLE s_down "
        "WITH (bucket_count = 8, bucketed_by = ARRAY['c_nationkey']) "
        "AS SELECT DISTINCT c_nationkey, "
        "cast(c_nationkey as varchar) as label FROM customer");
    auto plan = planQuery(
        "SELECT * FROM s_match JOIN s_down "
        "ON s_match.c_nationkey = s_down.c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  createBucketedTable(
      "s_composite",
      "CREATE TABLE s_composite "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey', 'c_mktsegment']) "
      "AS SELECT * FROM customer");
  {
    auto plan = planQuery(
        "SELECT c_nationkey, c_mktsegment, count(*) FROM s_composite "
        "GROUP BY c_nationkey, c_mktsegment");
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchHiveScan("s_composite")
            .localPartition({"c_nationkey", "c_mktsegment"})
            .singleAggregation({"c_nationkey", "c_mktsegment"}, {"count(*)"})
            .gather()
            .build());
  }

  // Strict subset of bucket keys.
  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) FROM s_composite "
        "GROUP BY c_nationkey");
    ASSERT_GT(plan.plan->fragments().size(), 1);
  }
}

TEST_F(HiveBucketedExecutionTest, unionall) {
  createBucketedTable(
      "u_a",
      "CREATE TABLE u_a "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");
  createBucketedTable(
      "u_b",
      "CREATE TABLE u_b "
      "WITH (bucket_count = 4, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");

  // gcd(16, 4) = 4.
  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) FROM ("
        "  SELECT c_nationkey, c_custkey FROM u_a"
        "  UNION ALL"
        "  SELECT c_nationkey, c_custkey FROM u_b"
        ") GROUP BY c_nationkey");
    assertBucketedPlan(*plan.plan);
  }

  // Bucketed on different keys (same type) — must split into separate
  // fragments.
  createBucketedTable(
      "u_diff_a",
      "CREATE TABLE u_diff_a "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_nationkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");
  createBucketedTable(
      "u_diff_b",
      "CREATE TABLE u_diff_b "
      "WITH (bucket_count = 16, bucketed_by = ARRAY['c_custkey']) "
      "AS SELECT c_nationkey, c_custkey FROM customer");

  {
    auto plan = planQuery(
        "SELECT c_nationkey, count(*) FROM ("
        "  SELECT c_nationkey FROM u_diff_a"
        "  UNION ALL"
        "  SELECT c_custkey AS c_nationkey FROM u_diff_b"
        ") GROUP BY c_nationkey");
    ASSERT_GT(plan.plan->fragments().size(), 1);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
