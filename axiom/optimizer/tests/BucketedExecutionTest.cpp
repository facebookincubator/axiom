// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
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

void assertAllNotBucketed(const MultiFragmentPlan& plan) {
  for (const auto& fragment : plan.fragments()) {
    ASSERT_TRUE(fragment.splitToWorkerFns.empty());
  }
}

void assertFragmentBucketingCounts(
    const MultiFragmentPlan& plan,
    int32_t expectedBucketed,
    int32_t expectedNotBucketed) {
  int32_t bucketed = 0;
  int32_t notBucketed = 0;
  for (const auto& fragment : plan.fragments()) {
    (fragment.splitToWorkerFns.empty() ? notBucketed : bucketed) += 1;
  }
  EXPECT_EQ(bucketed, expectedBucketed);
  EXPECT_EQ(notBucketed, expectedNotBucketed);
}

class BucketedExecutionTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }

  void SetUp() override {
    QueryTestBase::SetUp();
    optimizerOptions_.enableBucketedExecution = true;
  }

  void addBucketedTable(
      const std::string& name,
      const std::vector<std::string>& bucketColumns,
      int32_t numBuckets,
      const RowTypePtr& schema =
          ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      int64_t numRows = 1'000'000) {
    testConnector_->addTable(
        name,
        schema,
        velox::ROW({}),
        connector::TestBucketSpec{bucketColumns, numBuckets});
    std::unordered_map<std::string, connector::ColumnStatistics> stats;
    for (const auto& column : schema->names()) {
      stats[column] = {.numDistinct = numRows / 10};
    }
    testConnector_->setStats(name, numRows, stats);
  }

  PlanAndStats planDistributed(const lp::LogicalPlanNodePtr& logicalPlan) {
    return planVelox(
        logicalPlan, {.numWorkers = 4, .numDrivers = 4}, optimizerOptions_);
  }
};

TEST_F(BucketedExecutionTest, join) {
  addBucketedTable("j_orders", {"customer_id"}, 128);
  addBucketedTable(
      "j_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoin(matchScan("j_orders").build(), core::JoinType::kInner)
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders RIGHT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoin(matchScan("j_orders").build(), core::JoinType::kLeft)
            .project()
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders FULL OUTER JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoin(matchScan("j_orders").build(), core::JoinType::kFull)
            .gather()
            .build());
  }

  // LEFT currently emits a multi-fragment plan with a redundant repartition
  // (rewritten to RIGHT but the bucket-routed side flows through an
  // exchange). Bucket routing is in effect and results are correct.
  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders LEFT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan, /*fragmentIndex=*/1);
  }

  testConnector_->addTable(
      "j_unbucketed", ROW({"id", "label"}, {BIGINT(), VARCHAR()}));
  testConnector_->setStats(
      "j_unbucketed", 50'000, {{"id", {.numDistinct = 50'000}}});
  auto plan = planDistributed(parseSelect(
      "SELECT * FROM j_orders JOIN j_unbucketed "
      "ON j_orders.customer_id = j_unbucketed.id",
      kTestConnectorId));
  assertFragmentBucketingCounts(
      *plan.plan, /*expectedBucketed=*/1, /*expectedNotBucketed=*/2);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("j_orders")
          .hashJoin(
              matchScan("j_unbucketed").shuffle().build(),
              core::JoinType::kInner)
          .gather()
          .build());
}

TEST_F(BucketedExecutionTest, semijoin) {
  addBucketedTable("sj_orders", {"customer_id"}, 128);
  addBucketedTable(
      "sj_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM sj_orders WHERE customer_id IN "
        "(SELECT id FROM sj_customers)",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("sj_orders")
            .hashJoin(
                matchScan("sj_customers").build(),
                core::JoinType::kLeftSemiFilter)
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM sj_orders WHERE customer_id NOT IN "
        "(SELECT id FROM sj_customers)",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
  }
}

TEST_F(BucketedExecutionTest, aggregation) {
  testConnector_->addTable(
      "a_orders",
      ROW({"customer_id", "order_date", "amount"},
          {BIGINT(), BIGINT(), DOUBLE()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"customer_id"}, 128});
  testConnector_->setStats(
      "a_orders",
      1'000'000,
      {{"customer_id", {.numDistinct = 100'000}},
       {"order_date", {.numDistinct = 365}},
       {"amount", {.numDistinct = 500'000}}});

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id"}, {"sum(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .localPartition({"customer_id"})
            .singleAggregation({"customer_id"}, {"sum(amount)"})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id", "order_date"}, {"sum(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .localPartition({"customer_id", "order_date"})
            .singleAggregation({"customer_id", "order_date"}, {"sum(amount)"})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"},
                {"sum(amount)", "count(*)", "min(amount)", "max(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id"}, {"count(distinct amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT customer_id, sum(amount) AS total FROM a_orders "
        "GROUP BY customer_id HAVING sum(amount) > 1000",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"}, {"array_agg(amount ORDER BY order_date)"})
            .build());
    assertBucketedPlan(*plan.plan);
  }

  // Non-bucket grouping key falls through to partial+final.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"order_date"}, {"sum(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
    ASSERT_GT(plan.plan->fragments().size(), 1);
    for (size_t i = 1; i < plan.plan->fragments().size(); ++i) {
      assertNotBucketed(*plan.plan, /*fragmentIndex=*/static_cast<int32_t>(i));
    }
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .partialAggregation()
            .shuffle({"order_date"})
            .localPartition({"order_date"})
            .finalAggregation()
            .gather()
            .build());
  }
}

TEST_F(BucketedExecutionTest, select) {
  {
    addBucketedTable("s_one", {"customer_id"}, 1);
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_one")
            .aggregate({"customer_id"}, {"sum(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("s_one")
            .localPartition({"customer_id"})
            .singleAggregation({"customer_id"}, {"sum(amount)"})
            .gather()
            .build());
  }

  // gcd(128, 64) = 64.
  {
    addBucketedTable("s_down_a", {"customer_id"}, 128);
    addBucketedTable(
        "s_down_b",
        {"customer_id"},
        64,
        ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
        500'000);
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM s_down_a JOIN s_down_b "
        "ON s_down_a.customer_id = s_down_b.customer_id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("s_down_a")
            .hashJoin(matchScan("s_down_b").build(), core::JoinType::kInner)
            .gather()
            .project()
            .build());
  }

  // gcd(16, 9) = 1; copartition rejects.
  {
    addBucketedTable("s_incompat_a", {"customer_id"}, 16);
    addBucketedTable(
        "s_incompat_b",
        {"id"},
        9,
        ROW({"id", "name"}, {BIGINT(), VARCHAR()}),
        100'000);
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM s_incompat_a JOIN s_incompat_b "
        "ON s_incompat_a.customer_id = s_incompat_b.id",
        kTestConnectorId));
    ASSERT_GT(plan.plan->fragments().size(), 1);
  }

  testConnector_->addTable(
      "s_composite",
      ROW({"region", "customer_id", "amount"}, {BIGINT(), BIGINT(), DOUBLE()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"region", "customer_id"}, 128});
  testConnector_->setStats(
      "s_composite",
      1'000'000,
      {{"region", {.numDistinct = 10}},
       {"customer_id", {.numDistinct = 100'000}},
       {"amount", {.numDistinct = 500'000}}});
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_composite")
            .aggregate({"region", "customer_id"}, {"sum(amount)"})
            .build());
    assertBucketedPlan(*plan.plan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("s_composite")
            .localPartition({"region", "customer_id"})
            .singleAggregation({"region", "customer_id"}, {"sum(amount)"})
            .gather()
            .build());
  }

  // Strict subset of bucket keys.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_composite")
            .aggregate({"region"}, {"sum(amount)"})
            .build());
    ASSERT_GT(plan.plan->fragments().size(), 1);
  }
}

TEST_F(BucketedExecutionTest, unionall) {
  // gcd(128, 4) = 4.
  addBucketedTable("u_a", {"customer_id"}, 128);
  addBucketedTable(
      "u_b",
      {"customer_id"},
      4,
      ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      500'000);
  {
    auto plan = planDistributed(parseSelect(
        "SELECT customer_id, sum(amount) FROM ("
        "  SELECT customer_id, amount FROM u_a"
        "  UNION ALL"
        "  SELECT customer_id, amount FROM u_b"
        ") GROUP BY customer_id",
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
  }

  // 3-way mixed bucket counts (4, 8, 12), gcd=4. Must accept regardless of
  // insertion order — copartition() between any two is non-transitive.
  addBucketedTable("u3_a", {"customer_id"}, 4);
  addBucketedTable(
      "u3_b",
      {"customer_id"},
      8,
      ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      500'000);
  addBucketedTable(
      "u3_c",
      {"customer_id"},
      12,
      ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      500'000);
  for (const auto& [first, second, third] :
       std::vector<std::tuple<std::string, std::string, std::string>>{
           {"u3_a", "u3_b", "u3_c"},
           {"u3_b", "u3_a", "u3_c"},
           {"u3_c", "u3_b", "u3_a"}}) {
    SCOPED_TRACE(fmt::format("{} {} {}", first, second, third));
    auto plan = planDistributed(parseSelect(
        fmt::format(
            "SELECT customer_id, sum(amount) FROM ("
            "  SELECT customer_id, amount FROM {}"
            "  UNION ALL SELECT customer_id, amount FROM {}"
            "  UNION ALL SELECT customer_id, amount FROM {}"
            ") GROUP BY customer_id",
            first,
            second,
            third),
        kTestConnectorId));
    assertBucketedPlan(*plan.plan);
  }

  // Different bucket keys (same type) — must not co-fragment.
  addBucketedTable("u_diff_cust", {"customer_id"}, 16);
  addBucketedTable(
      "u_diff_acct",
      {"account_id"},
      16,
      ROW({"account_id", "amount"}, {BIGINT(), DOUBLE()}),
      500'000);
  {
    auto plan = planDistributed(parseSelect(
        "SELECT customer_id, sum(amount) FROM ("
        "  SELECT customer_id, amount FROM u_diff_cust"
        "  UNION ALL"
        "  SELECT account_id AS customer_id, amount FROM u_diff_acct"
        ") GROUP BY customer_id",
        kTestConnectorId));
    ASSERT_GT(plan.plan->fragments().size(), 1);
  }
}

TEST_F(BucketedExecutionTest, gating) {
  addBucketedTable("g_orders", {"customer_id"}, 128);

  auto plan = lp::PlanBuilder(makeContext())
                  .tableScan("g_orders")
                  .aggregate({"customer_id"}, {"sum(amount)"})
                  .build();

  assertBucketedPlan(*planDistributed(plan).plan);

  optimizerOptions_.enableBucketedExecution = false;
  assertAllNotBucketed(*planDistributed(plan).plan);

  optimizerOptions_.enableBucketedExecution = true;
  optimizerOptions_.alwaysPlanPartialAggregation = true;
  auto out = planDistributed(plan);
  assertBucketedPlan(*out.plan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      out.plan,
      matchScan("g_orders")
          .partialAggregation()
          .localPartition({"customer_id"})
          .finalAggregation()
          .gather()
          .build());
}

} // namespace
} // namespace facebook::axiom::optimizer
