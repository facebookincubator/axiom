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

#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class BucketedExecutionTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
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

  void addUnbucketedTable(
      const std::string& name,
      const RowTypePtr& schema =
          ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      int64_t numRows = 1'000'000) {
    testConnector_->addTable(name, schema);
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

  static void expectBucketedFragmentWithWidth(
      const MultiFragmentPlan& plan,
      int32_t expectedWidth,
      int32_t expectedBucketedScans = -1,
      int32_t expectedHashExchanges = -1) {
    bool found = false;
    for (const auto& fragment : plan.fragments()) {
      if (fragment.groupedNodes.empty()) {
        continue;
      }
      EXPECT_EQ(fragment.type, FragmentType::kFixed);
      ASSERT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, expectedWidth);
      int32_t bucketedScans = 0;
      int32_t hashExchanges = 0;
      for (const auto& [_, partitionType] : fragment.groupedNodes) {
        if (partitionType == nullptr) {
          ++hashExchanges;
        } else {
          ++bucketedScans;
        }
      }
      if (expectedBucketedScans >= 0) {
        EXPECT_EQ(bucketedScans, expectedBucketedScans);
      }
      if (expectedHashExchanges >= 0) {
        EXPECT_EQ(hashExchanges, expectedHashExchanges);
      }
      found = true;
    }
    EXPECT_TRUE(found) << "Expected at least one bucketed fragment";
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
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders RIGHT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders FULL OUTER JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  // LEFT join: bucketed fragment preserved on one side.
  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders LEFT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  testConnector_->addTable(
      "j_unbucketed", ROW({"id", "label"}, {BIGINT(), VARCHAR()}));
  testConnector_->setStats(
      "j_unbucketed", 50'000, {{"id", {.numDistinct = 50'000}}});
  auto plan = planDistributed(parseSelect(
      "SELECT * FROM j_orders JOIN j_unbucketed "
      "ON j_orders.customer_id = j_unbucketed.id",
      kTestConnectorId));
  expectBucketedFragmentWithWidth(
      *plan.plan,
      /*expectedWidth=*/4,
      /*expectedBucketedScans=*/1,
      /*expectedHashExchanges=*/1);
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("sj_orders")
            .hashJoin(
                matchScan("sj_customers").build(),
                core::JoinType::kLeftSemiFilter)
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM sj_orders WHERE customer_id NOT IN "
        "(SELECT id FROM sj_customers)",
        kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("sj_orders")
            .hashJoin(
                matchScan("sj_customers").build(),
                core::JoinType::kAnti,
                {.nullAware = true})
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
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
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id", "order_date"}, {"sum(amount)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"},
                {"sum(amount)", "count(*)", "min(amount)", "max(amount)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id"}, {"count(distinct amount)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT customer_id, sum(amount) AS total FROM a_orders "
        "GROUP BY customer_id HAVING sum(amount) > 1000",
        kTestConnectorId));
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext(), /*allowAmbiguousOutputNames=*/true)
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"}, {"array_agg(amount ORDER BY order_date)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  // LIMIT atop a bucketed aggregation keeps the bucketed annotation on
  // the source fragment.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id"}, {"sum(amount)"})
            .limit(0, 100)
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  // Non-bucket grouping key falls through to partial+final.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"order_date"}, {"sum(amount)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
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
    expectBucketedFragmentWithWidth(*plan.plan, 1);
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
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  // gcd(16, 9) = 1; copartition rejects. The two sides cannot co-fragment as
  // a bucketed pair, but each side may independently remain bucketed in its
  // own fragment.
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
    for (const auto& fragment : plan.plan->fragments()) {
      int32_t bucketedScans = 0;
      for (const auto& [_, partitionType] : fragment.groupedNodes) {
        if (partitionType != nullptr) {
          ++bucketedScans;
        }
      }
      EXPECT_LE(bucketedScans, 1)
          << "Incompatible bucket counts (16/9) should not pair-bucket";
    }
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
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }

  // Strict subset of bucket keys: bucketing on (region, customer_id) doesn't
  // satisfy aggregation grouped only by region, so it falls through to
  // partial+shuffle+final. Source fragment may still be bucketed for the
  // partial aggregation.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_composite")
            .aggregate({"region"}, {"sum(amount)"})
            .build());
    expectBucketedFragmentWithWidth(*plan.plan, 4);
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
    // 2-table union with gcd=4: both scans share the same bucketed fragment.
    int32_t bucketedFragments = 0;
    for (const auto& fragment : plan.plan->fragments()) {
      if (!fragment.groupedNodes.empty()) {
        ++bucketedFragments;
        EXPECT_EQ(fragment.type, FragmentType::kFixed);
        ASSERT_TRUE(fragment.width.has_value());
        EXPECT_EQ(*fragment.width, 4);
        int32_t bucketedScans = 0;
        for (const auto& [_, partitionType] : fragment.groupedNodes) {
          if (partitionType != nullptr) {
            ++bucketedScans;
          }
        }
        EXPECT_EQ(bucketedScans, 2);
      }
    }
    EXPECT_GE(bucketedFragments, 1);
  }

  // Different bucket keys (same type, same count) — TestConnector treats these
  // as copartition-compatible since it only checks key types and partition
  // counts, not column names. Both scans land in the same bucketed fragment.
  // Production connectors that distinguish key identity would not co-fragment.
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
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }
}

TEST_F(BucketedExecutionTest, alwaysPlanPartialAggregation) {
  addBucketedTable("g_orders", {"customer_id"}, 128);

  auto plan = lp::PlanBuilder(makeContext())
                  .tableScan("g_orders")
                  .aggregate({"customer_id"}, {"sum(amount)"})
                  .build();

  optimizerOptions_.alwaysPlanPartialAggregation = true;
  auto out = planDistributed(plan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      out.plan,
      matchScan("g_orders")
          .partialAggregation()
          .localPartition({"customer_id"})
          .finalAggregation()
          .bucketed()
          .fragmentWidth(4)
          .gather()
          .build());
}

TEST_F(BucketedExecutionTest, mixedJoinOneBucketed) {
  addBucketedTable("m_orders", {"customer_id"}, 8);
  addUnbucketedTable("m_extras");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m_orders JOIN m_extras "
      "ON m_orders.customer_id = m_extras.customer_id",
      kTestConnectorId));
  bool foundMixed = false;
  for (const auto& fragment : plan.plan->fragments()) {
    int32_t bucketedScans = 0;
    int32_t hashExchanges = 0;
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
    if (bucketedScans == 1 && hashExchanges == 1) {
      foundMixed = true;
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
    }
  }
  EXPECT_TRUE(foundMixed);
}

TEST_F(BucketedExecutionTest, mixedJoinTwoBucketedOneNot) {
  addBucketedTable("m2_orders", {"customer_id"}, 8);
  addBucketedTable(
      "m2_customers", {"id"}, 8, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));
  addUnbucketedTable("m2_extras");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m2_orders "
      "JOIN m2_customers ON m2_orders.customer_id = m2_customers.id "
      "JOIN m2_extras ON m2_orders.customer_id = m2_extras.customer_id",
      kTestConnectorId));
  bool foundMixed = false;
  for (const auto& fragment : plan.plan->fragments()) {
    int32_t bucketedScans = 0;
    int32_t hashExchanges = 0;
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
    if (bucketedScans == 2 && hashExchanges == 1) {
      foundMixed = true;
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
    }
  }
  EXPECT_TRUE(foundMixed);
}

TEST_F(BucketedExecutionTest, mixedJoinOneBucketedTwoNot) {
  addBucketedTable("m3_orders", {"customer_id"}, 8);
  addUnbucketedTable("m3_extras1");
  addUnbucketedTable("m3_extras2");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m3_orders "
      "JOIN m3_extras1 ON m3_orders.customer_id = m3_extras1.customer_id "
      "JOIN m3_extras2 ON m3_orders.customer_id = m3_extras2.customer_id",
      kTestConnectorId));
  bool foundMixed = false;
  for (const auto& fragment : plan.plan->fragments()) {
    int32_t bucketedScans = 0;
    int32_t hashExchanges = 0;
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
    if (bucketedScans == 1 && hashExchanges == 2) {
      foundMixed = true;
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
    }
  }
  EXPECT_TRUE(foundMixed);
}

TEST_F(BucketedExecutionTest, windowOnBucketKey) {
  addBucketedTable("w_orders", {"customer_id"}, 16);
  auto plan = planDistributed(parseSelect(
      "SELECT customer_id, amount, "
      "row_number() OVER (PARTITION BY customer_id ORDER BY amount) AS rn "
      "FROM w_orders",
      kTestConnectorId));
  bool foundBucketed = false;
  for (const auto& fragment : plan.plan->fragments()) {
    if (!fragment.groupedNodes.empty()) {
      EXPECT_EQ(fragment.type, FragmentType::kFixed);
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
      foundBucketed = true;
    }
  }
  EXPECT_TRUE(foundBucketed);
}

TEST_F(BucketedExecutionTest, threeWayCoBucketed) {
  addBucketedTable("tw_a", {"customer_id"}, 16);
  addBucketedTable("tw_b", {"customer_id"}, 8);
  addBucketedTable("tw_c", {"customer_id"}, 4);
  auto plan = planDistributed(parseSelect(
      "SELECT * FROM tw_a "
      "JOIN tw_b ON tw_a.customer_id = tw_b.customer_id "
      "JOIN tw_c ON tw_a.customer_id = tw_c.customer_id",
      kTestConnectorId));
  bool found = false;
  for (const auto& fragment : plan.plan->fragments()) {
    int32_t bucketedScans = 0;
    int32_t hashExchanges = 0;
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
    if (bucketedScans == 3 && hashExchanges == 0) {
      EXPECT_EQ(fragment.type, FragmentType::kFixed);
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(BucketedExecutionTest, bucketedAggThenBucketedJoin) {
  addBucketedTable("ab_orders", {"customer_id"}, 16);
  addBucketedTable(
      "ab_customers", {"id"}, 16, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));
  auto plan = planDistributed(parseSelect(
      "SELECT x.customer_id, x.cnt, ab_customers.name FROM ("
      "  SELECT customer_id, COUNT(*) AS cnt FROM ab_orders "
      "  GROUP BY customer_id"
      ") x JOIN ab_customers ON x.customer_id = ab_customers.id",
      kTestConnectorId));
  bool found = false;
  for (const auto& fragment : plan.plan->fragments()) {
    int32_t bucketedScans = 0;
    int32_t hashExchanges = 0;
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType == nullptr) {
        ++hashExchanges;
      } else {
        ++bucketedScans;
      }
    }
    if (bucketedScans == 2 && hashExchanges == 0) {
      EXPECT_TRUE(fragment.width.has_value());
      EXPECT_EQ(*fragment.width, 4);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(BucketedExecutionTest, bucketedAggThenBroadcastJoin) {
  addBucketedTable("bc_orders", {"customer_id"}, 16);
  addUnbucketedTable(
      "bc_dim",
      ROW({"customer_id", "label"}, {BIGINT(), VARCHAR()}),
      /*numRows=*/1'000);
  auto plan = planDistributed(parseSelect(
      "SELECT x.customer_id, x.cnt, bc_dim.label FROM ("
      "  SELECT customer_id, COUNT(*) AS cnt FROM bc_orders "
      "  GROUP BY customer_id"
      ") x JOIN bc_dim ON x.customer_id = bc_dim.customer_id",
      kTestConnectorId));
  bool foundBucketedScan = false;
  for (const auto& fragment : plan.plan->fragments()) {
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType != nullptr) {
        foundBucketedScan = true;
      }
    }
  }
  EXPECT_TRUE(foundBucketedScan);
}

TEST_F(BucketedExecutionTest, broadcastJoinThenBucketedAgg) {
  addBucketedTable("bj_orders", {"customer_id"}, 16);
  addUnbucketedTable(
      "bj_dim",
      ROW({"amount", "label"}, {DOUBLE(), VARCHAR()}),
      /*numRows=*/1'000);
  auto plan = planDistributed(parseSelect(
      "SELECT bj_orders.customer_id, COUNT(*) "
      "FROM bj_orders JOIN bj_dim ON bj_orders.amount = bj_dim.amount "
      "GROUP BY bj_orders.customer_id",
      kTestConnectorId));
  bool foundBucketedScan = false;
  for (const auto& fragment : plan.plan->fragments()) {
    for (const auto& [_, partitionType] : fragment.groupedNodes) {
      if (partitionType != nullptr) {
        foundBucketedScan = true;
      }
    }
  }
  EXPECT_TRUE(foundBucketedScan);
}

TEST_F(BucketedExecutionTest, greedyBucketed) {
  addBucketedTable("g_orders", {"customer_id"}, 128);
  addBucketedTable(
      "g_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  optimizerOptions_.greedyJoinThreshold = 2;

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM g_orders JOIN g_customers "
      "ON g_orders.customer_id = g_customers.id",
      kTestConnectorId));
  expectBucketedFragmentWithWidth(*plan.plan, 4);
}

TEST_F(BucketedExecutionTest, greedyBucketedWithDimensions) {
  constexpr int kNumDims = 8;

  std::vector<std::string> factColumns{"customer_id"};
  std::vector<TypePtr> factTypes{BIGINT()};
  for (int i = 0; i < kNumDims; ++i) {
    factColumns.push_back(fmt::format("dim_key_{}", i));
    factTypes.push_back(BIGINT());
  }
  auto factSchema = ROW(std::move(factColumns), std::move(factTypes));

  addBucketedTable("g_fact_a", {"customer_id"}, 128, factSchema);
  addBucketedTable("g_fact_b", {"customer_id"}, 128, factSchema);

  for (int i = 0; i < kNumDims; ++i) {
    addUnbucketedTable(
        fmt::format("g_dim_{}", i),
        ROW({"d_id", "d_val"}, {BIGINT(), VARCHAR()}),
        100);
  }

  std::string sql =
      "SELECT g_fact_a.customer_id FROM g_fact_a JOIN g_fact_b "
      "ON g_fact_a.customer_id = g_fact_b.customer_id";
  for (int i = 0; i < kNumDims; ++i) {
    sql += fmt::format(
        " JOIN g_dim_{0} ON g_fact_a.dim_key_{0} = g_dim_{0}.d_id", i);
  }

  auto plan = planDistributed(parseSelect(sql, kTestConnectorId));
  expectBucketedFragmentWithWidth(*plan.plan, 4);
}

} // namespace
} // namespace facebook::axiom::optimizer
