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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class BucketedExecutionTest : public test::QueryTestBase,
                              public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    useV2_ = GetParam();
  }

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

  // Asserts the bucketed fragment's Velox grouped-execution metadata.
  static void expectGroupedExecution(
      const MultiFragmentPlan& plan,
      bool grouped) {
    bool found = false;
    for (const auto& fragment : plan.fragments()) {
      if (fragment.groupedNodes.empty()) {
        continue;
      }
      found = true;
      ASSERT_TRUE(fragment.numSplitGroups.has_value());
      EXPECT_GT(*fragment.numSplitGroups, 0);

      const auto& veloxFragment = fragment.fragment;
      if (!grouped) {
        EXPECT_FALSE(veloxFragment.isGroupedExecution());
        EXPECT_TRUE(veloxFragment.groupedExecutionLeafNodeIds.empty());
        continue;
      }

      EXPECT_TRUE(veloxFragment.isGroupedExecution());
      EXPECT_EQ(veloxFragment.numSplitGroups, *fragment.numSplitGroups);

      size_t bucketedScans = 0;
      for (const auto& [nodeId, partitionType] : fragment.groupedNodes) {
        if (partitionType != nullptr) {
          ++bucketedScans;
          EXPECT_TRUE(veloxFragment.leafNodeRunsGroupedExecution(nodeId));
        }
      }
      EXPECT_EQ(
          veloxFragment.groupedExecutionLeafNodeIds.size(), bucketedScans);
    }
    EXPECT_TRUE(found) << "Expected at least one bucketed fragment";
  }
};

TEST_P(BucketedExecutionTest, join) {
  addBucketedTable("j_orders", {"customer_id"}, 128);
  addBucketedTable(
      "j_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoinInner(matchScan("j_orders"))
            .projectIf(useV2_)
            .fragment({.width = 4, .bucketedScans = 2})
            .gather()
            .build());
  }

  // Each outer join keeps both sides co-bucketed in one fragment. The plan
  // reads j_customers as the probe, which turns the query's RIGHT into a LEFT
  // and its LEFT into a RIGHT.
  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders RIGHT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoinLeft(matchScan("j_orders"))
            .projectIf(!useV2_)
            .fragment({.width = 4, .bucketedScans = 2})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders FULL OUTER JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("j_customers")
            .hashJoinFull(matchScan("j_orders"))
            .fragment({.width = 4, .bucketedScans = 2})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM j_orders LEFT JOIN j_customers "
        "ON j_orders.customer_id = j_customers.id",
        kTestConnectorId));
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("j_customers")
              .hashJoinRight(matchScan("j_orders"))
              .fragment({.width = 4, .bucketedScans = 2})
              .gather()
              .build());
    } else {
      // Wrong plan: v1 reads j_orders by its buckets and then shuffles it
      // into the join. A grouped read that is shuffled away caps the
      // fragment's width for no benefit; the pairing it was read for is lost.
      expectBucketedFragmentWithWidth(*plan.plan, 4);
    }
  }

  testConnector_->addTable(
      "j_unbucketed", ROW({"id", "label"}, {BIGINT(), VARCHAR()}));
  testConnector_->setStats(
      "j_unbucketed", 50'000, {{"id", {.numDistinct = 50'000}}});
  auto plan = planDistributed(parseSelect(
      "SELECT * FROM j_orders JOIN j_unbucketed "
      "ON j_orders.customer_id = j_unbucketed.id",
      kTestConnectorId));
  // The unbucketed side is shuffled into the bucketed fragment by the bucket
  // function, so it joins the grouped scan without moving it.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("j_orders")
          .hashJoinInner(
              matchScan("j_unbucketed").aliases({"jid"}).shuffle({"jid"}))
          .projectIf(useV2_)
          .fragment({.width = 4, .bucketedScans = 1, .bucketedExchanges = 1})
          .gather()
          .build());
}

TEST_P(BucketedExecutionTest, groupedExecutionFlag) {
  addBucketedTable("ge_orders", {"customer_id"}, 128);
  addBucketedTable(
      "ge_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  const auto logicalPlan = parseSelect(
      "SELECT * FROM ge_orders JOIN ge_customers "
      "ON ge_orders.customer_id = ge_customers.id",
      kTestConnectorId);

  {
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 4, .numDrivers = 4, .groupedExecution = true},
        optimizerOptions_);
    expectBucketedFragmentWithWidth(*plan.plan, 4);
    expectGroupedExecution(*plan.plan, /*grouped=*/useV2_);
  }

  {
    auto plan = planVelox(
        logicalPlan,
        {.numWorkers = 4, .numDrivers = 4, .groupedExecution = false},
        optimizerOptions_);
    expectBucketedFragmentWithWidth(*plan.plan, 4);
    expectGroupedExecution(*plan.plan, /*grouped=*/false);
  }
}

TEST_P(BucketedExecutionTest, groupedExecutionSkipsScanOnlyFragment) {
  addBucketedTable("ge_scan_only", {"customer_id"}, 128);

  auto plan = planVelox(
      parseSelect("SELECT customer_id FROM ge_scan_only", kTestConnectorId),
      {.numWorkers = 4, .numDrivers = 4, .groupedExecution = true},
      optimizerOptions_);

  for (const auto& fragment : plan.plan->fragments()) {
    EXPECT_FALSE(fragment.fragment.isGroupedExecution());
  }
}

TEST_P(BucketedExecutionTest, groupedExecutionRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  // 8 buckets over 2 workers, so each task processes multiple split groups.
  const auto schema = ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()});
  testConnector_->addTable(
      "ge_run",
      schema,
      velox::ROW({}),
      connector::TestBucketSpec{{"customer_id"}, 8});
  testConnector_->appendData(
      "ge_run",
      makeRowVector(
          {"customer_id", "amount"},
          {makeFlatVector<int64_t>(
               200, [](auto row) { return static_cast<int64_t>(row % 40); }),
           makeFlatVector<double>(
               200, [](auto row) { return static_cast<double>(row); })}));

  const auto logicalPlan = parseSelect(
      "SELECT customer_id, count(*), sum(amount) "
      "FROM ge_run GROUP BY customer_id",
      kTestConnectorId);

  auto grouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = true});
  auto ungrouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = false});

  velox::exec::test::assertEqualResults(grouped.results, ungrouped.results);

  size_t groupedSplitGroups = 0;
  for (const auto& stageStats : grouped.stats) {
    groupedSplitGroups += stageStats.completedSplitGroups.size();
  }
  EXPECT_GT(groupedSplitGroups, 0)
      << "grouped run must complete at least one split group";

  for (const auto& stageStats : ungrouped.stats) {
    EXPECT_TRUE(stageStats.completedSplitGroups.empty());
  }
}

TEST_P(BucketedExecutionTest, groupedJoinRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  // Two co-bucketed tables on the join key: build bucket N and probe bucket N
  // run in the same split group, so the hash join executes co-located without a
  // shuffle. Exercises a multi-source grouped pipeline (the case that a single
  // grouped scan does not).
  testConnector_->addTable(
      "gj_orders",
      ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"customer_id"}, 8});
  testConnector_->appendData(
      "gj_orders",
      makeRowVector(
          {"customer_id", "amount"},
          {makeFlatVector<int64_t>(
               200, [](auto row) { return static_cast<int64_t>(row % 40); }),
           makeFlatVector<double>(
               200, [](auto row) { return static_cast<double>(row); })}));

  testConnector_->addTable(
      "gj_customers",
      ROW({"id", "tag"}, {BIGINT(), BIGINT()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"id"}, 8});
  testConnector_->appendData(
      "gj_customers",
      makeRowVector(
          {"id", "tag"},
          {makeFlatVector<int64_t>(
               40, [](auto row) { return static_cast<int64_t>(row); }),
           makeFlatVector<int64_t>(
               40, [](auto row) { return static_cast<int64_t>(row * 10); })}));

  const auto logicalPlan = parseSelect(
      "SELECT o.customer_id, o.amount, c.tag "
      "FROM gj_orders o JOIN gj_customers c ON o.customer_id = c.id",
      kTestConnectorId);

  auto grouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = true});
  auto ungrouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = false});

  velox::exec::test::assertEqualResults(grouped.results, ungrouped.results);

  size_t groupedSplitGroups = 0;
  for (const auto& stageStats : grouped.stats) {
    groupedSplitGroups += stageStats.completedSplitGroups.size();
  }
  EXPECT_GT(groupedSplitGroups, 0)
      << "grouped join must complete at least one split group";
}

TEST_P(BucketedExecutionTest, groupedUnionRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  // UNION ALL of two co-bucketed tables feeding a bucketed aggregation. Both
  // union legs are grouped, so the local exchange under the union runs grouped
  // too (the case that errors when only some sources are grouped).
  for (const auto& name : {"gu_a", "gu_b"}) {
    testConnector_->addTable(
        name,
        ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
        velox::ROW({}),
        connector::TestBucketSpec{{"customer_id"}, 8});
    testConnector_->appendData(
        name,
        makeRowVector(
            {"customer_id", "amount"},
            {makeFlatVector<int64_t>(
                 200, [](auto row) { return static_cast<int64_t>(row % 40); }),
             makeFlatVector<double>(
                 200, [](auto row) { return static_cast<double>(row); })}));
  }

  const auto logicalPlan = parseSelect(
      "SELECT customer_id, count(*), sum(amount) FROM ("
      "  SELECT customer_id, amount FROM gu_a"
      "  UNION ALL"
      "  SELECT customer_id, amount FROM gu_b"
      ") GROUP BY customer_id",
      kTestConnectorId);

  auto grouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = true});
  auto ungrouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = false});

  velox::exec::test::assertEqualResults(grouped.results, ungrouped.results);

  size_t groupedSplitGroups = 0;
  for (const auto& stageStats : grouped.stats) {
    groupedSplitGroups += stageStats.completedSplitGroups.size();
  }
  EXPECT_GT(groupedSplitGroups, 0)
      << "grouped union must complete at least one split group";
}

TEST_P(BucketedExecutionTest, groupedMixedJoinRunsEndToEnd) {
  if (!useV2_) {
    GTEST_SKIP() << "Grouped execution requires scan certification in v2";
  }
  // Mixed grouped execution: a bucketed probe joined with an unbucketed build
  // that is shuffled into the bucketed fragment. The probe pipeline runs
  // grouped while the build (behind a remote exchange) runs ungrouped, sharing
  // a single build hash table across all probe split groups.
  testConnector_->addTable(
      "gmj_orders",
      ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"customer_id"}, 8});
  testConnector_->appendData(
      "gmj_orders",
      makeRowVector(
          {"customer_id", "amount"},
          {makeFlatVector<int64_t>(
               200, [](auto row) { return static_cast<int64_t>(row % 40); }),
           makeFlatVector<double>(
               200, [](auto row) { return static_cast<double>(row); })}));

  testConnector_->addTable(
      "gmj_extras", ROW({"customer_id", "extra"}, {BIGINT(), BIGINT()}));
  testConnector_->appendData(
      "gmj_extras",
      makeRowVector(
          {"customer_id", "extra"},
          {makeFlatVector<int64_t>(
               40, [](auto row) { return static_cast<int64_t>(row); }),
           makeFlatVector<int64_t>(
               40, [](auto row) { return static_cast<int64_t>(row * 100); })}));

  const auto logicalPlan = parseSelect(
      "SELECT o.customer_id, o.amount, e.extra "
      "FROM gmj_orders o JOIN gmj_extras e ON o.customer_id = e.customer_id",
      kTestConnectorId);

  auto grouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = true});
  auto ungrouped = runVelox(
      logicalPlan,
      {.numWorkers = 2, .numDrivers = 2, .groupedExecution = false});

  velox::exec::test::assertEqualResults(grouped.results, ungrouped.results);

  size_t groupedSplitGroups = 0;
  for (const auto& stageStats : grouped.stats) {
    groupedSplitGroups += stageStats.completedSplitGroups.size();
  }
  EXPECT_GT(groupedSplitGroups, 0)
      << "grouped mixed join must complete at least one split group";
}

TEST_P(BucketedExecutionTest, semijoin) {
  addBucketedTable("sj_orders", {"customer_id"}, 128);
  addBucketedTable(
      "sj_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM sj_orders WHERE customer_id IN "
        "(SELECT id FROM sj_customers)",
        kTestConnectorId));
    // V2 lowers WHERE-`IN` to a null-aware kLeftSemiProject (not
    // kLeftSemiFilter) and rejects bucketing for null-aware existence joins, so
    // it shuffles both sides instead of keeping the probe bucketed (sound, but
    // a missed co-location). The V2 check is gated off until WHERE-`IN` folds
    // to a non-null-aware semi-filter.
    // TODO: assert V2 co-location once WHERE-`IN` folds to a non-null-aware
    // semi-filter (v2 lowers it to a null-aware kLeftSemiProject DPhyp won't
    // co-bucket).
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("sj_orders")
              .hashJoin(
                  matchScan("sj_customers"), core::JoinType::kLeftSemiFilter)
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .build());
    }
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT * FROM sj_orders WHERE customer_id NOT IN "
        "(SELECT id FROM sj_customers)",
        kTestConnectorId));
    // V2 rejects bucketing for the null-aware anti join (a bucketed existence
    // side would confine a null key to one bucket) and shuffles both sides with
    // null/any replication instead of co-bucketing. The V2 check is gated off;
    // DPhyp skips co-bucketing for null-aware edges.
    // TODO: assert V2 co-location once null-aware existence joins co-bucket
    // with replication (DPhyp currently skips co-bucketing for null-aware
    // edges).
    if (!useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("sj_orders")
              .hashJoin(
                  matchScan("sj_customers"),
                  core::JoinType::kAnti,
                  {.nullAware = true})
              .bucketed()
              .fragmentWidth(4)
              .gather()
              .build());
    }
  }
}

TEST_P(BucketedExecutionTest, aggregation) {
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
  }

  // The bucket key is a prefix of the grouping keys, so each group still lands
  // whole on one task.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id", "order_date"}, {"sum(amount)"})
            .build());
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("a_orders")
              .partialAggregation()
              .localPartition({"customer_id", "order_date"})
              .finalAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    } else {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("a_orders")
              .localPartition({"customer_id", "order_date"})
              .singleAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    }
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"},
                {"sum(amount)", "count(*)", "min(amount)", "max(amount)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"customer_id"}, {"count(distinct amount)"})
            .build());
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("a_orders")
              .localPartition({"customer_id"})
              .singleAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    } else {
      // v1 makes the values distinct first, then counts them.
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("a_orders")
              .localPartition({"customer_id", "amount"})
              .singleAggregation()
              .partialAggregation()
              .localPartition({"customer_id"})
              .finalAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    }
  }

  {
    auto plan = planDistributed(parseSelect(
        "SELECT customer_id, sum(amount) AS total FROM a_orders "
        "GROUP BY customer_id HAVING sum(amount) > 1000",
        kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .filter()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
  }

  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext(), /*allowAmbiguousOutputNames=*/true)
            .tableScan("a_orders")
            .aggregate(
                {"customer_id"}, {"array_agg(amount ORDER BY order_date)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .localPartition({"customer_id"})
            .singleAggregation()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("a_orders")
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .localLimit(0, 100)
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .finalLimit(0, 100)
            .build());
  }

  // Grouping by a non-bucket key falls through to partial+final. The table's
  // bucketing on customer_id does not co-locate order_date groups, so the plan
  // does not read it grouped.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("a_orders")
            .aggregate({"order_date"}, {"sum(amount)"})
            .build());
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("a_orders")
              .partialAggregation()
              .notBucketed()
              .shuffle({"order_date"})
              .localPartition({"order_date"})
              .finalAggregation()
              .gather()
              .build());
    } else {
      // Wrong plan: v1 runs the scan grouped even though the aggregation
      // groups on a non-bucket key, then shuffles the partials anyway. The
      // grouping constrains the fragment's width and buys nothing, so only
      // its width is checked here.
      expectBucketedFragmentWithWidth(*plan.plan, 4);
    }
  }
}

TEST_P(BucketedExecutionTest, select) {
  {
    addBucketedTable("s_one", {"customer_id"}, 1);
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_one")
            .aggregate({"customer_id"}, {"sum(amount)"})
            .build());
    // One bucket holds every row, so pairing rows by it co-locates nothing.
    // The table is read plainly and the aggregation shuffles, spreading it
    // over the workers rather than running it on one task.
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        plan.plan,
        matchScan("s_one")
            .partialAggregation()
            .shuffle({"customer_id"})
            .localPartition({"customer_id"})
            .finalAggregation()
            .fragment({.width = 4})
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("s_down_a")
            .hashJoinInner(matchScan("s_down_b"))
            .fragment({.width = 4, .bucketedScans = 2})
            .gather()
            .project()
            .build());
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
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("s_composite")
              .partialAggregation()
              .localPartition({"region", "customer_id"})
              .finalAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    } else {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("s_composite")
              .localPartition({"region", "customer_id"})
              .singleAggregation()
              .fragment({.width = 4, .bucketedScans = 1})
              .gather()
              .build());
    }
  }

  // Strict subset of bucket keys: bucketing on (region, customer_id) does not
  // co-locate groups of region alone, so the aggregation falls through to
  // partial+shuffle+final and nothing reads the table grouped.
  {
    auto plan = planDistributed(
        lp::PlanBuilder(makeContext())
            .tableScan("s_composite")
            .aggregate({"region"}, {"sum(amount)"})
            .build());
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("s_composite")
              .partialAggregation()
              .notBucketed()
              .shuffle({"region"})
              .localPartition({"region"})
              .finalAggregation()
              .gather()
              .build());
    } else {
      // Wrong plan: region alone is not a bucket key of (region,
      // customer_id), so v1's grouped read cannot co-locate the groups and it
      // shuffles regardless.
      expectBucketedFragmentWithWidth(*plan.plan, 4);
    }
  }
}

TEST_P(BucketedExecutionTest, unionall) {
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
    // gcd(128, 4) = 4, so both legs are read by the same 4 bucket groups and
    // the union needs no shuffle.
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("u_a")
              .localPartition(matchScan("u_b").project())
              .partialAggregation()
              .localPartition({"customer_id"})
              .finalAggregation()
              .fragment({.width = 4, .bucketedScans = 2})
              .gather()
              .build());
    } else {
      // Wrong plan: v1 reads both legs by their buckets, which already
      // co-locates every group, and then shuffles the partial aggregate
      // anyway.
      expectBucketedFragmentWithWidth(*plan.plan, 4);
    }
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
    if (useV2_) {
      AXIOM_ASSERT_DISTRIBUTED_PLAN(
          plan.plan,
          matchScan("u_diff_cust")
              .localPartition(matchScan("u_diff_acct").project())
              .partialAggregation()
              .localPartition({"customer_id"})
              .finalAggregation()
              .fragment({.width = 4, .bucketedScans = 2})
              .gather()
              .build());
    } else {
      // Wrong plan: v1 reads both legs by their buckets, which already
      // co-locates every group, and then shuffles the partial aggregate
      // anyway.
      expectBucketedFragmentWithWidth(*plan.plan, 4);
    }
  }
}

TEST_P(BucketedExecutionTest, alwaysPlanPartialAggregation) {
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

TEST_P(BucketedExecutionTest, mixedJoinOneBucketed) {
  addBucketedTable("m_orders", {"customer_id"}, 8);
  addUnbucketedTable("m_extras");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m_orders JOIN m_extras "
      "ON m_orders.customer_id = m_extras.customer_id",
      kTestConnectorId));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("m_orders")
          .hashJoinInner(
              matchScan("m_extras").aliases({"mcid"}).shuffle({"mcid"}))
          .fragment({.width = 4, .bucketedScans = 1, .bucketedExchanges = 1})
          .gather()
          .project()
          .build());
}

TEST_P(BucketedExecutionTest, mixedJoinTwoBucketedOneNot) {
  addBucketedTable("m2_orders", {"customer_id"}, 8);
  addBucketedTable(
      "m2_customers", {"id"}, 8, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));
  addUnbucketedTable("m2_extras");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m2_orders "
      "JOIN m2_customers ON m2_orders.customer_id = m2_customers.id "
      "JOIN m2_extras ON m2_orders.customer_id = m2_extras.customer_id",
      kTestConnectorId));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("m2_customers")
          .hashJoinInner(matchScan("m2_orders"))
          .hashJoinInner(
              matchScan("m2_extras").aliases({"m2cid"}).shuffle({"m2cid"}))
          .fragment({.width = 4, .bucketedScans = 2, .bucketedExchanges = 1})
          .gather()
          .project()
          .build());
}

TEST_P(BucketedExecutionTest, mixedJoinOneBucketedTwoNot) {
  addBucketedTable("m3_orders", {"customer_id"}, 8);
  addUnbucketedTable("m3_extras1");
  addUnbucketedTable("m3_extras2");

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM m3_orders "
      "JOIN m3_extras1 ON m3_orders.customer_id = m3_extras1.customer_id "
      "JOIN m3_extras2 ON m3_orders.customer_id = m3_extras2.customer_id",
      kTestConnectorId));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("m3_orders")
          .hashJoinInner(
              matchScan("m3_extras1").aliases({"e1cid"}).shuffle({"e1cid"}))
          .hashJoinInner(
              matchScan("m3_extras2").aliases({"e2cid"}).shuffle({"e2cid"}))
          .fragment({.width = 4, .bucketedScans = 1, .bucketedExchanges = 2})
          .gather()
          .project()
          .build());
}

TEST_P(BucketedExecutionTest, windowOnBucketKey) {
  addBucketedTable("w_orders", {"customer_id"}, 16);
  auto plan = planDistributed(parseSelect(
      "SELECT customer_id, amount, "
      "row_number() OVER (PARTITION BY customer_id ORDER BY amount) AS rn "
      "FROM w_orders",
      kTestConnectorId));
  // The window partitions by the bucket key, so each partition is already
  // whole on one task.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("w_orders")
          .localPartition({"customer_id"})
          .window()
          .fragment({.width = 4, .bucketedScans = 1})
          .gather()
          .build());
}

TEST_P(BucketedExecutionTest, threeWayCoBucketed) {
  addBucketedTable("tw_a", {"customer_id"}, 16);
  addBucketedTable("tw_b", {"customer_id"}, 8);
  addBucketedTable("tw_c", {"customer_id"}, 4);
  auto plan = planDistributed(parseSelect(
      "SELECT * FROM tw_a "
      "JOIN tw_b ON tw_a.customer_id = tw_b.customer_id "
      "JOIN tw_c ON tw_a.customer_id = tw_c.customer_id",
      kTestConnectorId));
  // Bucket counts 16, 8 and 4 all divide down to a common 4, so all three
  // scans share one fragment with no shuffle.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("tw_a")
          .hashJoinInner(matchScan("tw_b"))
          .hashJoinInner(matchScan("tw_c"))
          .fragment({.width = 4, .bucketedScans = 3, .bucketedExchanges = 0})
          .gather()
          .project()
          .build());
}

// A chain of inner joins on a shared bucket key fuses into one bucketed
// fragment: all three scans local, no remote hash exchange.
TEST_P(BucketedExecutionTest, innerJoinChainFuses) {
  addBucketedTable("t", {"k"}, 16, ROW({"k"}, BIGINT()));
  addBucketedTable("u", {"k"}, 16, ROW({"k"}, BIGINT()));
  addBucketedTable("v", {"k"}, 16, ROW({"k"}, BIGINT()));

  // Both syntactic and reordered join orderings fuse into one bucketed
  // fragment.
  const std::string sql =
      "SELECT t.k FROM t, "
      "(SELECT v.k FROM u, v WHERE u.k = v.k) sub "
      "WHERE t.k = sub.k";

  // In syntactic order both optimizers co-bucket all three scans into one
  // fragment as t ⋈ (u ⋈ v).
  optimizerOptions_.syntacticJoinOrder = true;
  auto syntacticPlan = planDistributed(parseSelect(sql, kTestConnectorId)).plan;
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      syntacticPlan,
      matchScan("t")
          .hashJoin(
              matchScan("u").hashJoin(matchScan("v"), core::JoinType::kInner),
              core::JoinType::kInner)
          .fragment({.width = 4, .bucketedScans = 3})
          .gather()
          .build());

  // Under cost-based ordering each optimizer picks a different (equally valid)
  // order — v1 u ⋈ v ⋈ t, v2 (t ⋈ u) ⋈ v — but both co-bucket all three into
  // one fragment.
  optimizerOptions_.syntacticJoinOrder = false;
  auto reorderedPlan = planDistributed(parseSelect(sql, kTestConnectorId)).plan;
  if (!useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        reorderedPlan,
        matchScan("u")
            .hashJoin(matchScan("v"), core::JoinType::kInner)
            .hashJoin(matchScan("t"), core::JoinType::kInner)
            .fragment({.width = 4, .bucketedScans = 3})
            .gather()
            .build());
  } else {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        reorderedPlan,
        matchScan("t")
            .hashJoin(matchScan("u"), core::JoinType::kInner)
            .hashJoin(matchScan("v"), core::JoinType::kInner)
            .fragment({.width = 4, .bucketedScans = 3})
            .gather()
            .build());
  }
}

// Same shape with a LEFT join. In V1 the outer join forms no equivalence, so t
// cannot bucket-align with the chain and joins across a remote exchange
// (regression guard for if outer joins ever start forming equivalences). V2
// demotes the LEFT to INNER and fuses, so v1 asserts the shuffled shape and v2
// the co-location property.
TEST_P(BucketedExecutionTest, leftJoinChainDoesNotFuse) {
  addBucketedTable("t", {"k"}, 16, ROW({"k"}, BIGINT()));
  addBucketedTable("u", {"k"}, 16, ROW({"k"}, BIGINT()));
  addBucketedTable("v", {"k"}, 16, ROW({"k"}, BIGINT()));

  const std::string sql =
      "SELECT t.k FROM t, "
      "(SELECT v.k FROM u LEFT JOIN v ON u.k = v.k) sub "
      "WHERE t.k = sub.k";

  // V1 keeps the LEFT JOIN (its outer null-padding forms no equivalence), so t
  // can't bucket-align and joins across a remote exchange. V2 demotes the LEFT
  // to INNER (the outer t.k = v.k rejects null v.k) and co-buckets all three,
  // in syntactic order t ⋈ (u ⋈ v).
  optimizerOptions_.syntacticJoinOrder = true;
  auto syntacticPlan = planDistributed(parseSelect(sql, kTestConnectorId)).plan;
  if (!useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        syntacticPlan,
        matchScan("t")
            .hashJoin(
                matchScan("u")
                    .hashJoin(matchScan("v"), core::JoinType::kLeft)
                    .shuffle(),
                core::JoinType::kInner)
            .gather()
            .build());
  } else {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        syntacticPlan,
        matchScan("t")
            .hashJoin(
                matchScan("u").hashJoin(matchScan("v"), core::JoinType::kInner),
                core::JoinType::kInner)
            .fragment({.width = 4, .bucketedScans = 3})
            .gather()
            .build());
  }

  // Under cost-based ordering v1 still keeps the LEFT (broadcasting t); v2
  // demotes and co-buckets all three as (t ⋈ u) ⋈ v.
  optimizerOptions_.syntacticJoinOrder = false;
  auto reorderedPlan = planDistributed(parseSelect(sql, kTestConnectorId)).plan;
  if (!useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        reorderedPlan,
        matchScan("u")
            .hashJoin(matchScan("v"), core::JoinType::kLeft)
            .hashJoin(matchScan("t").broadcast(), core::JoinType::kInner)
            .gather()
            .build());
  } else {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        reorderedPlan,
        matchScan("t")
            .hashJoin(matchScan("u"), core::JoinType::kInner)
            .hashJoin(matchScan("v"), core::JoinType::kInner)
            .fragment({.width = 4, .bucketedScans = 3})
            .gather()
            .build());
  }
}

TEST_P(BucketedExecutionTest, bucketedAggThenBucketedJoin) {
  addBucketedTable("ab_orders", {"customer_id"}, 16);
  addBucketedTable(
      "ab_customers", {"id"}, 16, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));
  auto plan = planDistributed(parseSelect(
      "SELECT x.customer_id, x.cnt, ab_customers.name FROM ("
      "  SELECT customer_id, COUNT(*) AS cnt FROM ab_orders "
      "  GROUP BY customer_id"
      ") x JOIN ab_customers ON x.customer_id = ab_customers.id",
      kTestConnectorId));
  // The aggregation keeps its input's bucketing, so the join above it pairs
  // with the other bucketed table without a shuffle.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("ab_customers")
          .hashJoinInner(matchScan("ab_orders")
                             .partialAggregation()
                             .localPartition({"customer_id"})
                             .finalAggregation())
          .projectIf(!useV2_)
          .fragment({.width = 4, .bucketedScans = 2, .bucketedExchanges = 0})
          .gather()
          .build());
}

TEST_P(BucketedExecutionTest, bucketedAggThenBroadcastJoin) {
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
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("bc_orders")
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .hashJoinInner(
                matchScan("bc_dim").aliases({"bcid"}).shuffle({"bcid"}))
            .fragment({.width = 4, .bucketedScans = 1, .bucketedExchanges = 1})
            .gather()
            .build());
  } else {
    // Wrong plan: v1 scans bc_dim twice — once to semi-join filter the
    // aggregation's input, once to join it — and broadcasts the aggregate
    // between them.
    expectBucketedFragmentWithWidth(*plan.plan, 4);
  }
}

TEST_P(BucketedExecutionTest, broadcastJoinThenBucketedAgg) {
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
  // The join is on a non-bucket key but its build side is broadcast, so the
  // probe's rows stay put and the aggregation above still reads bj_orders by
  // its buckets.
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("bj_orders")
            .hashJoinInner(matchScan("bj_dim").broadcast())
            .partialAggregation()
            .localPartition({"customer_id"})
            .finalAggregation()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
  } else {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("bj_orders")
            .hashJoinInner(matchScan("bj_dim").broadcast())
            .localPartition({"customer_id"})
            .singleAggregation()
            .fragment({.width = 4, .bucketedScans = 1})
            .gather()
            .build());
  }
}

TEST_P(BucketedExecutionTest, greedyBucketed) {
  addBucketedTable("g_orders", {"customer_id"}, 128);
  addBucketedTable(
      "g_customers", {"id"}, 128, ROW({"id", "name"}, {BIGINT(), VARCHAR()}));

  optimizerOptions_.greedyJoinThreshold = 2;

  auto plan = planDistributed(parseSelect(
      "SELECT * FROM g_orders JOIN g_customers "
      "ON g_orders.customer_id = g_customers.id",
      kTestConnectorId));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("g_customers")
          .hashJoinInner(matchScan("g_orders"))
          .projectIf(useV2_)
          .fragment({.width = 4, .bucketedScans = 2})
          .gather()
          .build());
}

TEST_P(BucketedExecutionTest, greedyBucketedWithDimensions) {
  constexpr int kNumDims = 20;

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

// Bucketings that cannot be copartitioned (gcd(16, 9) = 1) leave the join with
// no bucketed pairing to exploit.
TEST_P(BucketedExecutionTest, incompatibleBucketingOnManyWorkers) {
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

  if (useV2_) {
    // No pairing is possible, and the smaller side is cheap to replicate, so
    // the join broadcasts it and neither table is read grouped.
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("s_incompat_a")
            .hashJoinInner(matchScan("s_incompat_b").broadcast())
            .project()
            .notBucketed()
            .gather()
            .build());
  }
  // v1 plan shape is not verified: v1 produces wrong plan which reads the
  // smaller side by its 9 buckets, which caps that fragment at width 3, and
  // then shuffles it anyway — grouping that buys nothing.
}

// Joining tables whose bucketings cannot be copartitioned plans on a single
// worker, as one ungrouped fragment.
TEST_P(BucketedExecutionTest, incompatibleBucketingOnOneWorker) {
  addBucketedTable("w1_t", {"customer_id"}, 16);
  addBucketedTable(
      "w1_u", {"id"}, 9, ROW({"id", "name"}, {BIGINT(), VARCHAR()}), 100'000);

  auto plan = planVelox(
      parseSelect(
          "SELECT * FROM w1_t JOIN w1_u "
          "ON w1_t.customer_id = w1_u.id",
          kTestConnectorId),
      {.numWorkers = 1, .numDrivers = 4},
      optimizerOptions_);

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("w1_t")
          .hashJoinInner(matchScan("w1_u"))
          .projectIf(useV2_)
          .notBucketed()
          .output(FragmentType::kSingle)
          .build());
}

// Two tables bucketed on different members of a two-key join must not be
// treated as a co-bucketed pair: their buckets align on different columns, so
// co-locating them would put matching rows on different tasks.
TEST_P(BucketedExecutionTest, joinKeysMustCorrespondToBucketing) {
  const auto schema = ROW({"k", "j", "v"}, {BIGINT(), BIGINT(), DOUBLE()});
  addBucketedTable("jk_t", {"k"}, 8, schema);
  addBucketedTable("jk_u", {"j"}, 8, schema);

  optimizerOptions_.syntacticJoinOrder = true;
  SCOPE_EXIT {
    optimizerOptions_.syntacticJoinOrder = false;
  };
  auto plan = planDistributed(parseSelect(
      "SELECT a.v, b.v FROM jk_t a JOIN jk_u b ON a.k = b.k AND a.j = b.j",
      kTestConnectorId));
  if (useV2_) {
    // Neither side's bucketing covers both keys, so both shuffle.
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("jk_t")
            .shuffle({"k", "j"})
            // The right side's columns are renamed by the join, so only the
            // boundary is asserted there.
            .hashJoinInner(matchScan("jk_u").shuffle())
            .notBucketed()
            .gather()
            .project()
            .build());
  }
}

// A join's keys are written in the query's order, its tables' bucketing in
// theirs. Two tables bucketed alike pair either way round.
TEST_P(BucketedExecutionTest, joinPairsWhateverOrderTheKeysAreWritten) {
  const auto schema = ROW({"k", "j", "v"}, {BIGINT(), BIGINT(), DOUBLE()});
  addBucketedTable("ko_t", {"k", "j"}, 8, schema);
  addBucketedTable("ko_u", {"k", "j"}, 8, schema);

  optimizerOptions_.syntacticJoinOrder = true;
  SCOPE_EXIT {
    optimizerOptions_.syntacticJoinOrder = false;
  };
  auto plan = planDistributed(parseSelect(
      "SELECT a.v, b.v FROM ko_t a JOIN ko_u b ON a.j = b.j AND a.k = b.k",
      kTestConnectorId));
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("ko_t")
            .hashJoinInner(matchScan("ko_u"))
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .project()
            .build());
  }
}

// An aggregation above a broadcast join reaches through the join to read its
// probe grouped: the build is replicated, so probe rows stay where their
// buckets put them.
TEST_P(BucketedExecutionTest, aggregationOverBroadcastJoinOnProbe) {
  const auto schema = ROW({"k", "j", "v"}, {BIGINT(), BIGINT(), DOUBLE()});
  addBucketedTable("ap_t", {"k"}, 8, schema, 1'000'000);
  addBucketedTable("ap_u", {"j"}, 2, schema, 1'000);

  auto plan = planDistributed(parseSelect(
      "SELECT a.k, count(*) FROM ap_t a JOIN ap_u b ON a.j = b.j GROUP BY a.k",
      kTestConnectorId));
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("ap_t")
            // The build is replicated, so reading it by its buckets would cap
            // its width for nothing.
            .hashJoinInner(matchScan("ap_u").notBucketed().broadcast())
            .partialAggregation()
            .localPartition({"k"})
            .finalAggregation()
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }
}

// Renaming the bucket column does not cost the bucketing.
TEST_P(BucketedExecutionTest, bucketColumnRenamedByProjection) {
  const auto schema = ROW({"k", "j", "v"}, {BIGINT(), BIGINT(), DOUBLE()});
  addBucketedTable("rn_t", {"k"}, 8, schema);

  auto plan = planDistributed(parseSelect(
      "SELECT nk, count(*) FROM (SELECT k AS nk, v FROM rn_t) GROUP BY nk",
      kTestConnectorId));
  if (useV2_) {
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("rn_t")
            .partialAggregation()
            .localPartition({"k"})
            .finalAggregation()
            .project()
            .bucketed()
            .fragmentWidth(4)
            .gather()
            .build());
  }
}

TEST_P(BucketedExecutionTest, partitionSelectionControlsStorageBucketing) {
  if (!useV2_) {
    GTEST_SKIP();
  }

  addBucketedTable("selected_orders", {"customer_id"}, 16);
  testConnector_->setPartitionsConsistent("selected_orders", false);

  auto plan = planDistributed(parseSelect(
      "SELECT customer_id, count(*) FROM selected_orders "
      "GROUP BY customer_id",
      kTestConnectorId));

  ASSERT_EQ(plan.plan->scanPartitionSelections().size(), 1);
  const auto& selection = plan.plan->scanPartitionSelections().begin()->second;
  ASSERT_NE(selection, nullptr);
  EXPECT_EQ(selection->partitions.size(), 16);
  EXPECT_EQ(selection->storagePartitionType, nullptr);
  for (const auto& fragment : plan.plan->fragments()) {
    EXPECT_TRUE(fragment.groupedNodes.empty());
  }
}

TEST_P(BucketedExecutionTest, partitionSelectionReachesPlanOutput) {
  if (!useV2_) {
    GTEST_SKIP();
  }

  addBucketedTable("selected_bucketed_orders", {"customer_id"}, 16);
  auto plan = planDistributed(parseSelect(
      "SELECT customer_id, count(*) FROM selected_bucketed_orders "
      "GROUP BY customer_id",
      kTestConnectorId));

  ASSERT_EQ(plan.plan->scanPartitionSelections().size(), 1);
  const auto& selection = plan.plan->scanPartitionSelections().begin()->second;
  ASSERT_NE(selection, nullptr);
  EXPECT_EQ(selection->partitions.size(), 16);
  EXPECT_NE(selection->storagePartitionType, nullptr);
  expectBucketedFragmentWithWidth(*plan.plan, 4);
}

AXIOM_INSTANTIATE_V1_V2(BucketedExecutionTest);

} // namespace
} // namespace facebook::axiom::optimizer
