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

#include <chrono>
#include <future>
#include <thread>

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class SyntacticJoinOrderTest : public test::HiveQueriesTestBase {
 protected:
  static const inline std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_CUSTOMER,
         velox::tpch::Table::TBL_LINEITEM,
         velox::tpch::Table::TBL_ORDERS});
  }
};

TEST_F(SyntacticJoinOrderTest, innerJoins) {
  lp::PlanBuilder::Context context(
      exec::test::kHiveConnectorId, kDefaultSchema);

  optimizerOptions_.sampleJoins = false;

  // Cardinalities after filters:
  //  - lineitem - 32.3K
  //  - orders - 7.2K
  //  - customer - 337

  // Optimized join order: lineitem x (orders x customer).
  auto optimizedMatcher =
      matchHiveScan("lineitem")
          .hashJoin(matchHiveScan("orders")
                        .hashJoin(matchHiveScan("customer").build())
                        .build())
          .aggregation()
          .build();

  // Reference Velox plan.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto startReferencePlan = [&](const auto& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, getSchema(tableName));
  };

  auto referencePlan =
      startReferencePlan("lineitem")
          .filter("l_shipdate > date '1995-03-15'")
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              startReferencePlan("customer")
                  .filter("c_mktsegment = 'BUILDING'")
                  .hashJoin(
                      {"c_custkey"},
                      {"o_custkey"},
                      startReferencePlan("orders")
                          .filter("o_orderdate < date '1995-03-15'")
                          .planNode(),
                      /*filter=*/"",
                      {"o_orderkey"})
                  .planNode(),
              /*filter=*/"",
              {})
          .singleAggregation({}, {"count(1)"})
          .planNode();

  auto reference = runVelox(referencePlan);
  auto referenceResults = reference.results;

  // Syntactic join order: (a x b) x c.
  {
    // Test all possible join orders that do not involve cross joins (i.e. do
    // not start with lineitem x customer).
    std::vector<std::vector<std::string>> testOrders = {
        {"customer", "orders", "lineitem"},
        // {"customer", "lineitem", "orders"},
        {"orders", "customer", "lineitem"},
        {"orders", "lineitem", "customer"},
        // {"lineitem", "customer", "orders"},
        {"lineitem", "orders", "customer"},
    };

    for (const auto& order : testOrders) {
      SCOPED_TRACE(folly::join(", ", order));

      auto logicalPlan =
          lp::PlanBuilder(context)
              .tableScan(order[0])
              .crossJoin(lp::PlanBuilder(context).tableScan(order[1]))
              .crossJoin(lp::PlanBuilder(context).tableScan(order[2]))
              .filter("c_mktsegment = 'BUILDING'")
              .filter("c_custkey = o_custkey")
              .filter("l_orderkey = o_orderkey")
              .filter("o_orderdate < date '1995-03-15'")
              .filter("l_shipdate > date '1995-03-15'")
              .aggregate({}, {"count(1)"})
              .build();

      {
        optimizerOptions_.syntacticJoinOrder = false;
        auto plan = toSingleNodePlan(logicalPlan);
        AXIOM_ASSERT_PLAN(plan, optimizedMatcher);
      }

      {
        optimizerOptions_.syntacticJoinOrder = true;
        auto plan = toSingleNodePlan(logicalPlan);

        auto matcher = matchHiveScan(order[0])
                           .hashJoin(matchHiveScan(order[1]).build())
                           .hashJoin(matchHiveScan(order[2]).build())
                           .aggregation()
                           .build();
        AXIOM_ASSERT_PLAN(plan, matcher);

        checkSame(logicalPlan, referenceResults);
      }
    }
  }

  // Syntactic join order: a x (b x c).
  {
    // Until we add support for x-joins, we need to make sure that intital DTs
    // do not contain x-joins. Hence, we provide a filter to join b x c in the
    // test orders below.

    // Test all possible join orders that do not involve
    // cross joins (i.e. do not join lineitem x customer).
    std::vector<std::pair<std::vector<std::string>, std::string>> testOrders = {
        {{"customer", "orders", "lineitem"}, "l_orderkey = o_orderkey"},
        {{"customer", "lineitem", "orders"}, "l_orderkey = o_orderkey"},
        // {"orders", "customer", "lineitem"},
        // {"orders", "lineitem", "customer"},
        {{"lineitem", "customer", "orders"}, "c_custkey = o_custkey"},
        {{"lineitem", "orders", "customer"}, "c_custkey = o_custkey"},
    };

    for (const auto& [order, filter] : testOrders) {
      SCOPED_TRACE(folly::join(", ", order));

      auto logicalPlan =
          lp::PlanBuilder(context)
              .tableScan(order[0])
              .crossJoin(
                  lp::PlanBuilder(context).tableScan(order[1]).join(
                      lp::PlanBuilder(context).tableScan(order[2]),
                      filter,
                      lp::JoinType::kInner))
              .filter("c_mktsegment = 'BUILDING'")
              .filter("c_custkey = o_custkey")
              .filter("l_orderkey = o_orderkey")
              .filter("o_orderdate < date '1995-03-15'")
              .filter("l_shipdate > date '1995-03-15'")
              .aggregate({}, {"count(1)"})
              .build();

      {
        optimizerOptions_.syntacticJoinOrder = false;
        auto plan = toSingleNodePlan(logicalPlan);
        AXIOM_ASSERT_PLAN(plan, optimizedMatcher);
      }

      {
        optimizerOptions_.syntacticJoinOrder = true;
        auto plan = toSingleNodePlan(logicalPlan);

        auto matcher =
            matchHiveScan(order[0])
                .hashJoin(matchHiveScan(order[1])
                              .hashJoin(matchHiveScan(order[2]).build())
                              .build())
                .aggregation()
                .build();
        AXIOM_ASSERT_PLAN(plan, matcher);

        checkSame(logicalPlan, referenceResults);
      }
    }
  }
}

TEST_F(SyntacticJoinOrderTest, outerJoins) {
  optimizerOptions_.sampleJoins = false;

  // Optimized join order: lineitem x orders.
  auto optimizedMatcher =
      matchHiveScan("lineitem")
          .hashJoin(matchHiveScan("orders").build(), core::JoinType::kLeft)
          .aggregation()
          .build();

  // Reference Velox plan.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto startReferencePlan = [&](const auto& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, getSchema(tableName));
  };

  auto referencePlan = startReferencePlan("lineitem")
                           .hashJoin(
                               {"l_orderkey"},
                               {"o_orderkey"},
                               startReferencePlan("orders").planNode(),
                               /*filter=*/"l_returnflag = 'R'",
                               {},
                               core::JoinType::kLeft)
                           .singleAggregation({}, {"count(1)"})
                           .planNode();

  auto reference = runVelox(referencePlan);
  auto referenceResults = reference.results;

  // Syntactic join order: a LEFT JOIN b.
  {
    auto logicalPlan = parseSelect(
        "SELECT count(*) FROM lineitem LEFT JOIN orders ON "
        "l_orderkey = o_orderkey and l_returnflag = 'R'");

    {
      optimizerOptions_.syntacticJoinOrder = false;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }

    {
      optimizerOptions_.syntacticJoinOrder = true;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }
  }

  // Syntactic join order: b RIGHT JOIN a.
  {
    auto logicalPlan = parseSelect(
        "SELECT count(*) FROM orders RIGHT JOIN lineitem ON "
        "l_orderkey = o_orderkey and l_returnflag = 'R'");

    {
      optimizerOptions_.syntacticJoinOrder = false;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }

    {
      optimizerOptions_.syntacticJoinOrder = true;
      auto plan = toSingleNodePlan(logicalPlan);

      auto matcher =
          matchHiveScan("orders")
              .hashJoin(
                  matchHiveScan("lineitem").build(), core::JoinType::kRight)
              .aggregation()
              .build();
      AXIOM_ASSERT_PLAN(plan, matcher);

      checkSame(logicalPlan, referenceResults);
    }
  }
}

TEST_F(SyntacticJoinOrderTest, crossJoinStartsWithSingleRowSubqueries) {
  optimizerOptions_.sampleJoins = false;

  auto logicalPlan = parseSelect(
      "WITH "
      "t AS (SELECT count(*) AS c FROM orders), "
      "u AS (SELECT count(*) AS c FROM lineitem), "
      "v AS (SELECT o_orderstatus, count(*) AS s FROM orders GROUP BY o_orderstatus) "
      "SELECT t.c, u.c, v.s FROM t, u, v");

  optimizerOptions_.syntacticJoinOrder = false;
  auto referenceResults = runVelox(logicalPlan).results;

  optimizerOptions_.syntacticJoinOrder = true;
  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher =
      matchHiveScan("orders")
          .aliases({"status"})
          .singleAggregation({"status"}, {"count(*) as s"})
          .project()
          .nestedLoopJoin(matchHiveScan("orders")
                              .singleAggregation({}, {"count(*) as c"})
                              .build())
          .nestedLoopJoin(matchHiveScan("lineitem")
                              .singleAggregation({}, {"count(*) as c"})
                              .build())
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
  checkSame(logicalPlan, referenceResults);
}

// 12-way inner-join chain. Without the per-step restriction that keeps
// the optimizer on the SQL-written order, the recursion enumerates
// candidates and strategies at every level and the optimization does
// not finish within the timeout.
TEST_F(SyntacticJoinOrderTest, manyJoinsBoundedByOrder) {
  constexpr int32_t kNumTables = 12;

  for (int32_t i = 0; i < kNumTables; ++i) {
    const auto key = fmt::format("k{}", i);
    const auto rowType = ROW({key, fmt::format("v{}", i)}, BIGINT());
    testConnector_->addTable(fmt::format("t{}", i), rowType)
        ->setStats(1'000, {{key, {.numDistinct = 1'000}}});
  }

  lp::PlanBuilder::Context context(kTestConnectorId, "default");
  lp::PlanBuilder builder(context);
  builder.tableScan("t0");
  for (int32_t i = 1; i < kNumTables; ++i) {
    builder.join(
        lp::PlanBuilder(context).tableScan(fmt::format("t{}", i)),
        fmt::format("k0 = k{}", i),
        lp::JoinType::kInner);
  }
  auto logicalPlan = builder.aggregate({}, {"count(1)"}).build();

  optimizerOptions_.syntacticJoinOrder = true;
  optimizerOptions_.sampleJoins = false;

  std::packaged_task<void()> task([&]() {
    auto plan = toSingleNodePlan(logicalPlan);
    ASSERT_NE(plan, nullptr);
  });
  auto future = task.get_future();
  std::thread worker(std::move(task));

  constexpr std::chrono::seconds kTimeout{30};
  const auto status = future.wait_for(kTimeout);
  if (status != std::future_status::ready) {
    worker.detach();
    FAIL() << "Optimization timed out after " << kTimeout.count() << "s; "
           << "syntactic_join_order failed to bound enumeration";
    return;
  }
  worker.join();
  future.get();
}

} // namespace
} // namespace facebook::axiom::optimizer
