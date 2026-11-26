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

class SyntacticJoinOrderTest : public test::HiveQueriesTestBase,
                               public ::testing::WithParamInterface<bool> {
 protected:
  static const inline std::string kDefaultSchema{
      connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

  void SetUp() override {
    useV2_ = GetParam();
    test::HiveQueriesTestBase::SetUp();
  }

  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_CUSTOMER,
         velox::tpch::Table::TBL_LINEITEM,
         velox::tpch::Table::TBL_NATION,
         velox::tpch::Table::TBL_ORDERS,
         velox::tpch::Table::TBL_REGION});
  }
};

TEST_P(SyntacticJoinOrderTest, innerJoins) {
  auto context = makeContext(exec::test::kHiveConnectorId);

  optimizerOptions_.sampleJoins = false;

  // Cardinalities after filters:
  //  - lineitem - 32.3K
  //  - orders - 7.2K
  //  - customer - 337

  // Optimized join order: lineitem x (orders x customer).
  auto optimizedMatcher =
      matchHiveScan("lineitem")
          .hashJoin(matchHiveScan("orders").hashJoin(matchHiveScan("customer")))
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
              "l_orderkey = o_orderkey",
              startReferencePlan("customer")
                  .filter("c_mktsegment = 'BUILDING'")
                  .hashJoin(
                      "c_custkey = o_custkey",
                      startReferencePlan("orders")
                          .filter("o_orderdate < date '1995-03-15'")
                          .planNode(),
                      {"o_orderkey"})
                  .planNode(),
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
                           .hashJoin(matchHiveScan(order[1]))
                           .hashJoin(matchHiveScan(order[2]))
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

        auto matcher = matchHiveScan(order[0])
                           .hashJoin(matchHiveScan(order[1]).hashJoin(
                               matchHiveScan(order[2])))
                           .aggregation()
                           .build();
        AXIOM_ASSERT_PLAN(plan, matcher);

        checkSame(logicalPlan, referenceResults);
      }
    }
  }
}

TEST_P(SyntacticJoinOrderTest, outerJoins) {
  optimizerOptions_.sampleJoins = false;

  // Optimized join order: lineitem x orders.
  auto optimizedMatcher =
      matchHiveScan("lineitem")
          .hashJoin(matchHiveScan("orders"), core::JoinType::kLeft)
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
                               "l_orderkey = o_orderkey AND l_returnflag = 'R'",
                               startReferencePlan("orders").planNode(),
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
              .hashJoin(matchHiveScan("lineitem"), core::JoinType::kRight)
              .aggregation()
              .build();
      AXIOM_ASSERT_PLAN(plan, matcher);

      checkSame(logicalPlan, referenceResults);
    }
  }
}

// In the written join order, the single-row input of an inner join without keys
// goes on the build side and computes none of the join's filter parts. Every
// other join keeps its written sides.
TEST_P(SyntacticJoinOrderTest, singleRowBuild) {
  optimizerOptions_.sampleJoins = false;
  optimizerOptions_.syntacticJoinOrder = true;
  using core::JoinType;

  {
    SCOPED_TRACE("two single-row inputs written first");
    auto logicalPlan = parseSelect(
        "WITH "
        "t AS (SELECT count(*) AS c FROM orders), "
        "u AS (SELECT count(*) AS c FROM lineitem), "
        "v AS (SELECT o_orderstatus, count(*) AS s FROM orders GROUP BY o_orderstatus) "
        "SELECT t.c, u.c, v.s FROM t, u, v");
    auto singleRows = matchHiveScan("orders")
                          .singleAggregation({}, {"count(*) as c"})
                          .nestedLoopJoin(
                              matchHiveScan("lineitem")
                                  .singleAggregation({}, {"count(*) as c2"}),
                              JoinType::kInner,
                              "");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("orders")
            .aliases({"status"})
            .singleAggregation({"status"}, {"count(*) as s"})
            .nestedLoopJoin(singleRows, JoinType::kInner, "")
            .project({"c", "c2", "s"})
            .build());
    // The grouped input stays partitioned, and the one-row build is broadcast
    // to its tasks.
    auto singleRowsDistributed =
        matchHiveScan("orders")
            .partialAggregation({}, {"count(*) as pc"})
            .gather()
            .localGather()
            .finalAggregation({}, {"count(pc) as c"})
            .nestedLoopJoin(
                matchHiveScan("lineitem")
                    .partialAggregation({}, {"count(*) as pc2"})
                    .gather()
                    .localGather()
                    .finalAggregation({}, {"count(pc2) as c2"}),
                JoinType::kInner,
                "")
            .broadcast();
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchHiveScan("orders")
            .aliases({"status"})
            .partialAggregation({"status"}, {"count(*) as ps"})
            .shuffle({"status"})
            .localPartition({"status"})
            .finalAggregation({"status"}, {"count(ps) as s"})
            .nestedLoopJoin(singleRowsDistributed, JoinType::kInner, "")
            .gather()
            .project({"c", "c2", "s"})
            .build());
  }

  {
    SCOPED_TRACE("single-row input already the build");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT "
            "  (SELECT max(l_quantity) FROM lineitem) AS x, "
            "  (SELECT count(*) FROM nation WHERE n_regionkey = r.r_regionkey) AS y "
            "FROM region r")),
        matchHiveScan("region")
            .nestedLoopJoin(
                matchHiveScan("lineitem")
                    .singleAggregation({}, {"max(l_quantity) as x"}),
                JoinType::kInner,
                "")
            .hashJoin(
                matchHiveScan("nation").singleAggregation(
                    {"n_regionkey"}, {"count(*) as cnt"}),
                JoinType::kLeft)
            .project({"x", "coalesce(cnt, 0)"})
            .build());
  }

  {
    SCOPED_TRACE("joined by a WHERE comparison");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_name "
            "FROM (SELECT max(r_regionkey) AS m FROM region) s, nation "
            "WHERE n_regionkey < s.m")),
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region").singleAggregation(
                    {}, {"max(r_regionkey) as m"}),
                JoinType::kInner,
                "m > n_regionkey")
            .build());
  }

  {
    SCOPED_TRACE("runtime-empty filtered aggregate");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_name, s.c "
            "FROM ("
            "  SELECT sum(r_regionkey) AS c "
            "  FROM region "
            "  HAVING sum(r_regionkey) < 0"
            ") s, nation")),
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"sum(r_regionkey) as c"})
                    .filter("c < 0"),
                JoinType::kInner,
                "")
            .project({"n_name", "c"})
            .build());
  }

  {
    SCOPED_TRACE("filter parts reading one input each");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_name "
            "FROM (SELECT count(*) AS c FROM region) s, nation "
            "WHERE contains(sequence(0, s.c), n_nationkey + 1)")),
        matchHiveScan("nation")
            .project({"n_name", "n_nationkey + 1 as k"})
            .nestedLoopJoin(
                matchHiveScan("region").singleAggregation(
                    {}, {"count(*) as c"}),
                JoinType::kInner,
                "contains(sequence(0, c), k)")
            .build());
  }

  {
    SCOPED_TRACE("filter part on a build already on the right");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_name FROM nation "
            "WHERE n_nationkey < (SELECT max(r_regionkey) FROM region) * 2")),
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region").singleAggregation(
                    {}, {"max(r_regionkey) as mx"}),
                JoinType::kInner,
                "n_nationkey < mx * 2")
            .build());
  }

  {
    SCOPED_TRACE("unknown size");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT element_at(s.v, n_nationkey + 1) "
            "FROM (SELECT array_agg(r_name) AS v FROM region) s, nation")),
        matchHiveScan("region")
            .singleAggregation({}, {"array_agg(r_name) as v"})
            .nestedLoopJoin(matchHiveScan("nation"), JoinType::kInner, "")
            .project({"element_at(v, n_nationkey + 1)"})
            .build());
  }

  {
    SCOPED_TRACE("both inputs single-row");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT s.c, q.m "
            "FROM (SELECT count(*) AS c FROM nation) s, "
            "(SELECT max(r_regionkey) AS m FROM region) q "
            "WHERE s.c < q.m * 2")),
        matchHiveScan("nation")
            .singleAggregation({}, {"count(*) as c"})
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"max(r_regionkey) as m"})
                    .project({"m", "m * 2 as m2"}),
                JoinType::kInner,
                "c < m2")
            .build());
  }

  {
    SCOPED_TRACE("keyed join");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_name "
            "FROM (SELECT max(r_regionkey) AS m FROM region) s "
            "JOIN nation ON n_regionkey = s.m")),
        matchHiveScan("region")
            .singleAggregation({}, {"max(r_regionkey) as m"})
            .hashJoinInner(
                matchHiveScan("nation"), {.keys = {{"m = n_regionkey"}}})
            .build());
  }

  {
    SCOPED_TRACE("two multi-row inputs");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT n_nationkey, r_regionkey FROM nation, region "
            "WHERE n_nationkey < r_regionkey * 2")),
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region").project(
                    {"r_regionkey", "r_regionkey * 2 as r2"}),
                JoinType::kInner,
                "n_nationkey < r2")
            .build());
  }

  {
    SCOPED_TRACE("preserved side of a LEFT join");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(
            "SELECT s.c, n_name "
            "FROM (SELECT count(*) AS c FROM region) s "
            "LEFT JOIN nation ON n_regionkey < s.c")),
        matchHiveScan("region")
            .singleAggregation({}, {"count(*) as c"})
            .nestedLoopJoin(
                matchHiveScan("nation"), JoinType::kLeft, "c > n_regionkey")
            .build());
  }

  // One BIGINT row is 8 bytes.
  const auto oneRow = parseSelect(
      "SELECT n_name, s.c FROM (SELECT count(*) AS c FROM region) s, nation");
  auto count = [] {
    return matchHiveScan("region").singleAggregation({}, {"count(*) as c"});
  };
  {
    SCOPED_TRACE("one BIGINT row, limit 0");
    optimizerOptions_.broadcastSizeLimit = 0;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(oneRow),
        count()
            .nestedLoopJoin(matchHiveScan("nation"), JoinType::kInner, "")
            .project({"n_name", "c"})
            .build());
  }
  {
    SCOPED_TRACE("one BIGINT row, limit 4");
    optimizerOptions_.broadcastSizeLimit = 4;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(oneRow),
        count()
            .nestedLoopJoin(matchHiveScan("nation"), JoinType::kInner, "")
            .project({"n_name", "c"})
            .build());
  }
  {
    SCOPED_TRACE("one BIGINT row, limit 8");
    optimizerOptions_.broadcastSizeLimit = 8;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(oneRow),
        matchHiveScan("nation")
            .nestedLoopJoin(count(), JoinType::kInner, "")
            .project({"n_name", "c"})
            .build());
  }

  // Four BIGINTs are 32 bytes.
  const auto fourColumns = parseSelect(
      "SELECT n_name, s.a, s.b, s.c, s.d "
      "FROM ("
      "  SELECT count(*) AS a, sum(r_regionkey) AS b, max(r_regionkey) AS c, "
      "    min(r_regionkey) AS d "
      "  FROM region"
      ") s, nation");
  auto aggregates = [] {
    return matchHiveScan("region").singleAggregation(
        {},
        {"count(*) as a",
         "sum(r_regionkey) as b",
         "max(r_regionkey) as c",
         "min(r_regionkey) as d"});
  };
  {
    SCOPED_TRACE("four BIGINT row, limit 16");
    optimizerOptions_.broadcastSizeLimit = 16;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(fourColumns),
        aggregates()
            .nestedLoopJoin(matchHiveScan("nation"), JoinType::kInner, "")
            .project({"n_name", "a", "b", "c", "d"})
            .build());
  }
  {
    SCOPED_TRACE("four BIGINT row, limit 32");
    optimizerOptions_.broadcastSizeLimit = 32;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(fourColumns),
        matchHiveScan("nation")
            .nestedLoopJoin(aggregates(), JoinType::kInner, "")
            .project({"n_name", "a", "b", "c", "d"})
            .build());
  }
}

// 12-way inner-join chain. Without the per-step restriction that keeps
// the optimizer on the SQL-written order, the recursion enumerates
// candidates and strategies at every level and the optimization does
// not finish within the timeout.
TEST_P(SyntacticJoinOrderTest, manyJoinsBoundedByOrder) {
  constexpr int32_t kNumTables = 12;

  for (int32_t i = 0; i < kNumTables; ++i) {
    const auto key = fmt::format("k{}", i);
    const auto rowType = ROW({key, fmt::format("v{}", i)}, BIGINT());
    testConnector_->addTable(fmt::format("t{}", i), rowType)
        ->setStats(1'000, {{key, {.numDistinct = 1'000}}});
  }

  auto context = makeContext();
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
  }
  worker.join();
  future.get();
}

AXIOM_INSTANTIATE_V1_V2(SyntacticJoinOrderTest);

} // namespace
} // namespace facebook::axiom::optimizer
