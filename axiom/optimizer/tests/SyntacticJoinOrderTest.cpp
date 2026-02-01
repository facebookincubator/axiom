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

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

DECLARE_double(tpch_scale);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class SyntacticJoinOrderTest : public test::QueryTestBase {
 protected:
  static void SetUpTestCase() {
    gOldTpchScale = FLAGS_tpch_scale;
    FLAGS_tpch_scale = 0.001;
    test::QueryTestBase::SetUpTestCase();

    gTempDirectory = velox::exec::test::TempDirectoryPath::create();

    auto path = gTempDirectory->getPath();
    test::ParquetTpchTest::createTables(path);

    LocalRunnerTestBase::localDataPath_ = path;
    LocalRunnerTestBase::localFileFormat_ =
        velox::dwio::common::FileFormat::PARQUET;
  }

  static void TearDownTestCase() {
    gTempDirectory.reset();
    test::QueryTestBase::TearDownTestCase();
    FLAGS_tpch_scale = gOldTpchScale;
  }

  lp::LogicalPlanNodePtr parseSql(const std::string& sql) const {
    ::axiom::sql::presto::PrestoParser parser{
        exec::test::kHiveConnectorId, std::nullopt};
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isSelect());

    return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
  }

  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;

  inline static double gOldTpchScale{};
};

TEST_F(SyntacticJoinOrderTest, innerJoins) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);

  optimizerOptions_.sampleJoins = false;

  auto startMatcher = [](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // Cardinalities after filters:
  //  - lineitem - 32.3K
  //  - orders - 7.2K
  //  - customer - 337

  // Optimized join order: lineitem x (orders x customer).
  auto optimizedMatcher =
      startMatcher("lineitem")
          .hashJoin(startMatcher("orders")
                        .hashJoin(startMatcher("customer").build())
                        .build())
          .aggregation()
          .build();

  // Reference Velox plan.
  auto* metadata =
      connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto startReferencePlan = [&](const auto& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, metadata->findTable(tableName)->type());
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

  struct Test {
    std::vector<std::string> order;
    std::function<std::shared_ptr<core::PlanMatcher>(
        const std::string&,
        const std::string&,
        const std::string&)>
        createMatcher;
  };

  // Syntactic join order: (a x b) x c.
  {
    auto createHashHash = [&](const std::string& t0,
                              const std::string& t1,
                              const std::string& t2) {
      return startMatcher(t0)
          .hashJoin(startMatcher(t1).build())
          .hashJoin(startMatcher(t2).build())
          .aggregation()
          .build();
    };

    auto createNestedHash = [&](const std::string& t0,
                                const std::string& t1,
                                const std::string& t2) {
      return startMatcher(t0)
          .nestedLoopJoin(startMatcher(t1).build())
          .hashJoin(startMatcher(t2).build())
          .aggregation()
          .build();
    };

    std::vector<Test> testOrders = {
        {{"orders", "customer", "lineitem"}, createHashHash},
        {{"customer", "orders", "lineitem"}, createHashHash},
        {{"orders", "lineitem", "customer"}, createHashHash},
        {{"lineitem", "orders", "customer"}, createHashHash},
        {{"customer", "lineitem", "orders"}, createNestedHash},
        {{"lineitem", "customer", "orders"}, createNestedHash},
    };

    for (const auto& [order, createMatcher] : testOrders) {
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
        optimizerOptions_.joinOrder = JoinOrder::kCost;
        auto plan = toSingleNodePlan(logicalPlan);
        AXIOM_ASSERT_PLAN(plan, optimizedMatcher);
      }

      {
        optimizerOptions_.joinOrder = JoinOrder::kSyntactic;
        auto plan = toSingleNodePlan(logicalPlan);

        auto matcher = createMatcher(order[0], order[1], order[2]);
        AXIOM_ASSERT_PLAN(plan, matcher);

        checkSame(logicalPlan, referenceResults);
      }
    }
  }

  // Syntactic join order: a x (b x c).
  {
    auto createHashHash = [&](const std::string& t0,
                              const std::string& t1,
                              const std::string& t2) {
      return startMatcher(t0)
          .hashJoin(startMatcher(t1).hashJoin(startMatcher(t2).build()).build())
          .aggregation()
          .build();
    };

    auto createNestedHash = [&](const std::string& t0,
                                const std::string& t1,
                                const std::string& t2) {
      return startMatcher(t0)
          .hashJoin(
              startMatcher(t1).nestedLoopJoin(startMatcher(t2).build()).build())
          .aggregation()
          .build();
    };

    std::vector<Test> testOrders = {
        {{"lineitem", "customer", "orders"}, createHashHash},
        {{"lineitem", "orders", "customer"}, createHashHash},
        {{"customer", "lineitem", "orders"}, createHashHash},
        {{"customer", "orders", "lineitem"}, createHashHash},
        {{"orders", "lineitem", "customer"}, createNestedHash},
        {{"orders", "customer", "lineitem"}, createNestedHash},
    };

    for (const auto& [order, createMatcher] : testOrders) {
      SCOPED_TRACE(folly::join(", ", order));

      auto logicalPlan =
          lp::PlanBuilder(context)
              .tableScan(order[0])
              .crossJoin(
                  lp::PlanBuilder(context).tableScan(order[1]).crossJoin(
                      lp::PlanBuilder(context).tableScan(order[2])))
              .filter("c_mktsegment = 'BUILDING'")
              .filter("c_custkey = o_custkey")
              .filter("l_orderkey = o_orderkey")
              .filter("o_orderdate < date '1995-03-15'")
              .filter("l_shipdate > date '1995-03-15'")
              .aggregate({}, {"count(1)"})
              .build();

      {
        optimizerOptions_.joinOrder = JoinOrder::kCost;
        auto plan = toSingleNodePlan(logicalPlan);
        AXIOM_ASSERT_PLAN(plan, optimizedMatcher);
      }

      {
        optimizerOptions_.joinOrder = JoinOrder::kSyntactic;
        auto plan = toSingleNodePlan(logicalPlan);

        auto matcher = createMatcher(order[0], order[1], order[2]);
        AXIOM_ASSERT_PLAN(plan, matcher);

        checkSame(logicalPlan, referenceResults);
      }
    }
  }
}

TEST_F(SyntacticJoinOrderTest, outerJoins) {
  optimizerOptions_.sampleJoins = false;

  auto startMatcher = [](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // Optimized join order: lineitem x orders.
  auto optimizedMatcher =
      startMatcher("lineitem")
          .hashJoin(startMatcher("orders").build(), core::JoinType::kLeft)
          .aggregation()
          .build();

  // Reference Velox plan.
  auto* metadata =
      connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto startReferencePlan = [&](const auto& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, metadata->findTable(tableName)->type());
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
    auto logicalPlan = parseSql(
        "SELECT count(*) FROM lineitem LEFT JOIN orders ON "
        "l_orderkey = o_orderkey and l_returnflag = 'R'");

    {
      optimizerOptions_.joinOrder = JoinOrder::kCost;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }

    {
      optimizerOptions_.joinOrder = JoinOrder::kSyntactic;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }
  }

  // Syntactic join order: b RIGHT JOIN a.
  {
    auto logicalPlan = parseSql(
        "SELECT count(*) FROM orders RIGHT JOIN lineitem ON "
        "l_orderkey = o_orderkey and l_returnflag = 'R'");

    {
      optimizerOptions_.joinOrder = JoinOrder::kCost;
      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, optimizedMatcher);

      checkSame(logicalPlan, referenceResults);
    }

    {
      optimizerOptions_.joinOrder = JoinOrder::kSyntactic;
      auto plan = toSingleNodePlan(logicalPlan);

      auto matcher =
          startMatcher("orders")
              .hashJoin(
                  startMatcher("lineitem").build(), core::JoinType::kRight)
              .aggregation()
              .build();
      AXIOM_ASSERT_PLAN(plan, matcher);

      checkSame(logicalPlan, referenceResults);
    }
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
