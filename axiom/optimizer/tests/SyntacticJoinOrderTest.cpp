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

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class SyntacticJoinOrderTest : public test::QueryTestBase {
 protected:
  static void SetUpTestCase() {
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
  }

  lp::LogicalPlanNodePtr parseSql(const std::string& sql) const {
    ::axiom::sql::presto::PrestoParser parser{
        exec::test::kHiveConnectorId, &optimizerPool()};
    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isSelect());

    return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
  }

  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;
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

        auto matcher = startMatcher(order[0])
                           .hashJoin(startMatcher(order[1]).build())
                           .hashJoin(startMatcher(order[2]).build())
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
            startMatcher(order[0])
                .hashJoin(startMatcher(order[1])
                              .hashJoin(startMatcher(order[2]).build())
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
    auto logicalPlan = parseSql(
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
