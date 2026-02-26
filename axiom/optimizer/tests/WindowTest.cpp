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

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class WindowTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    testConnector_->addTpchTables();
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
    test::QueryTestBase::TearDown();
  }

  velox::core::PlanNodePtr toSingleNodePlan(
      std::string_view sql,
      int32_t numDrivers = 1) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);
    return QueryTestBase::toSingleNodePlan(logicalPlan, numDrivers);
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(WindowTest, singleWindowFunction) {
  // No extra columns to drop, so no final project.
  auto plan = toSingleNodePlan(
      "SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation");

  auto matcher = matchScan("nation")
                     .window({"row_number() OVER (ORDER BY n_name)"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowWithPartitionBy) {
  // Project drops unused columns (n_nationkey).
  auto plan = toSingleNodePlan(
      "SELECT n_name, n_regionkey, "
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name) as s "
      "FROM nation");

  auto matcher =
      matchScan("nation")
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name) as s"})
          .project({"n_name", "n_regionkey", "s"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, multipleWindowFunctionsSameSpec) {
  // Multiple window functions with the same partition/order spec should be
  // grouped into a single Window operator.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn, "
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name) as s "
      "FROM nation");

  auto matcher =
      matchScan("nation")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)",
               "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, differentPartitionKeys) {
  // Window functions with different partition keys produce separate Window
  // operators.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn, "
      "sum(n_nationkey) OVER (ORDER BY n_name) as s "
      "FROM nation");

  // First project drops n_regionkey (not needed by second window).
  // Second project drops n_nationkey (not in SELECT).
  auto matcher =
      matchScan("nation")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .project({"n_nationkey", "n_name", "rn"})
          .window({"sum(n_nationkey) OVER (ORDER BY n_name)"})
          .project({"n_name", "rn", "s"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, differentOrderTypes) {
  // Window functions with same order keys but different sort orders (ASC vs
  // DESC) must produce separate Window operators.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name ASC) as rn, "
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name DESC) as s "
      "FROM nation");

  auto matcher =
      matchScan("nation")
          .window(
              {"row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name ASC)"})
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name DESC)"})
          .project({"n_name", "rn", "s"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowWithFrameBounds) {
  // Precompute projection materializes frame bound constant.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "sum(n_nationkey) OVER ("
      "  PARTITION BY n_regionkey ORDER BY n_name "
      "  ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"
      ") as s "
      "FROM nation");

  // Precompute project materializes the constant frame bounds. Use aliases to
  // capture the auto-generated column names for symbol propagation.
  auto matcher =
      matchScan("nation")
          .project({
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "1 as one",
          })
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name "
               "ROWS BETWEEN one PRECEDING AND one FOLLOWING)"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowWithExpressionFrameBounds) {
  // Precompute projection materializes expression frame bounds.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "sum(n_nationkey) OVER ("
      "  PARTITION BY n_regionkey ORDER BY n_name "
      "  ROWS BETWEEN n_regionkey PRECEDING AND n_regionkey + 1 FOLLOWING"
      ") as s "
      "FROM nation");

  // Precompute project materializes expression frame bounds. Use aliases to
  // capture the auto-generated column names for symbol propagation.
  auto matcher =
      matchScan("nation")
          .project({
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_regionkey + 1 as frameEnd",
          })
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name "
               "ROWS BETWEEN n_regionkey PRECEDING AND frameEnd FOLLOWING)"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowWithExpressionInputs) {
  // All inputs to window function are expressions: function args, partition
  // keys, sorting keys, frame start and end. Partition key and frame end share
  // the expression n_regionkey + 1 which is computed once.
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "sum(n_nationkey * 2) OVER ("
      "  PARTITION BY n_regionkey + 1 ORDER BY upper(n_name) "
      "  ROWS BETWEEN n_regionkey PRECEDING AND n_regionkey + 1 FOLLOWING"
      ") as s "
      "FROM nation");

  // Precompute project has 6 outputs: 3 pass-through + 3 computed. The
  // expression n_regionkey + 1 appears once (shared by partition key and frame
  // end). Use aliases to capture auto-generated names for symbol propagation.
  auto matcher =
      matchScan("nation")
          .project({
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_nationkey * 2 as sumArg",
              "n_regionkey + 1 as partKey",
              "upper(n_name) as orderKey",
          })
          .window({"sum(sumArg) OVER (PARTITION BY partKey ORDER BY orderKey "
                   "ROWS BETWEEN n_regionkey PRECEDING AND partKey FOLLOWING)"})
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowAfterFilter) {
  auto plan = toSingleNodePlan(
      "SELECT n_name, "
      "row_number() OVER (ORDER BY n_name) as rn "
      "FROM nation WHERE n_regionkey = 1");

  // Project drops n_regionkey after filter (not needed by the window).
  auto matcher = matchScan("nation")
                     .filter("n_regionkey = 1")
                     .project({"n_name"})
                     .window({"row_number() OVER (ORDER BY n_name)"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, filterOnWindowOutput) {
  // Filter on window output forces a DT boundary.
  auto plan = toSingleNodePlan(
      "SELECT * FROM ("
      "  SELECT n_name, row_number() OVER (ORDER BY n_name) as rn "
      "  FROM nation"
      ") WHERE rn <= 5");

  auto matcher = matchScan("nation")
                     .window({"row_number() OVER (ORDER BY n_name) as rn"})
                     .filter("rn <= 5")
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowOnWindowInArgs) {
  // Window function that references another window function's output in its
  // args forces a DT boundary.
  auto plan = toSingleNodePlan(
      "SELECT n_name, s FROM ("
      "  SELECT *, "
      "  sum(rn) OVER (ORDER BY n_name) as s "
      "  FROM ("
      "    SELECT n_name, "
      "    row_number() OVER (ORDER BY n_name) as rn "
      "    FROM nation"
      "  )"
      ")");

  // Two DTs: first computes row_number, second computes sum over rn.
  auto matcher = matchScan("nation")
                     .window({"row_number() OVER (ORDER BY n_name)"})
                     .window({"sum(rn) OVER (ORDER BY n_name) as s"})
                     .project({"n_name", "s"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, windowOnWindowInFrameBounds) {
  // Window function that references another window function's output in its
  // frame bounds forces a DT boundary.
  auto plan = toSingleNodePlan(
      "SELECT n_name, s FROM ("
      "  SELECT *, "
      "  sum(n_nationkey) OVER ("
      "    ORDER BY n_name "
      "    ROWS BETWEEN rn PRECEDING AND CURRENT ROW"
      "  ) as s "
      "  FROM ("
      "    SELECT n_name, n_nationkey, "
      "    row_number() OVER (ORDER BY n_name) as rn "
      "    FROM nation"
      "  )"
      ")");

  // Two DTs: first computes row_number, second computes sum with frame bound.
  auto matcher =
      matchScan("nation")
          .window({"row_number() OVER (ORDER BY n_name) as rn"})
          .project({"n_name", "n_nationkey", "rn"})
          .window({"sum(n_nationkey) OVER (ORDER BY n_name "
                   "ROWS BETWEEN rn PRECEDING AND CURRENT ROW) as s"})
          .project({"n_name", "s"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(WindowTest, distributed) {
  // Distributed plan should have repartitioning for window.
  auto logicalPlan = parseSelect(
      "SELECT n_name, "
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name) as s "
      "FROM nation",
      kTestConnectorId);

  auto distributedPlan = planVelox(logicalPlan);

  auto matcher =
      matchScan("nation")
          .shuffle({"n_regionkey"})
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
}

TEST_F(WindowTest, distributedMultipleShuffles) {
  // Window functions with different partition keys require separate shuffles.
  auto logicalPlan = parseSelect(
      "SELECT n_name, "
      "sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name) as s, "
      "row_number() OVER (PARTITION BY n_nationkey ORDER BY n_name) as rn "
      "FROM nation",
      kTestConnectorId);

  auto distributedPlan = planVelox(logicalPlan);

  auto matcher =
      matchScan("nation")
          .shuffle({"n_nationkey"})
          .window(
              {"row_number() OVER (PARTITION BY n_nationkey ORDER BY n_name)"})
          .shuffle({"n_regionkey"})
          .window(
              {"sum(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_name)"})
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
