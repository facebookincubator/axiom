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
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationPlanTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    test::QueryTestBase::TearDown();
  }

  velox::core::PlanNodePtr planVelox(const lp::LogicalPlanNodePtr& plan) {
    auto distributedPlan = Optimization::toVeloxPlan(
                               *plan,
                               optimizerPool(),
                               {}, // optimizerOptions
                               {.numWorkers = 1, .numDrivers = 1})
                               .plan;

    VELOX_CHECK_EQ(1, distributedPlan->fragments().size());
    return distributedPlan->fragments().at(0).fragment.planNode;
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(AggregationPlanTest, dedupGroupingKeysAndAggregates) {
  testConnector_->addTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "numbers")
                           .project({"a + b as x", "a + b as y", "c"})
                           .aggregate({"x", "y"}, {"count(1)", "count(1)"})
                           .build();

    auto plan = planVelox(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project({"a + b"})
                       .singleAggregation({"x"}, {"count(1)"})
                       .project({"x", "x", "count", "count"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(AggregationPlanTest, duplicatesBetweenGroupAndAggregate) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  auto logicalPlan = lp::PlanBuilder{}
                         .tableScan(kTestConnectorId, "t")
                         .project({"a + b AS ab1", "a + b AS ab2"})
                         .aggregate({"ab1", "ab2"}, {"count(ab2) AS c1"})
                         .project({"ab1 AS x", "ab2 AS y", "c1 AS z"})
                         .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"plus(a, b)"})
                     .singleAggregation({"ab1"}, {"count(ab1)"})
                     .project({"ab1", "ab1", "c1"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationPlanTest, dedupMask) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
                         .aggregate(
                             {},
                             {"sum(a) FILTER (WHERE b > 0)",
                              "sum(a) FILTER (WHERE b < 0)",
                              "sum(a) FILTER (WHERE b > 0)"})
                         .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"b > 0 as m1", "a", "b < 0 as m2"})
                     .singleAggregation(
                         {},
                         {
                             "sum(a) FILTER (WHERE m1) as s1",
                             "sum(a) FILTER (WHERE m2) as s2",
                         })
                     .project({"s1", "s2", "s1"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationPlanTest, orderByDedup) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
                         .aggregate(
                             {},
                             {"array_agg(a ORDER BY a, a)",
                              "array_agg(b ORDER BY b, a, b, a)",
                              "array_agg(a ORDER BY a + b, a + b DESC, c)",
                              "array_agg(c ORDER BY b * 2, b * 2)"})
                         .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"a", "b", "a + b as p0", "c", "b * 2 as p1"})
                     .singleAggregation(
                         {},
                         {"array_agg(a ORDER BY a)",
                          "array_agg(b ORDER BY b, a)",
                          "array_agg(a ORDER BY p0, c)",
                          "array_agg(c ORDER BY p1)"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationPlanTest, dedupSameOptions) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan =
      lp::PlanBuilder(/*enableCoersions=*/true)
          .tableScan(kTestConnectorId, "t")
          .aggregate(
              {},
              {"array_agg(a ORDER BY a, a, a)",
               "array_agg(a ORDER BY a DESC)",
               "array_agg(a ORDER BY a, a)",
               "array_agg(a ORDER BY a)",
               "sum(a) FILTER (WHERE b > 0)",
               "sum(a) FILTER (WHERE b < 0)",
               "sum(a) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a DESC) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a) FILTER (WHERE b > 0)"})
          .build();

  auto plan = planVelox(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"a", "b > 0 as m1", "b < 0 as m2"})
          .singleAggregation(
              {},
              {"array_agg(a ORDER BY a) as agg1",
               "array_agg(a ORDER BY a DESC) as agg2",
               "sum(a) FILTER (WHERE m1) as sum1",
               "sum(a) FILTER (WHERE m2) as sum2",
               "array_agg(a ORDER BY a) FILTER (WHERE m1) as combo1",
               "array_agg(a ORDER BY a DESC) FILTER (WHERE m1) as combo2"})
          .project(
              {"agg1",
               "agg2",
               "agg1",
               "agg1",
               "sum1",
               "sum2",
               "sum1",
               "combo1",
               "combo2",
               "combo1"})
          .build();

  ASSERT_TRUE(matcher->match(plan));
}

// Verifies that aggregation with ORDER BY keys always uses single-step
// aggregation, even in distributed mode where partial+final would normally
// be used. This is required because partial aggregation cannot preserve
// global ordering across workers.
TEST_F(AggregationPlanTest, orderByEnforcesSingleStepInDistributedMode) {
  auto schema =
      ROW({"group_key", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"},
          {BIGINT(),
           BIGINT(),
           DOUBLE(),
           VARCHAR(),
           BIGINT(),
           DOUBLE(),
           VARCHAR(),
           BIGINT(),
           DOUBLE()});
  testConnector_->addTable("wide_table", schema);

  // 10k rows with only 2 distinct group_key values (0 and 1)
  constexpr int kNumRows = 10000;
  const std::string stringValue = "this is a long string";

  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row % 2; }),
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row; }),
      makeFlatVector<double>(kNumRows, [](auto row) { return row * 1.5; }),
      makeFlatVector<StringView>(
          kNumRows,
          [&stringValue](auto row) { return StringView(stringValue); }),
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row * 100; }),
      makeFlatVector<double>(kNumRows, [](auto row) { return row * 2.5; }),
      makeFlatVector<StringView>(
          kNumRows,
          [&stringValue](auto row) { return StringView(stringValue); }),
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row * 1000; }),
      makeFlatVector<double>(kNumRows, [](auto row) { return row * 3.5; }),
  });
  testConnector_->appendData("wide_table", rowVector);

  // Query with ORDER BY in aggregate - should use single aggregation step
  auto logicalPlanWithOrderBy =
      lp::PlanBuilder(/*enableCoercions=*/true)
          .tableScan(kTestConnectorId, "wide_table")
          .aggregate({"group_key"}, {"array_agg(v1 ORDER BY v2)", "sum(v1)"})
          .build();
  auto planWithOrderBy = test::QueryTestBase::planVelox(logicalPlanWithOrderBy);

  auto matcherWithOrderBy =
      core::PlanMatcherBuilder()
          .tableScan()
          .shuffle()
          .localPartition()
          .singleAggregation(
              {"group_key"}, {"array_agg(v1 ORDER BY v2)", "sum(v1)"})
          .shuffle()
          .build();

  AXIOM_ASSERT_DISTRIBUTED_PLAN(planWithOrderBy.plan, matcherWithOrderBy);

  // Query without ORDER BY - should use partial + final aggregation
  // Wide table to maximize per-row shuffle cost
  auto logicalPlanWithoutOrderBy =
      lp::PlanBuilder(/*enableCoercions=*/true)
          .tableScan(kTestConnectorId, "wide_table")
          .aggregate({"group_key"}, {"sum(v1)"})
          .build();
  auto planWithoutOrderBy =
      test::QueryTestBase::planVelox(logicalPlanWithoutOrderBy);

  auto matcherWithoutOrderBy =
      core::PlanMatcherBuilder()
          .tableScan()
          .partialAggregation({"group_key"}, {"sum(v1)"})
          .shuffle()
          .localPartition()
          .finalAggregation()
          .shuffle()
          .build();

  AXIOM_ASSERT_DISTRIBUTED_PLAN(planWithoutOrderBy.plan, matcherWithoutOrderBy);
  testConnector_->dropTableIfExists("wide_table");
}

} // namespace
} // namespace facebook::axiom::optimizer
