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
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationPlanTest : public testing::Test {
 protected:
  static constexpr auto kTestConnectorId = "test";

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void SetUp() override {
    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    rootPool_ = memory::memoryManager()->addRootPool("root");
    optimizerPool_ = rootPool_->addLeafChild("optimizer");
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);
  }

  velox::core::PlanNodePtr planVelox(const lp::LogicalPlanNodePtr& plan) {
    auto distributedPlan = Optimization::toVeloxPlan(
                               *plan,
                               *optimizerPool_,
                               {}, // optimizerOptions
                               {.numWorkers = 1, .numDrivers = 1})
                               .plan;

    VELOX_CHECK_EQ(1, distributedPlan->fragments().size());
    return distributedPlan->fragments().at(0).fragment.planNode;
  }

  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
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

TEST_F(AggregationPlanTest, ignoreDuplicates) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BOOLEAN(), BOOLEAN(), BIGINT()}));

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
                         .aggregate(
                             {},
                             {"bool_and(DISTINCT a)",
                              "bool_or(DISTINCT b)",
                              "bool_and(a)",
                              "bool_or(DISTINCT a)",
                              "bool_and(DISTINCT a) FILTER (WHERE c > 0)",
                              "bool_or(DISTINCT a) FILTER (WHERE c < 0)"})
                         .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"a", "b", "c > 0 as m1", "c < 0 as m2"})
                     .singleAggregation(
                         {},
                         {"bool_and(a) as agg1",
                          "bool_or(b) as agg2",
                          "bool_or(a) as agg3",
                          "bool_and(a) FILTER (WHERE m1) as agg4",
                          "bool_or(a) FILTER (WHERE m2) as agg5"})
                     .project({"agg1", "agg2", "agg1", "agg3", "agg4", "agg5"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationPlanTest, orderNonSensitive) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
                         .aggregate(
                             {},
                             {"sum(a ORDER BY b)",
                              "sum(a ORDER BY a DESC, b)",
                              "count(b ORDER BY a)",
                              "sum(a ORDER BY b) FILTER (WHERE c > 0)",
                              "count(b ORDER BY a) FILTER (WHERE c < 0)"})
                         .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"a", "b", "c > 0 as m1", "c < 0 as m2"})
                     .singleAggregation(
                         {},
                         {"sum(a) as sum1",
                          "count(b) as count1",
                          "sum(a) FILTER (WHERE m1) as sum2",
                          "count(b) FILTER (WHERE m2) as count2"})
                     .project({"sum1", "sum1", "count1", "sum2", "count2"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationPlanTest, ignoreDuplicatesXOrderNonSensitive) {
  testConnector_->addTable(
      "t",
      ROW({"a", "b", "c", "d"}, {BOOLEAN(), BIGINT(), BIGINT(), BIGINT()}));

  auto logicalPlan =
      lp::PlanBuilder(/*enableCoersions=*/true)
          .tableScan(kTestConnectorId, "t")
          .aggregate(
              {},
              {"bool_and(DISTINCT a ORDER BY b)",
               "bool_or(DISTINCT a ORDER BY b DESC, c)",
               "sum(DISTINCT b ORDER BY c)",
               "bool_and(a ORDER BY b)",
               "bool_and(DISTINCT a ORDER BY b) FILTER (WHERE d > 0)",
               "sum(DISTINCT b ORDER BY c) FILTER (WHERE d < 0)"})
          .build();

  auto plan = planVelox(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"a", "b", "d > 0 as m1", "d < 0 as m2"})
                     .singleAggregation(
                         {},
                         {"bool_and(a) as agg1",
                          "bool_or(a) as agg2",
                          "sum(b) as agg3",
                          "bool_and(a) FILTER (WHERE m1) as agg4",
                          "sum(b) FILTER (WHERE m2) as agg5"})
                     .project({"agg1", "agg2", "agg3", "agg1", "agg4", "agg5"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

} // namespace
} // namespace facebook::axiom::optimizer
