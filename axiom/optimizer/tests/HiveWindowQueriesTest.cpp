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
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveWindowQueriesTest : public test::HiveQueriesTestBase {
  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }
};

TEST_F(HiveWindowQueriesTest, basicRowNumber) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, manySameSpec) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)",
               "rank() over (partition by n_regionkey order by n_nationkey)",
               "dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)",
               "rank() over (partition by n_regionkey order by n_nationkey)",
               "dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, differentSpecs) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_nationkey)",
               "row_number() over (partition by n_regionkey order by n_nationkey)",
               "row_number() over (partition by n_regionkey order by n_nationkey desc)",
               "row_number() over (partition by n_name)",
               "lag(n_name) over (order by n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .window()
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey desc)"})
          .window({"count(*) over (partition by n_name)"})
          .window({"lag(n_name) over (order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, rowsFrameTypes) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 2 preceding and 1 following)",
               "count(*) over (partition by n_regionkey order by n_nationkey rows between current row and unbounded following)",
               "min(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 1 preceding and 1 following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 3 following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 2 preceding and 1 following)",
               "count(*) over (partition by n_regionkey order by n_nationkey rows between current row and unbounded following)",
               "min(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 1 preceding and 1 following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 3 following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, rangeFrameTypes) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, mixedFrameTypesAndBounds) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between unbounded preceding and current row)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 2 following)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between unbounded preceding and current row)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 2 following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, specsWithoutOrderBy) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey rows between unbounded preceding and unbounded following)",
               "count(*) over (partition by n_regionkey range between unbounded preceding and unbounded following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey rows between unbounded preceding and unbounded following)"})
          .window(
              {"count(*) over (partition by n_regionkey range between unbounded preceding and unbounded following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, multipleOrderByInSpec) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey, n_name)",
               "rank() over (order by n_regionkey, n_nationkey desc, n_name)",
               "dense_rank() over (partition by n_regionkey order by n_name, n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey, n_name)"})
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name)"})
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_name, n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, orderBy) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn",
               "rank() over (order by n_regionkey, n_nationkey desc, n_name) as rnk"})
          .orderBy({"rn desc"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .project()
                       .orderBy()
                       .window()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name)"})
          .orderBy({"rn desc"}, false)
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, orderByExprs) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name) as rnk"})
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .orderBy({"rnk + rn"})
          .project({"rnk + rn"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .project()
                       .orderBy()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name) as rnk"})
          .project({"rnk + rn as s"})
          .orderBy({"s"}, false)
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, aggregate) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name) as rnk",
               "row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .aggregate({"rnk"}, {"max(rn)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .project()
                       .singleAggregation()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name) as rnk"})
          .singleAggregation({"rnk"}, {"max(rn)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, filters) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn",
               "rank() over (partition by n_regionkey order by n_nationkey) as rnk"})
          .filter("(rn + rnk > 5) and (rn > 2 or n_regionkey < 3)")
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .filter()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn",
               "rank() over (partition by n_regionkey order by n_nationkey) as rnk"})
          .filter("(rn + rnk > 5) and (rn > 2 or n_regionkey < 3)")
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, joinOn) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"}).as("n1")
          .join(
              lp::PlanBuilder(context).tableScan("nation").window(
                  {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"}).as("n2"),
              "n1.rn = n2.rn",
              lp::JoinType::kInner)
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    std::cerr << plan->toString(true, true) << std::endl;
    // auto matcher = core::PlanMatcherBuilder()
    //                    .tableScan("nation")
    //                    .window()
    //                    .project()
    //                    .hashJoin(core::PlanMatcherBuilder()
    //                                  .tableScan("nation")
    //                                  .window()
    //                                  .project()
    //                                  .build())
    //                    .build();
    // ASSERT_TRUE(matcher->match(plan));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto referencePlan =
      exec::test::PlanBuilder(planNodeIdGenerator)
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn"})
          .hashJoin({"rn"}, {"rn2"}, exec::test::PlanBuilder(planNodeIdGenerator)
          .tableScan("nation", getSchema("nation"))
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey) as rn2"})
          .planNode(), "", {})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer
