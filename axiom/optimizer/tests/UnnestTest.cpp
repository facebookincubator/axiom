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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class UnnestTest : public test::QueryTestBase {
 public:
  void SetUp() override {
    test::QueryTestBase::SetUp();

    rowVector_ = makeRowVector(
        {"x", "a_a_y", "a_a_z"},
        {
            makeFlatVector<int64_t>({
                7,
                10,
                8,
                9,
                10,
            }),
            makeNestedArrayVectorFromJson<int64_t>({
                "[[10, 20, 30], [100, 200, 300]]",
                "[[1, 3, 2], [1, 3, 2]]",
                "[[100, 200, 300], [10, 20, 30]]",
                "[[0, 0, 0], [0, 0, 0]]",
                "[[1, 3, 2], [1, 3, 2]]",
            }),
            makeNestedArrayVectorFromJson<int64_t>({
                "[[10, 30], [100, 300]]",
                "[[2, 1], [1, 2]]",
                "[[100, 300], [10, 30]]",
                "[[0, 0], [0, 0]]",
                "[[2, 1], [1, 2]]",
            }),
        });
  }

  void TearDown() override {
    rowVector_.reset();
    test::QueryTestBase::TearDown();
  }

  velox::core::PlanNodePtr toSingleThreadPlan(
      const logical_plan::LogicalPlanNodePtr& logicalPlan) {
    schema_ = std::make_shared<SchemaResolver>();

    auto plan = planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = 1}).plan;

    EXPECT_EQ(1, plan->fragments().size());
    return plan->fragments().at(0).fragment.planNode;
  }

  void checkSame(
      const logical_plan::LogicalPlanNodePtr& planNode,
      const velox::core::PlanNodePtr& referencePlan) {
    VELOX_CHECK_NOT_NULL(planNode);
    VELOX_CHECK_NOT_NULL(referencePlan);

    const axiom::runner::MultiFragmentPlan::Options options{
        .numWorkers = 4,
        .numDrivers = 4,
    };

    // Distributed plan.
    auto fragmentedPlan = planVelox(planNode, options);
    auto referenceResult = assertSame(referencePlan, fragmentedPlan);

    // Single-node multi-threaded plan.
    auto singleNodePlan = planVelox(
        planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
    auto singleNodeResult = runFragmentedPlan(singleNodePlan);

    velox::exec::test::assertEqualResults(
        referenceResult.results, singleNodeResult.results);

    // Single-node single-threaded plan.
    auto singleThreadPlan =
        planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
    auto singleThreadResult = runFragmentedPlan(singleThreadPlan);

    velox::exec::test::assertEqualResults(
        referenceResult.results, singleThreadResult.results);
  }

  RowVectorPtr rowVector_;
};

// We test the following cases:
//  If something is after unnest it can depend and not depend on unnested
//  columns. We also check that any expressions are allowed inside unnest, not
//  only input column references.
// - unnest
// - unnest after unnest
// - there's no extra columns in projections before unnest
// - project before and after unnest
//
// ---- after this we start to use project to simplify plans ----
// - filter before and after unnest
// - group by before and after unnest
// - order by before and after unnest
// - limit before and after unnest
// - join before and after unnest

TEST_F(UnnestTest, unnest) {
  {
    SCOPED_TRACE("unnest");

    auto logicalPlan = lp::PlanBuilder{}
                           .values({rowVector_})
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder{}.values().project().unnest().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan =
        exec::test::PlanBuilder{}
            .values({rowVector_})
            .project({
                "x",
                "a_a_y",
                "a_a_z",
                "array_distinct(a_a_y) AS a_a_y_d",
                "array_distinct(a_a_z) AS a_a_z_d",
            })
            .unnest({"x", "a_a_y", "a_a_z"}, {"a_a_y_d", "a_a_z_d"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("unnest after unnest");

    auto logicalPlan = lp::PlanBuilder{}
                           .values({rowVector_})
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .unnest({
                               lp::Sql("array_distinct(a_y)").unnestAs("y"),
                               lp::Sql("array_distinct(a_z)").unnestAs("z"),
                           })
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .unnest()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan =
        exec::test::PlanBuilder{}
            .values({rowVector_})
            .project({
                "x",
                "a_a_y",
                "a_a_z",
                "array_distinct(a_a_y) AS a_a_y_d",
                "array_distinct(a_a_z) AS a_a_z_d",
            })
            .unnest({"x", "a_a_y", "a_a_z"}, {"a_a_y_d", "a_a_z_d"})
            .project({
                "x",
                "a_a_y",
                "a_a_z",
                "a_a_y_d_e",
                "a_a_z_d_e",
                "array_distinct(a_a_y_d_e) AS a_y_d",
                "array_distinct(a_a_z_d_e) AS a_z_d",
            })
            .unnest(
                {"x", "a_a_y", "a_a_z", "a_a_y_d_e", "a_a_z_d_e"},
                {"a_y_d", "a_z_d"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("no extra columns in projections before unnest");

    const std::vector<std::string> expectedNames{"x", "y"};

    auto logicalPlan = lp::PlanBuilder{}
                           .values({rowVector_})
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .unnest({
                               lp::Sql("array_distinct(a_y)").unnestAs("y"),
                               lp::Sql("array_distinct(a_z)").unnestAs("z"),
                           })
                           .project(expectedNames)
                           .build();
    auto plan = toSingleThreadPlan(logicalPlan);

    // Names are auto-generated by the optimizer.
    auto matcher =
        core::PlanMatcherBuilder()
            .values()
            .project({"x", "array_distinct(a_a_y)", "array_distinct(a_a_z)"})
            .unnest({"x"}, {"__r3", "__r4"})
            .project({"x", "array_distinct(a_y)", "array_distinct(a_z)"})
            .unnest({"x"}, {"__r3", "__r4"})
            .project(expectedNames)
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);
    ASSERT_EQ(plan->outputType()->names(), expectedNames);
  }
  {
    SCOPED_TRACE("unnest without replicated columns");

    auto logicalPlan = lp::PlanBuilder{}
                           .values({rowVector_})
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                           })
                           .project({"a_y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder{}.values().project().unnest().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = exec::test::PlanBuilder{}
                             .values({rowVector_})
                             .project({"array_distinct(a_a_y) AS a_a_y_d"})
                             .unnest({}, {"a_a_y_d"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("unnest constant array");

    auto logicalPlan = lp::PlanBuilder{}
                           .unnest({
                               lp::Sql("array[1, 2, 3]").unnestAs("e"),
                           })
                           .project({"e"})
                           .build();

    auto referencePlan = exec::test::PlanBuilder{pool_.get()}
                             .values({makeRowVector(ROW({}), 1)})
                             .project({
                                 "array[1, 2, 3] AS a",
                             })
                             .unnest({}, {"a"})
                             .planNode();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder().values().project().unnest().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("unnest array and map");

    auto logicalPlan = lp::PlanBuilder{}
                           .unnest({
                               lp::Sql("array[1, 2, 3]").unnestAs("e"),
                               lp::Sql("map(array['1', '2'], array[10, 20])")
                                   .unnestAs("k", "v"),
                           })
                           .project({"v", "e"})
                           .build();

    auto referencePlan = exec::test::PlanBuilder{pool_.get()}
                             .values({makeRowVector(ROW({}), 1)})
                             .project({
                                 "array[1, 2, 3] AS a",
                                 "map(array['1', '2'], array[10, 20]) AS m",
                             })
                             .unnest({}, {"a", "m"})
                             .project({
                                 "m_v AS v",
                                 "a_e AS e",
                             })
                             .planNode();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .values()
                       .project()
                       .unnest()
                       .project({"v", "e"})
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, project) {
  auto startLogicalPlan = [&]() {
    return lp::PlanBuilder{}.values({rowVector_});
  };

  auto startReferencePlan = [&]() {
    return exec::test::PlanBuilder{}.values({rowVector_});
  };

  {
    SCOPED_TRACE("project before unnest");

    auto logicalPlan = startLogicalPlan()
                           .project({
                               "x + 1 AS x1",
                               "array_distinct(a_a_y) AS a_a_y_d",
                               "array_distinct(a_a_z) AS a_a_z_d",
                           })
                           .unnest({
                               lp::Col("a_a_y_d").unnestAs("a_y"),
                               lp::Col("a_a_z_d").unnestAs("a_z"),
                           })
                           .build();

    // TODO We probably want pushdown projection closer to data source.
    // Because compared to other joins, unnest only increase work.
    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan =
        startReferencePlan()
            .project({
                "x + 1 AS x1",
                "array_distinct(a_a_y) AS a_a_y_d",
                "array_distinct(a_a_z) AS a_a_z_d",
            })
            .unnest({"x1", "a_a_y_d", "a_a_z_d"}, {"a_a_y_d", "a_a_z_d"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("project after unnest (independent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x + 1 AS x1", "a_y"})
                           .build();

    // TODO We probably want pushdown projection closer to data source.
    // Because compared to other joins, unnest only increase work.
    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .project({"x + 1 AS x1", "a_a_y_d_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("project after unnest (dependent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "array_distinct(a_y) AS a_y_d"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan =
        startReferencePlan()
            .project({
                "x",
                "array_distinct(a_a_y) AS a_a_y_d",
                "array_distinct(a_a_z) AS a_a_z_d",
            })
            .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
            .project({"x", "array_distinct(a_a_y_d_e) AS a_y_d"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, filter) {
  auto startLogicalPlan = [&]() {
    return lp::PlanBuilder{}.values({rowVector_});
  };

  auto startReferencePlan = [&]() {
    return exec::test::PlanBuilder{}.values({rowVector_});
  };

  {
    SCOPED_TRACE("filter before unnest");

    auto logicalPlan = startLogicalPlan()
                           .filter("x % 2 = 0")
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .filter()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .filter("x % 2 = 0")
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .project({"x", "a_a_y_d_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("filter after unnest (independent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .filter("x % 2 = 0")
                           .project({"x", "a_y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .filter()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .filter("x % 2 = 0")
                             .project({"x", "a_a_y_d_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("filter after unnest (dependent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .unnest({
                               lp::Sql("array_distinct(a_y)").unnestAs("y"),
                               lp::Sql("array_distinct(a_z)").unnestAs("z"),
                           })
                           .filter("y % 2 = 0")
                           .project({"x", "y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .unnest()
                       .filter()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .project({
                                 "x",
                                 "array_distinct(a_a_y_d_e) AS a_y",
                                 "array_distinct(a_a_z_d_e) AS a_z",
                             })
                             .unnest({"x"}, {"a_y", "a_z"})
                             .filter("a_y_e % 2 = 0")
                             .project({"x", "a_y_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("filter between unnest (independent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .filter("x % 2 = 0")
                           .unnest({
                               lp::Sql("array_distinct(a_y)").unnestAs("y"),
                               lp::Sql("array_distinct(a_z)").unnestAs("z"),
                           })
                           .project({"x", "y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .filter()
                       .project()
                       .unnest()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .project({
                                 "x",
                                 "array_distinct(a_a_y_d_e) AS a_y",
                                 "array_distinct(a_a_z_d_e) AS a_z",
                             })
                             .unnest({"x"}, {"a_y", "a_z"})
                             .filter("x % 2 = 0")
                             .project({"x", "a_y_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("filter between unnest (dependent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .filter("cardinality(a_z) % 2 = 0")
                           .unnest({
                               lp::Sql("array_distinct(a_y)").unnestAs("y"),
                               lp::Sql("array_distinct(a_z)").unnestAs("z"),
                           })
                           .project({"x", "y"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .filter()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .filter("cardinality(a_a_z_d_e) % 2 = 0")
                             .project({
                                 "x",
                                 "array_distinct(a_a_y_d_e) AS a_y",
                                 "array_distinct(a_a_z_d_e) AS a_z",
                             })
                             .unnest({"x"}, {"a_y", "a_z"})
                             .project({"x", "a_y_e"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, groupBy) {
  const auto names = rowVector_->rowType()->names();

  auto startLogicalPlan = [&]() {
    return lp::PlanBuilder{}.values({rowVector_});
  };

  auto startReferencePlan = [&]() {
    return exec::test::PlanBuilder{}.values({rowVector_});
  };

  {
    SCOPED_TRACE("group by before unnest");

    auto logicalPlan = startLogicalPlan()
                           .aggregate(names, {})
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .singleAggregation()
                       .project()
                       .unnest()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .singleAggregation(names, {})
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("group by after unnest");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .aggregate({"x", "a_y", "a_z"}, {})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .singleAggregation()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan =
        startReferencePlan()
            .project({
                "x",
                "array_distinct(a_a_y) AS a_a_y_d",
                "array_distinct(a_a_z) AS a_a_z_d",
            })
            .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
            .singleAggregation({"x", "a_a_y_d_e", "a_a_z_d_e"}, {})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, orderBy) {
  const std::vector<std::string> names{"x", "a_a_y", "a_a_z"};

  auto startLogicalPlan = [&]() {
    return lp::PlanBuilder{}.values({rowVector_});
  };

  auto startReferencePlan = [&]() {
    return exec::test::PlanBuilder{}.values({rowVector_});
  };

  {
    SCOPED_TRACE("order by before unnest");

    auto logicalPlan = startLogicalPlan()
                           .orderBy(names)
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .orderBy()
                       .project()
                       .unnest()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .orderBy(names, {})
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("order by after unnest (independent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .orderBy({"x"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .orderBy()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .orderBy({"x"}, {})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("order by after unnest (dependent on unnested columns)");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .orderBy({"x", "a_y", "a_z"})
                           .build();

    // TODO We probably want pushdown orderBy closer to data source.
    // Because compared to other joins, unnest only increase work.
    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .orderBy()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .orderBy({"x", "a_a_y_d_e", "a_a_z_d_e"}, {})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, limit) {
  auto startLogicalPlan = [&]() {
    return lp::PlanBuilder{}.values({rowVector_});
  };

  auto startReferencePlan = [&]() {
    return exec::test::PlanBuilder{}.values({rowVector_});
  };

  {
    SCOPED_TRACE("limit before unnest");

    auto logicalPlan = startLogicalPlan()
                           .limit(1, 1)
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder{}.values().limit().project().unnest().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .limit(1, 1, {})
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("limit after unnest");

    auto logicalPlan = startLogicalPlan()
                           .unnest({
                               lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                               lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                           })
                           .project({"x", "a_y", "a_z"})
                           .limit(1, 1)
                           .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder{}.values().project().unnest().limit().build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);

    auto referencePlan = startReferencePlan()
                             .project({
                                 "x",
                                 "array_distinct(a_a_y) AS a_a_y_d",
                                 "array_distinct(a_a_z) AS a_a_z_d",
                             })
                             .unnest({"x"}, {"a_a_y_d", "a_a_z_d"})
                             .limit(1, 1, {})
                             .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

TEST_F(UnnestTest, join) {
  auto startLogicalPlan = [&](lp::PlanBuilder::Context& ctx) {
    return lp::PlanBuilder{ctx}.values({rowVector_});
  };

  auto startReferencePlan = [&](auto& planNodeIdGenerator) {
    return exec::test::PlanBuilder{planNodeIdGenerator}.values({rowVector_});
  };

  {
    SCOPED_TRACE("join before unnest (independent on unnested columns)");

    const std::vector<std::string> expectedNames{"x1", "a_y1", "a_z2"};

    lp::PlanBuilder::Context ctx;
    auto logicalPlan =
        startLogicalPlan(ctx)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .join(
                startLogicalPlan(ctx).project(
                    {"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"}),
                "x1 = x2",
                lp::JoinType::kInner)
            .unnest({
                lp::Sql("array_distinct(a_a_y1)").unnestAs("a_y1"),
                lp::Sql("array_distinct(a_a_z1)").unnestAs("a_z1"),
            })
            .unnest({
                lp::Sql("array_distinct(a_a_y2)").unnestAs("a_y2"),
                lp::Sql("array_distinct(a_a_z2)").unnestAs("a_z2"),
            })
            .project(expectedNames)
            .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .hashJoin(core::PlanMatcherBuilder{}.values().build())
                       .project()
                       .unnest()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);
    ASSERT_EQ(plan->outputType()->names(), expectedNames);

    auto generator = std::make_shared<core::PlanNodeIdGenerator>();
    auto referencePlan =
        startReferencePlan(generator)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .hashJoin(
                {"x1"},
                {"x2"},
                startReferencePlan(generator)
                    .project({"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"})
                    .planNode(),
                "",
                {"x1", "a_a_y1", "a_a_z1", "x2", "a_a_y2", "a_a_z2"})
            .project({
                "x1",
                "a_a_y2",
                "a_a_z2",
                "array_distinct(a_a_y1) AS a_a_y1_d",
                "array_distinct(a_a_z1) AS a_a_z1_d",
            })
            .project({
                "x1",
                "a_a_y1_d",
                "a_a_z1_d",
                "array_distinct(a_a_y2) AS a_a_y2_d",
                "array_distinct(a_a_z2) AS a_a_z2_d",
            })
            .unnest({"x1", "a_a_y2_d", "a_a_z2_d"}, {"a_a_y1_d", "a_a_z1_d"})
            .unnest(
                {"x1", "a_a_y1_d_e", "a_a_z1_d_e"}, {"a_a_y2_d", "a_a_z2_d"})
            .project({"x1", "a_a_y1_d_e AS a_y1", "a_a_z2_d_e AS a_z2"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("join before unnest (dependent on unnested columns)");

    const std::vector<std::string> expectedNames{"x1", "a_y1", "a_z2"};

    lp::PlanBuilder::Context ctx;
    auto logicalPlan =
        startLogicalPlan(ctx)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .join(
                startLogicalPlan(ctx).project(
                    {"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"}),
                "a_a_y1 = a_a_y2",
                lp::JoinType::kInner)
            .unnest({
                lp::Sql("array_distinct(a_a_y1)").unnestAs("a_y1"),
                lp::Sql("array_distinct(a_a_z1)").unnestAs("a_z1"),
            })
            .unnest({
                lp::Sql("array_distinct(a_a_y2)").unnestAs("a_y2"),
                lp::Sql("array_distinct(a_a_z2)").unnestAs("a_z2"),
            })
            .project(expectedNames)
            .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .hashJoin(core::PlanMatcherBuilder{}.values().build())
                       .project()
                       .unnest()
                       .project()
                       .unnest()
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);
    ASSERT_EQ(plan->outputType()->names(), expectedNames);

    auto generator = std::make_shared<core::PlanNodeIdGenerator>();
    auto referencePlan =
        startReferencePlan(generator)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .hashJoin(
                {"a_a_y1"},
                {"a_a_y2"},
                startReferencePlan(generator)
                    .project({"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"})
                    .planNode(),
                "",
                {"x1", "a_a_y1", "a_a_z1", "x2", "a_a_y2", "a_a_z2"})
            .project({
                "x1",
                "a_a_y2",
                "a_a_z2",
                "array_distinct(a_a_y1) AS a_a_y1_d",
                "array_distinct(a_a_z1) AS a_a_z1_d",
            })
            .project({
                "x1",
                "a_a_y1_d",
                "a_a_z1_d",
                "array_distinct(a_a_y2) AS a_a_y2_d",
                "array_distinct(a_a_z2) AS a_a_z2_d",
            })
            .unnest({"x1", "a_a_y2_d", "a_a_z2_d"}, {"a_a_y1_d", "a_a_z1_d"})
            .unnest(
                {"x1", "a_a_y1_d_e", "a_a_z1_d_e"}, {"a_a_y2_d", "a_a_z2_d"})
            .project({"x1", "a_a_y1_d_e AS a_y1", "a_a_z2_d_e AS a_z2"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("join after unnest (independent on unnested columns)");

    const std::vector<std::string> expectedNames{"x1", "a_y1", "a_z2"};

    lp::PlanBuilder::Context ctx;
    auto logicalPlan =
        startLogicalPlan(ctx)
            .unnest({
                lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
            })
            .as("t")
            .join(
                startLogicalPlan(ctx)
                    .unnest({
                        lp::Sql("array_distinct(a_a_y)").unnestAs("a_y"),
                        lp::Sql("array_distinct(a_a_z)").unnestAs("a_z"),
                    })
                    .as("u"),
                "t.x = u.x",
                lp::JoinType::kInner)
            .project({"t.x AS x1", "t.a_y AS a_y1", "u.a_z AS a_z2"})
            .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder{}
            .values()
            .project()
            .unnest()
            .project()
            .hashJoin(core::PlanMatcherBuilder{}
                          .values()
                          .project()
                          .unnest()
                          .project()
                          .build())
            .project() // TODO Fix the Optimizer to remove this project.
            .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);
    ASSERT_EQ(plan->outputType()->names(), expectedNames);

    auto generator = std::make_shared<core::PlanNodeIdGenerator>();
    auto referencePlan =
        startReferencePlan(generator)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .hashJoin(
                {"x1"},
                {"x2"},
                startReferencePlan(generator)
                    .project({"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"})
                    .planNode(),
                "",
                {"x1", "a_a_y1", "a_a_z1", "x2", "a_a_y2", "a_a_z2"})
            .project({
                "x1",
                "a_a_y2",
                "a_a_z2",
                "array_distinct(a_a_y1) AS a_a_y1_d",
                "array_distinct(a_a_z1) AS a_a_z1_d",
            })
            .project({
                "x1",
                "a_a_y1_d",
                "a_a_z1_d",
                "array_distinct(a_a_y2) AS a_a_y2_d",
                "array_distinct(a_a_z2) AS a_a_z2_d",
            })
            .unnest({"x1", "a_a_y2_d", "a_a_z2_d"}, {"a_a_y1_d", "a_a_z1_d"})
            .unnest(
                {"x1", "a_a_y1_d_e", "a_a_z1_d_e"}, {"a_a_y2_d", "a_a_z2_d"})
            .project({"x1", "a_a_y1_d_e AS a_y1", "a_a_z2_d_e AS a_z2"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
  {
    SCOPED_TRACE("join after unnest (dependent on unnested columns)");

    const std::vector<std::string> expectedNames{"x1", "a_y1", "x2", "a_z2"};

    lp::PlanBuilder::Context ctx;
    auto logicalPlan =
        startLogicalPlan(ctx)
            .project({"x AS x1", "a_a_y AS a_a_y1", "a_a_z AS a_a_z1"})
            .unnest({
                lp::Sql("array_distinct(a_a_y1)").unnestAs("a_y1"),
                lp::Sql("array_distinct(a_a_z1)").unnestAs("a_z1"),
            })
            .join(
                startLogicalPlan(ctx)
                    .project({"x AS x2", "a_a_y AS a_a_y2", "a_a_z AS a_a_z2"})
                    .unnest({
                        lp::Sql("array_distinct(a_a_y2)").unnestAs("a_y2"),
                        lp::Sql("array_distinct(a_a_z2)").unnestAs("a_z2"),
                    }),
                "a_y1 = a_y2",
                lp::JoinType::kInner)
            .project(expectedNames)
            .build();

    auto plan = toSingleThreadPlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder{}
                       .values()
                       .project()
                       .unnest()
                       .project()
                       .hashJoin(core::PlanMatcherBuilder{}
                                     .values()
                                     .project()
                                     .unnest()
                                     .project()
                                     .build())
                       .build();
    ASSERT_TRUE(matcher->match(plan)) << plan->toString(true, true);
    ASSERT_EQ(plan->outputType()->names(), expectedNames);

    auto generator = std::make_shared<core::PlanNodeIdGenerator>();
    auto referencePlan =
        startReferencePlan(generator)
            .project({
                "x AS x1",
                "array_distinct(a_a_y) AS a_a_y1_d",
                "array_distinct(a_a_z) AS a_a_z1_d",
            })
            .unnest({"x1"}, {"a_a_y1_d", "a_a_z1_d"})
            .hashJoin(
                {"a_a_y1_d_e"},
                {"a_a_y2_d_e"},
                startReferencePlan(generator)
                    .project({
                        "x AS x2",
                        "array_distinct(a_a_y) AS a_a_y2_d",
                        "array_distinct(a_a_z) AS a_a_z2_d",
                    })
                    .unnest({"x2"}, {"a_a_y2_d", "a_a_z2_d"})
                    .planNode(),
                "",
                {
                    "x1",
                    "a_a_y1_d_e",
                    "a_a_z1_d_e",
                    "x2",
                    "a_a_y2_d_e",
                    "a_a_z2_d_e",
                })
            .project({"x1", "a_a_y1_d_e AS a_y1", "x2", "a_a_z2_d_e AS a_z2"})
            .planNode();

    checkSame(logicalPlan, referencePlan);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
