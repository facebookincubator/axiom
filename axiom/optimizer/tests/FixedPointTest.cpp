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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class FixedPointTest : public test::QueryTestBase {
 protected:
  // Builds the canonical recursive-CTE counter: seed n=1, step n+1, filter
  // n<10 (so the frontier empties after iteration 10).
  static lp::LogicalPlanNodePtr makeCounter() {
    lp::PlanBuilder::Context context;
    auto anchor = lp::PlanBuilder(context).values(
        ROW({"n"}, {BIGINT()}),
        std::vector<Variant>{Variant::row({int64_t{1}})});
    auto step = lp::PlanBuilder(context)
                    .recursiveRef("counter", anchor)
                    .filter("n < 10")
                    .project({"n + 1 as n"})
                    .planNode();
    return anchor.fixedPoint("counter", step).build();
  }
};

// Asserts the lowered plan shape: anchor is Values; step is
// WorkingTableScan + Filter + Project.
TEST_F(FixedPointTest, planShape) {
  auto plan = toSingleNodePlan(makeCounter());

  core::FixedPointDetails details;
  details.name = "counter";
  details.anchor = core::PlanMatcherBuilder().values().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = "counter"})
                     .filter()
                     .project()
                     .build();

  auto matcher = core::PlanMatcherBuilder().fixedPoint(details).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Lowers and runs the counter; expects {1, 2, ..., 10}. DISABLED until
// LocalRunner / TaskCursor thread a per-iteration subTaskId through
// FixedPointTaskOptions (currently FixedPointTask::drainPlan aborts).
TEST_F(FixedPointTest, DISABLED_executableRoundTrip) {
  auto plan = toSingleNodePlan(makeCounter());

  auto result = runVelox(plan);
  ASSERT_FALSE(result.results.empty());
  std::vector<int64_t> rows;
  for (const auto& batch : result.results) {
    auto* values = batch->childAt(0)->asFlatVector<int64_t>();
    ASSERT_NE(values, nullptr);
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      rows.push_back(values->valueAt(i));
    }
  }
  std::ranges::sort(rows);
  const std::vector<int64_t> expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  EXPECT_EQ(rows, expected);
}

// Cross join in the step lowers to NestedLoopJoin.
TEST_F(FixedPointTest, crossJoinInStepPlanShape) {
  lp::PlanBuilder::Context context;
  auto anchor = lp::PlanBuilder(context).values(
      ROW({"n"}, {BIGINT()}), std::vector<Variant>{Variant::row({int64_t{1}})});

  auto rightLeg = lp::PlanBuilder(context).values(
      ROW({"m"}, {BIGINT()}), std::vector<Variant>{Variant::row({int64_t{1}})});

  auto step = lp::PlanBuilder(context)
                  .recursiveRef("counter", anchor)
                  .crossJoin(rightLeg)
                  .project({"n + 1 as n"})
                  .planNode();

  auto plan = anchor.fixedPoint("counter", step).build();

  core::FixedPointDetails details;
  details.name = "counter";
  details.anchor = core::PlanMatcherBuilder().values().build();
  details.step =
      core::PlanMatcherBuilder()
          .workingTableScan({.name = "counter"})
          .nestedLoopJoin(core::PlanMatcherBuilder().values().build())
          .project()
          .build();

  auto matcher = core::PlanMatcherBuilder().fixedPoint(details).build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(plan), matcher);
}

// A RecursiveReferenceNode outside any FixedPoint must fail with a clear
// user error.
TEST_F(FixedPointTest, recursiveRefWithoutFixedPointFails) {
  lp::PlanBuilder::Context context;
  auto anchor = lp::PlanBuilder(context).values(
      ROW({"n"}, {BIGINT()}), std::vector<Variant>{Variant::row({int64_t{1}})});

  auto orphan =
      lp::PlanBuilder(context).recursiveRef("counter", anchor).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(orphan),
      "RecursiveReferenceNode outside any enclosing FixedPoint");
}

// An outer filter on a FixedPoint is retained on the FP output (correctness:
// step's per-iteration rows must still be filtered) and is also pushed into
// the anchor as a pre-filter (safe: the anchor runs once).
TEST_F(FixedPointTest, outerFilterOnFixedPointRetained) {
  lp::PlanBuilder::Context context;
  auto anchor = lp::PlanBuilder(context).values(
      ROW({"n"}, {BIGINT()}), std::vector<Variant>{Variant::row({int64_t{1}})});
  auto step = lp::PlanBuilder(context)
                  .recursiveRef("counter", anchor)
                  .filter("n < 10")
                  .project({"n + 1 as n"})
                  .planNode();
  auto plan =
      anchor.fixedPoint("counter", step).filter("n < 5").project({"n"}).build();

  core::FixedPointDetails details;
  details.name = "counter";
  details.anchor = core::PlanMatcherBuilder().values().filter().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = "counter"})
                     .filter()
                     .project()
                     .build();

  auto matcher =
      core::PlanMatcherBuilder().fixedPoint(details).filter().build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(plan), matcher);
}

// Nested FixedPoints must fail at translation with a clear NYI message.
TEST_F(FixedPointTest, nestedFixedPointNyi) {
  const auto rowType = ROW({"n"}, {BIGINT()});
  const std::vector<Variant> oneRow{Variant::row({int64_t{1}})};

  auto innerAnchor =
      std::make_shared<lp::ValuesNode>("v_inner", rowType, oneRow);
  auto innerStep =
      std::make_shared<lp::RecursiveReferenceNode>("r_inner", "inner", rowType);
  auto innerFp = std::make_shared<lp::FixedPointNode>(
      "fp_inner", "inner", innerAnchor, innerStep);

  auto outerAnchor =
      std::make_shared<lp::ValuesNode>("v_outer", rowType, oneRow);
  auto outerRef =
      std::make_shared<lp::RecursiveReferenceNode>("r_outer", "outer", rowType);
  auto outerStep = std::make_shared<lp::SetNode>(
      "set_step",
      std::vector<lp::LogicalPlanNodePtr>{outerRef, innerFp},
      lp::SetOperation::kUnionAll);
  auto outerFp = std::make_shared<lp::FixedPointNode>(
      "fp_outer", "outer", outerAnchor, outerStep);

  VELOX_ASSERT_THROW(
      toSingleNodePlan(outerFp),
      "Nested FixedPoint translation is not yet implemented");
}

} // namespace
} // namespace facebook::axiom::optimizer
