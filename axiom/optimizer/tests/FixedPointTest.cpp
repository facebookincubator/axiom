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
  FixedPointTest() {
    useV2_ = true;
  }

  lp::PlanBuilder anchor() {
    return lp::PlanBuilder(context_).values(
        ROW({"n"}, {BIGINT()}),
        std::vector<Variant>{Variant::row({int64_t{1}})});
  }

  lp::PlanBuilder::Context context_;
};

TEST_F(FixedPointTest, planShape) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
      "SELECT * FROM counter",
      kTestConnectorId);
  auto plan = toSingleNodePlan(logicalPlan);

  core::FixedPointDetails details;
  details.name = "counter";
  details.anchor = matchValues().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = "counter"})
                     .filter("n < 10")
                     .project()
                     .build();

  auto matcher =
      core::PlanMatcherBuilder().fixedPoint(details).project().build();
  EXPECT_TRUE(matcher->match(plan));
}

TEST_F(FixedPointTest, multipleWorkersUseSingleFragment) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
      "SELECT * FROM counter",
      kTestConnectorId);

  auto plan = planVelox(logicalPlan, {.numWorkers = 2, .numDrivers = 1}).plan;
  ASSERT_EQ(plan->fragments().size(), 1);
  EXPECT_EQ(plan->fragments().front().type, FragmentType::kSingle);
}

// The outer filter stays on the fixed-point output and is not copied into the
// anchor.
TEST_F(FixedPointTest, outerFilterNotPushedIntoAnchor) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 20) "
      "SELECT n FROM counter WHERE n > 15",
      kTestConnectorId);

  core::FixedPointDetails details;
  details.name = "counter";
  details.anchor = matchValues().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = "counter"})
                     .filter("n < 20")
                     .project()
                     .build();

  auto matcher = core::PlanMatcherBuilder()
                     .fixedPoint(details)
                     .filter("n > 15")
                     .project()
                     .build();
  EXPECT_TRUE(matcher->match(toSingleNodePlan(logicalPlan)));
}

TEST_F(FixedPointTest, recursiveRefWithoutFixedPointFails) {
  auto seed = anchor();
  auto orphan = lp::PlanBuilder(context_).recursiveRef("counter", seed).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(orphan),
      "RecursiveReferenceNode outside any enclosing FixedPoint");
}

TEST_F(FixedPointTest, nestedFixedPointNyi) {
  auto innerSeed = anchor();
  auto innerBody = lp::PlanBuilder(context_)
                       .recursiveRef("inner", innerSeed)
                       .project({"n + 1 as n"})
                       .planNode();
  innerSeed.fixedPoint("inner", innerBody);

  auto outerSeed = anchor();
  auto outerBody = lp::PlanBuilder(context_)
                       .recursiveRef("outer", outerSeed)
                       .unionAll(innerSeed)
                       .planNode();
  auto plan = outerSeed.fixedPoint("outer", outerBody).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(plan),
      "Nested FixedPoint translation is not yet implemented");
}

} // namespace
} // namespace facebook::axiom::optimizer
