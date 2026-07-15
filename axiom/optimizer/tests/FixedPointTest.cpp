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
 public:
  FixedPointTest() {
    useV2_ = true;
  }

 protected:
  static constexpr auto kCounter = "counter";

  // Builds a single-row `n:BIGINT = 1` Values in `context_`, wired for
  // use as either an anchor or a `recursiveRef` target in the same plan.
  lp::PlanBuilder anchor() {
    return lp::PlanBuilder(context_).values(
        ROW({"n"}, {BIGINT()}),
        std::vector<Variant>{Variant::row({int64_t{1}})});
  }

  // Builds the canonical `step` sub-plan for a FixedPoint named `name`
  // anchored on `anchorNode`: read the working table, keep rows where
  // `n < limit`, project `n + 1 as n`.
  lp::LogicalPlanNodePtr step(
      std::string_view name,
      const lp::PlanBuilder& anchorNode,
      int64_t limit) {
    return lp::PlanBuilder(context_)
        .recursiveRef(std::string(name), anchorNode)
        .filter(fmt::format("n < {}", limit))
        .project({"n + 1 as n"})
        .planNode();
  }

  lp::PlanBuilder::Context context_;
};

TEST_F(FixedPointTest, planShape) {
  auto seed = anchor();
  auto body = step(kCounter, seed, 10);
  auto plan = toSingleNodePlan(seed.fixedPoint(kCounter, body).build());

  core::FixedPointDetails details;
  details.name = kCounter;
  details.anchor = core::PlanMatcherBuilder().values().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = kCounter})
                     .filter("n < 10")
                     .project()
                     .build();

  auto matcher = core::PlanMatcherBuilder().fixedPoint(details).build();
  EXPECT_TRUE(matcher->match(plan));
}

// The outer filter is retained on the FP output and NOT copied into the
// anchor. Copying is only sound when the predicate is provably invariant
// under the step transformation (see `rewriteFixedPoint` in
// PushdownAndPrunePass), and no such analysis exists yet. Concretely, for
// `WITH RECURSIVE t(n) AS (VALUES 1 UNION ALL SELECT n+1 FROM t WHERE n<20)
// SELECT n FROM t WHERE n>15`, copying `n>15` into the anchor drops seed
// row 1 whose descendants 16..20 would satisfy the predicate, producing an
// empty result instead of {16..20}.
TEST_F(FixedPointTest, outerFilterNotPushedIntoAnchor) {
  auto seed = anchor();
  auto body = step(kCounter, seed, 20);
  auto plan =
      seed.fixedPoint(kCounter, body).filter("n > 15").project({"n"}).build();

  core::FixedPointDetails details;
  details.name = kCounter;
  // Anchor stays a bare Values — the outer `n > 15` is NOT pushed here.
  details.anchor = core::PlanMatcherBuilder().values().build();
  details.step = core::PlanMatcherBuilder()
                     .workingTableScan({.name = kCounter})
                     .filter("n < 20")
                     .project()
                     .build();

  auto matcher =
      core::PlanMatcherBuilder().fixedPoint(details).filter("n > 15").build();
  EXPECT_TRUE(matcher->match(toSingleNodePlan(plan)));
}

TEST_F(FixedPointTest, recursiveRefWithoutFixedPointFails) {
  auto seed = anchor();
  auto orphan = lp::PlanBuilder(context_).recursiveRef(kCounter, seed).build();

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
  // innerSeed's node is now the inner FixedPoint; passed to unionAll below.
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
