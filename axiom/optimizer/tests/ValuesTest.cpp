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

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class ValuesTest : public test::QueryTestBase {};

TEST_F(ValuesTest, columnPruning) {
  auto rowVector = makeRowVector(
      {"a", "b", "c"},
      {
          makeFlatVector<int32_t>({1, 10}),
          makeFlatVector<int32_t>({2, 20}),
          makeFlatVector<int32_t>({3, 30}),
      });

  auto matcher = core::PlanMatcherBuilder()
                     .values(ROW({"a", "c"}, INTEGER()))
                     .project()
                     .build();

  {
    auto logicalPlan =
        lp::PlanBuilder()
            .values(rowVector->rowType(), {rowVector->variantAt(0)})
            .map({"a + c as x", "a - c as y"})
            .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = lp::PlanBuilder()
                           .values({rowVector})
                           .map({"a + c as x", "a - c as y"})
                           .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Tests that value nodes can have complex literal types.
TEST_F(ValuesTest, complexTypes) {
  auto rowVector = makeRowVector({
      makeArrayVector<std::string>({{"nation1.0", "nation1.1"}, {"nation2"}}),
      makeMapVectorFromJson<int32_t, int64_t>({"{1: 10, 2: 20}", "{3: 30}"}),
      makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector({"n1", "n2"}),
      }),
  });

  auto logicalPlan = lp::PlanBuilder().values({rowVector}).build();
  auto plan = toSingleNodePlan(logicalPlan);

  auto expectedType = ROW({
      ARRAY(VARCHAR()),
      MAP(INTEGER(), BIGINT()),
      ROW({BIGINT(), VARCHAR()}),
  });

  auto matcher = core::PlanMatcherBuilder().values(expectedType).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(ValuesTest, expressions) {
  auto logicalPlan = lp::PlanBuilder(/*enableCoercions=*/true)
                         .values(
                             {"a", "b", "c"},
                             {
                                 {"1 + 2", "cast(0.1 as real)", "'foo'"},
                                 {"10 + 20", "cast(0.2 as real)", "null"},
                             })
                         .build();
  auto plan = toSingleNodePlan(logicalPlan);

  auto expected = makeRowVector(
      {"a", "b", "c"},
      {
          makeFlatVector<int64_t>({3, 30}),
          makeFlatVector<float>({0.1, 0.2}),
          makeNullableFlatVector<std::string>({"foo", std::nullopt}),
      });

  auto matcher = core::PlanMatcherBuilder().values(expected->rowType()).build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  checkSame(logicalPlan, {expected});
}

} // namespace
} // namespace facebook::axiom::optimizer
