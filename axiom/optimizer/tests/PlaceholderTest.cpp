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
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class PlaceholderTest : public test::QueryTestBase {};

// The optimizer replaces each placeholder with the value at its position. A
// placeholder with no value, or a value of another type, fails.
TEST_F(PlaceholderTest, bind) {
  auto logicalPlan =
      lp::PlanBuilder()
          .values(
              {makeNamedRowVector({{"a", makeFlatVector<int64_t>({1, 2})}})})
          .project({"a + ?2::BIGINT AS x", "?1::VARCHAR AS y"})
          .build();

  {
    auto plan =
        toSingleNodePlan(logicalPlan, {Variant("abc"), Variant(int64_t{10})});
    AXIOM_ASSERT_PLAN(plan, matchValues().project({"a + 10", "'abc'"}).build());
  }

  // A NULL of the placeholder's type is a value. 'a + NULL' folds to NULL.
  {
    auto plan = toSingleNodePlan(
        logicalPlan,
        {Variant::null(TypeKind::VARCHAR), Variant::null(TypeKind::BIGINT)});
    AXIOM_ASSERT_PLAN(
        plan, matchValues().project({"null::bigint", "null::varchar"}).build());
  }

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan, {Variant("abc")}),
      "Placeholder has no value");
  VELOX_ASSERT_THROW(
      toSingleNodePlan(
          logicalPlan, {Variant(int64_t{1}), Variant(int64_t{10})}),
      "Constant value doesn't match its type: VARCHAR vs. BIGINT");
}

TEST_F(PlaceholderTest, values) {
  // VALUES (?1 + 1), (?2)
  auto logicalPlan = lp::PlanBuilder()
                         .values({"x"}, {{"?1::BIGINT + 1"}, {"?2::BIGINT"}})
                         .build();

  auto plan = toSingleNodePlan(
      logicalPlan, {Variant(int64_t{10}), Variant(int64_t{20})});
  AXIOM_ASSERT_PLAN(plan, matchValues<int64_t>({{11}, {20}}).build());
}

} // namespace
} // namespace facebook::axiom::optimizer
