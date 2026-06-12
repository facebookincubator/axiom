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
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

class FixedPointTest : public test::QueryTestBase {};

// Attempting to execute a recursive plan must fail with a clear NYI error.
TEST_F(FixedPointTest, withRecursiveThrowsNyi) {
  auto rowType = ROW("n", BIGINT());
  logical_plan::PlanBuilder::Context context;

  auto anchor = logical_plan::PlanBuilder(context).values(
      rowType, std::vector<Variant>{Variant::row({0LL})});

  auto step = logical_plan::PlanBuilder(context)
                  .recursiveRef("counter", anchor)
                  .project({"n + 1 as n"})
                  .planNode();

  auto plan = anchor.fixedPoint("counter", step).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(plan),
      "Fixed-point (recursive) plan execution is not yet implemented");
}

} // namespace
} // namespace facebook::axiom::optimizer
