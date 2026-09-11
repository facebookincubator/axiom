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
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// Counts the plan nodes of type T anywhere under 'plan'.
template <typename T>
int32_t countNodes(const core::PlanNodePtr& plan) {
  int32_t count = std::dynamic_pointer_cast<const T>(plan) != nullptr ? 1 : 0;
  for (const auto& source : plan->sources()) {
    count += countNodes<T>(source);
  }
  return count;
}

template <typename T>
int32_t countNodes(const optimizer::PlanAndStats& plan) {
  int32_t count = 0;
  for (const auto& fragment : plan.plan->fragments()) {
    count += countNodes<T>(fragment.fragment.planNode);
  }
  return count;
}

// A table that can only be read by key is reachable only through an index
// lookup. These tests cover the planning and lowering of that shape, which no
// scannable layout exercises.
class IndexLookupJoinTest : public QueryTestBase {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    testConnector_->addTable("probe", ROW({"a", "b"}, BIGINT()));
    testConnector_->metadata()->addLookupTable(
        "lookup", ROW({"id", "v"}, BIGINT()), {"id"});
  }

  // Plans `probe join lookup on probe.a = lookup.id`.
  optimizer::PlanAndStats planJoin() {
    lp::PlanBuilder::Context context(kTestConnectorId, kDefaultSchema);
    auto logicalPlan = lp::PlanBuilder(context)
                           .tableScan("probe")
                           .join(
                               lp::PlanBuilder(context).tableScan("lookup"),
                               "a = id",
                               lp::JoinType::kInner)
                           .build();
    return planVelox(logicalPlan);
  }
};

// The lookup side is joined by key rather than scanned and hash-joined. Before
// index lookup was lowered, ToVelox dropped the probe input and emitted a plain
// TableScanNode for the lookup side.
TEST_F(IndexLookupJoinTest, lowersToIndexLookupJoin) {
  auto plan = planJoin();
  EXPECT_EQ(countNodes<core::IndexLookupJoinNode>(plan), 1);
}

// joinByHash has to decline a build side it cannot scan. Without that, the
// optimizer plans a build that yields no plan and the failure surfaces far
// away, as an empty plan set inside the memo.
TEST_F(IndexLookupJoinTest, doesNotHashJoinALookupOnlyBuildSide) {
  auto plan = planJoin();
  EXPECT_EQ(countNodes<core::HashJoinNode>(plan), 0);
}

} // namespace
} // namespace facebook::axiom::optimizer::test
