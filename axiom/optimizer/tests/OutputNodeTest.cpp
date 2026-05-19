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

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class OutputNodeTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

// Verifies the optimizer honors OutputNode entry shapes documented in
// OutputNode::Entry — drop, reorder, duplicate, identity.
TEST_F(OutputNodeTest, entries) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  auto makePlan = [&](const std::vector<lp::OutputNode::Entry>& entries)
      -> velox::core::PlanNodePtr {
    auto ctx = makeContext();
    auto scan = lp::PlanBuilder(ctx).tableScan("t").planNode();
    auto logicalPlan = std::make_shared<lp::OutputNode>(
        ctx.planNodeIdGenerator->next(), scan, entries);
    return toSingleNodePlan(logicalPlan);
  };

  // Drop: column b is pruned. Scan reads only {a, c}.
  {
    auto plan = makePlan({{0, "a"}, {2, "c"}});
    VELOX_EXPECT_EQ_TYPES(plan->outputType(), ROW({"a", "c"}, BIGINT()));
    auto matcher = matchScan("t", ROW({"a", "c"}, BIGINT())).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Reorder: scan reads {a, c}, Project reorders to {c, a}.
  {
    auto plan = makePlan({{2, "c"}, {0, "a"}});
    VELOX_EXPECT_EQ_TYPES(plan->outputType(), ROW({"c", "a"}, BIGINT()));
    auto matcher =
        matchScan("t", ROW({"a", "c"}, BIGINT())).project({"c", "a"}).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Duplicate: source column a is referenced twice. Scan reads only {a}.
  {
    auto plan = makePlan({{0, "x"}, {0, "y"}});
    VELOX_EXPECT_EQ_TYPES(plan->outputType(), ROW({"x", "y"}, BIGINT()));
    auto matcher = matchScan("t", ROW({"a"}, BIGINT()))
                       .project({"a AS x", "a AS y"})
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Identity: all columns, same order. No ProjectNode added.
  {
    auto plan = makePlan({{0, "a"}, {1, "b"}, {2, "c"}});
    VELOX_EXPECT_EQ_TYPES(plan->outputType(), ROW({"a", "b", "c"}, BIGINT()));
    auto matcher = matchScan("t", ROW({"a", "b", "c"}, BIGINT())).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
