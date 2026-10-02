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
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Verifies which ORDER BYs survive optimization. The plans are built with
// PlanBuilder rather than SQL so that the operator sequence under test reaches
// the optimizer unchanged.
class OrderByTest : public test::QueryTestBase,
                    public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    test::QueryTestBase::SetUp();
  }

  lp::PlanBuilder scan(const std::string& tableName) {
    auto context = makePlanBuilderContext();
    return lp::PlanBuilder(context).tableScan(tableName);
  }
};

TEST_P(OrderByTest, lastOrderByWins) {
  auto plan = scan("nation").orderBy({"n_nationkey"}).orderBy({"n_name desc"});

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan.build()),
      matchScan("nation").orderBy({"n_name DESC NULLS LAST"}).build());
}

TEST_P(OrderByTest, aggregationDropsPrecedingOrderBy) {
  auto plan = scan("nation")
                  .orderBy({"n_nationkey"})
                  .aggregate({"n_name"}, {"count(1)"})
                  .orderBy({"n_name desc"});

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan.build()),
      matchScan("nation")
          .singleAggregation({"n_name"}, {"count(1)"})
          .orderBy({"n_name DESC NULLS LAST"})
          .build());
}

TEST_P(OrderByTest, orderByOfUnreadRowsDrops) {
  // Nothing reads a column of the ordered rows, so their order cannot be
  // observed: `SELECT 1 FROM t ORDER BY x` is `SELECT 1 FROM t`. v1 keeps the
  // ORDER BY.
  auto plan = scan("nation").orderBy({"n_nationkey"}).project({"1 as one"});

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(plan.build()),
      matchScan("nation")
          .orderByIf(!useV2_, {"n_nationkey ASC NULLS LAST"})
          .project({"1 as one"})
          .build());
}

TEST_P(OrderByTest, unreadTopN) {
  // Nothing reads a column of the rows an ORDER BY with LIMIT keeps, so only
  // their count is observable, and a plain limit keeps the same count.
  auto plan = scan("nation")
                  .orderBy({"n_nationkey"})
                  .limit(3)
                  .aggregate({}, {"count(1)"});

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(plan.build()),
      matchScan("nation")
          .finalLimit(0, 3)
          .singleAggregation({}, {"count(1)"})
          .build());
}

AXIOM_INSTANTIATE_V1_V2(OrderByTest);

} // namespace
} // namespace facebook::axiom::optimizer
