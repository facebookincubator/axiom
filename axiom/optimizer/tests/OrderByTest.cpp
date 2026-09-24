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
    lp::PlanBuilder::Context context{kTestConnectorId, kDefaultSchema};
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

TEST_P(OrderByTest, coalesceJoinKey) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("u", ROW("b", BIGINT()));

  const auto logicalPlan = parseSelect(
      "SELECT a FROM t LEFT JOIN u ON a = b "
      "ORDER BY coalesce(a, b) DESC NULLS FIRST",
      kTestConnectorId);

  // The coalesce order-by key is rewritten to a. No remote shuffle is needed
  // before the OrderBy node.
  AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
      planVelox(logicalPlan).plan,
      matchScan("t")
          .shuffle({"a"})
          .hashJoinLeft(
              matchScan("u").shuffle({"b"}),
              {.keys = {{"a = b"}}, .outputColumnNames = {{"a"}}})
          .project({"a", "a as key"})
          .orderBy({"key DESC NULLS FIRST"})
          .localMerge()
          .shuffleMerge()
          .project({"a"})
          .build());
}

AXIOM_INSTANTIATE_V1_V2(OrderByTest);

} // namespace
} // namespace facebook::axiom::optimizer
