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

#include <iostream>
#include <memory>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveCrossJoinQueriesTest : public test::HiveQueriesTestBase {
 protected:
  static core::PlanMatcherBuilder matchScan(const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  }

  exec::test::PlanBuilder execScan(const std::string& tableName) {
    return exec::test::PlanBuilder(planNodeIdGenerator)
        .tableScan(tableName, getSchema(tableName));
  }

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  }

  std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator;
};

TEST_F(HiveCrossJoinQueriesTest, basic) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("customer")
                         .project({"c_custkey"})
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("lineitem")
                                        .project({"l_orderkey"}))
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("lineitem")
                       .nestedLoopJoin(matchScan("customer").build())
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan);

    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(3, fragments.size());

    auto matcher = matchScan("customer").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = matchScan("lineitem")
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .project()
                  .partitionedOutput()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().build();
    ASSERT_TRUE(matcher->match(fragments.at(2).fragment.planNode));
  }
}

TEST_F(HiveCrossJoinQueriesTest, manyTables) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .project({"n_nationkey"})
          .crossJoin(lp::PlanBuilder(context).tableScan("region").project(
              {"r_regionkey"}))
          .crossJoin(lp::PlanBuilder(context)
                         .tableScan("customer")
                         .project({"c_custkey"}))
          .crossJoin(lp::PlanBuilder(context)
                         .tableScan("lineitem")
                         .project({"l_orderkey", "l_partkey"}))
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("nation")
                       .nestedLoopJoin(matchScan("region").build())
                       .nestedLoopJoin(matchScan("lineitem").build())
                       .nestedLoopJoin(matchScan("customer").build())
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan);

    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(5, fragments.size());

    auto matcher = matchScan("region").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = matchScan("nation").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    matcher = matchScan("customer").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(2).fragment.planNode));

    matcher = matchScan("lineitem")
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .project()
                  .partitionedOutput()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(3).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().build();
    ASSERT_TRUE(matcher->match(fragments.at(4).fragment.planNode));
  }
}

TEST_F(HiveCrossJoinQueriesTest, innerJoin) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .as("n")
          .project({"n.n_nationkey", "n.n_regionkey"})
          .join(
              lp::PlanBuilder(context)
                  .tableScan("region", getSchema("region")->names())
                  .as("r")
                  .project({"r.r_regionkey"}),
              "n.n_regionkey = r.r_regionkey",
              lp::JoinType::kInner)
          .project({"n.n_nationkey", "n.n_regionkey"})
          .crossJoin(lp::PlanBuilder(context)
                         .tableScan("customer", getSchema("customer")->names())
                         .project({"c_custkey"}))
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("customer")
                       .nestedLoopJoin(matchScan("region").build())
                       .hashJoin(matchScan("nation").build())
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan);

    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(4, fragments.size());

    auto matcher = matchScan("region").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = matchScan("nation").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    matcher = matchScan("customer")
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .hashJoin(core::PlanMatcherBuilder().exchange().build())
                  .partitionedOutput()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(2).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().build();
    ASSERT_TRUE(matcher->match(fragments.at(3).fragment.planNode));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::test
