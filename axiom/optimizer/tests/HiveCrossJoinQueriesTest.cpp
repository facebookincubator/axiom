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
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .project({"n_nationkey", "n_name"})
          .crossJoin(lp::PlanBuilder(context).tableScan("region").project(
              {"r_regionkey", "r_name"}))
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        matchScan("nation").nestedLoopJoin(matchScan("region").build()).build();
    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan);

    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(3, fragments.size());

    auto matcher = matchScan("region").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = matchScan("nation")
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .partitionedOutput()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().build();
    ASSERT_TRUE(matcher->match(fragments.at(2).fragment.planNode));
  }

  auto referencePlan =
      execScan("nation")
          .project({"n_nationkey", "n_name"})
          .nestedLoopJoin(
              execScan("region").project({"r_regionkey", "r_name"}).planNode(),
              {"n_nationkey", "n_name", "r_regionkey", "r_name"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveCrossJoinQueriesTest, filterPushdown) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("nation")
                         .as("n")
                         .filter("n_nationkey < 3")
                         .project({"n_nationkey", "n_regionkey"})
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("region")
                                        .as("r")
                                        .filter("r_regionkey < 2")
                                        .project({"r_regionkey"}))
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("customer")
                                        .as("c")
                                        .filter("c_custkey < 100")
                                        .project({"c_custkey"}))
                         .filter("c.c_custkey != r.r_regionkey")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = matchScan("customer")
                       .nestedLoopJoin(matchScan("region").build())
                       .filter("c_custkey != r_regionkey")
                       .nestedLoopJoin(matchScan("nation").build())
                       .project()
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
                  .filter("c_custkey != r_regionkey")
                  .nestedLoopJoin(core::PlanMatcherBuilder().exchange().build())
                  .project()
                  .partitionedOutput()
                  .build();
    ASSERT_TRUE(matcher->match(fragments.at(2).fragment.planNode));

    matcher = core::PlanMatcherBuilder().exchange().build();
    ASSERT_TRUE(matcher->match(fragments.at(3).fragment.planNode));
  }

  auto referencePlan =
      execScan("nation")
          .filter("n_nationkey < 3")
          .project({"n_nationkey", "n_regionkey"})
          .nestedLoopJoin(
              execScan("region")
                  .filter("r_regionkey < 2")
                  .project({"r_regionkey"})
                  .planNode(),
              {"n_nationkey", "n_regionkey", "r_regionkey"})
          .nestedLoopJoin(
              execScan("customer")
                  .filter("c_custkey < 100")
                  .project({"c_custkey"})
                  .planNode(),
              {"n_nationkey", "n_regionkey", "r_regionkey", "c_custkey"})
          .filter("c_custkey != r_regionkey")
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveCrossJoinQueriesTest, manyTables) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("nation")
                         .filter("n_nationkey < 2")
                         .project({"n_nationkey"})
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("region")
                                        .filter("r_regionkey < 2")
                                        .project({"r_regionkey"}))
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("customer")
                                        .filter("c_custkey < 15")
                                        .project({"c_custkey"}))
                         .crossJoin(lp::PlanBuilder(context)
                                        .tableScan("lineitem")
                                        .filter("l_orderkey < 50")
                                        .project({"l_orderkey"}))
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("lineitem")
                       .nestedLoopJoin(matchScan("nation").build())
                       .nestedLoopJoin(matchScan("customer").build())
                       .nestedLoopJoin(matchScan("region").build())
                       .project()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan);
    const auto& fragments = plan.plan->fragments();
    ASSERT_EQ(5, fragments.size());

    auto matcher = matchScan("nation").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = matchScan("customer").partitionedOutput().build();
    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));

    matcher = matchScan("region").partitionedOutput().build();
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

  auto referencePlan =
      execScan("nation")
          .filter("n_nationkey < 2")
          .project({"n_nationkey"})
          .nestedLoopJoin(
              execScan("region")
                  .filter("r_regionkey < 2")
                  .project({"r_regionkey"})
                  .planNode(),
              {"n_nationkey", "r_regionkey"})
          .nestedLoopJoin(
              execScan("customer")
                  .filter("c_custkey < 15")
                  .project({"c_custkey"})
                  .planNode(),
              {"n_nationkey", "r_regionkey", "c_custkey"})
          .nestedLoopJoin(
              execScan("lineitem")
                  .filter("l_orderkey < 50")
                  .project({"l_orderkey"})
                  .planNode(),
              {"n_nationkey", "r_regionkey", "c_custkey", "l_orderkey"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveCrossJoinQueriesTest, innerJoin) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .as("n")
          .filter("n.n_nationkey < 5")
          .project({"n.n_nationkey", "n.n_regionkey"})
          .join(
              lp::PlanBuilder(context)
                  .tableScan("region", getSchema("region")->names())
                  .as("r")
                  .filter("r.r_regionkey < 3")
                  .project({"r.r_regionkey"}),
              "n.n_regionkey = r.r_regionkey",
              lp::JoinType::kInner)
          .project({"n.n_nationkey", "n.n_regionkey"})
          .crossJoin(lp::PlanBuilder(context)
                         .tableScan("customer", getSchema("customer")->names())
                         .filter("c_custkey < 1000")
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

  auto referencePlan = execScan("nation")
                           .filter("n_nationkey < 5")
                           .project({"n_nationkey", "n_regionkey"})
                           .hashJoin(
                               {"n_regionkey"},
                               {"r_regionkey"},
                               execScan("region")
                                   .filter("r_regionkey < 3")
                                   .project({"r_regionkey"})
                                   .planNode(),
                               "",
                               {"n_nationkey", "n_regionkey"})
                           .nestedLoopJoin(
                               execScan("customer")
                                   .filter("c_custkey < 1000")
                                   .project({"c_custkey"})
                                   .planNode(),
                               {"n_nationkey", "n_regionkey", "c_custkey"})
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer::test
