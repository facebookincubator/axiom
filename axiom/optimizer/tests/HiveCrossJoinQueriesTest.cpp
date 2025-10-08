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

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveCrossJoinQueriesTest : public test::HiveQueriesTestBase {};

TEST_F(HiveCrossJoinQueriesTest, filterPushdown) {
  const auto connectorId = exec::test::kHiveConnectorId;

  auto startLogicalPlan = [&](lp::PlanBuilder::Context& ctx) {
    return lp::PlanBuilder(ctx);
  };

  auto startMatcher = [](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  lp::PlanBuilder::Context context;
  auto logicalPlan =
      startLogicalPlan(context)
          .tableScan(connectorId, "nation", getSchema("nation")->names())
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "region", getSchema("region")->names()))
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "customer", getSchema("customer")->names()))
          .filter("n_regionkey != r_regionkey")
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = startMatcher("nation")
                     .nestedLoopJoin(startMatcher("region").build())
                     .filter("n_regionkey != r_regionkey")
                     .nestedLoopJoin(startMatcher("customer").build())
                     .build();
  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(HiveCrossJoinQueriesTest, manyTables) {
  const auto connectorId = exec::test::kHiveConnectorId;

  auto startLogicalPlan = [&](lp::PlanBuilder::Context& ctx) {
    return lp::PlanBuilder(ctx);
  };

  auto startMatcher = [](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  lp::PlanBuilder::Context context;
  auto logicalPlan =
      startLogicalPlan(context)
          .tableScan(connectorId, "nation", getSchema("nation")->names())
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "region", getSchema("region")->names()))
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "customer", getSchema("customer")->names()))
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "lineitem", getSchema("lineitem")->names()))
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = startMatcher("lineitem")
                     .nestedLoopJoin(startMatcher("region").build())
                     .nestedLoopJoin(startMatcher("customer").build())
                     .nestedLoopJoin(startMatcher("lineitem").build())
                     .project()
                     .build();
  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(HiveCrossJoinQueriesTest, innerJoin) {
  const auto connectorId = exec::test::kHiveConnectorId;

  auto startLogicalPlan = [&](lp::PlanBuilder::Context& ctx) {
    return lp::PlanBuilder(ctx);
  };

  auto startMatcher = [](const std::string& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  lp::PlanBuilder::Context context;
  auto logicalPlan =
      startLogicalPlan(context)
          .tableScan(connectorId, "nation", getSchema("nation")->names())
          .join(
              startLogicalPlan(context).tableScan(
                  connectorId, "region", getSchema("region")->names()),
              "n_regionkey = r_regionkey",
              lp::JoinType::kInner)
          .crossJoin(startLogicalPlan(context).tableScan(
              connectorId, "customer", getSchema("customer")->names()))
          .build();

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = startMatcher("customer")
                     .nestedLoopJoin(startMatcher("region").build())
                     .hashJoin(startMatcher("nation").build())
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

} // namespace
} // namespace facebook::axiom::optimizer::test
