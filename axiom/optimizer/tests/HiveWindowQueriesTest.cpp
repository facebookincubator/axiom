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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveWindowQueriesTest : public test::HiveQueriesTestBase {
  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }
};

TEST_F(HiveWindowQueriesTest, basicRowNumber) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder().tableScan("nation").window().build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, multipleWindowFunctions) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)",
               "rank() over (partition by n_regionkey order by n_nationkey)",
               "dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder().tableScan("nation").window().build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)",
               "rank() over (partition by n_regionkey order by n_nationkey)",
               "dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, differentWindowSpecs) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_nationkey)",
               "row_number() over (partition by n_regionkey order by n_nationkey)",
               "row_number() over (partition by n_regionkey order by n_nationkey desc)",
               "row_number() over (partition by n_name)",
               "lag(n_name) over (order by n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .window()
                       .window()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_nationkey)"})
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey desc)"})
          .window({"count(*) over (partition by n_name)"})
          .window({"lag(n_name) over (order by n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, rowsFrameTypes) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 2 preceding and 1 following)",
               "count(*) over (partition by n_regionkey order by n_nationkey rows between current row and unbounded following)",
               "min(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 1 preceding and 1 following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 3 following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 2 preceding and 1 following)",
               "count(*) over (partition by n_regionkey order by n_nationkey rows between current row and unbounded following)",
               "min(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between 1 preceding and 1 following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 3 following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, rangeFrameTypes) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey range unbounded preceding)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, mixedFrameTypesAndBounds) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between unbounded preceding and current row)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 2 following)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder().tableScan("nation").window().build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between unbounded preceding and current row)",
               "avg(n_nationkey) over (partition by n_regionkey order by n_nationkey range between unbounded preceding and current row)",
               "count(*) over (partition by n_regionkey order by n_nationkey range between current row and unbounded following)",
               "max(n_nationkey) over (partition by n_regionkey order by n_nationkey rows between current row and 2 following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, frameTypesWithoutOrderBy) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey rows between unbounded preceding and unbounded following)",
               "count(*) over (partition by n_regionkey range between unbounded preceding and unbounded following)"})
          .build();

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"sum(n_nationkey) over (partition by n_regionkey rows between unbounded preceding and unbounded following)"})
          .window(
              {"count(*) over (partition by n_regionkey range between unbounded preceding and unbounded following)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, multipleOrderByColumns) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey, n_name)",
               "rank() over (order by n_regionkey, n_nationkey desc, n_name)",
               "dense_rank() over (partition by n_regionkey order by n_name, n_nationkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .window()
                       .window()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey, n_name)"})
          .window(
              {"rank() over (order by n_regionkey, n_nationkey desc, n_name)"})
          .window(
              {"dense_rank() over (partition by n_regionkey order by n_name, n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveWindowQueriesTest, windowExpressionWithAliasOrderByAlias) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const std::vector<std::string>& names = nationType->names();

  auto logicalPlan =
      lp::PlanBuilder()
          .tableScan(connectorId, "nation", names)
          .project({"row_number() over (partition by n_regionkey order by n_nationkey) as rn", "n_nationkey", "n_regionkey", "n_name", "n_comment"})
          .orderBy({"rn"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .window()
                       .orderBy()
                       .build();
    ASSERT_TRUE(matcher->match(plan));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", nationType)
          .window(
              {"row_number() over (partition by n_regionkey order by n_nationkey)"})
          .orderBy({"row_number() over (partition by n_regionkey order by n_nationkey)"}, false)
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer
