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

#include <velox/core/PlanNode.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class HiveAggregationQueriesTest : public test::HiveQueriesTestBase {
 protected:
  void checkSame(
      const lp::LogicalPlanNodePtr& planNode,
      const core::PlanNodePtr& referencePlan,
      const axiom::runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4}) {
    VELOX_CHECK_NOT_NULL(planNode);
    VELOX_CHECK_NOT_NULL(referencePlan);

    axiom::runner::MultiFragmentPlan::Options singleNodeOptions = {
        .numWorkers = 1, .numDrivers = 1};
    auto referenceResult = runVelox(referencePlan, singleNodeOptions);
    auto fragmentedPlan = planVelox(planNode, options);
    auto experimentResult = runFragmentedPlan(fragmentedPlan);

    exec::test::assertEqualResults(
        referenceResult.results, experimentResult.results);

    if (options.numWorkers != 1) {
      auto singleNodePlan = planVelox(
          planNode, {.numWorkers = 1, .numDrivers = options.numDrivers});
      auto singleNodeResult = runFragmentedPlan(singleNodePlan);

      exec::test::assertEqualResults(
          referenceResult.results, singleNodeResult.results);

      if (options.numDrivers != 1) {
        auto singleThreadPlan =
            planVelox(planNode, {.numWorkers = 1, .numDrivers = 1});
        auto singleThreadResult = runFragmentedPlan(singleThreadPlan);

        exec::test::assertEqualResults(
            referenceResult.results, singleThreadResult.results);
      }
    }
  }

  core::PlanNodePtr toSingleNodePlan(
      const lp::LogicalPlanNodePtr& logicalPlan,
      int32_t numDrivers = 1) {
    schema_ = std::make_shared<optimizer::SchemaResolver>();

    auto plan =
        planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers})
            .plan;

    EXPECT_EQ(1, plan->fragments().size());
    return plan->fragments().at(0).fragment.planNode;
  }

  runner::MultiFragmentPlanPtr toDistributedPlan(
      const lp::LogicalPlanNodePtr& logicalPlan) {
    schema_ = std::make_shared<optimizer::SchemaResolver>();

    auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4}).plan;
    EXPECT_GT(plan->fragments().size(), 1);
    return plan;
  }
};

TEST_F(HiveAggregationQueriesTest, mask) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {},
              {"sum(n_nationkey) FILTER (WHERE n_nationkey > 10)",
               "avg(n_regionkey)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .project()
                       .singleAggregation()
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = toDistributedPlan(logicalPlan);
    const auto& fragments = plan->fragments();
    ASSERT_EQ(2, fragments.size());

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .project()
                       .aggregation()
                       .partitionedOutput()
                       .build();

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder()
                  .exchange()
                  .localPartition()
                  .finalAggregation()
                  .build();

    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .project({"n_nationkey", "n_regionkey", "n_nationkey > 10 as mask"})
          .singleAggregation(
              {}, {"sum(n_nationkey) FILTER (WHERE mask)", "avg(n_regionkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, distinct) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("nation")
                         .aggregate({}, {"count(distinct n_regionkey)"})
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .singleAggregation()
                       .build();

    ASSERT_TRUE(matcher->match(plan));

    VELOX_ASSERT_THROW(
        toDistributedPlan(logicalPlan),
        "DISTINCT option for aggregation is supported only in single worker, single thread mode");
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .project({"n_regionkey"})
          .singleAggregation({}, {"count(distinct n_regionkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 1});
}

TEST_F(HiveAggregationQueriesTest, orderBy) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .singleAggregation()
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  VELOX_ASSERT_THROW(
      toDistributedPlan(logicalPlan),
      "ORDER BY option for aggregation is supported only in single worker, single thread mode");

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .planNode();

  checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 1});
}

TEST_F(HiveAggregationQueriesTest, maskWithOrderBy) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {"n_regionkey"},
              {"array_agg(n_name ORDER BY n_nationkey) FILTER (WHERE n_nationkey < 20)"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .project()
                     .singleAggregation()
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  VELOX_ASSERT_THROW(
      toDistributedPlan(logicalPlan),
      "ORDER BY option for aggregation is supported only in single worker, single thread mode");

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .project(
              {"n_name",
               "n_regionkey",
               "n_nationkey",
               "n_nationkey < 20 as mask"})
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_name ORDER BY n_nationkey) FILTER (WHERE mask)"})
          .planNode();

  checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 1});
}

} // namespace
} // namespace facebook::axiom::optimizer
