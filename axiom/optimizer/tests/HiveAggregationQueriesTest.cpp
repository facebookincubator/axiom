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

class HiveAggregationQueriesTest : public test::HiveQueriesTestBase {};

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

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project({"n_nationkey > 10 as mask", "n_nationkey", "n_regionkey"})
            .singleAggregation(
                {}, {"sum(n_nationkey) FILTER (mask)", "avg(n_regionkey)"})
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4}).plan;
    const auto& fragments = plan->fragments();
    ASSERT_EQ(2, fragments.size());

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project({"n_nationkey > 10 as mask", "n_nationkey", "n_regionkey"})
            .partialAggregation(
                {}, {"sum(n_nationkey) FILTER (mask)", "avg(n_regionkey)"})
            .partitionedOutput()
            .build();

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    // Verify mask is NOT present in final aggregation.
    matcher = core::PlanMatcherBuilder()
                  .exchange()
                  .localPartition()
                  .finalAggregation({}, {"sum(sum)", "avg(avg)"})
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
                       .singleAggregation({}, {"count(distinct n_regionkey)"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));

    VELOX_ASSERT_THROW(
        planVelox(logicalPlan),
        "DISTINCT option for aggregation is supported only in single worker, single thread mode");
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", ROW({"n_regionkey"}, BIGINT()))
          .singleAggregation({}, {"count(distinct n_regionkey)"})
          .planNode();

  checkSameSingleNode(logicalPlan, referencePlan);
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
                     .singleAggregation(
                         {"n_regionkey"},
                         {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
                          "array_agg(n_name ORDER BY n_nationkey)"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));

  VELOX_ASSERT_THROW(
      planVelox(logicalPlan),
      "ORDER BY option for aggregation is supported only in single worker, single thread mode");

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .planNode();

  checkSameSingleNode(logicalPlan, referencePlan);
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
      planVelox(logicalPlan),
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

  checkSameSingleNode(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, distinctWithOrderBy) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {"n_regionkey"},
              {"array_agg(DISTINCT n_name ORDER BY n_nationkey)"})
          .build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "DISTINCT with ORDER BY in same aggregation expression isn't supported yet");
  VELOX_ASSERT_THROW(
      planVelox(logicalPlan),
      "DISTINCT with ORDER BY in same aggregation expression isn't supported yet");
}

} // namespace
} // namespace facebook::axiom::optimizer
