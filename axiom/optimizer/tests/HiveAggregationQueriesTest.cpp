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

TEST_F(HiveAggregationQueriesTest, ignoreDuplicates) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {},
              {"bool_and(DISTINCT n_nationkey % 2 = 0)",
               "bool_or(DISTINCT n_regionkey % 2 = 0)",
               "bool_and(n_nationkey % 2 = 0)",
               "bool_or(DISTINCT n_nationkey % 2 = 0)",
               "bool_and(DISTINCT n_nationkey % 2 = 0) FILTER (WHERE n_nationkey > 10)",
               "bool_or(DISTINCT n_nationkey % 2 = 0) FILTER (WHERE n_nationkey < 20)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project(
                {"n_nationkey % 2 = 0 as m1",
                 "n_regionkey % 2 = 0 as m2",
                 "n_nationkey > 10 as m3",
                 "n_nationkey < 20 as m4"})
            .singleAggregation(
                {},
                {"bool_and(m1) as agg1",
                 "bool_or(m2) as agg2",
                 "bool_or(m1) as agg3",
                 "bool_and(m1) FILTER (WHERE m3) as agg4",
                 "bool_or(m1) FILTER (WHERE m4) as agg5"})
            .project({"agg1", "agg2", "agg1", "agg3", "agg4", "agg5"})
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan).plan;
    const auto& fragments = plan->fragments();
    ASSERT_EQ(2, fragments.size());

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .project(
                           {"n_nationkey % 2 = 0 as m1",
                            "n_regionkey % 2 = 0 as m2",
                            "n_nationkey > 10 as m3",
                            "n_nationkey < 20 as m4"})
                       .partialAggregation(
                           {},
                           {"bool_and(m1)",
                            "bool_or(m2)",
                            "bool_or(m1)",
                            "bool_and(m1) FILTER (WHERE m3)",
                            "bool_or(m1) FILTER (WHERE m4)"})
                       .partitionedOutput()
                       .build();

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder()
                  .exchange()
                  .localPartition()
                  .finalAggregation()
                  .project()
                  .build();

    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .project(
              {"n_nationkey % 2 = 0 as m1",
               "n_regionkey % 2 = 0 as m2",
               "n_nationkey > 10 as m3",
               "n_nationkey < 20 as m4"})
          .singleAggregation(
              {},
              {"bool_and(m1) as agg1",
               "bool_or(m2) as agg2",
               "bool_or(m1) as agg3",
               "bool_and(m1) FILTER (WHERE m3) as agg4",
               "bool_or(m1) FILTER (WHERE m4) as agg5"})
          .project({"agg1", "agg2", "agg1", "agg3", "agg4", "agg5"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, orderNonSensitive) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {},
              {"sum(n_nationkey ORDER BY n_regionkey)",
               "sum(n_nationkey ORDER BY n_nationkey DESC, n_regionkey)",
               "count(n_regionkey ORDER BY n_nationkey)",
               "sum(n_nationkey ORDER BY n_regionkey) FILTER (WHERE n_nationkey > 10)",
               "count(n_regionkey ORDER BY n_nationkey) FILTER (WHERE n_nationkey < 20)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .project(
                           {"n_nationkey",
                            "n_regionkey",
                            "n_nationkey > 10 as m1",
                            "n_nationkey < 20 as m2"})
                       .singleAggregation(
                           {},
                           {"sum(n_nationkey) as agg1",
                            "count(n_regionkey) as agg2",
                            "sum(n_nationkey) FILTER (WHERE m1) as agg3",
                            "count(n_regionkey) FILTER (WHERE m2) as agg4"})
                       .project({"agg1", "agg1", "agg2", "agg3", "agg4"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4}).plan;
    const auto& fragments = plan->fragments();
    ASSERT_EQ(2, fragments.size());

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       .project(
                           {"n_nationkey",
                            "n_regionkey",
                            "n_nationkey > 10 as m1",
                            "n_nationkey < 20 as m2"})
                       .partialAggregation(
                           {},
                           {"sum(n_nationkey)",
                            "count(n_regionkey)",
                            "sum(n_nationkey) FILTER (WHERE m1)",
                            "count(n_regionkey) FILTER (WHERE m2)"})
                       .partitionedOutput()
                       .build();

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder()
                  .exchange()
                  .localPartition()
                  .finalAggregation()
                  .project()
                  .build();

    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));
  }

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", getSchema("nation"))
                           .project(
                               {"n_nationkey",
                                "n_regionkey",
                                "n_nationkey > 10 as m1",
                                "n_nationkey < 20 as m2"})
                           .singleAggregation(
                               {},
                               {"sum(n_nationkey) as agg1",
                                "count(n_regionkey) as agg2",
                                "sum(n_nationkey) FILTER (WHERE m1) as agg3",
                                "count(n_regionkey) FILTER (WHERE m2) as agg4"})
                           .project({"agg1", "agg1", "agg2", "agg3", "agg4"})
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, ignoreDuplicatesXOrderNonSensitive) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(context)
          .tableScan("nation")
          .aggregate(
              {},
              {
                  "bool_and(DISTINCT n_nationkey % 2 = 0 ORDER BY n_regionkey)",
                  "bool_or(DISTINCT n_nationkey % 2 = 0 ORDER BY n_regionkey DESC, n_nationkey)",
                  "bool_and(n_nationkey % 2 = 0 ORDER BY n_regionkey)",
                  "bool_and(DISTINCT n_nationkey % 2 = 0 ORDER BY n_regionkey) FILTER (WHERE n_nationkey > 10)",
              })
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project({"n_nationkey % 2 = 0 as m1", "n_nationkey > 10 as m2"})
            .singleAggregation(
                {},
                {"bool_and(m1) as agg1",
                 "bool_or(m1) as agg2",
                 "bool_and(m1) FILTER (WHERE m2) as agg3"})
            .project({"agg1", "agg2", "agg1", "agg3"})
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  {
    auto plan = planVelox(logicalPlan).plan;
    const auto& fragments = plan->fragments();
    ASSERT_EQ(2, fragments.size());

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project({"n_nationkey % 2 = 0 as m1", "n_nationkey > 10 as m2"})
            .partialAggregation(
                {},
                {
                    "bool_and(m1)",
                    "bool_or(m1)",
                    "bool_and(m1) FILTER (WHERE m2)",
                })
            .partitionedOutput()
            .build();

    ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));

    matcher = core::PlanMatcherBuilder()
                  .exchange()
                  .localPartition()
                  .finalAggregation()
                  .project()
                  .build();

    ASSERT_TRUE(matcher->match(fragments.at(1).fragment.planNode));
  }

  auto referencePlan = exec::test::PlanBuilder()
                           .tableScan("nation", getSchema("nation"))
                           .project(
                               {"n_nationkey % 2 = 0 as m1",
                                "n_nationkey",
                                "n_regionkey",
                                "n_nationkey > 10 as m2",
                                "n_nationkey < 20 as m3"})
                           .singleAggregation(
                               {},
                               {
                                   "bool_and(m1) as agg1",
                                   "bool_or(m1) as agg2",
                                   "bool_and(m1) FILTER (WHERE m2) as agg3",
                               })
                           .project({"agg1", "agg2", "agg1", "agg3"})
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer
