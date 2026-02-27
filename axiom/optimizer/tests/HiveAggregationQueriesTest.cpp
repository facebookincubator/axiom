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

    // Verify mask is NOT present in final aggregation.
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .project({"n_nationkey > 10 as mask", "n_nationkey", "n_regionkey"})
            .partialAggregation(
                {}, {"sum(n_nationkey) FILTER (mask)", "avg(n_regionkey)"})
            .shuffle()
            .localPartition()
            .finalAggregation({}, {"sum(sum)", "avg(avg)"})
            .build();

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
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

    auto distributedPlan = planVelox(logicalPlan);
    matcher = core::PlanMatcherBuilder()
                  .tableScan("nation")
                  .partialAggregation({"n_regionkey"}, {})
                  .shuffle()
                  .localPartition()
                  .finalAggregation({"n_regionkey"}, {})
                  .partialAggregation({}, {"count(n_regionkey)"})
                  .shuffle()
                  .localPartition()
                  .finalAggregation({}, {"count(count)"})
                  .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
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

  auto distributedPlan = planVelox(logicalPlan);
  matcher = core::PlanMatcherBuilder()
                .tableScan("nation")
                .shuffle()
                .localPartition()
                .singleAggregation(
                    {"n_regionkey"},
                    {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
                     "array_agg(n_name ORDER BY n_nationkey)"})
                .shuffle()
                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);

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

  auto distributedPlan = planVelox(logicalPlan);
  matcher = core::PlanMatcherBuilder()
                .tableScan("nation")
                .project()
                .shuffle()
                .localPartition()
                .singleAggregation()
                .shuffle()
                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);

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
                       .shuffle()
                       .localPartition()
                       .finalAggregation()
                       .project()
                       .build();

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
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
                       .shuffle()
                       .localPartition()
                       .finalAggregation()
                       .project()
                       .build();

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
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
            .shuffle()
            .localPartition()
            .finalAggregation()
            .project()
            .build();

    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
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

TEST_F(HiveAggregationQueriesTest, rollup) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context)
                         .tableScan("nation")
                         .rollup(
                             {"n_regionkey"},
                             {"count(1) as cnt", "sum(n_nationkey) as total"},
                             "$grouping_set_id")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan()
          .groupId({{"n_regionkey"}, {}}, {"n_nationkey"}, "$grouping_set_id")
          .singleAggregation(
              {"n_regionkey", "\"$grouping_set_id\""},
              {"count(1) as cnt", "sum(n_nationkey) as total"})
          .project({"n_regionkey", "cnt", "total", "\"$grouping_set_id\""})
          .build();
  ASSERT_TRUE(matcher->match(plan));

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .groupId({"n_regionkey"}, {{"n_regionkey"}, {}}, {"n_nationkey"})
          .singleAggregation(
              {"n_regionkey", "group_id"},
              {"count(1) as cnt", "sum(n_nationkey) as total"})
          .project({"n_regionkey", "cnt", "total", "group_id"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, rollupMultiKey) {
  auto logicalPlan = parseSelect(
      "SELECT n_regionkey, n_name, count(1) as cnt "
      "FROM nation "
      "GROUP BY ROLLUP(n_regionkey, n_name)");

  // ROLLUP(a, b) produces 3 grouping sets: {a, b}, {a}, {}.
  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .groupId(
              {"n_regionkey", "n_name"},
              {{"n_regionkey", "n_name"}, {"n_regionkey"}, {}},
              {"n_nationkey"})
          .singleAggregation(
              {"n_regionkey", "n_name", "group_id"}, {"count(1) as cnt"})
          .project({"n_regionkey", "n_name", "cnt", "group_id"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, cube) {
  auto logicalPlan = parseSelect(
      "SELECT n_regionkey, n_name, count(1) as cnt "
      "FROM nation "
      "GROUP BY CUBE(n_regionkey, n_name)");

  // CUBE(a, b) produces 4 grouping sets: {a, b}, {a}, {b}, {}.
  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .groupId(
              {"n_regionkey", "n_name"},
              {{"n_regionkey", "n_name"}, {"n_regionkey"}, {"n_name"}, {}},
              {"n_nationkey"})
          .singleAggregation(
              {"n_regionkey", "n_name", "group_id"}, {"count(1) as cnt"})
          .project({"n_regionkey", "n_name", "cnt", "group_id"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, groupingSets) {
  auto logicalPlan = parseSelect(
      "SELECT n_regionkey, n_name, sum(n_nationkey) as total "
      "FROM nation "
      "GROUP BY GROUPING SETS ((n_regionkey), (n_name))");

  // Explicit grouping sets: {n_regionkey}, {n_name}.
  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("nation", getSchema("nation"))
          .groupId(
              {"n_regionkey", "n_name"},
              {{"n_regionkey"}, {"n_name"}},
              {"n_nationkey"})
          .singleAggregation(
              {"n_regionkey", "n_name", "group_id"},
              {"sum(n_nationkey) as total"})
          .project({"n_regionkey", "n_name", "total", "group_id"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(HiveAggregationQueriesTest, rollupWithGrouping) {
  auto logicalPlan = parseSelect(
      "SELECT n_regionkey, count(1) as cnt, grouping(n_regionkey) as grp "
      "FROM nation "
      "GROUP BY ROLLUP(n_regionkey)");

  // ROLLUP(a) produces 2 grouping sets: {a}, {}.
  // GROUPING(n_regionkey) returns 0 when present, 1 when aggregated.
  auto referencePlan =
      exec::test::PlanBuilder(pool_.get())
          .tableScan("nation", getSchema("nation"))
          .groupId({"n_regionkey"}, {{"n_regionkey"}, {}}, {"n_nationkey"})
          .singleAggregation({"n_regionkey", "group_id"}, {"count(1) as cnt"})
          .project(
              {"n_regionkey",
               "cnt",
               "cast(element_at(array[0, 1], group_id + 1) as bigint) as grp"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer
