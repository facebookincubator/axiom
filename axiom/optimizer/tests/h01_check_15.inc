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
void h01DefineCheckers15() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(15, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4 project 2 columns  SINGLE agg project 2 columns *H  (lineitem
    // t6 project 2 columns  SINGLE agg project 1 columns  SINGLE agg project 1
    // columns   Build )*H  (supplier t2  Build ) order by 1 columns  project 5
    // columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey_2\" AS l_suppkey_2",
                   "multiply(\"l_extendedprice_5\",minus(1,\"l_discount_6\")) AS dt5___p71"})
              .singleAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"dt5___p71\") AS total_revenue_17"})
              .project({"\"total_revenue_17\" AS total_revenue_19"})
              .singleAggregation({}, {"max(\"total_revenue_19\") AS max"})
              .build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("supplier").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey\" AS l_suppkey",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt3___p49"})
              .singleAggregation(
                  {"\"l_suppkey\""}, {"sum(\"dt3___p49\") AS total_revenue"})
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .orderBy({"\"s_suppkey\" ASC NULLS LAST"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(15, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4 project 2 columns  PARTIAL agg FINAL agg project 2 columns *H
    // (lineitem t6 project 2 columns  PARTIAL agg FINAL agg project 1 columns
    // PARTIAL agg FINAL agg project 1 columns   Build )*H  (supplier t2  Build
    // ) order by 1 columns  project 5 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey_2\" AS l_suppkey_2",
                   "multiply(\"l_extendedprice_5\",minus(1,\"l_discount_6\")) AS dt5___p71"})
              .partialAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"dt5___p71\") AS total_revenue_17"})
              .localPartition()
              .finalAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"total_revenue_17\") AS total_revenue_17"})
              .project({"\"total_revenue_17\" AS total_revenue_19"})
              .partialAggregation({}, {"max(\"total_revenue_19\") AS max"})
              .localPartition()
              .finalAggregation({}, {"max(\"max\") AS max"})
              .build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("supplier").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey\" AS l_suppkey",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt3___p49"})
              .partialAggregation(
                  {"\"l_suppkey\""}, {"sum(\"dt3___p49\") AS total_revenue"})
              .localPartition()
              .finalAggregation(
                  {"\"l_suppkey\""},
                  {"sum(\"total_revenue\") AS total_revenue"})
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .orderBy({"\"s_suppkey\" ASC NULLS LAST"})
              .localMerge()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(15, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4 project 2 columns  PARTIAL agg repartition  FINAL agg project
    // 2 columns *H  (lineitem t6 project 2 columns  PARTIAL agg repartition
    // FINAL agg project 1 columns  PARTIAL agg gather  FINAL agg project 1
    // columns  broadcast   Build )*H  (supplier t2 repartition   Build ) order
    // by 1 columns  project 5 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey\" AS l_suppkey",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt3___p49"})
              .partialAggregation(
                  {"\"l_suppkey\""}, {"sum(\"dt3___p49\") AS total_revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey_2\" AS l_suppkey_2",
                   "multiply(\"l_extendedprice_5\",minus(1,\"l_discount_6\")) AS dt5___p71"})
              .partialAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"dt5___p71\") AS total_revenue_17"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .finalAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"total_revenue_17\") AS total_revenue_17"})
              .project({"\"total_revenue_17\" AS total_revenue_19"})
              .partialAggregation({}, {"max(\"total_revenue_19\") AS max"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation({}, {"max(\"max\") AS max"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation(
                             {"\"l_suppkey\""},
                             {"sum(\"total_revenue\") AS total_revenue"})
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .orderBy({"\"s_suppkey\" ASC NULLS LAST"})
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(15, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4 project 2 columns  PARTIAL agg repartition  FINAL agg project
    // 2 columns *H  (lineitem t6 project 2 columns  PARTIAL agg repartition
    // FINAL agg project 1 columns  PARTIAL agg gather  FINAL agg project 1
    // columns  broadcast   Build )*H  (supplier t2 repartition   Build ) order
    // by 1 columns  project 5 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey\" AS l_suppkey",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt3___p49"})
              .partialAggregation(
                  {"\"l_suppkey\""}, {"sum(\"dt3___p49\") AS total_revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"\"l_suppkey_2\" AS l_suppkey_2",
                   "multiply(\"l_extendedprice_5\",minus(1,\"l_discount_6\")) AS dt5___p71"})
              .partialAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"dt5___p71\") AS total_revenue_17"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .localPartition()
              .finalAggregation(
                  {"\"l_suppkey_2\""},
                  {"sum(\"total_revenue_17\") AS total_revenue_17"})
              .project({"\"total_revenue_17\" AS total_revenue_19"})
              .partialAggregation({}, {"max(\"total_revenue_19\") AS max"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation({}, {"max(\"max\") AS max"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation(
                             {"\"l_suppkey\""},
                             {"sum(\"total_revenue\") AS total_revenue"})
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .orderBy({"\"s_suppkey\" ASC NULLS LAST"})
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });
}
