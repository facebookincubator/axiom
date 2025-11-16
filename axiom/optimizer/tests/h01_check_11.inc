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
void h01DefineCheckers11() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(11, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t2*H  (supplier t3*H  (nation t4  Build ) project 1 columns
    // Build ) project 2 columns  SINGLE agg project 2 columns *M  (partsupp
    // t14*H  (supplier t15*H  (nation t16  Build ) project 1 columns   Build )
    // project 1 columns  SINGLE agg project 1 columns ) filter 1 exprs  order
    // by 1 columns  project 2 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .project(
                  {"multiply(\"ps_supplycost_3\",cast(\"ps_availqty_2\" as DOUBLE)) AS dt13___p65"})
              .singleAggregation({}, {"sum(\"dt13___p65\") AS sum"})
              .project({"multiply(\"sum\",0.0001) AS expr"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"ps_partkey\" AS ps_partkey",
                   "multiply(\"ps_supplycost\",cast(\"ps_availqty\" as DOUBLE)) AS dt1___p33"})
              .singleAggregation(
                  {"\"ps_partkey\""}, {"sum(\"dt1___p33\") AS value"})
              .nestedLoopJoin(rightMatcher4, velox::core::JoinType::kInner)
              .filter("gt(\"value\",\"expr\")")
              .orderBy({"\"value\" DESC NULLS LAST"})
              .project({"\"ps_partkey\" AS ps_partkey", "\"value\" AS value"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(11, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t2*H  (supplier t3*H  (nation t4  Build ) project 1 columns
    // Build ) project 2 columns  PARTIAL agg FINAL agg project 2 columns *M
    // (partsupp t14*H  (supplier t15*H  (nation t16  Build ) project 1 columns
    // Build ) project 1 columns  PARTIAL agg FINAL agg project 1 columns )
    // filter 1 exprs  order by 1 columns  project 2 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .project(
                  {"multiply(\"ps_supplycost_3\",cast(\"ps_availqty_2\" as DOUBLE)) AS dt13___p65"})
              .partialAggregation({}, {"sum(\"dt13___p65\") AS sum"})
              .localPartition()
              .finalAggregation({}, {"sum(\"sum\") AS sum"})
              .project({"multiply(\"sum\",0.0001) AS expr"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"ps_partkey\" AS ps_partkey",
                   "multiply(\"ps_supplycost\",cast(\"ps_availqty\" as DOUBLE)) AS dt1___p33"})
              .partialAggregation(
                  {"\"ps_partkey\""}, {"sum(\"dt1___p33\") AS value"})
              .localPartition()
              .finalAggregation({"\"ps_partkey\""}, {"sum(\"value\") AS value"})
              .nestedLoopJoin(rightMatcher4, velox::core::JoinType::kInner)
              .filter("gt(\"value\",\"expr\")")
              .orderBy({"\"value\" DESC NULLS LAST"})
              .localMerge()
              .project({"\"ps_partkey\" AS ps_partkey", "\"value\" AS value"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(11, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t2*H  (supplier t3*H  (nation t4 broadcast   Build ) project 1
    // columns  broadcast   Build ) project 2 columns  PARTIAL agg repartition
    // FINAL agg project 2 columns *M  (partsupp t14*H  (supplier t15*H  (nation
    // t16 broadcast   Build ) project 1 columns  broadcast   Build ) project 1
    // columns  PARTIAL agg gather  FINAL agg project 1 columns  broadcast )
    // filter 1 exprs  order by 1 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"\"ps_partkey\" AS ps_partkey",
                   "multiply(\"ps_supplycost\",cast(\"ps_availqty\" as DOUBLE)) AS dt1___p33"})
              .partialAggregation(
                  {"\"ps_partkey\""}, {"sum(\"dt1___p33\") AS value"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"multiply(\"ps_supplycost_3\",cast(\"ps_availqty_2\" as DOUBLE)) AS dt13___p65"})
              .partialAggregation({}, {"sum(\"dt13___p65\") AS sum"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation({}, {"sum(\"sum\") AS sum"})
                         .project({"multiply(\"sum\",0.0001) AS expr"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .finalAggregation({"\"ps_partkey\""}, {"sum(\"value\") AS value"})
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"value\",\"expr\")")
              .orderBy({"\"value\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .mergeExchange()
              .project({"\"ps_partkey\" AS ps_partkey", "\"value\" AS value"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[8].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(11, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t2*H  (supplier t3*H  (nation t4 broadcast   Build ) project 1
    // columns  broadcast   Build ) project 2 columns  PARTIAL agg repartition
    // FINAL agg project 2 columns *M  (partsupp t14*H  (supplier t15*H  (nation
    // t16 broadcast   Build ) project 1 columns  broadcast   Build ) project 1
    // columns  PARTIAL agg gather  FINAL agg project 1 columns  broadcast )
    // filter 1 exprs  order by 1 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"\"ps_partkey\" AS ps_partkey",
                   "multiply(\"ps_supplycost\",cast(\"ps_availqty\" as DOUBLE)) AS dt1___p33"})
              .partialAggregation(
                  {"\"ps_partkey\""}, {"sum(\"dt1___p33\") AS value"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"multiply(\"ps_supplycost_3\",cast(\"ps_availqty_2\" as DOUBLE)) AS dt13___p65"})
              .partialAggregation({}, {"sum(\"dt13___p65\") AS sum"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation({}, {"sum(\"sum\") AS sum"})
                         .project({"multiply(\"sum\",0.0001) AS expr"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .localPartition()
              .finalAggregation({"\"ps_partkey\""}, {"sum(\"value\") AS value"})
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"value\",\"expr\")")
              .orderBy({"\"value\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .mergeExchange()
              .project({"\"ps_partkey\" AS ps_partkey", "\"value\" AS value"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[8].fragment.planNode));
    }
  });
}
