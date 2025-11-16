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
void h01DefineCheckers20() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(20, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 SINGLE agg project 3 columns *H right (partsupp t5*H
    // exists-flag (part t11 project 1 columns   Build ) filter 1 exprs *H
    // exists (supplier t2*H  (nation t3  Build ) project 1 columns   Build )
    // Build ) filter 1 exprs  project 1 columns *H right exists-flag (supplier
    // t2*H  (nation t3  Build )  Build ) filter 1 exprs  order by 1 columns
    // project 2 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project({"\"s_suppkey\" AS edt14_edt14_t2_s_suppkey"})
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt4___mark0\"")
              .hashJoin(rightMatcher2, velox::core::JoinType::kLeftSemiFilter)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher5 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .singleAggregation(
                  {"\"l_partkey\"", "\"l_suppkey\""},
                  {"sum(\"l_quantity\") AS sum"})
              .project(
                  {"\"l_partkey\" AS dt6___gk8",
                   "\"l_suppkey\" AS dt6___gk9",
                   "multiply(\"sum\",0.5) AS expr"})
              .hashJoin(rightMatcher3, velox::core::JoinType::kRight)
              .filter("lt(\"expr\",cast(\"ps_availqty\" as DOUBLE))")
              .project({"\"ps_suppkey\" AS ps_suppkey"})
              .hashJoin(rightMatcher5, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark1\"")
              .orderBy({"\"s_name\" ASC NULLS LAST"})
              .project({"\"s_name\" AS s_name", "\"s_address\" AS s_address"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(20, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 PARTIAL agg FINAL agg project 3 columns *H right (partsupp
    // t5*H exists-flag (part t11 project 1 columns   Build ) filter 1 exprs *H
    // exists (supplier t2*H  (nation t3  Build ) project 1 columns   Build )
    // Build ) filter 1 exprs  project 1 columns *H right exists-flag (supplier
    // t2*H  (nation t3  Build )  Build ) filter 1 exprs  order by 1 columns
    // project 2 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project({"\"s_suppkey\" AS edt14_edt14_t2_s_suppkey"})
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt4___mark0\"")
              .hashJoin(rightMatcher2, velox::core::JoinType::kLeftSemiFilter)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher5 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .partialAggregation(
                  {"\"l_partkey\"", "\"l_suppkey\""},
                  {"sum(\"l_quantity\") AS sum"})
              .localPartition()
              .finalAggregation(
                  {"\"l_partkey\"", "\"l_suppkey\""}, {"sum(\"sum\") AS sum"})
              .project(
                  {"\"l_partkey\" AS dt6___gk8",
                   "\"l_suppkey\" AS dt6___gk9",
                   "multiply(\"sum\",0.5) AS expr"})
              .hashJoin(rightMatcher3, velox::core::JoinType::kRight)
              .filter("lt(\"expr\",cast(\"ps_availqty\" as DOUBLE))")
              .project({"\"ps_suppkey\" AS ps_suppkey"})
              .hashJoin(rightMatcher5, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark1\"")
              .orderBy({"\"s_name\" ASC NULLS LAST"})
              .localMerge()
              .project({"\"s_name\" AS s_name", "\"s_address\" AS s_address"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(20, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 PARTIAL agg repartition  FINAL agg project 3 columns *H right
    // (partsupp t5*H exists-flag (part t11 project 1 columns  broadcast   Build
    // ) filter 1 exprs *H exists (supplier t2*H  (nation t3 broadcast   Build )
    // project 1 columns  broadcast   Build ) repartition   Build ) filter 1
    // exprs  project 1 columns  repartition *H right exists-flag (supplier t2*H
    // (nation t3 broadcast   Build ) repartition   Build ) filter 1 exprs order
    // by 1 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .partialAggregation(
                             {"\"l_partkey\"", "\"l_suppkey\""},
                             {"sum(\"l_quantity\") AS sum"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .project({"\"s_suppkey\" AS edt14_edt14_t2_s_suppkey"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt4___mark0\"")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiFilter)
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
              .exchange()
              .finalAggregation(
                  {"\"l_partkey\"", "\"l_suppkey\""}, {"sum(\"sum\") AS sum"})
              .project(
                  {"\"l_partkey\" AS dt6___gk8",
                   "\"l_suppkey\" AS dt6___gk9",
                   "multiply(\"sum\",0.5) AS expr"})
              .hashJoin(rightMatcher, velox::core::JoinType::kRight)
              .filter("lt(\"expr\",cast(\"ps_availqty\" as DOUBLE))")
              .project({"\"ps_suppkey\" AS ps_suppkey"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark1\"")
              .orderBy({"\"s_name\" ASC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[8].fragment.planNode));
    }
    // Fragment 9
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .mergeExchange()
              .project({"\"s_name\" AS s_name", "\"s_address\" AS s_address"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(20, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 PARTIAL agg repartition  FINAL agg project 3 columns *H right
    // (partsupp t5*H exists-flag (part t11 project 1 columns  broadcast   Build
    // ) filter 1 exprs *H exists (supplier t2*H  (nation t3 broadcast   Build )
    // project 1 columns  broadcast   Build ) repartition   Build ) filter 1
    // exprs  project 1 columns  repartition *H right exists-flag (supplier t2*H
    // (nation t3 broadcast   Build ) repartition   Build ) filter 1 exprs order
    // by 1 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .partialAggregation(
                             {"\"l_partkey\"", "\"l_suppkey\""},
                             {"sum(\"l_quantity\") AS sum"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .project({"\"s_suppkey\" AS edt14_edt14_t2_s_suppkey"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt4___mark0\"")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiFilter)
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
              .exchange()
              .localPartition()
              .finalAggregation(
                  {"\"l_partkey\"", "\"l_suppkey\""}, {"sum(\"sum\") AS sum"})
              .project(
                  {"\"l_partkey\" AS dt6___gk8",
                   "\"l_suppkey\" AS dt6___gk9",
                   "multiply(\"sum\",0.5) AS expr"})
              .hashJoin(rightMatcher, velox::core::JoinType::kRight)
              .filter("lt(\"expr\",cast(\"ps_availqty\" as DOUBLE))")
              .project({"\"ps_suppkey\" AS ps_suppkey"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("supplier")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark1\"")
              .orderBy({"\"s_name\" ASC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[8].fragment.planNode));
    }
    // Fragment 9
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .mergeExchange()
              .project({"\"s_name\" AS s_name", "\"s_address\" AS s_address"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
  });
}
