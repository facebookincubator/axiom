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
void h01DefineCheckers2() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(2, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t4*H  (part t2  Build )*H  (supplier t3*H  (nation t5*H  (region
    // t6  Build ) project 2 columns   Build ) project 7 columns   Build )*H
    // left (supplier t9*H  (partsupp t8*H exists (part t2  Build ) project 3
    // columns   Build )*H  (nation t10*H  (region t11  Build ) project 1
    // columns   Build ) SINGLE agg project 1 columns   Build ) order by 4
    // columns  project 8 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher5 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher4, velox::core::JoinType::kLeftSemiFilter)
              .build();

      auto rightMatcher6 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher7 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher6, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher8 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher5, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher7, velox::core::JoinType::kInner)
              .singleAggregation(
                  {"\"ps_partkey_0\""}, {"min(\"ps_supplycost_3\") AS min"})
              .project({"\"ps_partkey_0\" AS dt7___gk12"})
              .build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher8, velox::core::JoinType::kLeft)
                         .topN(100)
                         .project(
                             {"\"s_acctbal\" AS s_acctbal",
                              "\"s_name\" AS s_name",
                              "\"n_name\" AS n_name",
                              "\"p_partkey\" AS p_partkey",
                              "\"p_mfgr\" AS p_mfgr",
                              "\"s_address\" AS s_address",
                              "\"s_phone\" AS s_phone",
                              "\"s_comment\" AS s_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(2, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t4*H  (part t2  Build )*H  (supplier t3*H  (nation t5*H  (region
    // t6  Build ) project 2 columns   Build ) project 7 columns   Build )*H
    // left (supplier t9*H  (partsupp t8*H exists (part t2  Build ) project 3
    // columns   Build )*H  (nation t10*H  (region t11  Build ) project 1
    // columns   Build ) PARTIAL agg FINAL agg project 1 columns   Build ) order
    // by 4 columns  project 8 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher5 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher4, velox::core::JoinType::kLeftSemiFilter)
              .build();

      auto rightMatcher6 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher7 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher6, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher8 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher5, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher7, velox::core::JoinType::kInner)
              .partialAggregation(
                  {"\"ps_partkey_0\""}, {"min(\"ps_supplycost_3\") AS min"})
              .localPartition()
              .finalAggregation({"\"ps_partkey_0\""}, {"min(\"min\") AS min"})
              .project({"\"ps_partkey_0\" AS dt7___gk12"})
              .build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher8, velox::core::JoinType::kLeft)
                         .topN(100)
                         .localMerge()
                         .finalLimit(0, 100)
                         .project(
                             {"\"s_acctbal\" AS s_acctbal",
                              "\"s_name\" AS s_name",
                              "\"n_name\" AS n_name",
                              "\"p_partkey\" AS p_partkey",
                              "\"p_mfgr\" AS p_mfgr",
                              "\"s_address\" AS s_address",
                              "\"s_phone\" AS s_phone",
                              "\"s_comment\" AS s_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(2, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t4*H  (part t2 broadcast   Build )*H  (supplier t3*H  (nation
    // t5*H  (region t6 broadcast   Build ) project 2 columns  broadcast   Build
    // ) project 7 columns  broadcast   Build )*H left (partsupp t8*H exists
    // (part t2 broadcast   Build )*H  (supplier t9*H  (nation t10*H  (region
    // t11 broadcast   Build ) project 1 columns  broadcast   Build ) project 1
    // columns  broadcast   Build ) PARTIAL agg repartition  FINAL agg project 1
    // columns  broadcast   Build ) order by 4 columns  project 8 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .partialAggregation(
                  {"\"ps_partkey_0\""}, {"min(\"ps_supplycost_3\") AS min"})
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
              .exchange()
              .finalAggregation({"\"ps_partkey_0\""}, {"min(\"min\") AS min"})
              .project({"\"ps_partkey_0\" AS dt7___gk12"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
    // Fragment 10
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher2 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher2, velox::core::JoinType::kLeft)
                         .topN(100)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[10].fragment.planNode));
    }
    // Fragment 11
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .finalLimit(0, 100)
                         .project(
                             {"\"s_acctbal\" AS s_acctbal",
                              "\"s_name\" AS s_name",
                              "\"n_name\" AS n_name",
                              "\"p_partkey\" AS p_partkey",
                              "\"p_mfgr\" AS p_mfgr",
                              "\"s_address\" AS s_address",
                              "\"s_phone\" AS s_phone",
                              "\"s_comment\" AS s_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[11].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(2, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // partsupp t4*H  (part t2 broadcast   Build )*H  (supplier t3*H  (nation
    // t5*H  (region t6 broadcast   Build ) project 2 columns  broadcast   Build
    // ) project 7 columns  broadcast   Build )*H left (partsupp t8*H exists
    // (part t2 broadcast   Build )*H  (supplier t9*H  (nation t10*H  (region
    // t11 broadcast   Build ) project 1 columns  broadcast   Build ) project 1
    // columns  broadcast   Build ) PARTIAL agg repartition  FINAL agg project 1
    // columns  broadcast   Build ) order by 4 columns  project 8 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .partialAggregation(
                  {"\"ps_partkey_0\""}, {"min(\"ps_supplycost_3\") AS min"})
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
              .exchange()
              .localPartition()
              .finalAggregation({"\"ps_partkey_0\""}, {"min(\"min\") AS min"})
              .project({"\"ps_partkey_0\" AS dt7___gk12"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
    // Fragment 10
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher2 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher2, velox::core::JoinType::kLeft)
                         .topN(100)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[10].fragment.planNode));
    }
    // Fragment 11
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .finalLimit(0, 100)
                         .project(
                             {"\"s_acctbal\" AS s_acctbal",
                              "\"s_name\" AS s_name",
                              "\"n_name\" AS n_name",
                              "\"p_partkey\" AS p_partkey",
                              "\"p_mfgr\" AS p_mfgr",
                              "\"s_address\" AS s_address",
                              "\"s_phone\" AS s_phone",
                              "\"s_comment\" AS s_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[11].fragment.planNode));
    }
  });
}
