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
void h01DefineCheckers9() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(9, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // orders t6*H  (lineitem t4*H  (partsupp t5*H  (part t2  Build ) project 3
    // columns   Build ) project 7 columns   Build )*H  (supplier t3  Build )*H
    // (nation t7  Build ) project 3 columns  SINGLE agg order by 2 columns
    // project 3 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder().tableScan("supplier").build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "year(\"o_orderdate\") AS o_year",
                   "minus(multiply(\"l_extendedprice\",minus(1,\"l_discount\")),multiply(\"l_quantity\",\"ps_supplycost\")) AS dt1___p89"})
              .singleAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"dt1___p89\") AS sum_profit"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST", "\"o_year\" DESC NULLS LAST"})
              .project(
                  {"\"n_name\" AS nation",
                   "\"o_year\" AS o_year",
                   "\"sum_profit\" AS sum_profit"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(9, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // supplier t3*H  (orders t6*H  (lineitem t4*H  (partsupp t5*H  (part t2
    // Build ) project 3 columns   Build ) project 6 columns   Build ) project 6
    // columns   Build )*H  (nation t7  Build ) project 3 columns  PARTIAL agg
    // FINAL agg order by 2 columns  project 3 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("partsupp")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "year(\"o_orderdate\") AS o_year",
                   "minus(multiply(\"l_extendedprice\",minus(1,\"l_discount\")),multiply(\"l_quantity\",\"ps_supplycost\")) AS dt1___p89"})
              .partialAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"dt1___p89\") AS sum_profit"})
              .localPartition()
              .finalAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"sum_profit\") AS sum_profit"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST", "\"o_year\" DESC NULLS LAST"})
              .localMerge()
              .project(
                  {"\"n_name\" AS nation",
                   "\"o_year\" AS o_year",
                   "\"sum_profit\" AS sum_profit"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(9, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // supplier t3*H  (orders t6*H  (lineitem t4*H  (partsupp t5*H  (part t2
    // broadcast   Build ) project 3 columns  broadcast   Build ) project 6
    // columns  broadcast   Build ) project 6 columns  broadcast   Build )*H
    // (nation t7 broadcast   Build ) project 3 columns  PARTIAL agg repartition
    // FINAL agg order by 2 columns  project 3 columns

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
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
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

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
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
                         .tableScan("orders")
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
                         .tableScan("nation")
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

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "year(\"o_orderdate\") AS o_year",
                   "minus(multiply(\"l_extendedprice\",minus(1,\"l_discount\")),multiply(\"l_quantity\",\"ps_supplycost\")) AS dt1___p89"})
              .partialAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"dt1___p89\") AS sum_profit"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .finalAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"sum_profit\") AS sum_profit"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST", "\"o_year\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .project(
                             {"\"n_name\" AS nation",
                              "\"o_year\" AS o_year",
                              "\"sum_profit\" AS sum_profit"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(9, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // supplier t3*H  (orders t6*H  (lineitem t4*H  (partsupp t5*H  (part t2
    // broadcast   Build ) project 3 columns  broadcast   Build ) project 6
    // columns  broadcast   Build ) project 6 columns  broadcast   Build )*H
    // (nation t7 broadcast   Build ) project 3 columns  PARTIAL agg repartition
    // FINAL agg order by 2 columns  project 3 columns

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
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("partsupp")
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

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
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
                         .tableScan("orders")
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
                         .tableScan("nation")
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

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "year(\"o_orderdate\") AS o_year",
                   "minus(multiply(\"l_extendedprice\",minus(1,\"l_discount\")),multiply(\"l_quantity\",\"ps_supplycost\")) AS dt1___p89"})
              .partialAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"dt1___p89\") AS sum_profit"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .localPartition()
              .finalAggregation(
                  {"\"n_name\"", "\"o_year\""},
                  {"sum(\"sum_profit\") AS sum_profit"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST", "\"o_year\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .project(
                             {"\"n_name\" AS nation",
                              "\"o_year\" AS o_year",
                              "\"sum_profit\" AS sum_profit"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });
}
