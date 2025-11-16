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
void h01DefineCheckers7() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(7, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t3*H  (orders t4*H  (customer t5*H  (nation t7  Build ) project
    // 2 columns   Build ) project 2 columns   Build )*H  (supplier t2*H (nation
    // t6  Build ) project 2 columns   Build ) filter 1 exprs  project 4 columns
    // SINGLE agg order by 3 columns  project 4 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(eq(\"n_name\",'FRANCE'),eq(\"n_name_1\",'GERMANY')),\"and\"(eq(\"n_name\",'GERMANY'),eq(\"n_name_1\",'FRANCE')))")
              .project(
                  {"\"n_name\" AS n_name",
                   "\"n_name_1\" AS n_name_1",
                   "year(\"l_shipdate\") AS l_year",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .singleAggregation(
                  {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                  {"sum(\"dt1___p92\") AS revenue"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST",
                   "\"n_name_1\" ASC NULLS LAST",
                   "\"l_year\" ASC NULLS LAST"})
              .project(
                  {"\"n_name\" AS supp_nation",
                   "\"n_name_1\" AS cust_nation",
                   "\"l_year\" AS l_year",
                   "\"revenue\" AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(7, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t3*H  (orders t4*H  (customer t5*H  (nation t7  Build ) project
    // 2 columns   Build ) project 2 columns   Build )*H  (supplier t2*H (nation
    // t6  Build ) project 2 columns   Build ) filter 1 exprs  project 4 columns
    // PARTIAL agg FINAL agg order by 3 columns  project 4 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("supplier")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(eq(\"n_name\",'FRANCE'),eq(\"n_name_1\",'GERMANY')),\"and\"(eq(\"n_name\",'GERMANY'),eq(\"n_name_1\",'FRANCE')))")
              .project(
                  {"\"n_name\" AS n_name",
                   "\"n_name_1\" AS n_name_1",
                   "year(\"l_shipdate\") AS l_year",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                  {"sum(\"dt1___p92\") AS revenue"})
              .localPartition()
              .finalAggregation(
                  {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                  {"sum(\"revenue\") AS revenue"})
              .orderBy(
                  {"\"n_name\" ASC NULLS LAST",
                   "\"n_name_1\" ASC NULLS LAST",
                   "\"l_year\" ASC NULLS LAST"})
              .localMerge()
              .project(
                  {"\"n_name\" AS supp_nation",
                   "\"n_name_1\" AS cust_nation",
                   "\"l_year\" AS l_year",
                   "\"revenue\" AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(7, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t3*H  (orders t4*H  (customer t5*H  (nation t7 broadcast   Build
    // ) project 2 columns  broadcast   Build ) project 2 columns  broadcast
    // Build )*H  (supplier t2*H  (nation t6 broadcast   Build ) project 2
    // columns  broadcast   Build ) filter 1 exprs  project 4 columns  PARTIAL
    // agg repartition  FINAL agg order by 3 columns  project 4 columns

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
                         .tableScan("customer")
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
                         .tableScan("orders")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(eq(\"n_name\",'FRANCE'),eq(\"n_name_1\",'GERMANY')),\"and\"(eq(\"n_name\",'GERMANY'),eq(\"n_name_1\",'FRANCE')))")
              .project(
                  {"\"n_name\" AS n_name",
                   "\"n_name_1\" AS n_name_1",
                   "year(\"l_shipdate\") AS l_year",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                  {"sum(\"dt1___p92\") AS revenue"})
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
                         .finalAggregation(
                             {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                             {"sum(\"revenue\") AS revenue"})
                         .orderBy(
                             {"\"n_name\" ASC NULLS LAST",
                              "\"n_name_1\" ASC NULLS LAST",
                              "\"l_year\" ASC NULLS LAST"})
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
                             {"\"n_name\" AS supp_nation",
                              "\"n_name_1\" AS cust_nation",
                              "\"l_year\" AS l_year",
                              "\"revenue\" AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(7, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t3*H  (orders t4*H  (customer t5*H  (nation t7 broadcast   Build
    // ) project 2 columns  broadcast   Build ) project 2 columns  broadcast
    // Build )*H  (supplier t2*H  (nation t6 broadcast   Build ) project 2
    // columns  broadcast   Build ) filter 1 exprs  project 4 columns  PARTIAL
    // agg repartition  FINAL agg order by 3 columns  project 4 columns

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
                         .tableScan("customer")
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
                         .tableScan("orders")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(eq(\"n_name\",'FRANCE'),eq(\"n_name_1\",'GERMANY')),\"and\"(eq(\"n_name\",'GERMANY'),eq(\"n_name_1\",'FRANCE')))")
              .project(
                  {"\"n_name\" AS n_name",
                   "\"n_name_1\" AS n_name_1",
                   "year(\"l_shipdate\") AS l_year",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                  {"sum(\"dt1___p92\") AS revenue"})
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
                         .finalAggregation(
                             {"\"n_name\"", "\"n_name_1\"", "\"l_year\""},
                             {"sum(\"revenue\") AS revenue"})
                         .orderBy(
                             {"\"n_name\" ASC NULLS LAST",
                              "\"n_name_1\" ASC NULLS LAST",
                              "\"l_year\" ASC NULLS LAST"})
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
                             {"\"n_name\" AS supp_nation",
                              "\"n_name_1\" AS cust_nation",
                              "\"l_year\" AS l_year",
                              "\"revenue\" AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });
}
