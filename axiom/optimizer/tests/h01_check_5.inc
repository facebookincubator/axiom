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
void h01DefineCheckers5() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(5, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2  Build )*H  (nation t6*H
    // (region t7  Build ) project 2 columns   Build ) project 4 columns   Build
    // )*H  (supplier t5  Build ) project 2 columns  SINGLE agg order by 1
    // columns  project 2 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("customer").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("supplier").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .singleAggregation(
                  {"\"n_name\""}, {"sum(\"dt1___p92\") AS revenue"})
              .orderBy({"\"revenue\" DESC NULLS LAST"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(5, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2  Build )*H  (nation t6*H
    // (region t7  Build ) project 2 columns   Build ) project 4 columns   Build
    // )*H  (supplier t5  Build ) project 2 columns  PARTIAL agg FINAL agg order
    // by 1 columns  project 2 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("customer").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("region").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("nation")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder().tableScan("supplier").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher3, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\""}, {"sum(\"dt1___p92\") AS revenue"})
              .localPartition()
              .finalAggregation({"\"n_name\""}, {"sum(\"revenue\") AS revenue"})
              .orderBy({"\"revenue\" DESC NULLS LAST"})
              .localMerge()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(5, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2 broadcast   Build )*H  (nation
    // t6*H  (region t7 broadcast   Build ) project 2 columns  broadcast   Build
    // ) project 4 columns  broadcast   Build )*H  (supplier t5 broadcast Build
    // ) project 2 columns  PARTIAL agg repartition  FINAL agg order by 1
    // columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
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

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\""}, {"sum(\"dt1___p92\") AS revenue"})
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
              .finalAggregation({"\"n_name\""}, {"sum(\"revenue\") AS revenue"})
              .orderBy({"\"revenue\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(5, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2 broadcast   Build )*H  (nation
    // t6*H  (region t7 broadcast   Build ) project 2 columns  broadcast   Build
    // ) project 4 columns  broadcast   Build )*H  (supplier t5 broadcast Build
    // ) project 2 columns  PARTIAL agg repartition  FINAL agg order by 1
    // columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
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

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
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

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .project(
                  {"\"n_name\" AS n_name",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"})
              .partialAggregation(
                  {"\"n_name\""}, {"sum(\"dt1___p92\") AS revenue"})
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
              .finalAggregation({"\"n_name\""}, {"sum(\"revenue\") AS revenue"})
              .orderBy({"\"revenue\" DESC NULLS LAST"})
              .localMerge()
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
    // Fragment 7
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
  });
}
