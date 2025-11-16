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
void h01DefineCheckers10() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(10, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (customer t2*H  (orders t3  Build )*H  (nation t5  Build )
    // project 8 columns   Build ) project 8 columns  SINGLE agg order by 1
    // columns  project 8 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("orders").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"c_phone\" AS c_phone",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_comment\" AS c_comment",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p77"})
              .singleAggregation(
                  {"\"c_custkey\"",
                   "\"c_name\"",
                   "\"c_acctbal\"",
                   "\"c_phone\"",
                   "\"n_name\"",
                   "\"c_address\"",
                   "\"c_comment\""},
                  {"sum(\"dt1___p77\") AS revenue"})
              .topN(20)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"revenue\" AS revenue",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_phone\" AS c_phone",
                   "\"c_comment\" AS c_comment"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(10, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (customer t2*H  (orders t3  Build )*H  (nation t5  Build )
    // project 8 columns   Build ) project 8 columns  PARTIAL agg FINAL agg
    // order by 1 columns  project 8 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("orders").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder().tableScan("nation").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"c_phone\" AS c_phone",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_comment\" AS c_comment",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p77"})
              .partialAggregation(
                  {"\"c_custkey\"",
                   "\"c_name\"",
                   "\"c_acctbal\"",
                   "\"c_phone\"",
                   "\"n_name\"",
                   "\"c_address\"",
                   "\"c_comment\""},
                  {"sum(\"dt1___p77\") AS revenue"})
              .localPartition()
              .finalAggregation(
                  {"\"c_custkey\"",
                   "\"c_name\"",
                   "\"c_acctbal\"",
                   "\"c_phone\"",
                   "\"n_name\"",
                   "\"c_address\"",
                   "\"c_comment\""},
                  {"sum(\"revenue\") AS revenue"})
              .topN(20)
              .localMerge()
              .finalLimit(0, 20)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"revenue\" AS revenue",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_phone\" AS c_phone",
                   "\"c_comment\" AS c_comment"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(10, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (customer t2*H  (orders t3 broadcast   Build )*H  (nation
    // t5 broadcast   Build ) project 8 columns  broadcast   Build ) project 8
    // columns  PARTIAL agg repartition  FINAL agg order by 1 columns  project 8
    // columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"c_phone\" AS c_phone",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_comment\" AS c_comment",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p77"})
              .partialAggregation(
                  {"\"c_custkey\"",
                   "\"c_name\"",
                   "\"c_acctbal\"",
                   "\"c_phone\"",
                   "\"n_name\"",
                   "\"c_address\"",
                   "\"c_comment\""},
                  {"sum(\"dt1___p77\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation(
                             {"\"c_custkey\"",
                              "\"c_name\"",
                              "\"c_acctbal\"",
                              "\"c_phone\"",
                              "\"n_name\"",
                              "\"c_address\"",
                              "\"c_comment\""},
                             {"sum(\"revenue\") AS revenue"})
                         .topN(20)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .finalLimit(0, 20)
                         .project(
                             {"\"c_custkey\" AS c_custkey",
                              "\"c_name\" AS c_name",
                              "\"revenue\" AS revenue",
                              "\"c_acctbal\" AS c_acctbal",
                              "\"n_name\" AS n_name",
                              "\"c_address\" AS c_address",
                              "\"c_phone\" AS c_phone",
                              "\"c_comment\" AS c_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(10, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (customer t2*H  (orders t3 broadcast   Build )*H  (nation
    // t5 broadcast   Build ) project 8 columns  broadcast   Build ) project 8
    // columns  PARTIAL agg repartition  FINAL agg order by 1 columns  project 8
    // columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("nation")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto rightMatcher1 = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
    // Fragment 3
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .project(
                  {"\"c_custkey\" AS c_custkey",
                   "\"c_name\" AS c_name",
                   "\"c_acctbal\" AS c_acctbal",
                   "\"c_phone\" AS c_phone",
                   "\"n_name\" AS n_name",
                   "\"c_address\" AS c_address",
                   "\"c_comment\" AS c_comment",
                   "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p77"})
              .partialAggregation(
                  {"\"c_custkey\"",
                   "\"c_name\"",
                   "\"c_acctbal\"",
                   "\"c_phone\"",
                   "\"n_name\"",
                   "\"c_address\"",
                   "\"c_comment\""},
                  {"sum(\"dt1___p77\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation(
                             {"\"c_custkey\"",
                              "\"c_name\"",
                              "\"c_acctbal\"",
                              "\"c_phone\"",
                              "\"n_name\"",
                              "\"c_address\"",
                              "\"c_comment\""},
                             {"sum(\"revenue\") AS revenue"})
                         .topN(20)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .mergeExchange()
                         .finalLimit(0, 20)
                         .project(
                             {"\"c_custkey\" AS c_custkey",
                              "\"c_name\" AS c_name",
                              "\"revenue\" AS revenue",
                              "\"c_acctbal\" AS c_acctbal",
                              "\"n_name\" AS n_name",
                              "\"c_address\" AS c_address",
                              "\"c_phone\" AS c_phone",
                              "\"c_comment\" AS c_comment"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
  });
}
