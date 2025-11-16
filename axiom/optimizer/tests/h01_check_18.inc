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
void h01DefineCheckers18() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(18, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2  Build ) project 5 columns
    // Build )*H exists-flag (lineitem t6*H exists (orders t3*H  (customer t2
    // Build ) project 1 columns   Build ) SINGLE agg filter 1 exprs  project 1
    // columns   Build ) filter 1 exprs  SINGLE agg order by 2 columns  project
    // 6 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("customer").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder().tableScan("customer").build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .project({"\"o_orderkey\" AS edt13_edt13_t3_o_orderkey"})
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher3, velox::core::JoinType::kLeftSemiFilter)
              .singleAggregation(
                  {"\"l_orderkey_0\""}, {"sum(\"l_quantity_4\") AS sum"})
              .filter("gt(\"sum\",300)")
              .project({"\"l_orderkey_0\" AS l_orderkey_17"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher4, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt1___mark0\"")
              .singleAggregation(
                  {"\"c_name\"",
                   "\"c_custkey\"",
                   "\"o_orderkey\"",
                   "\"o_orderdate\"",
                   "\"o_totalprice\""},
                  {"sum(\"l_quantity\") AS sum_18"})
              .topN(100)
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(18, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2  Build ) project 5 columns
    // Build )*H exists-flag (lineitem t6 PARTIAL agg FINAL agg filter 1 exprs
    // project 1 columns   Build ) filter 1 exprs  PARTIAL agg FINAL agg order
    // by 2 columns  project 6 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder().tableScan("customer").build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .partialAggregation(
                  {"\"l_orderkey_0\""}, {"sum(\"l_quantity_4\") AS sum"})
              .localPartition()
              .finalAggregation({"\"l_orderkey_0\""}, {"sum(\"sum\") AS sum"})
              .filter("gt(\"sum\",300)")
              .project({"\"l_orderkey_0\" AS l_orderkey_17"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation(
                  {"\"c_name\"",
                   "\"c_custkey\"",
                   "\"o_orderkey\"",
                   "\"o_orderdate\"",
                   "\"o_totalprice\""},
                  {"sum(\"l_quantity\") AS sum_18"})
              .localPartition()
              .finalAggregation(
                  {"\"c_name\"",
                   "\"c_custkey\"",
                   "\"o_orderkey\"",
                   "\"o_orderdate\"",
                   "\"o_totalprice\""},
                  {"sum(\"sum_18\") AS sum_18"})
              .topN(100)
              .localMerge()
              .finalLimit(0, 100)
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(18, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2 broadcast   Build ) project 5
    // columns  broadcast   Build ) repartition *H exists-flag (lineitem t6
    // PARTIAL agg repartition  FINAL agg filter 1 exprs  project 1 columns
    // Build ) filter 1 exprs  PARTIAL agg repartition  FINAL agg order by 2
    // columns  project 6 columns

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
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
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
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .partialAggregation(
                  {"\"l_orderkey_0\""}, {"sum(\"l_quantity_4\") AS sum"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .exchange()
              .finalAggregation({"\"l_orderkey_0\""}, {"sum(\"sum\") AS sum"})
              .filter("gt(\"sum\",300)")
              .project({"\"l_orderkey_0\" AS l_orderkey_17"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation(
                  {"\"c_name\"",
                   "\"c_custkey\"",
                   "\"o_orderkey\"",
                   "\"o_orderdate\"",
                   "\"o_totalprice\""},
                  {"sum(\"l_quantity\") AS sum_18"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation(
                             {"\"c_name\"",
                              "\"c_custkey\"",
                              "\"o_orderkey\"",
                              "\"o_orderdate\"",
                              "\"o_totalprice\""},
                             {"sum(\"sum_18\") AS sum_18"})
                         .topN(100)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher =
          core::PlanMatcherBuilder().mergeExchange().finalLimit(0, 100).build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(18, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t4*H  (orders t3*H  (customer t2 broadcast   Build ) project 5
    // columns  broadcast   Build ) repartition *H exists-flag (lineitem t6
    // PARTIAL agg repartition  FINAL agg filter 1 exprs  project 1 columns
    // Build ) filter 1 exprs  PARTIAL agg repartition  FINAL agg order by 2
    // columns  project 6 columns

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
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
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
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .partialAggregation(
                  {"\"l_orderkey_0\""}, {"sum(\"l_quantity_4\") AS sum"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .exchange()
              .localPartition()
              .finalAggregation({"\"l_orderkey_0\""}, {"sum(\"sum\") AS sum"})
              .filter("gt(\"sum\",300)")
              .project({"\"l_orderkey_0\" AS l_orderkey_17"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation(
                  {"\"c_name\"",
                   "\"c_custkey\"",
                   "\"o_orderkey\"",
                   "\"o_orderdate\"",
                   "\"o_totalprice\""},
                  {"sum(\"l_quantity\") AS sum_18"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[4].fragment.planNode));
    }
    // Fragment 5
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation(
                             {"\"c_name\"",
                              "\"c_custkey\"",
                              "\"o_orderkey\"",
                              "\"o_orderdate\"",
                              "\"o_totalprice\""},
                             {"sum(\"sum_18\") AS sum_18"})
                         .topN(100)
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher =
          core::PlanMatcherBuilder().mergeExchange().finalLimit(0, 100).build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });
}
