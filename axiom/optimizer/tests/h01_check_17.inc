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
void h01DefineCheckers17() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(17, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3  Build )*H left (lineitem t5*H exists (part t3
    // Build ) SINGLE agg project 2 columns   Build ) filter 1 exprs  SINGLE agg
    // project 1 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiFilter)
              .singleAggregation(
                  {"\"l_partkey_1\""}, {"avg(\"l_quantity_4\") AS avg"})
              .project(
                  {"\"l_partkey_1\" AS dt4___gk6",
                   "multiply(\"avg\",0.2) AS expr"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kLeft)
              .filter("lt(\"l_quantity\",\"expr\")")
              .singleAggregation({}, {"sum(\"l_extendedprice\") AS sum"})
              .project({"divide(\"sum\",7) AS avg_yearly"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(17, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3  Build )*H left (lineitem t5*H exists (part t3
    // Build ) PARTIAL agg FINAL agg project 2 columns   Build ) filter 1 exprs
    // PARTIAL agg FINAL agg project 1 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("part").build();

      auto rightMatcher2 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiFilter)
              .partialAggregation(
                  {"\"l_partkey_1\""}, {"avg(\"l_quantity_4\") AS avg"})
              .localPartition()
              .finalAggregation({"\"l_partkey_1\""}, {"avg(\"avg\") AS avg"})
              .project(
                  {"\"l_partkey_1\" AS dt4___gk6",
                   "multiply(\"avg\",0.2) AS expr"})
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kLeft)
              .filter("lt(\"l_quantity\",\"expr\")")
              .partialAggregation({}, {"sum(\"l_extendedprice\") AS sum"})
              .localPartition()
              .finalAggregation({}, {"sum(\"sum\") AS sum"})
              .project({"divide(\"sum\",7) AS avg_yearly"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(17, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3 broadcast   Build )*H left (lineitem t5*H exists
    // (part t3 broadcast   Build ) PARTIAL agg repartition  FINAL agg project 2
    // columns  broadcast   Build ) filter 1 exprs  PARTIAL agg gather  FINAL
    // agg project 1 columns

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
                         .tableScan("part")
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
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
              .partialAggregation(
                  {"\"l_partkey_1\""}, {"avg(\"l_quantity_4\") AS avg"})
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
              .exchange()
              .finalAggregation({"\"l_partkey_1\""}, {"avg(\"avg\") AS avg"})
              .project(
                  {"\"l_partkey_1\" AS dt4___gk6",
                   "multiply(\"avg\",0.2) AS expr"})
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
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeft)
              .filter("lt(\"l_quantity\",\"expr\")")
              .partialAggregation({}, {"sum(\"l_extendedprice\") AS sum"})
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
                         .finalAggregation({}, {"sum(\"sum\") AS sum"})
                         .project({"divide(\"sum\",7) AS avg_yearly"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(17, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3 broadcast   Build )*H left (lineitem t5*H exists
    // (part t3 broadcast   Build ) PARTIAL agg repartition  FINAL agg project 2
    // columns  broadcast   Build ) filter 1 exprs  PARTIAL agg gather  FINAL
    // agg project 1 columns

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
                         .tableScan("part")
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
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
              .partialAggregation(
                  {"\"l_partkey_1\""}, {"avg(\"l_quantity_4\") AS avg"})
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
              .exchange()
              .localPartition()
              .finalAggregation({"\"l_partkey_1\""}, {"avg(\"avg\") AS avg"})
              .project(
                  {"\"l_partkey_1\" AS dt4___gk6",
                   "multiply(\"avg\",0.2) AS expr"})
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
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeft)
              .filter("lt(\"l_quantity\",\"expr\")")
              .partialAggregation({}, {"sum(\"l_extendedprice\") AS sum"})
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
                         .finalAggregation({}, {"sum(\"sum\") AS sum"})
                         .project({"divide(\"sum\",7) AS avg_yearly"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
  });
}
