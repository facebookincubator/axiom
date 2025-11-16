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
void h01DefineCheckers6() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(6, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2 project 1 columns  SINGLE agg project 1 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"multiply(\"l_extendedprice\",\"l_discount\") AS dt1___p48"})
              .singleAggregation({}, {"sum(\"dt1___p48\") AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(6, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2 project 1 columns  PARTIAL agg FINAL agg project 1 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"multiply(\"l_extendedprice\",\"l_discount\") AS dt1___p48"})
              .partialAggregation({}, {"sum(\"dt1___p48\") AS revenue"})
              .localPartition()
              .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(6, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2 project 1 columns  PARTIAL agg gather  FINAL agg project 1
    // columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"multiply(\"l_extendedprice\",\"l_discount\") AS dt1___p48"})
              .partialAggregation({}, {"sum(\"dt1___p48\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(6, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2 project 1 columns  PARTIAL agg gather  FINAL agg project 1
    // columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .project(
                  {"multiply(\"l_extendedprice\",\"l_discount\") AS dt1___p48"})
              .partialAggregation({}, {"sum(\"dt1___p48\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
  });
}
