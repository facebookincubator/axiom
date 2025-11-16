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
void h01DefineCheckers21() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(21, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 project 2 columns *H right exists-flag (lineitem t9 project 2
    // columns *H right exists-flag (lineitem t3*H  (supplier t2*H  (nation t5
    // Build ) project 2 columns   Build )*H  (orders t4  Build )  Build )
    // filter 1 exprs   Build ) filter 1 exprs  SINGLE agg order by 2 columns
    // project 2 columns

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
          core::PlanMatcherBuilder().tableScan("orders").build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher3, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark1\")")
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher4, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark0\"")
              .singleAggregation({"\"s_name\""}, {"count() AS numwait"})
              .topN(100)
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(21, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 project 2 columns *H right exists-flag (lineitem t9 project 2
    // columns *H right exists-flag (lineitem t3*H  (supplier t2*H  (nation t5
    // Build ) project 2 columns   Build )*H  (orders t4  Build )  Build )
    // filter 1 exprs   Build ) filter 1 exprs  PARTIAL agg FINAL agg order by 2
    // columns  project 2 columns

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
          core::PlanMatcherBuilder().tableScan("orders").build();

      auto rightMatcher3 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
              .hashJoin(rightMatcher2, velox::core::JoinType::kInner)
              .build();

      auto rightMatcher4 =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher3, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark1\")")
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher4, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation({"\"s_name\""}, {"count() AS numwait"})
              .localPartition()
              .finalAggregation(
                  {"\"s_name\""}, {"count(\"numwait\") AS numwait"})
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
  setChecker(21, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 project 2 columns  repartition *H right exists-flag (lineitem
    // t9 project 2 columns  repartition *H right exists-flag (lineitem t3*H
    // (supplier t2*H  (nation t5 broadcast   Build ) project 2 columns
    // broadcast   Build )*H  (orders t4 broadcast   Build ) repartition   Build
    // ) filter 1 exprs  repartition   Build ) filter 1 exprs  PARTIAL agg
    // repartition  FINAL agg order by 2 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
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
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
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

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark1\")")
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
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation({"\"s_name\""}, {"count() AS numwait"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation(
                             {"\"s_name\""}, {"count(\"numwait\") AS numwait"})
                         .topN(100)
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
          core::PlanMatcherBuilder().mergeExchange().finalLimit(0, 100).build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(21, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t7 project 2 columns  repartition *H right exists-flag (lineitem
    // t9 project 2 columns  repartition *H right exists-flag (lineitem t3*H
    // (supplier t2*H  (nation t5 broadcast   Build ) project 2 columns
    // broadcast   Build )*H  (orders t4 broadcast   Build ) repartition   Build
    // ) filter 1 exprs  repartition   Build ) filter 1 exprs  PARTIAL agg
    // repartition  FINAL agg order by 2 columns  project 2 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
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
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[3].fragment.planNode));
    }
    // Fragment 4
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("orders")
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

      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("lineitem")
                         .hashJoin(rightMatcher, velox::core::JoinType::kInner)
                         .hashJoin(rightMatcher1, velox::core::JoinType::kInner)
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .exchange()
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark1\")")
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
              .hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject)
              .filter("\"dt1___mark0\"")
              .partialAggregation({"\"s_name\""}, {"count() AS numwait"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[7].fragment.planNode));
    }
    // Fragment 8
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation(
                             {"\"s_name\""}, {"count(\"numwait\") AS numwait"})
                         .topN(100)
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
          core::PlanMatcherBuilder().mergeExchange().finalLimit(0, 100).build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[9].fragment.planNode));
    }
  });
}
