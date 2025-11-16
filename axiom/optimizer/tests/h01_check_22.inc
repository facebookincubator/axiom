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
void h01DefineCheckers22() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(22, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // orders t6 project 1 columns *H right exists-flag (customer t2*M (customer
    // t4 SINGLE agg project 1 columns ) filter 1 exprs   Build ) filter 1 exprs
    // project 2 columns  SINGLE agg order by 1 columns  project 3 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .singleAggregation({}, {"avg(\"c_acctbal_5\") AS avg"})
              .build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"c_acctbal\",\"avg\")")
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher1, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark0\")")
              .project(
                  {"substr(\"c_phone\",1,2) AS cntrycode",
                   "\"c_acctbal\" AS c_acctbal"})
              .singleAggregation(
                  {"\"cntrycode\""},
                  {"count() AS numcust", "sum(\"c_acctbal\") AS totacctbal"})
              .orderBy({"\"cntrycode\" ASC NULLS LAST"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(22, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // orders t6 project 1 columns *H right exists-flag (customer t2*M (customer
    // t4 PARTIAL agg FINAL agg project 1 columns ) filter 1 exprs   Build )
    // filter 1 exprs  project 2 columns  PARTIAL agg FINAL agg order by 1
    // columns  project 3 columns

    // Fragment 0
    {
      auto rightMatcher =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .partialAggregation({}, {"avg(\"c_acctbal_5\") AS avg"})
              .localPartition()
              .finalAggregation({}, {"avg(\"avg\") AS avg"})
              .build();

      auto rightMatcher1 =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"c_acctbal\",\"avg\")")
              .build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("orders")
              .hashJoin(rightMatcher1, velox::core::JoinType::kRightSemiProject)
              .filter("\"not\"(\"dt1___mark0\")")
              .project(
                  {"substr(\"c_phone\",1,2) AS cntrycode",
                   "\"c_acctbal\" AS c_acctbal"})
              .partialAggregation(
                  {"\"cntrycode\""},
                  {"count() AS numcust", "sum(\"c_acctbal\") AS totacctbal"})
              .localPartition()
              .finalAggregation(
                  {"\"cntrycode\""},
                  {"count(\"numcust\") AS numcust",
                   "sum(\"totacctbal\") AS totacctbal"})
              .orderBy({"\"cntrycode\" ASC NULLS LAST"})
              .localMerge()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(22, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // customer t2*M  (customer t4 PARTIAL agg gather  FINAL agg project 1
    // columns  broadcast ) filter 1 exprs *H exists-flag (orders t6*H exists
    // (customer t2 broadcast   Build ) project 1 columns  broadcast   Build )
    // filter 1 exprs  project 2 columns  PARTIAL agg repartition  FINAL agg
    // order by 1 columns  project 3 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .partialAggregation({}, {"avg(\"c_acctbal_5\") AS avg"})
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
                         .finalAggregation({}, {"avg(\"avg\") AS avg"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
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
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
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
              .tableScan("customer")
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"c_acctbal\",\"avg\")")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiProject)
              .filter("\"not\"(\"dt1___mark0\")")
              .project(
                  {"substr(\"c_phone\",1,2) AS cntrycode",
                   "\"c_acctbal\" AS c_acctbal"})
              .partialAggregation(
                  {"\"cntrycode\""},
                  {"count() AS numcust", "sum(\"c_acctbal\") AS totacctbal"})
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
                             {"\"cntrycode\""},
                             {"count(\"numcust\") AS numcust",
                              "sum(\"totacctbal\") AS totacctbal"})
                         .orderBy({"\"cntrycode\" ASC NULLS LAST"})
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(22, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // customer t2*M  (customer t4 PARTIAL agg gather  FINAL agg project 1
    // columns  broadcast ) filter 1 exprs *H exists-flag (orders t6*H exists
    // (customer t2 broadcast   Build ) project 1 columns  broadcast   Build )
    // filter 1 exprs  project 2 columns  PARTIAL agg repartition  FINAL agg
    // order by 1 columns  project 3 columns

    // Fragment 0
    {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("customer")
              .partialAggregation({}, {"avg(\"c_acctbal_5\") AS avg"})
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
                         .finalAggregation({}, {"avg(\"avg\") AS avg"})
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("customer")
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
              .tableScan("orders")
              .hashJoin(rightMatcher, velox::core::JoinType::kLeftSemiFilter)
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
              .tableScan("customer")
              .nestedLoopJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter("gt(\"c_acctbal\",\"avg\")")
              .hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiProject)
              .filter("\"not\"(\"dt1___mark0\")")
              .project(
                  {"substr(\"c_phone\",1,2) AS cntrycode",
                   "\"c_acctbal\" AS c_acctbal"})
              .partialAggregation(
                  {"\"cntrycode\""},
                  {"count() AS numcust", "sum(\"c_acctbal\") AS totacctbal"})
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
                             {"\"cntrycode\""},
                             {"count(\"numcust\") AS numcust",
                              "sum(\"totacctbal\") AS totacctbal"})
                         .orderBy({"\"cntrycode\" ASC NULLS LAST"})
                         .localMerge()
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[5].fragment.planNode));
    }
    // Fragment 6
    {
      auto matcher = core::PlanMatcherBuilder().mergeExchange().build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[6].fragment.planNode));
    }
  });
}
