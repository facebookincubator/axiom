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
void h30DefineCheckers11() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(11, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // partsupp t2*H  (supplier t3*H  (nation t4  Build ) project 1 columns   Build ) project 2 columns  PARTIAL agg FINAL agg project 2 columns *M  (partsupp t14*H  (supplier t15*H  (nation t16  Build ) project 1 columns   Build ) project 1 columns  PARTIAL agg FINAL agg project 1 columns ) filter 1 exprs  order by 1 columns  project 2 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("nation").build();

auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("supplier").hashJoin(rightMatcher, velox::core::JoinType::kInner).build();

auto rightMatcher2 = core::PlanMatcherBuilder().tableScan("nation").build();

auto rightMatcher3 = core::PlanMatcherBuilder().tableScan("supplier").hashJoin(rightMatcher2, velox::core::JoinType::kInner).build();

auto rightMatcher4 = core::PlanMatcherBuilder().tableScan("partsupp").hashJoin(rightMatcher3, velox::core::JoinType::kInner).project({"multiply(\"ps_supplycost_3\",cast(\"ps_availqty_2\" as DOUBLE)) AS dt13___p65"}).partialAggregation({}, {"sum(\"dt13___p65\") AS sum"}).localPartition().finalAggregation({}, {"sum(\"sum\") AS sum"}).project({"multiply(\"sum\",0.0001) AS expr"}).build();

auto matcher = core::PlanMatcherBuilder().tableScan("partsupp").hashJoin(rightMatcher1, velox::core::JoinType::kInner).project({"\"ps_partkey\" AS ps_partkey", "multiply(\"ps_supplycost\",cast(\"ps_availqty\" as DOUBLE)) AS dt1___p33"}).partialAggregation({"\"ps_partkey\""}, {"sum(\"dt1___p33\") AS value"}).localPartition().finalAggregation({"\"ps_partkey\""}, {"sum(\"value\") AS value"}).nestedLoopJoin(rightMatcher4, velox::core::JoinType::kInner).filter("gt(\"value\",\"expr\")").orderBy({"\"value\" DESC NULLS LAST"}).localMerge().project({"\"ps_partkey\" AS ps_partkey", "\"value\" AS value"}).build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
