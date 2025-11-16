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
void h30DefineCheckers8() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(8, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // lineitem t4*H  (part t2  Build )*H  (orders t5*H  (customer t6*H  (nation t7*H  (region t9  Build ) project 1 columns   Build ) project 1 columns   Build ) project 2 columns   Build )*H  (supplier t3  Build )*H  (nation t8  Build ) project 3 columns  PARTIAL agg FINAL agg order by 1 columns  project 2 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("region").build();

auto rightMatcher2 = core::PlanMatcherBuilder().tableScan("nation").hashJoin(rightMatcher1, velox::core::JoinType::kInner).build();

auto rightMatcher3 = core::PlanMatcherBuilder().tableScan("customer").hashJoin(rightMatcher2, velox::core::JoinType::kInner).build();

auto rightMatcher4 = core::PlanMatcherBuilder().tableScan("orders").hashJoin(rightMatcher3, velox::core::JoinType::kInner).build();

auto rightMatcher5 = core::PlanMatcherBuilder().tableScan("supplier").build();

auto rightMatcher6 = core::PlanMatcherBuilder().tableScan("nation").build();

auto matcher = core::PlanMatcherBuilder().tableScan("lineitem").hashJoin(rightMatcher, velox::core::JoinType::kInner).hashJoin(rightMatcher4, velox::core::JoinType::kInner).hashJoin(rightMatcher5, velox::core::JoinType::kInner).hashJoin(rightMatcher6, velox::core::JoinType::kInner).project({"year(\"o_orderdate\") AS o_year", "switch(eq(\"n_name_1\",'BRAZIL'),multiply(\"l_extendedprice\",minus(1,\"l_discount\")),0) AS dt1___p113", "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p108"}).partialAggregation({"\"o_year\""}, {"sum(\"dt1___p113\") AS sum", "sum(\"dt1___p108\") AS sum_4"}).localPartition().finalAggregation({"\"o_year\""}, {"sum(\"sum\") AS sum", "sum(\"sum_4\") AS sum_4"}).orderBy({"\"o_year\" ASC NULLS LAST"}).localMerge().project({"\"o_year\" AS o_year", "divide(\"sum\",\"sum_4\") AS mkt_share"}).build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
