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
void h30DefineCheckers3() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(3, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // lineitem t4*H  (orders t3*H  (customer t2  Build ) project 3 columns   Build ) project 4 columns  PARTIAL agg FINAL agg order by 2 columns  project 4 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("customer").build();

auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("orders").hashJoin(rightMatcher, velox::core::JoinType::kInner).build();

auto matcher = core::PlanMatcherBuilder().tableScan("lineitem").hashJoin(rightMatcher1, velox::core::JoinType::kInner).project({"\"l_orderkey\" AS l_orderkey", "\"o_orderdate\" AS o_orderdate", "\"o_shippriority\" AS o_shippriority", "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p61"}).partialAggregation({"\"l_orderkey\"", "\"o_orderdate\"", "\"o_shippriority\""}, {"sum(\"dt1___p61\") AS revenue"}).localPartition().finalAggregation({"\"l_orderkey\"", "\"o_orderdate\"", "\"o_shippriority\""}, {"sum(\"revenue\") AS revenue"}).topN(10).localMerge().finalLimit(0, 10).project({"\"l_orderkey\" AS l_orderkey", "\"revenue\" AS revenue", "\"o_orderdate\" AS o_orderdate", "\"o_shippriority\" AS o_shippriority"}).build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
