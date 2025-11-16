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
void h30DefineCheckers4() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(4, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // lineitem t4 project 1 columns *H right exists-flag (orders t2  Build ) filter 1 exprs  PARTIAL agg FINAL agg order by 1 columns  project 2 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("orders").build();

auto matcher = core::PlanMatcherBuilder().tableScan("lineitem").hashJoin(rightMatcher, velox::core::JoinType::kRightSemiProject).filter("\"dt1___mark0\"").partialAggregation({"\"o_orderpriority\""}, {"count() AS order_count"}).localPartition().finalAggregation({"\"o_orderpriority\""}, {"count(\"order_count\") AS order_count"}).orderBy({"\"o_orderpriority\" ASC NULLS LAST"}).localMerge().build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
