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
void h30DefineCheckers12() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(12, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // orders t2*H  (lineitem t3  Build ) project 3 columns  PARTIAL agg FINAL agg order by 1 columns  project 3 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("lineitem").build();

auto matcher = core::PlanMatcherBuilder().tableScan("orders").hashJoin(rightMatcher, velox::core::JoinType::kInner).project({"\"l_shipmode\" AS l_shipmode", "switch(\"or\"(eq(\"o_orderpriority\",'1-URGENT'),eq(\"o_orderpriority\",'2-HIGH')),1,0) AS dt1___p58", "switch(\"and\"(neq(\"o_orderpriority\",'1-URGENT'),neq(\"o_orderpriority\",'2-HIGH')),1,0) AS dt1___p65"}).partialAggregation({"\"l_shipmode\""}, {"sum(\"dt1___p58\") AS high_line_count", "sum(\"dt1___p65\") AS low_line_count"}).localPartition().finalAggregation({"\"l_shipmode\""}, {"sum(\"high_line_count\") AS high_line_count", "sum(\"low_line_count\") AS low_line_count"}).orderBy({"\"l_shipmode\" ASC NULLS LAST"}).localMerge().build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
