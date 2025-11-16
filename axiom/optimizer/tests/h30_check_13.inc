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
void h30DefineCheckers13() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(13, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // orders t3*H right (customer t2  Build ) PARTIAL agg FINAL agg project 1 columns  PARTIAL agg FINAL agg order by 2 columns  project 2 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("customer").build();

auto matcher = core::PlanMatcherBuilder().tableScan("orders").hashJoin(rightMatcher, velox::core::JoinType::kRight).partialAggregation({"\"c_custkey\""}, {"count(\"o_orderkey\") AS count"}).localPartition().finalAggregation({"\"c_custkey\""}, {"count(\"count\") AS count"}).project({"\"count\" AS c_count"}).partialAggregation({"\"c_count\""}, {"count() AS custdist"}).localPartition().finalAggregation({"\"c_count\""}, {"count(\"custdist\") AS custdist"}).orderBy({"\"custdist\" DESC NULLS LAST", "\"c_count\" DESC NULLS LAST"}).localMerge().build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
