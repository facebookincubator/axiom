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
void h30DefineCheckers14() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(14, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // part t3*H  (lineitem t2  Build ) project 2 columns  PARTIAL agg FINAL agg project 1 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("lineitem").build();

auto matcher = core::PlanMatcherBuilder().tableScan("part").hashJoin(rightMatcher, velox::core::JoinType::kInner).project({"switch(\"like\"(\"p_type\",'PROMO%'),multiply(\"l_extendedprice\",minus(1,\"l_discount\")),0) AS dt1___p53", "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p51"}).partialAggregation({}, {"sum(\"dt1___p53\") AS sum", "sum(\"dt1___p51\") AS sum_0"}).localPartition().finalAggregation({}, {"sum(\"sum\") AS sum", "sum(\"sum_0\") AS sum_0"}).project({"divide(multiply(\"sum\",100),\"sum_0\") AS promo_revenue"}).build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
