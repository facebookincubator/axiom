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
void h30DefineCheckers7() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(7, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // lineitem t3*H  (supplier t2*H  (nation t6  Build ) project 2 columns   Build )*H  (orders t4*H  (customer t5*H  (nation t7  Build ) project 2 columns   Build ) project 2 columns   Build ) filter 1 exprs  project 4 columns  PARTIAL agg FINAL agg order by 3 columns  project 4 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("nation").build();

auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("supplier").hashJoin(rightMatcher, velox::core::JoinType::kInner).build();

auto rightMatcher2 = core::PlanMatcherBuilder().tableScan("nation").build();

auto rightMatcher3 = core::PlanMatcherBuilder().tableScan("customer").hashJoin(rightMatcher2, velox::core::JoinType::kInner).build();

auto rightMatcher4 = core::PlanMatcherBuilder().tableScan("orders").hashJoin(rightMatcher3, velox::core::JoinType::kInner).build();

auto matcher = core::PlanMatcherBuilder().tableScan("lineitem").hashJoin(rightMatcher1, velox::core::JoinType::kInner).hashJoin(rightMatcher4, velox::core::JoinType::kInner).filter("\"or\"(\"and\"(eq(\"n_name\",'FRANCE'),eq(\"n_name_1\",'GERMANY')),\"and\"(eq(\"n_name\",'GERMANY'),eq(\"n_name_1\",'FRANCE')))").project({"\"n_name\" AS n_name", "\"n_name_1\" AS n_name_1", "year(\"l_shipdate\") AS l_year", "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p92"}).partialAggregation({"\"n_name\"", "\"n_name_1\"", "\"l_year\""}, {"sum(\"dt1___p92\") AS revenue"}).localPartition().finalAggregation({"\"n_name\"", "\"n_name_1\"", "\"l_year\""}, {"sum(\"revenue\") AS revenue"}).orderBy({"\"n_name\" ASC NULLS LAST", "\"n_name_1\" ASC NULLS LAST", "\"l_year\" ASC NULLS LAST"}).localMerge().project({"\"n_name\" AS supp_nation", "\"n_name_1\" AS cust_nation", "\"l_year\" AS l_year", "\"revenue\" AS revenue"}).build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
