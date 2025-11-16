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
void h30DefineCheckers1() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(1, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // lineitem t2 project 7 columns  PARTIAL agg FINAL agg order by 2 columns  project 10 columns

  // Fragment 0
  {
    auto matcher = core::PlanMatcherBuilder().tableScan("lineitem").project({"\"l_returnflag\" AS l_returnflag", "\"l_linestatus\" AS l_linestatus", "\"l_quantity\" AS l_quantity", "\"l_extendedprice\" AS l_extendedprice", "multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p39", "multiply(multiply(\"l_extendedprice\",minus(1,\"l_discount\")),plus(\"l_tax\",1)) AS dt1___p46", "\"l_discount\" AS l_discount"}).partialAggregation({"\"l_returnflag\"", "\"l_linestatus\""}, {"sum(\"l_quantity\") AS sum_qty", "sum(\"l_extendedprice\") AS sum_base_price", "sum(\"dt1___p39\") AS sum_disc_price", "sum(\"dt1___p46\") AS sum_charge", "avg(\"l_quantity\") AS avg_qty", "avg(\"l_extendedprice\") AS avg_price", "avg(\"l_discount\") AS avg_disc", "count() AS count_order"}).localPartition().finalAggregation({"\"l_returnflag\"", "\"l_linestatus\""}, {"sum(\"sum_qty\") AS sum_qty", "sum(\"sum_base_price\") AS sum_base_price", "sum(\"sum_disc_price\") AS sum_disc_price", "sum(\"sum_charge\") AS sum_charge", "avg(\"avg_qty\") AS avg_qty", "avg(\"avg_price\") AS avg_price", "avg(\"avg_disc\") AS avg_disc", "count(\"count_order\") AS count_order"}).orderBy({"\"l_returnflag\" ASC NULLS LAST", "\"l_linestatus\" ASC NULLS LAST"}).localMerge().build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
