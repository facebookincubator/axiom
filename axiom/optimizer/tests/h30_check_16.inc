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
void h30DefineCheckers16() {
// Configuration: numWorkers=1, numDrivers=4
setChecker(16, 1, 4, [](const PlanAndStats& planAndStats) {
  // Plan:
  // partsupp t2*H  (part t3  Build )*H exists-flag (supplier t5 project 1 columns   Build ) filter 1 exprs  PARTIAL agg FINAL agg order by 4 columns  project 4 columns

  // Fragment 0
  {
    auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

auto rightMatcher1 = core::PlanMatcherBuilder().tableScan("supplier").build();

auto matcher = core::PlanMatcherBuilder().tableScan("partsupp").hashJoin(rightMatcher, velox::core::JoinType::kInner).hashJoin(rightMatcher1, velox::core::JoinType::kLeftSemiProject).filter("\"not\"(\"dt1___mark0\")").partialAggregation({"\"p_brand\"", "\"p_type\"", "\"p_size\""}, {"count(\"ps_suppkey\") AS supplier_cnt"}).localPartition().finalAggregation({"\"p_brand\"", "\"p_type\"", "\"p_size\""}, {"count(\"supplier_cnt\") AS supplier_cnt"}).orderBy({"\"supplier_cnt\" DESC NULLS LAST", "\"p_brand\" ASC NULLS LAST", "\"p_type\" ASC NULLS LAST", "\"p_size\" ASC NULLS LAST"}).localMerge().build();
;
    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
  }
});

}
