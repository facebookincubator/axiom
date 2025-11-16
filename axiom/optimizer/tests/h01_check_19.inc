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
void h01DefineCheckers19() {
  // Configuration: numWorkers=1, numDrivers=1
  setChecker(19, 1, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3  Build ) filter 1 exprs  project 1 columns SINGLE
    // agg project 1 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,15),\"and\"(lte(\"l_quantity\",30),\"and\"(gte(\"l_quantity\",20),\"and\"(eq(\"p_brand\",'Brand#34'),\"in\"(\"p_container\",array['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))))),\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,5),\"and\"(lte(\"l_quantity\",11),\"and\"(gte(\"l_quantity\",1),\"and\"(eq(\"p_brand\",'Brand#12'),\"in\"(\"p_container\",array['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))))),\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,10),\"and\"(lte(\"l_quantity\",20),\"and\"(gte(\"l_quantity\",10),\"and\"(eq(\"p_brand\",'Brand#23'),\"in\"(\"p_container\",array['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))))))")
              .project(
                  {"multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p121"})
              .singleAggregation({}, {"sum(\"dt1___p121\") AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=1, numDrivers=4
  setChecker(19, 1, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3  Build ) filter 1 exprs  project 1 columns
    // PARTIAL agg FINAL agg project 1 columns

    // Fragment 0
    {
      auto rightMatcher = core::PlanMatcherBuilder().tableScan("part").build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,15),\"and\"(lte(\"l_quantity\",30),\"and\"(gte(\"l_quantity\",20),\"and\"(eq(\"p_brand\",'Brand#34'),\"in\"(\"p_container\",array['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))))),\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,5),\"and\"(lte(\"l_quantity\",11),\"and\"(gte(\"l_quantity\",1),\"and\"(eq(\"p_brand\",'Brand#12'),\"in\"(\"p_container\",array['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))))),\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,10),\"and\"(lte(\"l_quantity\",20),\"and\"(gte(\"l_quantity\",10),\"and\"(eq(\"p_brand\",'Brand#23'),\"in\"(\"p_container\",array['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))))))")
              .project(
                  {"multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p121"})
              .partialAggregation({}, {"sum(\"dt1___p121\") AS revenue"})
              .localPartition()
              .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=1
  setChecker(19, 4, 1, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3 broadcast   Build ) filter 1 exprs  project 1
    // columns  PARTIAL agg gather  FINAL agg project 1 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,15),\"and\"(lte(\"l_quantity\",30),\"and\"(gte(\"l_quantity\",20),\"and\"(eq(\"p_brand\",'Brand#34'),\"in\"(\"p_container\",array['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))))),\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,5),\"and\"(lte(\"l_quantity\",11),\"and\"(gte(\"l_quantity\",1),\"and\"(eq(\"p_brand\",'Brand#12'),\"in\"(\"p_container\",array['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))))),\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,10),\"and\"(lte(\"l_quantity\",20),\"and\"(gte(\"l_quantity\",10),\"and\"(eq(\"p_brand\",'Brand#23'),\"in\"(\"p_container\",array['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))))))")
              .project(
                  {"multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p121"})
              .partialAggregation({}, {"sum(\"dt1___p121\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
  });

  // Configuration: numWorkers=4, numDrivers=4
  setChecker(19, 4, 4, [](const PlanAndStats& planAndStats) {
    // Plan:
    // lineitem t2*H  (part t3 broadcast   Build ) filter 1 exprs  project 1
    // columns  PARTIAL agg gather  FINAL agg project 1 columns

    // Fragment 0
    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("part")
                         .partitionedOutput()
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[0].fragment.planNode));
    }
    // Fragment 1
    {
      auto rightMatcher = core::PlanMatcherBuilder().exchange().build();

      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan("lineitem")
              .hashJoin(rightMatcher, velox::core::JoinType::kInner)
              .filter(
                  "\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,15),\"and\"(lte(\"l_quantity\",30),\"and\"(gte(\"l_quantity\",20),\"and\"(eq(\"p_brand\",'Brand#34'),\"in\"(\"p_container\",array['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))))),\"or\"(\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,5),\"and\"(lte(\"l_quantity\",11),\"and\"(gte(\"l_quantity\",1),\"and\"(eq(\"p_brand\",'Brand#12'),\"in\"(\"p_container\",array['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))))),\"and\"(\"between\"(cast(\"p_size\" as BIGINT),1,10),\"and\"(lte(\"l_quantity\",20),\"and\"(gte(\"l_quantity\",10),\"and\"(eq(\"p_brand\",'Brand#23'),\"in\"(\"p_container\",array['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))))))")
              .project(
                  {"multiply(\"l_extendedprice\",minus(1,\"l_discount\")) AS dt1___p121"})
              .partialAggregation({}, {"sum(\"dt1___p121\") AS revenue"})
              .partitionedOutput()
              .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[1].fragment.planNode));
    }
    // Fragment 2
    {
      auto matcher = core::PlanMatcherBuilder()
                         .exchange()
                         .localPartition()
                         .finalAggregation({}, {"sum(\"revenue\") AS revenue"})
                         .build();
      ;
      EXPECT_TRUE(
          matcher->match(planAndStats.plan->fragments()[2].fragment.planNode));
    }
  });
}
