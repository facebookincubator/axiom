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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class FilterPushdownTest : public test::HiveQueriesTestBase {};

TEST_F(FilterPushdownTest, throughAggregation) {
  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(exec::test::kHiveConnectorId, "orders")
                         .aggregate({"o_custkey"}, {"sum(o_totalprice) as a0"})
                         .filter("o_custkey < 100 and a0 > 200.0")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan("orders", test::lt("o_custkey", (int64_t)100))
                       .singleAggregation()
                       .filter("a0 > 200.0")
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan =
      exec::test::PlanBuilder()
          .tableScan("orders", getSchema("orders"))
          .singleAggregation({"o_custkey"}, {"sum(o_totalprice)"})
          .filter("o_custkey < 100 and a0 > 200.0")
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(FilterPushdownTest, redundantCast) {
  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(exec::test::kHiveConnectorId, "nation")
                         .filter("cast(n_nationkey as bigint) < 10")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = core::PlanMatcherBuilder()
                     .hiveScan("nation", test::lt("n_nationkey", (int64_t)10))
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(FilterPushdownTest, throughJoin) {
  auto startMatcher = [](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // Filter uses columns from both sides of the join. Expected to stay after the
  // join.
  for (const auto& filter : {
           "n_nationkey < r_regionkey",
           "cardinality(filter(array[n_name], n -> n = r_name)) > 0",
       }) {
    lp::PlanBuilder::Context ctx(exec::test::kHiveConnectorId);
    auto logicalPlan =
        lp::PlanBuilder(ctx)
            .from({"nation", "region"})
            .filter(fmt::format("n_nationkey = r_regionkey and {}", filter))
            .aggregate({}, {"count(*)"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        startMatcher("nation")
            .hashJoin(
                startMatcher("region").build(), velox::core::JoinType::kInner)
            .filter()
            .aggregation()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Filter uses columns from only one sides of the join. Expected to be pushed
  // down below the join.
  {
    lp::PlanBuilder::Context ctx(exec::test::kHiveConnectorId);
    auto logicalPlan =
        lp::PlanBuilder(ctx)
            .from({"nation", "region"})
            .filter("n_nationkey = r_regionkey and n_nationkey < 10")
            .aggregate({}, {"count(*)"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan("nation", test::lt("n_nationkey", (int64_t)10))
            .hashJoin(
                startMatcher("region").build(), velox::core::JoinType::kInner)
            .aggregation()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    lp::PlanBuilder::Context ctx(exec::test::kHiveConnectorId);
    auto logicalPlan =
        lp::PlanBuilder(ctx)
            .from({"nation", "region"})
            .filter("n_nationkey = r_regionkey and r_regionkey < 10")
            .aggregate({}, {"count(*)"})
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        startMatcher("nation")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("region", test::lt("r_regionkey", (int64_t)10))
                    .build(),
                velox::core::JoinType::kInner)
            .aggregation()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Verify that OR with a disjunct that is fully subsumed by extracted common
// factors does not crash. E.g. A OR (A AND B): the first disjunct is fully
// subsumed, leaving an empty residual. The result should be just A.
TEST_F(FilterPushdownTest, orWithSubsumedDisjunct) {
  auto logicalPlan = lp::PlanBuilder()
                         .tableScan(exec::test::kHiveConnectorId, "nation")
                         .filter(
                             "(n_regionkey = 1) OR "
                             "(n_regionkey = 1 AND n_name like 'A%')")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);
  auto matcher = core::PlanMatcherBuilder()
                     .hiveScan("nation", test::eq("n_regionkey", 1LL))
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Verify that multi-table OR filter with per-table extraction preserves the
// cross-combination filter. Per-table filters like n_name IN ('FRANCE',
// 'JAPAN') are extracted and pushed down as subfield filters. The original OR
// is kept as a remaining filter to ensure correct cross-table combinations.
TEST_F(FilterPushdownTest, multiTableOrFilter) {
  lp::PlanBuilder::Context ctx(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .from({"nation", "region"})
                         .filter(
                             "n_regionkey = r_regionkey AND "
                             "((n_name = 'FRANCE' AND r_name = 'EUROPE') OR "
                             "(n_name = 'JAPAN' AND r_name = 'ASIA'))")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto nationFilters =
      common::test::SubfieldFiltersBuilder()
          .add("n_name", exec::in(std::vector<std::string>{"FRANCE", "JAPAN"}))
          .build();

  auto regionFilters =
      common::test::SubfieldFiltersBuilder()
          .add("r_name", exec::in(std::vector<std::string>{"EUROPE", "ASIA"}))
          .build();

  auto matcher = core::PlanMatcherBuilder()
                     .hiveScan("nation", std::move(nationFilters))
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .hiveScan("region", std::move(regionFilters))
                             .build(),
                         velox::core::JoinType::kInner)
                     .filter(
                         "(n_name = 'FRANCE' AND r_name = 'EUROPE') OR "
                         "(n_name = 'JAPAN' AND r_name = 'ASIA')")
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
