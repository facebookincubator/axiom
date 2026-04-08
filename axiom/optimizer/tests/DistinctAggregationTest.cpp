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

#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class DistinctAggregationTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

// Verifies that when all aggregates are DISTINCT with the same input columns
// and no filters, the optimizer transforms them into a two-level aggregation:
// 1. Inner: GROUP BY (original_keys + distinct_args) - for deduplication
// 2. Outer: Regular aggregation without DISTINCT flag
// This avoids the overhead of tracking distinct values in each aggregate.
TEST_F(DistinctAggregationTest, singleDistinctToGroupBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));

  auto buildMatcher = [](const std::vector<std::string>& projections,
                         const std::vector<std::string>& innerGroupingKeys,
                         const std::vector<std::string>& outerGroupingKeys,
                         const std::vector<std::string>& aggregates,
                         bool useSingleStepOuterAgg = false) {
    auto builder = core::PlanMatcherBuilder().tableScan();
    if (!projections.empty()) {
      builder.project(projections);
    }
    builder.partialAggregation(innerGroupingKeys, {})
        .shuffle()
        .localPartition(innerGroupingKeys)
        .finalAggregation(innerGroupingKeys, {});
    // When inner and outer keys are the same, the optimizer merges the
    // inner and outer shuffles, so we skip the shuffle between inner final
    // and outer aggregation.
    bool sameKeys = innerGroupingKeys == outerGroupingKeys;
    if (useSingleStepOuterAgg) {
      if (!sameKeys) {
        builder.shuffle();
      }
      if (outerGroupingKeys.empty()) {
        builder.localGather();
      } else {
        builder.localPartition(outerGroupingKeys);
      }
      builder.singleAggregation(outerGroupingKeys, aggregates);
    } else {
      builder.partialAggregation(outerGroupingKeys, aggregates);
      if (!sameKeys) {
        builder.shuffle();
      }
      if (outerGroupingKeys.empty()) {
        builder.localGather();
      } else {
        builder.localPartition(outerGroupingKeys);
      }
      builder.finalAggregation();
    }
    if (!outerGroupingKeys.empty()) {
      builder.shuffle();
    }
    return builder.build();
  };

  // Builds a logical plan, optimizes it, and asserts it matches the expected
  // distributed plan.
  OptimizerOptions options{.alwaysPlanPartialAggregation = true};
  auto assertPlan =
      [&](const std::vector<std::string>& groupingKeys,
          const std::vector<std::string>& aggregates,
          const std::shared_ptr<core::PlanMatcher>& expectedMatcher) {
        auto logicalPlan = lp::PlanBuilder(makeContext())
                               .tableScan("t")
                               .aggregate(groupingKeys, aggregates)
                               .build();
        auto plan = planVelox(
            logicalPlan,
            MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
            options);
        AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, expectedMatcher);
      };

  {
    // Test global aggregation with multiple DISTINCT aggregates on the same set
    // of columns.
    assertPlan(
        {},
        {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"b"},
            /*outerGroupingKeys=*/{},
            /*aggregates=*/{"count(b)", "covar_pop(b, b)"}));
  }

  {
    // Test single DISTINCT aggregate with grouping keys.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(b)"}));
  }

  {
    // Test multiple DISTINCT aggregates on the same set of columns.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(b)", "covar_pop(b, b)"}));
  }

  {
    // Test expression-based grouping keys and distinct args.
    assertPlan(
        {"a + 1"},
        {"count(DISTINCT b + c)", "sum(DISTINCT b + c)"},
        buildMatcher(
            /*projections=*/{"a + 1 as p0", "b + c as p1"},
            /*innerGroupingKeys=*/{"p0", "p1"},
            /*outerGroupingKeys=*/{"p0"},
            /*aggregates=*/{"count(p1)", "sum(p1)"}));
  }

  {
    // Test same set of distinct args with different order and duplicates: (b,
    // c) and (c, b) have the same set {b, c}.
    assertPlan(
        {"a"},
        {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b", "c"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"covar_pop(b, c)", "covar_samp(c, b)"}));
  }

  {
    // Test DISTINCT argument overlap with grouping keys.
    assertPlan(
        {"b"},
        {"covar_pop(DISTINCT b, c)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"b", "c"},
            /*outerGroupingKeys=*/{"b"},
            /*aggregates=*/{"covar_pop(b, c)"}));
  }

  {
    // Test DISTINCT with ORDER BY where ORDER BY keys are a subset of distinct
    // args.
    assertPlan(
        {"c"},
        {"max_by(DISTINCT a, b ORDER BY a)",
         "min_by(DISTINCT a, b ORDER BY b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"c", "a", "b"},
            /*outerGroupingKeys=*/{"c"},
            /*aggregates=*/
            {"max_by(a, b ORDER BY a)", "min_by(a, b ORDER BY b)"},
            /*useSingleStepOuterAgg=*/true));
  }

  {
    // Test DISTINCT with ORDER BY and literal args. The literal should be
    // skipped while the column is kept in inner GROUP BY keys.
    assertPlan(
        {"a"},
        {"max_by(DISTINCT b, 1 ORDER BY b)",
         "min_by(DISTINCT b, 2 ORDER BY b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/
            {"max_by(b, 1 ORDER BY b)", "min_by(b, 2 ORDER BY b)"},
            /*useSingleStepOuterAgg=*/true));
  }

  {
    // Test DISTINCT aggregate with mixed column and literal args. The literal
    // should be skipped while the column is kept in inner GROUP BY keys.
    assertPlan(
        {"a"},
        {"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"max_by(b, 1)", "min_by(b, 2)"}));
  }

  {
    // Test DISTINCT aggregate where all arguments are literals. The inner
    // GROUP BY keys should be just the grouping keys with no additions.
    assertPlan(
        {"a"},
        {"count(DISTINCT 1)", "count(DISTINCT 2)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(1)", "count(2)"}));
  }
}

TEST_F(DistinctAggregationTest, unsupportedAggregationOverDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));

  {
    // Different DISTINCT arguments across aggregates is not supported yet.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "sum(DISTINCT c)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }

  {
    // Different DISTINCT argument sets: {b, c} vs {b}.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"},
                {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT b, b)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }

  {
    // Mix of DISTINCT and non-DISTINCT aggregates is not supported yet.
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a"}, {"count(DISTINCT b)", "sum(c)"})
                           .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "Mix of DISTINCT and non-DISTINCT aggregates");
  }

  {
    // First aggregate has all-literal args, second has column args. The column
    // arg sets differ (empty vs {b}), so this is unsupported.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT 1)", "count(DISTINCT b)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
