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

  /// Builds a logical plan, optimizes it, and asserts it matches the expected
  /// distributed and single-node plans. When 'singleNodeMatcher' is nullptr,
  /// only verifies that single-node planning succeeds without checking the
  /// plan structure.
  void assertPlan(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::shared_ptr<core::PlanMatcher>& distributedMatcher,
      const std::shared_ptr<core::PlanMatcher>& singleNodeMatcher = nullptr) {
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(groupingKeys, aggregates)
                           .build();
    OptimizerOptions options;
    options.alwaysPlanPartialAggregation = true;
    auto plan = planVelox(
        logicalPlan,
        MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
        options);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, distributedMatcher);

    auto singleNodePlan = toSingleNodePlan(logicalPlan);
    if (singleNodeMatcher) {
      AXIOM_ASSERT_PLAN(singleNodePlan, singleNodeMatcher);
    }
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
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "Global aggregation with multiple DISTINCT aggregates on the same set of columns.");
    assertPlan(
        /*groupingKeys=*/{},
        /*aggregates=*/{"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"b"}, {})
            .splitAggregation({}, {"count(b)", "covar_pop(b, b)"})
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build());
  }

  {
    SCOPED_TRACE("Single DISTINCT aggregate with grouping keys.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a", "b"}, {})
            .splitAggregation({"a"}, {"count(b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation({"a"}, {"count(DISTINCT b)"})
            .build());
  }

  {
    SCOPED_TRACE("Multiple DISTINCT aggregates on the same set of columns.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a", "b"}, {})
            .splitAggregation({"a"}, {"count(b)", "covar_pop(b, b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithExpressionInputs) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("Expression-based grouping keys and distinct args.");
    assertPlan(
        /*groupingKeys=*/{"a + 1"},
        /*aggregates=*/{"count(DISTINCT b + c)", "sum(DISTINCT b + c)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"a + 1 as p0", "b + c as p1"})
            .splitAggregation({"p0", "p1"}, {})
            .splitAggregation({"p0"}, {"count(p1)", "sum(p1)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"a + 1 as p0", "b + c as p1"})
            .singleAggregation(
                {"p0"}, {"count(DISTINCT p1)", "sum(DISTINCT p1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Same set of distinct args with different order and duplicates: (b, c) and (c, b) have the same set {b, c}.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/
        {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a", "b", "c"}, {})
            .splitAggregation({"a"}, {"covar_pop(b, c)", "covar_samp(c, b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"},
                {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT argument overlap with grouping keys.");
    assertPlan(
        /*groupingKeys=*/{"b"},
        /*aggregates=*/{"covar_pop(DISTINCT b, c)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"b", "c"}, {})
            .splitAggregation({"b"}, {"covar_pop(b, c)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation({"b"}, {"covar_pop(DISTINCT b, c)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithOrderBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "DISTINCT with ORDER BY where ORDER BY keys are a subset of distinct args.");
    assertPlan(
        /*groupingKeys=*/{"c"},
        /*aggregates=*/
        {"max_by(DISTINCT a, b ORDER BY a)",
         "min_by(DISTINCT a, b ORDER BY b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"c", "a", "b"}, {})
            .distributedSingleAggregation(
                {"c"}, {"max_by(a, b ORDER BY a)", "min_by(a, b ORDER BY b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"c"},
                {"max_by(DISTINCT a, b ORDER BY a)",
                 "min_by(DISTINCT a, b ORDER BY b)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT with ORDER BY and literal args.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/
        {"max_by(DISTINCT b, 1 ORDER BY b)",
         "min_by(DISTINCT b, 2 ORDER BY b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a", "b"}, {})
            .distributedSingleAggregation(
                {"a"}, {"max_by(b, 1 ORDER BY b)", "min_by(b, 2 ORDER BY b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"},
                {"max_by(DISTINCT b, 1 ORDER BY b)",
                 "min_by(DISTINCT b, 2 ORDER BY b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithLiterals) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("DISTINCT aggregate with mixed column and literal args.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a", "b"}, {})
            .splitAggregation({"a"}, {"max_by(b, 1)", "min_by(b, 2)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT aggregate where all arguments are literals.");
    // The inner GROUP BY keys are just the grouping keys. There is no shuffle
    // between inner final and outer partial aggregation since inner and outer
    // keys are the same.
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT 1)", "count(DISTINCT 2)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .splitAggregation({"a"}, {})
            .partialAggregation({"a"}, {"count(1)", "count(2)"})
            .localPartition({"a"})
            .finalAggregation()
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT 1)", "count(DISTINCT 2)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctDifferentArgSets) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  assertPlan(
      /*groupingKeys=*/{"a"},
      /*aggregates=*/{"count(DISTINCT b)", "sum(DISTINCT d % 5)"},
      /*distributedMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"a", "b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"a", "p0"}, "m0")
          .distributedMarkDistinct({"a", "p1"}, "m1")
          .splitAggregation(
              {"a"},
              {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"})
          .shuffle()
          .build(),
      /*singleNodeMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"a", "b", "d % 5 as p0"})
          .singleAggregation({"a"}, {"count(DISTINCT b)", "sum(DISTINCT p0)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctGlobalWithMultipleSets) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  assertPlan(
      /*groupingKeys=*/{},
      /*aggregates=*/{"count(DISTINCT b)", "sum(DISTINCT d % 5)"},
      /*distributedMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"p0"}, "m0")
          .distributedMarkDistinct({"p1"}, "m1")
          .partialAggregation(
              {}, {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"})
          .shuffle()
          .localGather()
          .finalAggregation()
          .build(),
      /*singleNodeMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"b", "d % 5 as p0"})
          .singleAggregation({}, {"count(DISTINCT b)", "sum(DISTINCT p0)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctMixedDistinctAndNonDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  assertPlan(
      /*groupingKeys=*/{"a"},
      /*aggregates=*/{"count(DISTINCT b)", "sum(DISTINCT d % 5)", "avg(b)"},
      /*distributedMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"a", "b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"a", "p0"}, "m0")
          .distributedMarkDistinct({"a", "p1"}, "m1")
          .splitAggregation(
              {"a"},
              {"count(p0) filter (where m0)",
               "sum(p1) filter (where m1)",
               "avg(p0)"})
          .shuffle()
          .build(),
      /*singleNodeMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .project({"a", "b", "d % 5 as p0"})
          .singleAggregation(
              {"a"}, {"count(DISTINCT b)", "sum(DISTINCT p0)", "avg(b)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctMultiArgAggregates) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  assertPlan(
      /*groupingKeys=*/{"a"},
      /*aggregates=*/{"covar_pop(DISTINCT b, c)", "count(DISTINCT d)"},
      /*distributedMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .distributedMarkDistinct({"a", "b", "c"}, "m0")
          .distributedMarkDistinct({"a", "d"}, "m1")
          .splitAggregation(
              {"a"},
              {"covar_pop(b, c) filter (where m0)",
               "count(d) filter (where m1)"})
          .shuffle()
          .build(),
      /*singleNodeMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .singleAggregation(
              {"a"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT d)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctSharedMarkers) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "DISTINCT aggregates with the same set of non-grouping-key arguments share a single marker column.");
    assertPlan(
        /*groupingKeys=*/{"b"},
        /*aggregates=*/
        {"count(DISTINCT c)", "covar_pop(DISTINCT b, c)", "sum(c)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"b", "c"}, "m0")
            .splitAggregation(
                {"b"},
                {"count(c) filter (where m0)",
                 "covar_pop(b, c) filter (where m0)",
                 "sum(c)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"b"},
                {"count(DISTINCT c)", "covar_pop(DISTINCT b, c)", "sum(c)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT args overlapping with grouping keys are deduplicated in MarkDistinct keys.");
    assertPlan(
        /*groupingKeys=*/{"b"},
        /*aggregates=*/{"covar_pop(DISTINCT b, c)", "count(DISTINCT b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"b", "c"}, "m0")
            .distributedSingleAggregation(
                {"b"},
                {"covar_pop(b, c) filter (where m0)", "count(DISTINCT b)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"b"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctOrderBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("GroupBy with DISTINCT + ORDER BY via MarkDistinct.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/
        {"array_agg(DISTINCT b ORDER BY b)",
         "array_agg(DISTINCT d % 5 ORDER BY d % 5)",
         "array_agg(b ORDER BY b)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"a", "b as p0", "d % 5 as p1"})
            .distributedMarkDistinct({"a", "p0"}, "m0")
            .distributedMarkDistinct({"a", "p1"}, "m1")
            .distributedSingleAggregation(
                {"a"},
                {"array_agg(p0 ORDER BY p0 ASC NULLS LAST) filter (where m0)",
                 "array_agg(p1 ORDER BY p1 ASC NULLS LAST) filter (where m1)",
                 "array_agg(p0 ORDER BY p0 ASC NULLS LAST)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"a", "b as p0", "d % 5 as p1"})
            .singleAggregation(
                {"a"},
                {"array_agg(DISTINCT p0 ORDER BY p0)",
                 "array_agg(DISTINCT p1 ORDER BY p1)",
                 "array_agg(p0 ORDER BY p0)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Global aggregation with DISTINCT + ORDER BY via MarkDistinct.");
    assertPlan(
        /*groupingKeys=*/{},
        /*aggregates=*/
        {"array_agg(DISTINCT b ORDER BY b)",
         "array_agg(DISTINCT d % 5 ORDER BY d % 5)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"b as p0", "d % 5 as p1"})
            .distributedMarkDistinct({"p0"}, "m0")
            .distributedMarkDistinct({"p1"}, "m1")
            .distributedSingleAggregation(
                {},
                {"array_agg(p0 ORDER BY p0 ASC NULLS LAST) filter (where m0)",
                 "array_agg(p1 ORDER BY p1 ASC NULLS LAST) filter (where m1)"})
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .project({"b as p0", "d % 5 as p1"})
            .singleAggregation(
                {},
                {"array_agg(DISTINCT p0 ORDER BY p0)",
                 "array_agg(DISTINCT p1 ORDER BY p1)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctLiterals) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("Literal in args alongside different DISTINCT column sets.");
    // The literal should not be included as MarkDistinct keys.
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)", "max_by(DISTINCT d, 1)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"a", "b"}, "m0")
            .distributedMarkDistinct({"a", "d"}, "m1")
            .splitAggregation(
                {"a"},
                {"count(b) filter (where m0)",
                 "max_by(d, 1) filter (where m1)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "max_by(DISTINCT d, 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT aggregate whose column args are all grouping keys plus a literal.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)", "max_by(DISTINCT a, 1)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"a", "b"}, "m0")
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "max_by(DISTINCT a, 1)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "max_by(DISTINCT a, 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT aggregate with all-literal args alongside DISTINCT aggregate with column args.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)", "count(DISTINCT 1)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"a", "b"}, "m0")
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, multipleMarkDistinctWithNoShuffleInBetween) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  assertPlan(
      /*groupingKeys=*/{"a"},
      /*aggregates=*/{"count(DISTINCT b)", "covar_pop(DISTINCT b, c)"},
      /*distributedMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .shuffle()
          .localPartition({"a", "b"})
          .markDistinct({"a", "b"}, "m0")
          .markDistinct({"a", "b", "c"}, "m1")
          .splitAggregation(
              {"a"},
              {"count(b) filter (where m0)",
               "covar_pop(b, c) filter (where m1)"})
          .shuffle()
          .build(),
      /*singleNodeMatcher=*/
      core::PlanMatcherBuilder()
          .tableScan()
          .singleAggregation(
              {"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, c)"})
          .build());
}

TEST_F(DistinctAggregationTest, unsupportedAggregationOverDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "DISTINCT aggregate with a filter condition is not supported yet.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b) FILTER (WHERE c > 0)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "Distinct aggregation with FILTER is not supported");
  }
}

TEST_F(
    DistinctAggregationTest,
    markDistinctAllLiteralDistinctMixColumnDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "Global aggregation mixing column DISTINCT and all-literal DISTINCT.");
    assertPlan(
        /*groupingKeys=*/{},
        /*aggregates=*/{"count(DISTINCT b)", "count(DISTINCT 1)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"b"}, "m0")
            .distributedSingleAggregation(
                {}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation({}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Grouped aggregation mixing column DISTINCT and all-literal DISTINCT.");
    assertPlan(
        /*groupingKeys=*/{"a"},
        /*aggregates=*/{"count(DISTINCT b)", "count(DISTINCT 1)"},
        /*distributedMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .distributedMarkDistinct({"a", "b"}, "m0")
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .shuffle()
            .build(),
        /*singleNodeMatcher=*/
        core::PlanMatcherBuilder()
            .tableScan()
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
