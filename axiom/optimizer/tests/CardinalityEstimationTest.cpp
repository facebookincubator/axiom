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
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

constexpr double kTolerance = 0.001;
constexpr double kCardinalityTolerance = 1;

// Asserts that the constraint has the expected min and max bounds.
// Use std::nullopt for either bound to assert it is absent.
#define AXIOM_ASSERT_RANGE(constraint, expectedMin, expectedMax) \
  do {                                                           \
    std::optional<int64_t> _min = (expectedMin);                 \
    std::optional<int64_t> _max = (expectedMax);                 \
    if (_min.has_value()) {                                      \
      ASSERT_NE((constraint).min, nullptr);                      \
      EXPECT_EQ((constraint).min->value<int64_t>(), *_min);      \
    } else {                                                     \
      ASSERT_EQ((constraint).min, nullptr);                      \
    }                                                            \
    if (_max.has_value()) {                                      \
      ASSERT_NE((constraint).max, nullptr);                      \
      EXPECT_EQ((constraint).max->value<int64_t>(), *_max);      \
    } else {                                                     \
      ASSERT_EQ((constraint).max, nullptr);                      \
    }                                                            \
  } while (false)

// Asserts that the constraint has no min or max bounds.
#define AXIOM_ASSERT_NORANGE(constraint)  \
  do {                                    \
    ASSERT_EQ((constraint).min, nullptr); \
    ASSERT_EQ((constraint).max, nullptr); \
  } while (false)

class CardinalityEstimationTest : public test::QueryTestBase {
 protected:
  // Parses SQL, optimizes, and invokes the callback with the best plan.
  void verifyPlan(
      const std::string& sql,
      const std::function<void(const Plan&)>& callback) {
    auto logicalPlan = parseSelect(sql, kTestConnectorId);

    verifyOptimization(
        *logicalPlan,
        [&](Optimization& optimization) {
          auto* plan = optimization.bestPlan();
          ASSERT_NE(plan, nullptr) << "Best plan should not be null";

          callback(*plan);

          verifyDtConstraints(*optimization.rootDt(), *plan->op);
        },
        [] {
          OptimizerOptions options;
          options.sampleJoins = false;
          options.sampleFilters = false;
          return options;
        }());
  }

  // Verifies that the DT's column constraints match the plan's constraints.
  // Note: DT constraints are copied from the initial plan, which may differ
  // from the best plan used here if multiple plans with different distributions
  // exist.
  static void verifyDtConstraints(
      const DerivedTable& dt,
      const RelationOp& plan) {
    const auto& planColumns = plan.columns();
    const auto& constraints = plan.constraints();
    ASSERT_EQ(dt.columns.size(), planColumns.size());

    for (size_t i = 0; i < planColumns.size(); ++i) {
      SCOPED_TRACE(dt.columns[i]->toString());
      ASSERT_EQ(dt.columns[i]->id(), planColumns[i]->id());

      auto it = constraints.find(planColumns[i]->id());
      ASSERT_NE(it, constraints.end());

      const auto& dtValue = dt.columns[i]->value();
      const auto& planValue = it->second;
      EXPECT_EQ(dtValue.cardinality, planValue.cardinality);
      EXPECT_EQ(dtValue.nullFraction, planValue.nullFraction);
      EXPECT_EQ(dtValue.trueFraction, planValue.trueFraction);
      EXPECT_EQ(dtValue.nullable, planValue.nullable);

      // Compare min/max by value, not pointer identity.
      EXPECT_EQ(dtValue.min == nullptr, planValue.min == nullptr);
      if (dtValue.min && planValue.min) {
        EXPECT_TRUE(dtValue.min->equals(*planValue.min));
      }
      EXPECT_EQ(dtValue.max == nullptr, planValue.max == nullptr);
      if (dtValue.max && planValue.max) {
        EXPECT_TRUE(dtValue.max->equals(*planValue.max));
      }
    }
  }

  // Returns the constraint for the column at the given position in the
  // operator's output columns.
  static std::optional<Value> findConstraint(
      const RelationOp& op,
      int32_t index) {
    return findConstraint(op, op.columns()[index]);
  }

  static std::optional<Value> findConstraint(
      const RelationOp& op,
      const Column* column) {
    auto it = op.constraints().find(column->id());
    if (it != op.constraints().end()) {
      return it->second;
    }
    return std::nullopt;
  }

  std::shared_ptr<connector::TestConnector> connector_;
};

// Verifies that a Values node produces correct cardinality and constraints.
TEST_F(CardinalityEstimationTest, values) {
  verifyPlan(
      "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300))",
      [](const Plan& plan) {
        const auto& op = *plan.op;

        ASSERT_EQ(op.columns().size(), 2);
        EXPECT_NEAR(op.resultCardinality(), 3, kCardinalityTolerance);

        auto a = findConstraint(op, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_NEAR(a->cardinality, 3, kCardinalityTolerance);
        AXIOM_ASSERT_NORANGE(*a);

        auto b = findConstraint(op, 1);
        ASSERT_TRUE(b.has_value());
        EXPECT_NEAR(b->cardinality, 3, kCardinalityTolerance);
        AXIOM_ASSERT_NORANGE(*b);
      });
}

// Verifies that a table scan without filters produces cardinality, min/max,
// numDistinct, and null fraction from connector statistics.
TEST_F(CardinalityEstimationTest, scan) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), VARCHAR()}))
      ->setStats(
          1'000,
          {
              {"a",
               {.nullPct = 50, .min = 10LL, .max = 100LL, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  verifyPlan("SELECT a, b FROM t", [](const Plan& plan) {
    const auto& op = *plan.op;

    ASSERT_EQ(op.columns().size(), 2);
    EXPECT_NEAR(op.resultCardinality(), 1'000, kCardinalityTolerance);

    auto a = findConstraint(op, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 100, kCardinalityTolerance);
    EXPECT_NEAR(a->nullFraction, 0.5, kTolerance);
    AXIOM_ASSERT_RANGE(*a, 10, 100);

    auto b = findConstraint(op, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 500, kCardinalityTolerance);
    AXIOM_ASSERT_NORANGE(*b);
  });
}

// Verifies that a range filter tightens constraints: the filtered column
// should have reduced cardinality.
TEST_F(CardinalityEstimationTest, scanWithFilter) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.min = 1LL, .max = 1'000LL, .numDistinct = 1'000}},
              {"b", {.numDistinct = 500}},
          });

  verifyPlan("SELECT a, b FROM t WHERE a > 500", [](const Plan& plan) {
    const auto& op = *plan.op;
    ASSERT_EQ(op.columns().size(), 2);

    // a > 500 on [1, 1000] should give ~50% selectivity.
    EXPECT_NEAR(op.resultCardinality(), 500, kCardinalityTolerance);

    auto a = findConstraint(op, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 500, kCardinalityTolerance);
    AXIOM_ASSERT_RANGE(*a, 501, 1'000);

    auto b = findConstraint(op, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 500, kCardinalityTolerance);
    AXIOM_ASSERT_NORANGE(*b);
  });
}

// Verifies cardinality estimation for aggregation: output cardinality
// should be capped at the number of distinct values of the grouping key.
TEST_F(CardinalityEstimationTest, aggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          10'000,
          {
              {"a", {.numDistinct = 50}},
              {"b", {.numDistinct = 1'000}},
          });

  verifyPlan(
      "SELECT a, count(*), sum(b) FROM t GROUP BY a", [](const Plan& plan) {
        const auto& op = *plan.op;
        ASSERT_EQ(op.columns().size(), 3);
        EXPECT_NEAR(op.resultCardinality(), 50, kCardinalityTolerance);

        auto a = findConstraint(op, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_NEAR(a->cardinality, 50, kCardinalityTolerance);
        AXIOM_ASSERT_NORANGE(*a);
      });
}

// Verifies that aggregation without grouping keys produces a single row.
TEST_F(CardinalityEstimationTest, globalAggregation) {
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));

  verifyPlan("SELECT count(*), sum(a) FROM t", [](const Plan& plan) {
    const auto& op = *plan.op;
    EXPECT_NEAR(op.resultCardinality(), 1, kCardinalityTolerance);
  });
}

// Verifies that a global aggregation on top of a subquery correctly
// propagates constraints. The inner subquery DT (GROUP BY) should get
// column constraints from its plan, and the outer DT (global count) should
// produce a single row.
TEST_F(CardinalityEstimationTest, globalAggregationOnSubquery) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          10'000,
          {
              {"a", {.numDistinct = 50}},
              {"b", {.numDistinct = 1'000}},
          });

  verifyPlan(
      "SELECT count(*) FROM (SELECT a, sum(b) FROM t GROUP BY a) sub",
      [](const Plan& plan) {
        const auto& op = *plan.op;
        EXPECT_NEAR(op.resultCardinality(), 1, kCardinalityTolerance);

        ASSERT_EQ(op.columns().size(), 1);
        auto count = findConstraint(op, 0);
        ASSERT_TRUE(count.has_value());
        EXPECT_NEAR(count->cardinality, 1, kCardinalityTolerance);
      });
}

template <typename T = RelationOp>
const T& findOp(const RelationOp& op, RelType relType) {
  if (op.is(relType)) {
    if constexpr (std::is_same_v<T, RelationOp>) {
      return op;
    } else {
      return *op.as<T>();
    }
  }

  if (op.is(RelType::kProject)) {
    return findOp<T>(*op.input(), relType);
  }

  VELOX_FAIL("Cannot find {}: {}", RelTypeName::toName(relType), op.toString());
  folly::assume_unreachable();
}

// Verifies that a Filter operator scales NDV of non-filtered columns using
// the coupon collector formula. Without scaling, column 'b' would retain
// NDV=3 despite the filter reducing output to ~1 row.
TEST_F(CardinalityEstimationTest, filterOverValues) {
  verifyPlan(
      "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(a, b) "
      "WHERE a > 1",
      [](const Plan& plan) {
        // Plan structure: Project → Filter → Values.
        const auto& filter = findOp(*plan.op, RelType::kFilter);

        // Filter 'a > 1' uses default selectivity 0.10 (no min/max on VALUES).
        // 3 * 0.10 = 0.3, clamped to at least 1 row.
        EXPECT_NEAR(filter.resultCardinality(), 1, kCardinalityTolerance);
        ASSERT_EQ(filter.columns().size(), 2);

        // Filtered column 'a': conjunctsSelectivity sets NDV. sampledNdv
        // preserves it since NDV=1 can't decrease further.
        auto a = findConstraint(filter, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_NEAR(a->cardinality, 1, kCardinalityTolerance);

        // Non-filtered column 'b': sampledNdv(3, 3, 0.10) scales NDV from 3
        // to 1.
        auto b = findConstraint(filter, 1);
        ASSERT_TRUE(b.has_value());
        EXPECT_NEAR(b->cardinality, 1, kCardinalityTolerance);
      });
}

// Verifies that a Filter operator over Unnest scales NDV of non-filtered
// columns. The replicate column 'b' has high NDV from the base table;
// after the filter reduces rows, its NDV should scale down.
TEST_F(CardinalityEstimationTest, filterOverUnnest) {
  testConnector_->addTable("t", ROW({"a", "b"}, {ARRAY(BIGINT()), BIGINT()}))
      ->setStats(
          1'000,
          {
              {"a", {.numDistinct = 100}},
              {"b", {.numDistinct = 800}},
          });

  verifyPlan(
      "SELECT b, e FROM t CROSS JOIN UNNEST(a) AS u(e) WHERE e > 1",
      [](const Plan& plan) {
        // Plan structure: Project → Filter → Unnest → TableScan.
        const auto& filter = findOp(*plan.op, RelType::kFilter);

        // Unnest produces 10,000 rows (1,000 * fanout 10). Filter 'e > 1'
        // uses default selectivity 0.10 → 1,000 output rows.
        EXPECT_NEAR(filter.resultCardinality(), 1'000, kCardinalityTolerance);

        // Non-filtered replicate column 'b': NDV=800 from table scan.
        // sampledNdv(800, 10000, 0.10) ≈ 571.
        auto b = findConstraint(filter, 0);
        ASSERT_TRUE(b.has_value());
        EXPECT_LT(b->cardinality, 800);
      });
}

// Verifies that a Filter operator over a join scales NDV of non-filtered
// columns. The cross-table WHERE clause becomes a Filter on top of the join.
// Without scaling, column 'c' would retain its join-output NDV even when
// the filter reduces the row count below the NDV.
TEST_F(CardinalityEstimationTest, filterOverJoin) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.numDistinct = 100}},
              {"b", {.min = 1LL, .max = 1'000LL, .numDistinct = 1'000}},
              {"c", {.numDistinct = 800}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          10,
          {
              {"x", {.numDistinct = 10}},
              {"y", {.min = 1LL, .max = 1'000LL, .numDistinct = 10}},
          });

  verifyPlan(
      "SELECT a, c, x FROM t JOIN u ON a = x WHERE b > y",
      [](const Plan& plan) {
        const auto& filter = findOp(*plan.op, RelType::kFilter);

        // Join on a=x: fanout = 10 / max(100, 10) = 0.1.
        // resultCardinality = 1'000 * 0.1 = 100.
        // Filter 'b > y': ~50% selectivity → ~50 rows.
        EXPECT_NEAR(filter.resultCardinality(), 50, kCardinalityTolerance);

        // Column 'c' is not referenced in any filter or join key. After the
        // join, c NDV was scaled by sampledNdv to ~94. The Filter should
        // further scale it: sampledNdv(94, 100, 0.5) ≈ 39.
        auto c = findConstraint(filter, 1);
        ASSERT_TRUE(c.has_value());
        EXPECT_LT(c->cardinality, filter.resultCardinality());
      });
}

// Verifies cardinality estimation for inner join: output cardinality
// should reflect the join fanout based on key cardinalities. Payload
// cardinalities should scale when the join eliminates rows (fanout < 1).
TEST_F(CardinalityEstimationTest, innerJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  const auto sql = "SELECT a, b, x, y FROM t JOIN u ON a = x";

  struct Expected {
    float resultCardinality;
    float keyCardinality;
    float payloadBCardinality;
    float payloadYCardinality;
  };

  auto verify = [&](const Expected& expected) {
    verifyPlan(sql, [&](const Plan& plan) {
      const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
      EXPECT_EQ(join.joinType, velox::core::JoinType::kInner);
      ASSERT_EQ(join.columns().size(), 4);
      EXPECT_NEAR(
          join.resultCardinality(),
          expected.resultCardinality,
          kCardinalityTolerance);

      // Inner join on a = x eliminates NULLs from both join keys.
      auto a = findConstraint(join, 0);
      ASSERT_TRUE(a.has_value());
      EXPECT_NEAR(
          a->cardinality, expected.keyCardinality, kCardinalityTolerance);
      EXPECT_EQ(a->nullFraction, 0.0f);
      AXIOM_ASSERT_NORANGE(*a);

      auto b = findConstraint(join, 1);
      ASSERT_TRUE(b.has_value());
      EXPECT_NEAR(
          b->cardinality, expected.payloadBCardinality, kCardinalityTolerance);
      AXIOM_ASSERT_NORANGE(*b);

      auto x = findConstraint(join, 2);
      ASSERT_TRUE(x.has_value());
      EXPECT_NEAR(
          x->cardinality, expected.keyCardinality, kCardinalityTolerance);
      EXPECT_EQ(x->nullFraction, 0.0f);
      AXIOM_ASSERT_NORANGE(*x);

      auto y = findConstraint(join, 3);
      ASSERT_TRUE(y.has_value());
      EXPECT_NEAR(
          y->cardinality, expected.payloadYCardinality, kCardinalityTolerance);
      AXIOM_ASSERT_NORANGE(*y);
    });
  };

  // fanout > 1: all left rows match, some multiple times.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 500 / 100 = 5.
  // resultCardinality = 1'000 * 5 = 5'000.
  // All rows survive, so payload cardinalities are preserved from scan.
  testConnector_->setStats(
      "u", 500, {{"x", {.numDistinct = 100}}, {"y", {.numDistinct = 200}}});
  verify({
      .resultCardinality = 5'000,
      .keyCardinality = 100,
      .payloadBCardinality = 500,
      .payloadYCardinality = 200,
  });

  // fanout < 1: some left rows don't match.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 50 / max(100, 50) = 0.5.
  // resultCardinality = 1'000 * 0.5 = 500.
  // leftSelectivity = min(1, 0.5) * 1.0 = 0.5.
  // b->cardinality = expectedNumDistincts(1'000 * 0.5, 500)
  //                = 500 * (1 - (499/500)^500) ≈ 316.
  // rlFanout = |t| / max(ndv(t.a), ndv(u.x)) = 1'000 / 100 = 10.
  // Right-side payloads are not filtered (rlFanout > 1).
  testConnector_->setStats(
      "u", 50, {{"x", {.numDistinct = 50}}, {"y", {.numDistinct = 40}}});
  verify({
      .resultCardinality = 500,
      .keyCardinality = 50,
      .payloadBCardinality = 316,
      .payloadYCardinality = 40,
  });
}

// Verifies that a join with an aggregation plans without throwing when the
// join key is constrained by a literal outside the connector-reported
// [min, max] range (zero-cardinality constraint must not divide by zero in
// estimateFanout).
TEST_F(CardinalityEstimationTest, joinWithFilterOutsideMinMax) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {{"a",
            {.min = velox::Variant::create<int64_t>(1),
             .max = velox::Variant::create<int64_t>(100),
             .numDistinct = 100}},
           {"b", {.numDistinct = 500}}});

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          1'000,
          {{"x",
            {.min = velox::Variant::create<int64_t>(1),
             .max = velox::Variant::create<int64_t>(100),
             .numDistinct = 100}},
           {"y", {.numDistinct = 500}}});

  // 999 is outside [1, 100] for both join keys.
  const auto sql =
      "SELECT a, sum(y) FROM t JOIN u ON a = x "
      "WHERE a = 999 AND x = 999 GROUP BY a";

  EXPECT_NO_THROW(verifyPlan(sql, [](const Plan&) {}));
}

// Verifies that inner join with a unique right side (DerivedTable with GROUP
// BY) uses the right fanout to scale cardinality. The right side's fanout
// reflects that not all left key values exist in the right side.
TEST_F(CardinalityEstimationTest, innerJoinUniqueRight) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          500,
          {
              {"x", {.numDistinct = 50}},
              {"y", {.numDistinct = 200}},
          });

  verifyPlan(
      "SELECT * FROM t JOIN (SELECT x, min(y) FROM u GROUP BY x) sub ON t.a = sub.x",
      [](const Plan& plan) {
        const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
        EXPECT_EQ(join.joinType, velox::core::JoinType::kInner);

        // The subquery produces 50 rows (one per distinct x), all unique.
        // rightUnique = true since sub.x covers all grouping keys.
        // right.fanout = estimateFanout(50, {sub.x}, {t.a})
        //              = 50 / max(50, 100) = 0.5.
        // lrFanout = right.fanout * baseSelectivity(rightTable)
        //          = 0.5 * 1 = 0.5.
        // resultCardinality = |t| * 0.5 = 500.
        EXPECT_NEAR(join.resultCardinality(), 500, kCardinalityTolerance);
      });
}

// Verifies that multi-key existence pushdown uses the outer join's fanout
// for the inner semijoin. The DerivedTable has a uniqueness constraint on
// grouping keys while the inner BaseTable does not. By using the outer
// join's fanout we reflect that only some output rows have matching keys.
TEST_F(CardinalityEstimationTest, multiKeyExistencePushdown) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()))
      ->setStats(
          100,
          {
              {"a", {.numDistinct = 100}},
              {"b", {.numDistinct = 100}},
              {"c", {.numDistinct = 100}},
          });

  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()))
      ->setStats(
          10'000,
          {
              {"x", {.numDistinct = 10'000}},
              {"y", {.numDistinct = 10'000}},
              {"z", {.numDistinct = 10'000}},
          });

  verifyPlan(
      "SELECT t.a, t.b, dt.cnt FROM t, "
      "(SELECT x, y, count(*) as cnt FROM u GROUP BY x, y) dt "
      "WHERE t.a = dt.x AND t.b = dt.y AND t.c < 100",
      [](const Plan& plan) {
        // Plan: Project → Join(INNER) → Agg → Join(LEFT_SEMI_FILTER).
        // We want the semi-join, so we extract the inner join and
        // search down the left side.
        const auto& inner = findOp<Join>(*plan.op, RelType::kJoin);
        const RelationOp* node = inner.input().get();
        while (node && !node->is(RelType::kJoin)) {
          node = node->input().get();
        }
        ASSERT_NE(node, nullptr);
        const auto& semi = *node->as<Join>();
        ASSERT_EQ(semi.joinType, velox::core::JoinType::kLeftSemiFilter);

        // The outer join's rlFanout = |t| / |DT| = 100 / 10'000 = 0.01.
        // setFanouts(0.01, 1) gives lrFanout = 0.01, so the semijoin
        // reduces u from 10'000 to ~100 rows. With guessFanout(), the
        // multi-key fanout on raw base tables would clamp to 1, producing
        // lrFanout ≈ 1 and no reduction (10'000 rows).
        EXPECT_LT(semi.resultCardinality(), 200);
      });
}

// Verifies that left join preserves left-side row count and left-side
// columns have zero null fraction.
TEST_F(CardinalityEstimationTest, leftJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          100,
          {
              {"x", {.numDistinct = 50}},
              {"y", {.numDistinct = 100}},
          });

  verifyPlan("SELECT * FROM t LEFT JOIN u ON a = x", [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
    EXPECT_EQ(join.joinType, velox::core::JoinType::kLeft);
    ASSERT_EQ(join.columns().size(), 4);

    // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 100 / max(100, 50) = 1.
    // resultCardinality = |t| * max(1, 1) = 1'000.
    EXPECT_NEAR(join.resultCardinality(), 1'000, kCardinalityTolerance);

    // Left key/payload: not modified (left side is not optional).
    auto a = findConstraint(join, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 100, kCardinalityTolerance);
    EXPECT_EQ(a->nullFraction, 0.1f);

    auto b = findConstraint(join, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 500, kCardinalityTolerance);
    EXPECT_EQ(b->nullFraction, 0.0f);

    // Right key: cardinality adjusted by updateKey to min(ndv(t.a), ndv(u.x)).
    // nullFraction = 1 - ndv(u.x) / ndv(t.a) = 0.5.
    auto x = findConstraint(join, 2);
    ASSERT_TRUE(x.has_value());
    EXPECT_NEAR(x->cardinality, 50, kCardinalityTolerance);
    EXPECT_EQ(x->nullFraction, 0.5f);

    // Right payload: only nullable and nullFraction set by updatePayload.
    // nullFraction = 1 - ndv(u.x) / ndv(t.a) = 0.5. Cardinality preserved
    // from scan.
    auto y = findConstraint(join, 3);
    ASSERT_TRUE(y.has_value());
    EXPECT_NEAR(y->cardinality, 100, kCardinalityTolerance);
    EXPECT_TRUE(y->nullable);
    EXPECT_EQ(y->nullFraction, 0.5f);
  });
}

// Verifies that a non-equi predicate in a left join ON clause produces a
// join with a non-empty filter. The filter selectivity is computed via
// conjunctsSelectivity in Join::initConstraints.
TEST_F(CardinalityEstimationTest, leftJoinWithFilter) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {{"a",
            {
                .numDistinct = 100,
            }}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(500, {{"x", {.numDistinct = 50}}});
  verifyPlan(
      "SELECT a, b, x, y FROM t LEFT JOIN u ON a = x AND b > coalesce(y, 0)",
      [](const Plan& plan) {
        SCOPED_TRACE(plan.op->toString());

        const auto& join = findOp<Join>(*plan.op, RelType::kJoin);

        EXPECT_FALSE(join.filter.empty())
            << "Left join should have a non-equi filter (b > coalesce(y, 0))";

        ASSERT_EQ(join.columns().size(), 4);

        // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 500 / max(100, 50) = 5.
        // coalesce() doesn't propagate min/max, so the optimizer uses the
        // default selectivity of 0.5 for column-vs-column comparisons.
        // resultCardinality = |t| * max(1, 5 * 0.5) = 1'000 * 2.5 = 2'500.
        EXPECT_NEAR(join.resultCardinality(), 2'500, kCardinalityTolerance);

        // Left join preserves all left-side rows.
        auto a = findConstraint(join, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_EQ(a->nullFraction, 0.0f);
        EXPECT_NEAR(a->cardinality, 100, kCardinalityTolerance);

        // Right-side key column becomes nullable due to unmatched left rows.
        // nullFraction = 1 - ndv(u.x) / ndv(t.a) = 1 - 50 / 100 = 0.5.
        auto x = findConstraint(join, 2);
        ASSERT_TRUE(x.has_value());
        EXPECT_NEAR(x->nullFraction, 0.5f, kTolerance);
        EXPECT_NEAR(x->cardinality, 50, kCardinalityTolerance);
      });
}

// Verifies that right join applies
// (optional) columns and preserves right-side constraints.
// Uses LEFT JOIN SQL with t smaller than u to force the optimizer to swap
// sides, producing a RIGHT join (build smaller t, probe larger u).
TEST_F(CardinalityEstimationTest, rightJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          100,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 80}},
          });
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"x", {.numDistinct = 50}},
              {"y", {.numDistinct = 500}},
          });

  verifyPlan("SELECT * FROM t LEFT JOIN u ON a = x", [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);

    EXPECT_EQ(join.joinType, velox::core::JoinType::kRight);
    ASSERT_EQ(join.columns().size(), 4);

    // rlFanout = |u| / max(ndv(u.x), ndv(t.a)) = 1'000 / max(50, 100) = 10.
    // adjustFanoutForJoinType(kRight) = max(1, rlFanout) * (|t| / |u|)
    //   = 10 * 0.1 = 1.
    // resultCardinality = |u| * 1 = 1'000.
    EXPECT_NEAR(join.resultCardinality(), 1'000, kCardinalityTolerance);

    // The optimizer converts 't LEFT JOIN u' to 'u RIGHT JOIN t'
    // (build smaller t, probe larger u). In the RIGHT join:
    // - Left side (u): optional (leftOptional = true)
    // - Right side (t): not optional

    // Right key/payload: not modified (right side is not optional).
    auto a = findConstraint(join, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 100, kCardinalityTolerance);
    EXPECT_EQ(a->nullFraction, 0.1f);

    auto b = findConstraint(join, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 80, kCardinalityTolerance);
    EXPECT_EQ(b->nullFraction, 0.0f);

    // Left key (u.x): cardinality adjusted by updateKey.
    // nullFraction = max(0, 1 - ndv(u.x) / ndv(t.a)) = 1 - 50 / 100 = 0.5.
    auto x = findConstraint(join, 2);
    ASSERT_TRUE(x.has_value());
    EXPECT_NEAR(x->cardinality, 50, kCardinalityTolerance);
    EXPECT_TRUE(x->nullable);
    EXPECT_EQ(x->nullFraction, 0.5f);

    // Left payload (u.y): only nullable and nullFraction set by updatePayload.
    // Cardinality preserved from scan.
    auto y = findConstraint(join, 3);
    ASSERT_TRUE(y.has_value());
    EXPECT_NEAR(y->cardinality, 500, kCardinalityTolerance);
    EXPECT_TRUE(y->nullable);
    EXPECT_EQ(y->nullFraction, 0.5f);
  });
}

// Verifies that full join applies updateKey / updatePayload to both sides.
// Both sides are optional: optionality returns {true, true}.
TEST_F(CardinalityEstimationTest, fullJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"x", {.numDistinct = 50}},
              {"y", {.numDistinct = 200}},
          });

  verifyPlan(
      "SELECT * FROM t FULL OUTER JOIN u ON a = x", [](const Plan& plan) {
        const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
        EXPECT_EQ(join.joinType, velox::core::JoinType::kFull);
        ASSERT_EQ(join.columns().size(), 4);

        // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 1'000 / 100 = 10.
        // adjustFanoutForJoinType(kFull) = max(max(1, 10), 1.0) = 10.
        // resultCardinality = |t| * 10 = 10'000.
        EXPECT_NEAR(join.resultCardinality(), 10'000, kCardinalityTolerance);

        // Left key: leftOptional = true.
        // nullFraction = max(0, 1 - ndv(t.a) / ndv(u.x)) = 1 - 100/50 < 0 => 0.
        auto a = findConstraint(join, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_NEAR(a->cardinality, 50, kCardinalityTolerance);
        EXPECT_TRUE(a->nullable);
        EXPECT_EQ(a->nullFraction, 0.0f);

        // Left payload: updatePayload sets nullable and nullFraction.
        // Cardinality preserved from scan.
        auto b = findConstraint(join, 1);
        ASSERT_TRUE(b.has_value());
        EXPECT_NEAR(b->cardinality, 500, kCardinalityTolerance);
        EXPECT_TRUE(b->nullable);
        EXPECT_EQ(b->nullFraction, 0.0f);

        // Right key: rightOptional = true.
        // nullFraction = max(0, 1 - ndv(u.x) / ndv(t.a)) = 1 - 50/100 = 0.5.
        auto x = findConstraint(join, 2);
        ASSERT_TRUE(x.has_value());
        EXPECT_NEAR(x->cardinality, 50, kCardinalityTolerance);
        EXPECT_TRUE(x->nullable);
        EXPECT_EQ(x->nullFraction, 0.5f);

        // Right payload: updatePayload uses computeNullFraction on
        // post-updateKey constraints. After updateKey, both keys have
        // cardinality 50, so nullFraction = max(0, 1 - 50/50) = 0. Cardinality
        // preserved from scan.
        auto y = findConstraint(join, 3);
        ASSERT_TRUE(y.has_value());
        EXPECT_NEAR(y->cardinality, 200, kCardinalityTolerance);
        EXPECT_TRUE(y->nullable);
        EXPECT_EQ(y->nullFraction, 0.0f);
      });
}

// Verifies mark column constraint for left semi project join (EXISTS in the
// SELECT list). The mark column's trueFraction should be min(1, fanout) *
// filterSelectivity.
TEST_F(CardinalityEstimationTest, semiProjectMark) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(1'000, {{"a", {.numDistinct = 100}}});

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  const auto sql = "SELECT a, EXISTS(SELECT * FROM u WHERE x = a) FROM t";

  auto verify = [&](float expectedTrueFraction) {
    verifyPlan(sql, [&](const Plan& plan) {
      const auto& join = findOp<Join>(*plan.op, RelType::kJoin);

      EXPECT_EQ(join.joinType, velox::core::JoinType::kLeftSemiProject);

      // Semi project preserves all left-side rows.
      EXPECT_NEAR(join.resultCardinality(), 1'000, kCardinalityTolerance);

      // The last column of a kLeftSemiProject join is the mark column.
      auto markColumn = join.columns().back();
      ASSERT_TRUE(markColumn->value().type->isBoolean());

      auto mark = findConstraint(join, markColumn);
      ASSERT_TRUE(mark.has_value());
      EXPECT_NEAR(mark->trueFraction, expectedTrueFraction, kTolerance);
      EXPECT_FALSE(mark->nullable);
      EXPECT_EQ(mark->nullFraction, 0);
    });
  };

  // fanout > 1: trueFraction clamped to 1.0.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 500 / 200 = 2.5.
  // trueFraction = min(1, 2.5) * 1.0 = 1.0.
  testConnector_->setStats("u", 500, {{"x", {.numDistinct = 200}}});
  verify(1.0f);

  // fanout < 1: trueFraction reflects the fraction of matching rows.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 50 / max(100, 50) = 0.5.
  // trueFraction = min(1, 0.5) * 1.0 = 0.5.
  testConnector_->setStats("u", 50, {{"x", {.numDistinct = 50}}});
  verify(0.5f);
}

// Verifies cardinality estimation for left semi filter join (WHERE EXISTS).
// Only left-side rows with a match are returned; fanout is clamped to at most 1
// (each matching row appears once). Constraints use inner-join semantics
// (neither side is optional).
TEST_F(CardinalityEstimationTest, semiFilter) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  const auto sql =
      "SELECT a, b FROM t WHERE EXISTS (SELECT * FROM u WHERE x = a)";

  struct Expected {
    float resultCardinality;
    float keyCardinality;
    float payloadCardinality;
  };

  auto verify = [&](const Expected& expected) {
    verifyPlan(sql, [&](const Plan& plan) {
      const auto& join = findOp<Join>(*plan.op, RelType::kJoin);

      EXPECT_EQ(join.joinType, velox::core::JoinType::kLeftSemiFilter);

      // Only left-side columns appear in output.
      ASSERT_EQ(join.columns().size(), 2);

      EXPECT_NEAR(
          join.resultCardinality(),
          expected.resultCardinality,
          kCardinalityTolerance);

      // Key: inner-join semantics.
      // NULL (not true), so EXISTS returns false. Null keys are eliminated.
      auto a = findConstraint(join, 0);
      ASSERT_TRUE(a.has_value());
      EXPECT_NEAR(
          a->cardinality, expected.keyCardinality, kCardinalityTolerance);
      EXPECT_EQ(a->nullFraction, 0.0f);
      AXIOM_ASSERT_NORANGE(*a);

      // Payload: preserved from scan (neither side is optional for semi
      // filter, so updatePayload is not called).
      auto b = findConstraint(join, 1);
      ASSERT_TRUE(b.has_value());
      EXPECT_NEAR(
          b->cardinality, expected.payloadCardinality, kCardinalityTolerance);
      EXPECT_EQ(b->nullFraction, 0.0f);
      AXIOM_ASSERT_NORANGE(*b);
    });
  };

  // fanout > 1: min(1, fanout) = 1 => all left rows match.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 500 / 100 = 5.
  // adjustedFanout = min(1, 5) = 1.
  // resultCardinality = 1'000 * 1 = 1'000.
  testConnector_->setStats("u", 500, {{"x", {.numDistinct = 100}}});
  verify({
      .resultCardinality = 1'000,
      .keyCardinality = 100,
      .payloadCardinality = 500,
  });

  // fanout < 1: not all left rows find a match.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 50 / max(100, 50) = 0.5.
  // adjustedFanout = min(1, 0.5) = 0.5.
  // resultCardinality = 1'000 * 0.5 = 500.
  // leftSelectivity = min(1, 0.5) * 1.0 = 0.5.
  // b->cardinality = expectedNumDistincts(1'000 * 0.5, 500)
  //                = 500 * (1 - (499/500)^500) ≈ 316.
  testConnector_->setStats("u", 50, {{"x", {.numDistinct = 50}}});
  verify({
      .resultCardinality = 500,
      .keyCardinality = 50,
      .payloadCardinality = 316,
  });
}

// Verifies cardinality estimation for anti join (NOT EXISTS / NOT IN).
// Returns left-side rows that have no match. Fanout is max(0, 1 - fanout):
// if fanout >= 1 (every left row matches), the anti join returns nothing.
TEST_F(CardinalityEstimationTest, antiJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.nullPct = 10, .numDistinct = 100}},
              {"b", {.numDistinct = 500}},
          });

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  const auto sql =
      "SELECT a, b FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE x = a)";

  struct Expected {
    float resultCardinality;
    float keyCardinality;
    float keyNullFraction;
    float payloadCardinality;
  };

  auto verify = [&](const Expected& expected) {
    verifyPlan(sql, [&](const Plan& plan) {
      const auto& join = findOp<Join>(*plan.op, RelType::kJoin);

      EXPECT_EQ(join.joinType, velox::core::JoinType::kAnti);

      // Only left-side columns appear in output.
      ASSERT_EQ(join.columns().size(), 2);

      EXPECT_NEAR(
          join.resultCardinality(),
          expected.resultCardinality,
          kCardinalityTolerance);

      // Key: if 'a' is NULL, 'x = a' evaluates to NULL (not true), so
      // NOT EXISTS returns true. Null keys are preserved in anti join
      // output. Cardinality is scaled by antiSelectivity =
      // max(0, 1 - fanout * filterSelectivity). nullFraction increases
      // because all nulls survive but only a fraction of non-nulls survive.
      auto a = findConstraint(join, 0);
      ASSERT_TRUE(a.has_value());
      EXPECT_NEAR(
          a->cardinality, expected.keyCardinality, kCardinalityTolerance);
      EXPECT_TRUE(a->nullable);
      EXPECT_NEAR(a->nullFraction, expected.keyNullFraction, kTolerance);
      AXIOM_ASSERT_NORANGE(*a);

      // Payload: cardinality scaled by antiSelectivity (fewer rows => fewer
      // distinct values). nullFraction preserved from scan (the filter is on
      // the key, not the payload).
      auto b = findConstraint(join, 1);
      ASSERT_TRUE(b.has_value());
      EXPECT_NEAR(
          b->cardinality, expected.payloadCardinality, kCardinalityTolerance);
      EXPECT_EQ(b->nullFraction, 0.0f);
      AXIOM_ASSERT_NORANGE(*b);
    });
  };

  // fanout > 1: every left row matches, so anti returns (almost) nothing.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 500 / 100 = 5.
  // antiSelectivity = max(0, 1 - 5) = 0.
  // resultCardinality = max(1, 1'000 * 0) = 1 (floor of 1).
  // keyCardinality = max(1, 100 * 0) = 1.
  // payloadCardinality = max(1, 500 * 0) = 1.
  // Only NULL keys survive: nullFraction = 0.1 / (0.1 + 0.9 * 0) = 1.0.
  testConnector_->setStats("u", 500, {{"x", {.numDistinct = 100}}});
  verify({
      .resultCardinality = 1,
      .keyCardinality = 1,
      .keyNullFraction = 1.0f,
      .payloadCardinality = 1,
  });

  // fanout < 1: some left rows don't match.
  // fanout = |u| / max(ndv(t.a), ndv(u.x)) = 50 / max(100, 50) = 0.5.
  // antiSelectivity = max(0, 1 - 0.5) = 0.5.
  // resultCardinality = 1'000 * 0.5 = 500.
  // keyCardinality = max(1, ndv(leftKey) - minNdv) = max(1, 100 - 50) = 50.
  // payloadCardinality = expectedNumDistincts(1'000 * 0.5, 500)
  //                    = 500 * (1 - (499/500)^500) ≈ 316.
  // nullFraction = 0.1 / (0.1 + 0.9 * 0.5) = 0.1 / 0.55 ≈ 0.182.
  testConnector_->setStats("u", 50, {{"x", {.numDistinct = 50}}});
  verify({
      .resultCardinality = 500,
      .keyCardinality = 50,
      .keyNullFraction = 0.1f / 0.55f,
      .payloadCardinality = 316,
  });
}

// Verifies cardinality estimation for counting left semi filter join
// (INTERSECT ALL). Uses the same formula as left semi filter:
// resultCardinality = |left| * min(1, fanout).
TEST_F(CardinalityEstimationTest, countingSemiFilter) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->setStats(1'000, {{"a", {.numDistinct = 100}}});
  testConnector_->addTable("u", ROW("x", BIGINT()));

  const auto sql = "SELECT a FROM t INTERSECT ALL SELECT x FROM u";

  // fanout > 1: all left rows match.
  testConnector_->setStats("u", 500, {{"x", {.numDistinct = 100}}});
  verifyPlan(sql, [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
    EXPECT_EQ(join.joinType, velox::core::JoinType::kCountingLeftSemiFilter);
    EXPECT_NEAR(join.resultCardinality(), 1'000, kCardinalityTolerance);
  });

  // fanout < 1: resultCardinality = 1'000 * 0.5 = 500.
  testConnector_->setStats("u", 50, {{"x", {.numDistinct = 50}}});
  verifyPlan(sql, [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
    EXPECT_EQ(join.joinType, velox::core::JoinType::kCountingLeftSemiFilter);
    EXPECT_NEAR(join.resultCardinality(), 500, kCardinalityTolerance);
  });
}

// Verifies cardinality estimation for counting anti join (EXCEPT ALL).
// Uses the same formula as anti join:
// resultCardinality = |left| * max(0, 1 - fanout).
TEST_F(CardinalityEstimationTest, countingAnti) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->setStats(1'000, {{"a", {.numDistinct = 100}}});
  testConnector_->addTable("u", ROW("x", BIGINT()));

  const auto sql = "SELECT a FROM t EXCEPT ALL SELECT x FROM u";

  // fanout > 1: anti returns (almost) nothing.
  testConnector_->setStats("u", 500, {{"x", {.numDistinct = 100}}});
  verifyPlan(sql, [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
    EXPECT_EQ(join.joinType, velox::core::JoinType::kCountingAnti);
    EXPECT_NEAR(join.resultCardinality(), 1, kCardinalityTolerance);
  });

  // fanout < 1: resultCardinality = 1'000 * 0.5 = 500.
  testConnector_->setStats("u", 50, {{"x", {.numDistinct = 50}}});
  verifyPlan(sql, [](const Plan& plan) {
    const auto& join = findOp<Join>(*plan.op, RelType::kJoin);
    EXPECT_EQ(join.joinType, velox::core::JoinType::kCountingAnti);
    EXPECT_NEAR(join.resultCardinality(), 500, kCardinalityTolerance);
  });
}

// Verifies cardinality estimation for multi-key aggregation.
TEST_F(CardinalityEstimationTest, multiKeyAggregation) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()))
      ->setStats(
          100'000,
          {
              {"a", {.numDistinct = 10}},
              {"b", {.numDistinct = 20}},
              {"c", {.numDistinct = 1'000}},
          });

  verifyPlan(
      "SELECT a, b, count(*) FROM t GROUP BY a, b", [](const Plan& plan) {
        const auto& op = *plan.op;
        ASSERT_EQ(op.columns().size(), 3);
        // Grouping on (a, b): expected ~10 * 20 = 200 groups.
        EXPECT_NEAR(op.resultCardinality(), 200, kCardinalityTolerance);

        auto a = findConstraint(op, 0);
        ASSERT_TRUE(a.has_value());
        AXIOM_ASSERT_NORANGE(*a);

        auto b = findConstraint(op, 1);
        ASSERT_TRUE(b.has_value());
        AXIOM_ASSERT_NORANGE(*b);
      });
}

// Verifies constraint propagation through a join followed by aggregation.
TEST_F(CardinalityEstimationTest, joinThenAggregate) {
  testConnector_->addTable("o", ROW({"o_custkey", "o_total"}, BIGINT()))
      ->setStats(
          10'000,
          {
              {"o_custkey", {.numDistinct = 1'000}},
              {"o_total", {.numDistinct = 5'000}},
          });
  testConnector_->addTable("c", ROW({"c_custkey", "c_name"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"c_custkey", {.numDistinct = 1'000}},
              {"c_name", {.numDistinct = 1'000}},
          });

  verifyPlan(
      "SELECT c_custkey, sum(o_total) FROM o JOIN c ON o_custkey = c_custkey GROUP BY 1",
      [](const Plan& plan) {
        const auto& op = *plan.op;
        ASSERT_EQ(op.columns().size(), 2);

        // After join and group by c_custkey: expect ~1000 groups.
        EXPECT_NEAR(op.resultCardinality(), 1'000, kCardinalityTolerance);

        auto custkey = findConstraint(op, 0);
        ASSERT_TRUE(custkey.has_value());
        EXPECT_NEAR(custkey->cardinality, 1'000, kCardinalityTolerance);
        AXIOM_ASSERT_NORANGE(*custkey);
      });
}

// Verifies cardinality estimation for unnest: output cardinality reflects
// input cardinality * fanout heuristic. Replicate columns preserve input
// constraints; unnested columns have no detailed statistics.
TEST_F(CardinalityEstimationTest, unnest) {
  testConnector_->addTable("t", ROW({"a", "b"}, {ARRAY(BIGINT()), BIGINT()}))
      ->setStats(
          1'000,
          {
              {"a", {.numDistinct = 100}},
              {"b", {.min = 1LL, .max = 500LL, .numDistinct = 200}},
          });

  verifyPlan(
      "SELECT b, e FROM t CROSS JOIN UNNEST(a) AS u(e)", [](const Plan& plan) {
        const auto& op = findOp(*plan.op, RelType::kUnnest);

        // Unnest uses a default fanout of 10.
        EXPECT_NEAR(op.resultCardinality(), 10'000, kCardinalityTolerance);

        ASSERT_EQ(op.columns().size(), 2);

        // Replicate column 'b' preserves input constraints.
        auto b = findConstraint(op, 0);
        ASSERT_TRUE(b.has_value());
        EXPECT_NEAR(b->cardinality, 200, kCardinalityTolerance);
        AXIOM_ASSERT_RANGE(*b, 1, 500);

        // Unnested column 'e' has no detailed statistics.
        auto e = findConstraint(op, 1);
        ASSERT_TRUE(e.has_value());
        AXIOM_ASSERT_NORANGE(*e);
      });
}

// Verifies that LIMIT reduces output cardinality and adjusts column
// constraints accordingly.
TEST_F(CardinalityEstimationTest, limit) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.min = 1LL, .max = 500LL, .numDistinct = 100}},
              {"b", {.numDistinct = 800}},
          });

  verifyPlan("SELECT a, b FROM t LIMIT 50", [](const Plan& plan) {
    const auto& op = findOp(*plan.op, RelType::kLimit);

    EXPECT_NEAR(op.resultCardinality(), 50, kCardinalityTolerance);
    ASSERT_EQ(op.columns().size(), 2);

    // sampledNdv(100, 1000, 50/1000) =
    //   100 * (1 - (1 - 1/100)^50) = 100 * 0.395 ≈ 39.5.
    auto a = findConstraint(op, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 39.5, kCardinalityTolerance);
    AXIOM_ASSERT_RANGE(*a, 1, 500);

    // sampledNdv(800, 1000, 50/1000) =
    //   800 * (1 - (1 - 1/800)^50) = 800 * 0.0607 ≈ 48.5.
    auto b = findConstraint(op, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 48.5, kCardinalityTolerance);
  });
}

// Verifies that UNION ALL produces combined cardinality.
TEST_F(CardinalityEstimationTest, unionAll) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a",
               {.nullPct = 10, .min = 1LL, .max = 500LL, .numDistinct = 100}},
              {"b",
               {.nullPct = 30, .min = 10LL, .max = 200LL, .numDistinct = 400}},
          });
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          2'000,
          {
              {"x",
               {.nullPct = 20, .min = 200LL, .max = 800LL, .numDistinct = 200}},
              {"y",
               {.nullPct = 50, .min = 100LL, .max = 300LL, .numDistinct = 600}},
          });

  verifyPlan(
      "SELECT a, b FROM t UNION ALL SELECT x, y FROM u", [](const Plan& plan) {
        const auto& op = *plan.op;

        ASSERT_EQ(op.columns().size(), 2);
        EXPECT_NEAR(op.resultCardinality(), 3'000, kCardinalityTolerance);

        auto a = findConstraint(op, 0);
        ASSERT_TRUE(a.has_value());
        EXPECT_NEAR(a->cardinality, 300, kCardinalityTolerance);
        // (1000 * 0.1 + 2000 * 0.2) / 3000.
        EXPECT_NEAR(a->nullFraction, 500.0 / 3000, kTolerance);
        AXIOM_ASSERT_RANGE(*a, 1, 800);

        auto b = findConstraint(op, 1);
        ASSERT_TRUE(b.has_value());
        EXPECT_NEAR(b->cardinality, 1'000, kCardinalityTolerance);
        // (1000 * 0.3 + 2000 * 0.5) / 3000.
        EXPECT_NEAR(b->nullFraction, 1300.0 / 3000, kTolerance);
        AXIOM_ASSERT_RANGE(*b, 10, 300);
      });
}

// Verifies that UNION produces deduplicated cardinality.
TEST_F(CardinalityEstimationTest, unionDistinct) {
  testConnector_->addTable("t", ROW({"a"}, BIGINT()))
      ->setStats(
          1'000,
          {{"a", {.nullPct = 10, .min = 1LL, .max = 500LL, .numDistinct = 100}},
           {"b", {.numDistinct = 500}}});

  testConnector_->addTable("u", ROW({"a"}, BIGINT()))
      ->setStats(
          2'000,
          {{"a",
            {.nullPct = 20, .min = 200LL, .max = 800LL, .numDistinct = 200}}});

  verifyPlan("SELECT a FROM t UNION SELECT a FROM u", [](const Plan& plan) {
    const auto& op = *plan.op;
    ASSERT_EQ(op.columns().size(), 1);
    // UNION = UNION ALL + GROUP BY. UNION ALL produces 3'000 rows with
    // ndv(t.a) + ndv(u.a) = 300 distinct values. GROUP BY reduces to ~300
    // rows.
    EXPECT_NEAR(op.resultCardinality(), 300, kCardinalityTolerance);

    auto a = findConstraint(op, 0);
    ASSERT_TRUE(a.has_value());
    // NDV stays at ndv(t.a) + ndv(u.a) = 300.
    EXPECT_NEAR(a->cardinality, 300, kCardinalityTolerance);
    // Combined null fraction: (1'000 * 0.1 + 2'000 * 0.2) / 3'000 = 500/3'000.
    EXPECT_NEAR(a->nullFraction, 500.0 / 3'000, kTolerance);
    EXPECT_TRUE(a->nullable);
    // Min/max should be the union of both ranges: [1, 800].
    AXIOM_ASSERT_RANGE(*a, 1, 800);
  });
}

// Verifies that ORDER BY with LIMIT scales column constraints using
// sampledNdv, same as Limit.
TEST_F(CardinalityEstimationTest, orderByWithLimit) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"a", {.min = 1LL, .max = 500LL, .numDistinct = 100}},
              {"b", {.numDistinct = 800}},
          });

  verifyPlan("SELECT a, b FROM t ORDER BY a LIMIT 50", [](const Plan& plan) {
    const auto& op = findOp(*plan.op, RelType::kOrderBy);

    EXPECT_NEAR(op.resultCardinality(), 50, kCardinalityTolerance);
    ASSERT_EQ(op.columns().size(), 2);

    // sampledNdv(100, 1000, 50/1000) ≈ 39.5.
    auto a = findConstraint(op, 0);
    ASSERT_TRUE(a.has_value());
    EXPECT_NEAR(a->cardinality, 39.5, kCardinalityTolerance);
    AXIOM_ASSERT_RANGE(*a, 1, 500);

    // sampledNdv(800, 1000, 50/1000) ≈ 48.5.
    auto b = findConstraint(op, 1);
    ASSERT_TRUE(b.has_value());
    EXPECT_NEAR(b->cardinality, 48.5, kCardinalityTolerance);
  });
}

#undef AXIOM_ASSERT_RANGE
#undef AXIOM_ASSERT_NORANGE

} // namespace
} // namespace facebook::axiom::optimizer
