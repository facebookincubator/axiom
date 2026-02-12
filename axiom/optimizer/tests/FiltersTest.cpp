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

#include "axiom/optimizer/Filters.h"
#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <functional>
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/expression/Expr.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

// Absolute tolerance for floating-point comparisons in selectivity tests.
// Selectivity values are estimates based on statistics and heuristics, not
// exact calculations. A tolerance of 0.001 (0.1%) provides sufficient precision
// for verifying selectivity estimates while accommodating minor variations from
// floating-point arithmetic and statistical approximations.
constexpr double kTolerance = 0.001;

// Specifies expected filter results for a SQL condition on an integer column.
// Optional fields are only verified when set.
struct FilterTestCase {
  std::string condition;
  std::optional<double> expectedSelectivity;
  std::optional<int64_t> expectedMin;
  std::optional<int64_t> expectedMax;
  std::optional<double> expectedCardinality;
};

template <typename T>
Value makeValue(float cardinality, T min, T max, float nullFraction = 0.0) {
  Value value(velox::CppToType<T>::create().get(), cardinality);
  value.min = registerVariant(velox::Variant::create<T>(min));
  value.max = registerVariant(velox::Variant::create<T>(max));
  value.nullFraction = nullFraction;
  return value;
}

// Estimates selectivity for 'funcName' comparison (eq, lt, gt, etc.) using
// Value statistics (min, max, cardinality).
Selectivity columnComparisonSelectivity(
    const Value& leftValue,
    const Value& rightValue,
    std::string_view funcName) {
  ConstraintMap unused;
  return columnComparisonSelectivity(
      /*left=*/nullptr,
      /*right=*/nullptr,
      leftValue,
      rightValue,
      toName(funcName),
      /*updateConstraints=*/false,
      unused);
}

class FiltersTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    HiveQueriesTestBase::TearDownTestCase();
  }

  // Runs the callback with a QueryGraphContext set up.
  void withContext(const std::function<void()>& callback) {
    HashStringAllocator allocator(&optimizerPool());
    auto context = std::make_unique<optimizer::QueryGraphContext>(allocator);
    optimizer::queryCtx() = context.get();

    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    callback();
  }

  // Parses SQL, builds the query graph and invokes the callback with rootDt.
  void verifyQueryGraph(
      std::string_view sql,
      const std::function<void(DerivedTableCP)>& callback,
      const std::string& connectorId = exec::test::kHiveConnectorId) {
    auto logicalPlan = parseSelect(sql, connectorId);

    auto& veloxQueryCtx = getQueryCtx();

    HashStringAllocator allocator(&optimizerPool());
    auto context = std::make_unique<optimizer::QueryGraphContext>(allocator);

    optimizer::queryCtx() = context.get();
    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    exec::SimpleExpressionEvaluator evaluator(
        veloxQueryCtx.get(), &optimizerPool());

    auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

    connector::SchemaResolver schemaResolver;

    runner::MultiFragmentPlan::Options options;
    options.numWorkers = 1;
    options.numDrivers = 1;

    optimizer::VeloxHistory history;

    optimizer::Optimization optimization(
        session,
        *logicalPlan,
        schemaResolver,
        history,
        veloxQueryCtx,
        evaluator,
        optimizerOptions_,
        options);

    auto* rootDt = optimization.rootDt();

    callback(rootDt);
  }

  // Returns nullptr if the table is not found.
  static const BaseTable* findBaseTable(
      DerivedTableCP dt,
      std::string_view tableName) {
    for (auto* table : dt->tables) {
      if (table->is(PlanType::kTableNode)) {
        auto* bt = table->as<BaseTable>();
        if (bt->schemaTable && bt->schemaTable->name() == tableName) {
          return bt;
        }
      }
    }
    return nullptr;
  }

  static ExprVector getAllFilters(
      DerivedTableCP rootDt,
      std::string_view tableName) {
    const BaseTable* table = findBaseTable(rootDt, tableName);
    VELOX_CHECK_NOT_NULL(table, "Table '{}' not found", tableName);

    auto allFilters = table->columnFilters;
    allFilters.insert(
        allFilters.end(), table->filter.begin(), table->filter.end());
    return allFilters;
  }

  // Creates a ConstraintMap for all columns referenced by the given expressions
  // using their schema values.
  static ConstraintMap makeSchemaConstraints(const ExprVector& exprs) {
    ConstraintMap constraints;
    for (auto* expr : exprs) {
      expr->columns().forEach<Column>([&](auto* column) {
        if (auto* schemaColumn = column->schemaColumn()) {
          constraints.emplace(column->id(), schemaColumn->value());
        }
      });
    }
    return constraints;
  }

  // Parses SQL, extracts filters for 'tableName', computes selectivity with
  // constraint updates, and invokes 'verify' with the selectivity and updated
  // constraint for the filtered column (left side of the first filter).
  void verifyFilter(
      std::string_view sql,
      std::string_view tableName,
      const std::function<void(const Selectivity&, const Value&)>& verify) {
    verifyQueryGraph(sql, [&](DerivedTableCP rootDt) {
      auto allFilters = getAllFilters(rootDt, tableName);
      ASSERT_FALSE(allFilters.empty());

      auto constraints = makeSchemaConstraints(allFilters);
      auto selectivity = conjunctsSelectivity(constraints, allFilters, true);

      auto* call = allFilters[0]->as<Call>();
      auto columnId = call->args()[0]->id();

      auto it = constraints.find(columnId);
      ASSERT_NE(it, constraints.end());
      verify(selectivity, it->second);
    });
  }

  // Runs a list of FilterTestCases against 'tableName', verifying whichever
  // expected fields are set.
  void verifyFilterTestCases(
      std::string_view tableName,
      const std::vector<FilterTestCase>& testCases) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE("Testing condition: " + testCase.condition);

      auto sql = fmt::format(
          "SELECT * FROM {} WHERE {}", tableName, testCase.condition);
      verifyFilter(
          sql,
          tableName,
          [&](const Selectivity& selectivity, const Value& constraint) {
            if (testCase.expectedSelectivity.has_value()) {
              EXPECT_NEAR(
                  selectivity.trueFraction,
                  *testCase.expectedSelectivity,
                  kTolerance);
            }

            if (testCase.expectedMin.has_value()) {
              ASSERT_NE(constraint.min, nullptr);
              EXPECT_EQ(
                  constraint.min->value<int64_t>(), *testCase.expectedMin);
            }

            if (testCase.expectedMax.has_value()) {
              ASSERT_NE(constraint.max, nullptr);
              EXPECT_EQ(
                  constraint.max->value<int64_t>(), *testCase.expectedMax);
            }

            if (testCase.expectedCardinality.has_value()) {
              EXPECT_NEAR(
                  constraint.cardinality,
                  *testCase.expectedCardinality,
                  kTolerance);
            }
          });
    }
  }
};

// ============================================================
// Selectivity algebra unit tests
// ============================================================

TEST_F(FiltersTest, combineConjunctsBasic) {
  std::vector<Selectivity> conjuncts = {
      {0.8, 0.0}, // 80% true, 0% null, 20% false
      {0.6, 0.0} // 60% true, 0% null, 40% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = product of trueFractions.
  EXPECT_NEAR(result.trueFraction, 0.8 * 0.6, kTolerance);
  // P(NULL) = P(TRUE|NULL) - P(TRUE) = 0.8 * 0.6 - 0.48 = 0.
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsWithNulls) {
  std::vector<Selectivity> conjuncts = {
      {0.5, 0.2}, // 50% true, 20% null, 30% false
      {0.6, 0.1} // 60% true, 10% null, 30% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = product of trueFractions.
  EXPECT_NEAR(result.trueFraction, 0.5 * 0.6, kTolerance);
  // P(NULL) = P(TRUE|NULL) - P(TRUE).
  EXPECT_NEAR(
      result.nullFraction, (0.5 + 0.2) * (0.6 + 0.1) - 0.5 * 0.6, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsMultiple) {
  std::vector<Selectivity> conjuncts = {
      {0.8, 0.1}, // 80% true, 10% null, 10% false
      {0.7, 0.0}, // 70% true, 0% null, 30% false
      {0.9, 0.05} // 90% true, 5% null, 5% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  EXPECT_NEAR(result.trueFraction, 0.8 * 0.7 * 0.9, kTolerance);
  EXPECT_NEAR(
      result.nullFraction,
      (0.8 + 0.1) * 0.7 * (0.9 + 0.05) - 0.8 * 0.7 * 0.9,
      kTolerance);
}

TEST_F(FiltersTest, combineConjunctsEmpty) {
  std::vector<Selectivity> empty;
  Selectivity result = combineConjuncts(empty);
  EXPECT_NEAR(result.trueFraction, 1.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsAllNull) {
  std::vector<Selectivity> conjuncts = {
      {0.0, 1.0}, // 0% true, 100% null, 0% false
      {0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 1.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsBasic) {
  std::vector<Selectivity> disjuncts = {
      {0.3, 0.0}, // 30% true, 0% null, 70% false
      {0.4, 0.0} // 40% true, 0% null, 60% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - product of (1 - trueFraction_i).
  EXPECT_NEAR(result.trueFraction, 1.0 - (1.0 - 0.3) * (1.0 - 0.4), kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsWithNulls) {
  std::vector<Selectivity> disjuncts = {
      {0.3, 0.2}, // 30% true, 20% null, 50% false
      {0.4, 0.1} // 40% true, 10% null, 50% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - product of (1 - trueFraction_i).
  EXPECT_NEAR(result.trueFraction, 1.0 - (1.0 - 0.3) * (1.0 - 0.4), kTolerance);
  // P(NULL) = 1 - P(TRUE) - P(FALSE), where P(FALSE) = product of
  // falseFractions.
  EXPECT_NEAR(
      result.nullFraction,
      1.0 - (1.0 - (1.0 - 0.3) * (1.0 - 0.4)) - 0.5 * 0.5,
      kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsMultiple) {
  std::vector<Selectivity> disjuncts = {
      {0.2, 0.1}, // 20% true, 10% null, 70% false
      {0.3, 0.0}, // 30% true, 0% null, 70% false
      {0.1, 0.05} // 10% true, 5% null, 85% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  EXPECT_NEAR(
      result.trueFraction,
      1.0 - (1.0 - 0.2) * (1.0 - 0.3) * (1.0 - 0.1),
      kTolerance);
  EXPECT_NEAR(
      result.nullFraction,
      1.0 - (1.0 - (1.0 - 0.2) * (1.0 - 0.3) * (1.0 - 0.1)) - 0.7 * 0.7 * 0.85,
      kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsEmpty) {
  std::vector<Selectivity> empty;
  Selectivity result = combineDisjuncts(empty);
  EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsAllNull) {
  std::vector<Selectivity> disjuncts = {
      {0.0, 1.0}, // 0% true, 100% null, 0% false
      {0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 1.0, kTolerance);
}

// ============================================================
// Column comparison unit tests
// ============================================================

TEST_F(FiltersTest, columnComparisonNoOverlapBelow) {
  withContext([&]() {
    auto leftValue = makeValue<int64_t>(50, 100, 200);
    auto rightValue = makeValue<int64_t>(50, 300, 400);

    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "lt");
    EXPECT_NEAR(result.trueFraction, 1.0, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    result = columnComparisonSelectivity(leftValue, rightValue, "gt");
    EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    result = columnComparisonSelectivity(leftValue, rightValue, "eq");
    EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
  });
}

TEST_F(FiltersTest, columnComparisonNoOverlapAbove) {
  withContext([&]() {
    auto leftValue = makeValue<int64_t>(50, 300, 400);
    auto rightValue = makeValue<int64_t>(50, 100, 200);

    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "gt");
    EXPECT_NEAR(result.trueFraction, 1.0, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    result = columnComparisonSelectivity(leftValue, rightValue, "lt");
    EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
  });
}

TEST_F(FiltersTest, columnComparisonPartialOverlap) {
  withContext([&]() {
    // left [1000, 2000], right [1500, 2500]. Overlap = [1500, 2000].
    auto leftValue = makeValue<int64_t>(100, 1000, 2000);
    auto rightValue = makeValue<int64_t>(100, 1500, 2500);

    // P(eq) = overlapFraction / maxCardinality = 0.5 / 100.
    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "eq");
    EXPECT_NEAR(result.trueFraction, 0.5 / 100, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    result = columnComparisonSelectivity(leftValue, rightValue, "lt");
    EXPECT_NEAR(result.trueFraction, 0.875, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    result = columnComparisonSelectivity(leftValue, rightValue, "gt");
    EXPECT_NEAR(result.trueFraction, 0.125, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
  });
}

TEST_F(FiltersTest, columnComparisonFullOverlap) {
  withContext([&]() {
    auto leftValue = makeValue<int64_t>(50, 100, 200);
    auto rightValue = makeValue<int64_t>(50, 100, 200);

    // P(eq) = 1 / maxCardinality = 1 / 50.
    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "eq");
    EXPECT_NEAR(result.trueFraction, 1.0 / 50, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);

    // Identical ranges: P(lt) ≈ P(gt) ≈ 0.5.
    result = columnComparisonSelectivity(leftValue, rightValue, "lt");
    EXPECT_GT(result.trueFraction, 0.4);
    EXPECT_LT(result.trueFraction, 0.6);

    result = columnComparisonSelectivity(leftValue, rightValue, "gt");
    EXPECT_GT(result.trueFraction, 0.4);
    EXPECT_LT(result.trueFraction, 0.6);
  });
}

TEST_F(FiltersTest, columnComparisonWithNulls) {
  withContext([&]() {
    auto leftValue = makeValue<int64_t>(50, 100, 200, 0.1);
    auto rightValue = makeValue<int64_t>(50, 150, 250, 0.2);

    // P(NULL) = 1 - (1-0.1)*(1-0.2) = 0.28.
    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "eq");
    EXPECT_NEAR(
        result.nullFraction, 1.0 - (1.0 - 0.1) * (1.0 - 0.2), kTolerance);
    EXPECT_GT(result.trueFraction, 0.0);
    EXPECT_LT(result.trueFraction, (1.0 - 0.1) * (1.0 - 0.2));

    result = columnComparisonSelectivity(leftValue, rightValue, "lt");
    EXPECT_NEAR(
        result.nullFraction, 1.0 - (1.0 - 0.1) * (1.0 - 0.2), kTolerance);
    EXPECT_GT(result.trueFraction, 0.0);
    EXPECT_LT(result.trueFraction, (1.0 - 0.1) * (1.0 - 0.2));

    result = columnComparisonSelectivity(leftValue, rightValue, "gt");
    EXPECT_NEAR(
        result.nullFraction, 1.0 - (1.0 - 0.1) * (1.0 - 0.2), kTolerance);
    EXPECT_GT(result.trueFraction, 0.0);
    EXPECT_LT(result.trueFraction, (1.0 - 0.1) * (1.0 - 0.2));
  });
}

TEST_F(FiltersTest, columnComparisonDifferentCardinalities) {
  withContext([&]() {
    auto leftValue = makeValue<int64_t>(10, 100, 200);
    auto rightValue = makeValue<int64_t>(100, 150, 250);

    // P(eq) = overlapFraction / maxCardinality = 0.5 / 100.
    Selectivity result =
        columnComparisonSelectivity(leftValue, rightValue, "eq");
    EXPECT_NEAR(result.trueFraction, 0.5 / 100, kTolerance);
    EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
  });
}

TEST_F(FiltersTest, columnComparisonLteGte) {
  withContext([&]() {
    // Non-overlapping ranges: lt/lte and gt/gte should be equivalent.
    auto leftValue = makeValue<int64_t>(50, 100, 200);
    auto rightValue = makeValue<int64_t>(50, 300, 400);

    Selectivity resultLt =
        columnComparisonSelectivity(leftValue, rightValue, "lt");
    Selectivity resultLte =
        columnComparisonSelectivity(leftValue, rightValue, "lte");
    EXPECT_NEAR(resultLt.trueFraction, resultLte.trueFraction, kTolerance);

    Selectivity resultGt =
        columnComparisonSelectivity(leftValue, rightValue, "gt");
    Selectivity resultGte =
        columnComparisonSelectivity(leftValue, rightValue, "gte");
    EXPECT_NEAR(resultGt.trueFraction, resultGte.trueFraction, kTolerance);
  });
}

// ============================================================
// SQL-level filter selectivity and constraint tests
// ============================================================

TEST_F(FiltersTest, rangeSelectivity) {
  // Multi-column range filter on lineitem.
  verifyFilter(
      "SELECT l_shipdate, l_partkey, l_suppkey "
      "FROM lineitem "
      // TODO Replace with BETWEEN once it is supported.
      "WHERE l_shipdate >= CAST('1995-01-01' AS date) AND l_shipdate <= CAST('1995-06-01' AS date) "
      "   AND l_partkey < 10000 AND l_suppkey > 10",
      "lineitem",
      [](const Selectivity& selectivity, const Value&) {
        EXPECT_GE(selectivity.trueFraction, 0.0);
        EXPECT_LE(selectivity.trueFraction, 1.0);
      });

  // Overlapping range conditions on n_nationkey (BIGINT, min=0, max=24,
  // cardinality=25). Ranges [2, 15] AND [5, 20] should combine to [5, 15].
  verifyFilterTestCases(
      "nation",
      {
          // 11 values in [5, 15] out of 25.
          {.condition =
               // TODO Replace with BETWEEN once it is supported.
           "n_nationkey >= 2 AND n_nationkey <= 15 "
           "AND n_nationkey >= 5 AND n_nationkey <= 20",
           .expectedSelectivity = 11.0 / 25,
           .expectedMin = 5,
           .expectedMax = 15},
      });
}

TEST_F(FiltersTest, strictInequalityIntegerBounds) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      {
          // Strict lower and upper bounds are adjusted for integers.
          {.condition = "n_nationkey > 2 AND n_nationkey < 22",
           .expectedMin = 3,
           .expectedMax = 21},
          // Non-strict bounds are not adjusted.
          {.condition = "n_nationkey >= 2 AND n_nationkey <= 22",
           .expectedMin = 2,
           .expectedMax = 22},
          // Mixed: strict lower, non-strict upper.
          {.condition = "n_nationkey > 2 AND n_nationkey <= 15",
           .expectedMin = 3,
           .expectedMax = 15},
          // Mixed: non-strict lower, strict upper.
          {.condition = "n_nationkey >= 5 AND n_nationkey < 20",
           .expectedMin = 5,
           .expectedMax = 19},
          // Only strict lower bound.
          {.condition = "n_nationkey > 10",
           .expectedMin = 11,
           .expectedMax = 24},
          // Only strict upper bound.
          {.condition = "n_nationkey < 10", .expectedMin = 0, .expectedMax = 9},
          // Strict lower bound at column minimum (> 0 on column with min=0).
          {.condition = "n_nationkey > 0", .expectedMin = 1, .expectedMax = 24},
      });
}

TEST_F(FiltersTest, strictInequalityStringBounds) {
  struct StringFilterTestCase {
    std::string condition;
    double expectedSelectivity;
    std::string expectedMin;
    std::string expectedMax;
    float expectedCardinality;
  };

  // Strict inequalities on VARCHAR do not adjust bounds.
  verifyFilter(
      "SELECT * FROM nation WHERE n_name > 'B' AND n_name < 'P'",
      "nation",
      [](const Selectivity& selectivity, const Value& constraint) {
        EXPECT_GE(selectivity.trueFraction, 0.0);
        EXPECT_LE(selectivity.trueFraction, 1.0);

        ASSERT_NE(constraint.min, nullptr);
        EXPECT_EQ(constraint.min->value<std::string>(), "B");

        ASSERT_NE(constraint.max, nullptr);
        EXPECT_EQ(constraint.max->value<std::string>(), "P");
      });

  // String range selectivity with VARCHAR bounds. n_name has 25 values with
  // 22 distinct first characters (first-character approximation).
  std::vector<StringFilterTestCase> testCases = {
      // Different first characters: 6 letters in ['B', 'G'] out of 22.
      {
          "n_name >= 'B' AND n_name <= 'P' AND n_name >= 'A' AND n_name <= 'G'",
          6.0 / 22,
          "B",
          "G",
          6.8f,
      },
      // Same first character. The first-character approximation would give 0,
      // but the floor ensures a small positive estimate.
      {
          "n_name >= 'BRAZIL' AND n_name <= 'BRITAIN'",
          1.0 / 22,
          "BRAZIL",
          "BRITAIN",
          1.0f,
      },
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE("Testing condition: " + testCase.condition);

    auto sql = fmt::format("SELECT * FROM nation WHERE {}", testCase.condition);
    verifyFilter(
        sql,
        "nation",
        [&](const Selectivity& selectivity, const Value& constraint) {
          EXPECT_NEAR(
              selectivity.trueFraction,
              testCase.expectedSelectivity,
              kTolerance);

          ASSERT_NE(constraint.min, nullptr);
          EXPECT_EQ(constraint.min->value<std::string>(), testCase.expectedMin);

          ASSERT_NE(constraint.max, nullptr);
          EXPECT_EQ(constraint.max->value<std::string>(), testCase.expectedMax);

          EXPECT_NEAR(
              constraint.cardinality, testCase.expectedCardinality, 1.0f);
        });
  }
}

TEST_F(FiltersTest, contradictoryFilters) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // Contradictory conditions get likelyZero selectivity (small but non-zero
  // to avoid zeroing out downstream estimates) and empty constraints
  // (cardinality=0, min=nullptr, max=nullptr).
  std::vector<std::string> conditions = {
      "n_nationkey = 1 AND n_nationkey = 2",
      "n_nationkey = 3 AND n_nationkey > 10",
      "n_nationkey = 100",
      "n_nationkey IN (1, 2, 3) AND n_nationkey = 10",
      "n_nationkey IN (1, 2, 3) AND n_nationkey IN (10, 11, 12)",
      "n_nationkey IN (50, 100)",
      "n_nationkey > 10 AND n_nationkey < 5",
      // TODO Replace with BETWEEN once it is supported.
      "n_nationkey >= 1 AND n_nationkey <= 3 AND n_nationkey >= 10 AND n_nationkey <= 13",
      // Strict inequality at column max (n_nationkey has max=24). After
      // adjusting strict bound for integer type, lower becomes 25 > upper 24.
      "n_nationkey > 24",
  };

  for (const auto& condition : conditions) {
    SCOPED_TRACE("Testing condition: " + condition);

    auto sql = fmt::format("SELECT * FROM nation WHERE {}", condition);
    verifyFilter(
        sql,
        "nation",
        [](const Selectivity& selectivity, const Value& constraint) {
          EXPECT_NEAR(
              selectivity.trueFraction, Selectivity::kLikelyZero, kTolerance);
          EXPECT_EQ(constraint.cardinality, 0);
          EXPECT_EQ(constraint.min, nullptr);
          EXPECT_EQ(constraint.max, nullptr);
        });
  }
}

TEST_F(FiltersTest, equalitySelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // 1 value out of 25.
  verifyFilterTestCases(
      "nation",
      {
          {
              .condition = "n_nationkey = 10",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 10,
              .expectedMax = 10,
              .expectedCardinality = 1.0,
          },
          {
              .condition = "n_nationkey = 0",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 0,
              .expectedMax = 0,
              .expectedCardinality = 1.0,
          },
          {
              .condition = "n_nationkey = 24",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 24,
              .expectedMax = 24,
              .expectedCardinality = 1.0,
          },
      });
}

TEST_F(FiltersTest, inListSelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      {
          // 3 values out of 25.
          {
              .condition = "n_nationkey IN (1, 2, 3)",
              .expectedSelectivity = 3.0 / 25,
              .expectedMin = 1,
              .expectedMax = 3,
          },
          // 4 values out of 25.
          {
              .condition = "n_nationkey IN (5, 10, 15, 20)",
              .expectedSelectivity = 4.0 / 25,
              .expectedMin = 5,
              .expectedMax = 20,
          },
      });
}

TEST_F(FiltersTest, doubleRangeSelectivity) {
  // o_totalprice is DOUBLE on orders table (150,000 rows, min ≈ 857.71).
  // Range filter within valid range.
  verifyFilter(
      "SELECT o_totalprice FROM orders WHERE o_totalprice > 1000",
      "orders",
      [](const Selectivity& selectivity, const Value& constraint) {
        // 1000 is near the column minimum (~857.71) with max in the hundreds
        // of thousands, so nearly all rows pass.
        EXPECT_GT(selectivity.trueFraction, 0.99);

        // Strict inequality on DOUBLE is not adjusted (unlike integers).
        ASSERT_NE(constraint.min, nullptr);
        EXPECT_EQ(constraint.min->value<double>(), 1000.0);
      });

  // Contradictory: column min ≈ 857.71, so o_totalprice < 10 has no rows.
  verifyFilter(
      "SELECT o_totalprice FROM orders WHERE o_totalprice < 10",
      "orders",
      [](const Selectivity& selectivity, const Value& constraint) {
        EXPECT_NEAR(
            selectivity.trueFraction, Selectivity::kLikelyZero, kTolerance);

        EXPECT_EQ(constraint.cardinality, 0);
        EXPECT_EQ(constraint.min, nullptr);
        EXPECT_EQ(constraint.max, nullptr);
      });
}

TEST_F(FiltersTest, orSelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // P(OR) = 1 - product of (1 - P(each disjunct)).
  verifyFilterTestCases(
      "nation",
      {
          // P(< 5) = 5/25, P(> 20) = 4/25.
          {.condition = "n_nationkey < 5 OR n_nationkey > 20",
           .expectedSelectivity = 1.0 - (1.0 - 5.0 / 25) * (1.0 - 4.0 / 25)},
          // P(= 10) = 1/25, P(> 20) = 4/25.
          {.condition = "n_nationkey = 10 OR n_nationkey > 20",
           .expectedSelectivity = 1.0 - (1.0 - 1.0 / 25) * (1.0 - 4.0 / 25)},
          // P(> 5) = 19/25, P(< 20) = 20/25.
          {.condition = "n_nationkey > 5 OR n_nationkey < 20",
           .expectedSelectivity = 1.0 - (1.0 - 19.0 / 25) * (1.0 - 20.0 / 25)},
          // P(= 3) = 1/25, P(= 100) = likelyZero (out of range).
          {.condition = "n_nationkey = 3 OR n_nationkey = 100",
           .expectedSelectivity =
               1.0 - (1.0 - 1.0 / 25) * (1.0 - Selectivity::kLikelyZero)},
      });
}

TEST_F(FiltersTest, notSelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      {
          // NOT(P(= 10)) = 1 - 1/25.
          {.condition = "NOT(n_nationkey = 10)",
           .expectedSelectivity = 1.0 - 1.0 / 25},
          // NOT(P(= 100)) = 1 - likelyZero (100 is out of range).
          {.condition = "NOT(n_nationkey = 100)",
           .expectedSelectivity = 1.0 - Selectivity::kLikelyZero},
          // NOT IN with 3 in-range values (2, 7, 8) and 1 out-of-range (100).
          {.condition = "n_nationkey NOT IN (2, 7, 8, 100)",
           .expectedSelectivity = 1.0 - 3.0 / 25},
      });
}

TEST_F(FiltersTest, unsupportedExpressions) {
  verifyFilterTestCases(
      "nation",
      {
          // The <> operator is not normalized to NOT(eq) and is treated as an
          // unknown function, receiving default selectivity kLikelyTrue. This
          // differs from NOT(n_nationkey = 10) which correctly computes
          // 1 - 1/25 = 0.96.
          {.condition = "n_nationkey <> 10",
           .expectedSelectivity = Selectivity::kLikelyTrue},
          // BETWEEN is not yet supported and is treated as an unknown function.
          // Use x >= lower AND x <= upper for better estimates.
          {.condition = "n_nationkey BETWEEN 5 AND 15",
           .expectedSelectivity = Selectivity::kLikelyTrue},
      });
}

TEST_F(FiltersTest, multiColumnConjuncts) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      {
          // P(n_nationkey > 10) = 14/25, combined with P(n_name > 'M')
          // estimated
          // from string range statistics.
          {.condition = "n_nationkey > 10 AND n_name > 'M'",
           .expectedSelectivity = 0.255},
          {.condition =
               "(n_nationkey = 1 AND n_regionkey = 2 AND n_name = 'FRANCE') "
               "OR (n_regionkey = 2 AND n_name = 'EGYPT' AND n_nationkey = 11)",
           .expectedSelectivity = 0.000639498},
      });
}

TEST_F(FiltersTest, equalityConstraintPropagation) {
  verifyQueryGraph(
      "SELECT n_regionkey FROM nation WHERE n_regionkey = n_nationkey",
      [](DerivedTableCP rootDt) {
        auto allFilters = getAllFilters(rootDt, "nation");
        ASSERT_EQ(allFilters.size(), 1);

        auto constraints = makeSchemaConstraints(allFilters);
        auto selectivity = exprSelectivity(constraints, allFilters[0], true);

        EXPECT_GE(selectivity.trueFraction, 0.0);
        EXPECT_LE(selectivity.trueFraction, 1.0);

        // Both columns are constrained to the overlapping range [0, 4]
        // (n_regionkey's range).
        ASSERT_EQ(constraints.size(), 2);

        for (const auto& [id, constraintValue] : constraints) {
          EXPECT_GE(constraintValue.cardinality, 4.0f);
          EXPECT_LE(constraintValue.cardinality, 5.5f);

          ASSERT_NE(constraintValue.min, nullptr);
          EXPECT_EQ(constraintValue.min->value<int64_t>(), 0);

          ASSERT_NE(constraintValue.max, nullptr);
          EXPECT_EQ(constraintValue.max->value<int64_t>(), 4);
        }
      });
}

TEST_F(FiltersTest, isNullSelectivity) {
  // n_comment is VARCHAR and nullable.
  verifyQueryGraph(
      "SELECT * FROM nation WHERE n_comment IS NULL",
      [](DerivedTableCP rootDt) {
        auto allFilters = getAllFilters(rootDt, "nation");
        ASSERT_FALSE(allFilters.empty());

        auto constraints = makeSchemaConstraints(allFilters);
        auto selectivity = conjunctsSelectivity(constraints, allFilters, true);

        // ISNULL: trueFraction = argument's nullFraction (0 for non-nullable
        // columns, > 0 for nullable columns).
        EXPECT_GE(selectivity.trueFraction, 0.0);
        EXPECT_LE(selectivity.trueFraction, 1.0);
        // nullFraction of IS NULL is 0 (the result is always TRUE or FALSE,
        // never NULL).
        EXPECT_NEAR(selectivity.nullFraction, 0.0, kTolerance);
      });
}

TEST_F(FiltersTest, inListWithRangeBounds) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // IN list combined with range bounds prunes the list to values within
  // the range, then computes selectivity from the pruned list size.
  verifyFilterTestCases(
      "nation",
      {
          // IN (1, 5, 10, 20) AND n_nationkey > 3 prunes to (5, 10, 20).
          {
              .condition = "n_nationkey IN (1, 5, 10, 20) AND n_nationkey > 3",
              .expectedSelectivity = 3.0 / 25,
              .expectedMin = 5,
              .expectedMax = 20,
          },
          // IN (1, 5, 10, 20) AND n_nationkey < 12 prunes to (1, 5, 10).
          {
              .condition = "n_nationkey IN (1, 5, 10, 20) AND n_nationkey < 12",
              .expectedSelectivity = 3.0 / 25,
              .expectedMin = 1,
              .expectedMax = 10,
          },
          // IN (1, 5, 10, 20) AND > 3 AND < 15 prunes to (5, 10).
          {
              .condition =
                  "n_nationkey IN (1, 5, 10, 20) AND n_nationkey > 3 AND n_nationkey < 15",
              .expectedSelectivity = 2.0 / 25,
              .expectedMin = 5,
              .expectedMax = 10,
          },
      });
}

TEST_F(FiltersTest, overlappingInLists) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // Multiple IN lists are intersected: only values present in both lists
  // are kept.
  verifyFilterTestCases(
      "nation",
      {
          // IN (1,2,3,4) AND IN (3,4,5,6) → intersection is (3, 4).
          {
              .condition =
                  "n_nationkey IN (1, 2, 3, 4) AND n_nationkey IN (3, 4, 5, 6)",
              .expectedSelectivity = 2.0 / 25,
              .expectedMin = 3,
              .expectedMax = 4,
          },
          // IN (1,2,3) AND IN (1,2,3) → intersection is (1, 2, 3).
          {
              .condition =
                  "n_nationkey IN (1, 2, 3) AND n_nationkey IN (1, 2, 3)",
              .expectedSelectivity = 3.0 / 25,
              .expectedMin = 1,
              .expectedMax = 3,
          },
      });
}

TEST_F(FiltersTest, equalityWithRange) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // Equality combined with compatible range bound: equality dominates
  // selectivity (1/cardinality) but bounds are tightened.
  verifyFilterTestCases(
      "nation",
      {
          // x = 5 AND x >= 3: non-contradictory, equality wins.
          {
              .condition = "n_nationkey = 5 AND n_nationkey >= 3",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 5,
              .expectedMax = 5,
              .expectedCardinality = 1.0,
          },
          // x = 10 AND x > 5 AND x < 20: non-contradictory, equality wins.
          {
              .condition =
                  "n_nationkey = 10 AND n_nationkey > 5 AND n_nationkey < 20",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 10,
              .expectedMax = 10,
              .expectedCardinality = 1.0,
          },
      });
}

TEST_F(FiltersTest, cardinalityBasedSelectivity) {
  // TestConnector creates columns without min/max statistics, triggering
  // cardinality-based selectivity instead of range-based.
  // TODO: Enhance TestConnector to allow specifying partial stats for columns
  // (e.g. cardinality, nullFraction) so we don't need to override constraints
  // manually after exprConstraint.
  static constexpr auto kTestConnectorId = "test_filters";

  auto testConnector =
      std::make_shared<connector::TestConnector>(kTestConnectorId);
  velox::connector::registerConnector(testConnector);

  SCOPE_EXIT {
    velox::connector::unregisterConnector(kTestConnectorId);
  };

  testConnector->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  constexpr float kCardinality = 100;
  constexpr float kNullFraction = 0.1;
  // P(either null) = P(a null) + P(b null) - P(a null) * P(b null).
  constexpr double kCombinedNull =
      kNullFraction + kNullFraction - kNullFraction * kNullFraction;

  auto verify = [&](const std::string& condition,
                    double expectedSelectivity,
                    double expectedNullFraction) {
    SCOPED_TRACE("Testing condition: " + condition);
    auto sql = fmt::format("SELECT * FROM t WHERE {}", condition);
    verifyQueryGraph(
        sql,
        [&](DerivedTableCP rootDt) {
          auto allFilters = getAllFilters(rootDt, "t");
          ASSERT_EQ(1, allFilters.size());

          auto constraints = makeSchemaConstraints(allFilters);

          // Populate constraints for the filter expression first,
          // then override with controlled values. Must be done after
          // exprConstraint to avoid being overwritten by
          // conjunctsSelectivity's internal exprConstraint calls.
          exprConstraint(allFilters[0], constraints, true);

          for (auto& [id, constraint] : constraints) {
            constraint.cardinality = kCardinality;
            constraint.nullFraction = kNullFraction;
          }

          auto selectivity = exprSelectivity(constraints, allFilters[0], true);

          EXPECT_NEAR(
              selectivity.trueFraction, expectedSelectivity, kTolerance);
          EXPECT_NEAR(
              selectivity.nullFraction, expectedNullFraction, kTolerance);
        },
        kTestConnectorId);
  };

  // Equality between two columns: selectivity = (1/c) * (1 - combinedNull).
  verify("a = b", (1.0 / kCardinality) * (1.0 - kCombinedNull), kCombinedNull);

  // Range comparisons between two columns without min/max:
  // kUnknown * (1 - combinedNull).
  double expectedRange = Selectivity::kUnknown * (1.0 - kCombinedNull);
  verify("a < b", expectedRange, kCombinedNull);
  verify("a > b", expectedRange, kCombinedNull);
  verify("a <= b", expectedRange, kCombinedNull);
  verify("a >= b", expectedRange, kCombinedNull);

  // Equality with literal: selectivity = (1/c) * (1 - nullFraction).
  // Only one column involved, so null fraction is kNullFraction, not combined.
  verify("a = 3", (1.0 / kCardinality) * (1.0 - kNullFraction), kNullFraction);

  // IN list with literal values: selectivity = (listSize/c) * (1 -
  // nullFraction).
  verify(
      "a IN (1, 10, 15)",
      (3.0 / kCardinality) * (1.0 - kNullFraction),
      kNullFraction);

  // Function expression as LHS: coalesce has no min/max, so uses noRange.
  // The selectivity system doesn't know that coalesce eliminates nulls, so
  // it still applies the null fraction from the underlying column.
  // TODO: Enhance Filters to recognize coalesce special form and set
  // nullFraction to 0 when a non-null default is provided.
  verify(
      "coalesce(a, 10) > 5",
      Selectivity::kNoRange * (1.0 - kNullFraction),
      kNullFraction);
}

} // namespace
} // namespace facebook::axiom::optimizer
