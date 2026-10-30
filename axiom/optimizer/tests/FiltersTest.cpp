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
#include <gtest/gtest.h>
#include <functional>
#include <type_traits>
#include "axiom/optimizer/ExprEmitter.h"
#include "axiom/optimizer/Node.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/StatsFilterSelectivityEstimator.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/Filter.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

// Absolute tolerance for floating-point comparisons in selectivity tests.
// Selectivity values are estimates based on statistics and heuristics, not
// exact calculations. A tolerance of 0.001 (0.1%) provides sufficient precision
// for verifying selectivity estimates while accommodating minor variations from
// floating-point arithmetic and statistical approximations.
constexpr double kTolerance = 0.001;

// Describes a SQL filter, its optional pushed-down representation and expected
// estimate.
template <typename T>
struct FilterTestCase {
  std::string name;
  std::string condition;
  velox::common::FilterPtr pushedDownFilter;
  std::optional<double> expectedSelectivity;
  std::optional<T> expectedMin;
  std::optional<T> expectedMax;
  std::optional<double> expectedCardinality;
  double cardinalityTolerance{kTolerance};
};

using IntegerFilterTestCase = FilterTestCase<int64_t>;
using StringFilterTestCase = FilterTestCase<std::string>;

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
  auto selectivity = columnComparisonSelectivity(
      /*left=*/nullptr,
      /*right=*/nullptr,
      leftValue,
      rightValue,
      toName(funcName),
      /*updateConstraints=*/false,
      unused);
  VELOX_CHECK(selectivity.has_value());
  return *selectivity;
}

class FiltersTest : public test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTpchTables(0.1);
  }

  // Runs the callback with a QueryGraphContext set up.
  void withContext(const std::function<void()>& callback) {
    HashStringAllocator allocator(&optimizerPool());
    auto context = std::make_unique<optimizer::QueryGraphContext>(
        allocator,
        OptimizerOptions::kMaxPlanObjectsDefault,
        functionRegistry());
    optimizer::queryCtx() = context.get();

    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    callback();
  }

  // Parses SQL, translates it to the v2 tree IR and invokes 'callback'.
  void verifyTranslation(
      std::string_view sql,
      const std::function<void(NodeCP)>& callback,
      const std::string& connectorId = kTestConnectorId) {
    auto logicalPlan = parseSelect(sql, connectorId);
    verifyOptimization(*logicalPlan, Optimizer::Pass::kTranslate, callback);
  }

  static bool
  collectFilters(NodeCP node, std::string_view tableName, ExprVector& filters) {
    if (node->is(NodeType::kScan)) {
      const auto* table = node->as<Scan>()->baseTable();
      return table->schemaTable &&
          table->schemaTable->name().table == tableName;
    }

    for (NodeCP input : node->inputs()) {
      if (collectFilters(input, tableName, filters)) {
        if (node->is(NodeType::kFilter)) {
          const auto& predicates = node->as<Filter>()->predicates();
          filters.insert(filters.end(), predicates.begin(), predicates.end());
        }
        return true;
      }
    }
    return false;
  }

  static ExprVector getAllFilters(NodeCP root, std::string_view tableName) {
    ExprVector filters;
    VELOX_CHECK(
        collectFilters(root, tableName, filters),
        "Table '{}' not found",
        tableName);
    return filters;
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

  // Converts an optimizer Value to the connector::ColumnStatistics the
  // TypedExpr estimator consumes, preserving min/max/NDV/nullFraction so both
  // paths see the same statistics.
  static connector::ColumnStatistics toColumnStatistics(const Value& value) {
    connector::ColumnStatistics stats;
    if (value.cardinality.has_value()) {
      stats.numDistinct = static_cast<int64_t>(*value.cardinality);
    }
    stats.nullPct = value.nullFraction.value_or(0) * 100.0f;
    stats.nonNull = !value.nullable;
    if (value.min != nullptr) {
      stats.min = *value.min;
    }
    if (value.max != nullptr) {
      stats.max = *value.max;
    }
    return stats;
  }

  // Differential check: lowers the same filters to velox TypedExpr, estimates
  // via the connector-facing StatsFilterSelectivityEstimator over
  // ColumnStatistics derived from the same schema statistics the ExprCP path
  // used, and asserts both the row-count selectivity and the refined per-column
  // statistics agree with 'expectedSelectivity'/'refinedConstraints' from the
  // ExprCP SelectivityEngine. Exercises the second Policy instantiation and the
  // velox->canonical name mapping.
  void verifyEstimatorAgrees(
      const ExprVector& filters,
      const Selectivity& expectedSelectivity,
      const ConstraintMap& refinedConstraints) {
    ExprEmitter emitter{&optimizerPool()};
    std::vector<velox::core::TypedExprPtr> typedFilters;
    folly::F14FastMap<std::string, connector::TypedColumnStatistics>
        columnStats;
    folly::F14FastMap<std::string, int32_t> columnIdByName;
    for (auto* filter : filters) {
      typedFilters.push_back(emitter.toTypedExpr(filter));
      filter->columns().forEach<Column>([&](auto* column) {
        if (auto* schemaColumn = column->schemaColumn()) {
          const auto& value = schemaColumn->value();
          columnStats.insert_or_assign(
              column->outputName(),
              connector::TypedColumnStatistics{
                  value.type, toColumnStatistics(value)});
          columnIdByName.insert_or_assign(column->outputName(), column->id());
        }
      });
    }

    StatsFilterSelectivityEstimator estimator;
    auto estimate = estimator.estimate(typedFilters, columnStats);

    EXPECT_NEAR(
        estimate.selectivity, expectedSelectivity.trueFraction, kTolerance);

    // Refined per-column statistics must match the ExprCP constraints.
    for (const auto& [name, stats] : estimate.columnStats) {
      auto idIt = columnIdByName.find(name);
      ASSERT_NE(idIt, columnIdByName.end());
      auto constraintIt = refinedConstraints.find(idIt->second);
      ASSERT_NE(constraintIt, refinedConstraints.end());
      auto expected = toColumnStatistics(constraintIt->second);

      EXPECT_EQ(stats.numDistinct, expected.numDistinct);
      EXPECT_EQ(stats.min.has_value(), expected.min.has_value());
      if (stats.min.has_value() && expected.min.has_value()) {
        EXPECT_TRUE(stats.min->equals(*expected.min));
      }
      EXPECT_EQ(stats.max.has_value(), expected.max.has_value());
      if (stats.max.has_value() && expected.max.has_value()) {
        EXPECT_TRUE(stats.max->equals(*expected.max));
      }
      EXPECT_NEAR(stats.nullPct, expected.nullPct, 0.1);
    }
  }

  // Parses SQL, estimates its filters and runs the TypedExpr estimator
  // differential check. A three-argument verifier also receives the original
  // value of the filter's sole input column.
  template <typename Verify>
  void verifyFilter(
      std::string_view sql,
      std::string_view tableName,
      Verify&& verify,
      std::string_view connectorId = kTestConnectorId) {
    constexpr bool kNeedsInputValue = std::
        is_invocable_v<Verify, const Selectivity&, const Value&, const Value&>;
    static_assert(
        kNeedsInputValue ||
        std::is_invocable_v<Verify, const Selectivity&, const Value&>);

    auto logicalPlan = parseSelect(sql, std::string{connectorId});
    verifyOptimization(
        *logicalPlan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
          auto allFilters = getAllFilters(root, tableName);
          ASSERT_FALSE(allFilters.empty());

          auto constraints = makeSchemaConstraints(allFilters);
          int32_t constraintId;
          std::optional<Value> inputValue;
          if constexpr (kNeedsInputValue) {
            const auto& columns = allFilters[0]->columns();
            ASSERT_EQ(columns.size(), 1);
            constraintId = columns.onlyObject<Column>()->id();

            auto it = constraints.find(constraintId);
            ASSERT_NE(it, constraints.end());
            inputValue = it->second;
          } else {
            auto* call = allFilters[0]->as<Call>();
            constraintId = call->args()[0]->id();
          }

          auto selectivity =
              conjunctsSelectivity(constraints, allFilters, true);
          ASSERT_TRUE(selectivity.has_value());

          auto it = constraints.find(constraintId);
          ASSERT_NE(it, constraints.end());
          if constexpr (kNeedsInputValue) {
            verify(*selectivity, it->second, *inputValue);
          } else {
            verify(*selectivity, it->second);
          }

          verifyEstimatorAgrees(allFilters, *selectivity, constraints);
        });
  }

  template <typename T>
  static void verifyFilterEstimate(
      const FilterTestCase<T>& testCase,
      double selectivity,
      const Value& constraint) {
    if (testCase.expectedSelectivity.has_value()) {
      EXPECT_NEAR(selectivity, *testCase.expectedSelectivity, kTolerance);
    }

    if (testCase.expectedMin.has_value()) {
      ASSERT_NE(constraint.min, nullptr);
      EXPECT_EQ(constraint.min->value<T>(), *testCase.expectedMin);
    }

    if (testCase.expectedMax.has_value()) {
      ASSERT_NE(constraint.max, nullptr);
      EXPECT_EQ(constraint.max->value<T>(), *testCase.expectedMax);
    }

    if (testCase.expectedCardinality.has_value()) {
      EXPECT_NEAR(
          constraint.cardinality.value(),
          *testCase.expectedCardinality,
          testCase.cardinalityTolerance);
    }
  }

  // Verifies each SQL condition and its optional pushed-down representation.
  template <typename T>
  void verifyFilterTestCases(
      std::string_view tableName,
      const std::vector<FilterTestCase<T>>& testCases,
      std::string_view connectorId = kTestConnectorId) {
    for (const auto& testCase : testCases) {
      SCOPED_TRACE(
          testCase.name.empty()
              ? testCase.condition
              : fmt::format("{}: {}", testCase.name, testCase.condition));
      auto sql = fmt::format(
          "SELECT * FROM {} WHERE {}", tableName, testCase.condition);
      auto verifySqlEstimate = [&](const Selectivity& selectivity,
                                   const Value& constraint) {
        SCOPED_TRACE("SQL expression");
        verifyFilterEstimate(testCase, selectivity.trueFraction, constraint);
      };

      if (testCase.pushedDownFilter == nullptr) {
        verifyFilter(sql, tableName, verifySqlEstimate, connectorId);
        continue;
      }

      verifyFilter(
          sql,
          tableName,
          [&](const Selectivity& selectivity,
              const Value& constraint,
              const Value& inputValue) {
            verifySqlEstimate(selectivity, constraint);

            SCOPED_TRACE("Pushed-down filter");
            verifyPushedDownFilter(
                inputValue,
                *testCase.pushedDownFilter,
                [&](double filterSelectivity, const Value& filterConstraint) {
                  verifyFilterEstimate(
                      testCase, filterSelectivity, filterConstraint);
                });
          },
          connectorId);
    }
  }

  void verifyPushedDownFilter(
      const Value& inputValue,
      const velox::common::Filter& filter,
      const std::function<void(double, const Value&)>& verify) {
    folly::F14FastMap<std::string, const velox::common::Filter*> filters{
        {"c", &filter}};
    folly::F14FastMap<std::string, connector::TypedColumnStatistics>
        columnStats{{"c", {inputValue.type, toColumnStatistics(inputValue)}}};

    const auto estimate =
        StatsFilterSelectivityEstimator{}.estimate(filters, columnStats);
    verify(
        estimate.selectivity,
        Value::fromColumnStatistics(
            inputValue.type, estimate.columnStats.at("c")));
  }
};

// ============================================================
// Selectivity algebra unit tests
// ============================================================

TEST_F(FiltersTest, combineConjunctsBasic) {
  std::vector<std::optional<Selectivity>> conjuncts = {
      Selectivity{0.8, 0.0}, // 80% true, 0% null, 20% false
      Selectivity{0.6, 0.0} // 60% true, 0% null, 40% false
  };

  Selectivity result = combineConjuncts(conjuncts).value();

  // P(TRUE) = product of trueFractions.
  EXPECT_NEAR(result.trueFraction, 0.8 * 0.6, kTolerance);
  // P(NULL) = P(TRUE|NULL) - P(TRUE) = 0.8 * 0.6 - 0.48 = 0.
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsWithNulls) {
  std::vector<std::optional<Selectivity>> conjuncts = {
      Selectivity{0.5, 0.2}, // 50% true, 20% null, 30% false
      Selectivity{0.6, 0.1} // 60% true, 10% null, 30% false
  };

  Selectivity result = combineConjuncts(conjuncts).value();

  // P(TRUE) = product of trueFractions.
  EXPECT_NEAR(result.trueFraction, 0.5 * 0.6, kTolerance);
  // P(NULL) = P(TRUE|NULL) - P(TRUE).
  EXPECT_NEAR(
      result.nullFraction, (0.5 + 0.2) * (0.6 + 0.1) - 0.5 * 0.6, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsMultiple) {
  std::vector<std::optional<Selectivity>> conjuncts = {
      Selectivity{0.8, 0.1}, // 80% true, 10% null, 10% false
      Selectivity{0.7, 0.0}, // 70% true, 0% null, 30% false
      Selectivity{0.9, 0.05} // 90% true, 5% null, 5% false
  };

  Selectivity result = combineConjuncts(conjuncts).value();

  EXPECT_NEAR(result.trueFraction, 0.8 * 0.7 * 0.9, kTolerance);
  EXPECT_NEAR(
      result.nullFraction,
      (0.8 + 0.1) * 0.7 * (0.9 + 0.05) - 0.8 * 0.7 * 0.9,
      kTolerance);
}

TEST_F(FiltersTest, combineConjunctsEmpty) {
  std::vector<std::optional<Selectivity>> empty;
  Selectivity result = combineConjuncts(empty).value();
  EXPECT_NEAR(result.trueFraction, 1.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineConjunctsAllNull) {
  std::vector<std::optional<Selectivity>> conjuncts = {
      Selectivity{0.0, 1.0}, // 0% true, 100% null, 0% false
      Selectivity{0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineConjuncts(conjuncts).value();

  EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 1.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsBasic) {
  std::vector<std::optional<Selectivity>> disjuncts = {
      Selectivity{0.3, 0.0}, // 30% true, 0% null, 70% false
      Selectivity{0.4, 0.0} // 40% true, 0% null, 60% false
  };

  Selectivity result = combineDisjuncts(disjuncts).value();

  // P(TRUE) = 1 - product of (1 - trueFraction_i).
  EXPECT_NEAR(result.trueFraction, 1.0 - (1.0 - 0.3) * (1.0 - 0.4), kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsWithNulls) {
  std::vector<std::optional<Selectivity>> disjuncts = {
      Selectivity{0.3, 0.2}, // 30% true, 20% null, 50% false
      Selectivity{0.4, 0.1} // 40% true, 10% null, 50% false
  };

  Selectivity result = combineDisjuncts(disjuncts).value();

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
  std::vector<std::optional<Selectivity>> disjuncts = {
      Selectivity{0.2, 0.1}, // 20% true, 10% null, 70% false
      Selectivity{0.3, 0.0}, // 30% true, 0% null, 70% false
      Selectivity{0.1, 0.05} // 10% true, 5% null, 85% false
  };

  Selectivity result = combineDisjuncts(disjuncts).value();

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
  std::vector<std::optional<Selectivity>> empty;
  Selectivity result = combineDisjuncts(empty).value();
  EXPECT_NEAR(result.trueFraction, 0.0, kTolerance);
  EXPECT_NEAR(result.nullFraction, 0.0, kTolerance);
}

TEST_F(FiltersTest, combineDisjunctsAllNull) {
  std::vector<std::optional<Selectivity>> disjuncts = {
      Selectivity{0.0, 1.0}, // 0% true, 100% null, 0% false
      Selectivity{0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineDisjuncts(disjuncts).value();

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
      "WHERE l_shipdate BETWEEN CAST('1995-01-01' AS date) AND CAST('1995-06-01' AS date) "
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
      std::vector<IntegerFilterTestCase>{
          // 11 values in [5, 15] out of 25.
          {.condition = "n_nationkey BETWEEN 2 AND 15 "
                        "AND n_nationkey BETWEEN 5 AND 20",
           .expectedSelectivity = 11.0 / 25,
           .expectedMin = 5,
           .expectedMax = 15},
      });
}

// A stats bound whose kind disagrees with the column type is a connector
// contract violation; ingestion rejects it loudly rather than build a Value
// that would be read as the wrong kind.
TEST_F(FiltersTest, statsBoundKindMismatchRejected) {
  withContext([&]() {
    connector::ColumnStatistics stats;
    stats.nonNull = true;
    stats.numDistinct = 1'000;
    stats.min = velox::Variant::create<int64_t>(0);
    // max's kind disagrees with the BIGINT column type.
    stats.max = velox::Variant::create<std::string>("Z");

    VELOX_ASSERT_THROW(
        Value::fromColumnStatistics(toType(BIGINT()), stats),
        "max kind must match the column type");
  });
}

TEST_F(FiltersTest, equalitySelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // 1 value out of 25.
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
          {
              .condition = "n_nationkey = 10",
              .pushedDownFilter = velox::exec::equal(10),
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 10,
              .expectedMax = 10,
              .expectedCardinality = 1.0,
          },
          {
              .condition = "n_nationkey = 0",
              .pushedDownFilter = velox::exec::equal(0),
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 0,
              .expectedMax = 0,
              .expectedCardinality = 1.0,
          },
          {
              .condition = "n_nationkey = 24",
              .pushedDownFilter = velox::exec::equal(24),
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
      std::vector<IntegerFilterTestCase>{
          // 3 values out of 25.
          {
              .condition = "n_nationkey IN (1, 2, 3)",
              .pushedDownFilter =
                  velox::common::createBigintValues({1, 2, 3}, false),
              .expectedSelectivity = 3.0 / 25,
              .expectedMin = 1,
              .expectedMax = 3,
          },
          // 4 values out of 25.
          {
              .condition = "n_nationkey IN (5, 10, 15, 20)",
              .pushedDownFilter =
                  velox::common::createBigintValues({5, 10, 15, 20}, false),
              .expectedSelectivity = 4.0 / 25,
              .expectedMin = 5,
              .expectedMax = 20,
          },
      });
}

TEST_F(FiltersTest, integerFilterEdgeCases) {
  constexpr int64_t kMin = 10;
  // Integers above 2^53 cannot all be represented exactly as double.
  constexpr int64_t kMax = (int64_t{1} << 53) + 3;
  constexpr int64_t kMidpoint = kMin + (kMax - kMin) / 2;
  const connector::ColumnStatistics sparseStats{
      .nonNull = true,
      .min = velox::Variant::create<int64_t>(kMin),
      .max = velox::Variant::create<int64_t>(kMax),
      .numDistinct = 6};
  const connector::ColumnStatistics fullRangeStats{
      .nonNull = true,
      .min =
          velox::Variant::create<int64_t>(std::numeric_limits<int64_t>::min()),
      .max =
          velox::Variant::create<int64_t>(std::numeric_limits<int64_t>::max()),
      .numDistinct = 1'000};

  testConnector_->addTable("t", ROW({"c", "d"}, BIGINT()))
      ->setStats(10'000, {{"c", sparseStats}, {"d", fullRangeStats}});

  verifyFilterTestCases(
      "t",
      std::vector<IntegerFilterTestCase>{
          {
              .condition = fmt::format("c = {}", kMax),
              .pushedDownFilter = velox::exec::equal(kMax),
              .expectedSelectivity = 1.0 / 6,
              .expectedMin = kMax,
              .expectedMax = kMax,
              .expectedCardinality = 1.0,
          },
          {
              .condition = fmt::format("c IN ({}, 100, {})", kMin, kMax + 1),
              .pushedDownFilter = velox::common::createBigintValues(
                  {kMin, 100, kMax + 1}, false),
              .expectedSelectivity = 2.0 / 6,
              .expectedMin = kMin,
              .expectedMax = 100,
              .expectedCardinality = 2.0,
          },
          {
              .condition = fmt::format("c = {}", kMax + 1),
              .pushedDownFilter = velox::exec::equal(kMax + 1),
              .expectedSelectivity = Selectivity::kLikelyZero,
          },
          {
              .condition = fmt::format("c BETWEEN {} AND {}", kMin, kMidpoint),
              .pushedDownFilter = velox::exec::between(kMin, kMidpoint),
              .expectedSelectivity = 0.5,
              .expectedMin = kMin,
              .expectedMax = kMidpoint,
              .expectedCardinality = 3.0,
          },
          {
              .condition = "d > 0",
              .pushedDownFilter = velox::exec::greaterThan(0),
              .expectedSelectivity = 0.5,
              .expectedMin = 1,
              .expectedMax = std::numeric_limits<int64_t>::max(),
              .expectedCardinality = 500,
          },
      },
      kTestConnectorId);
}

TEST_F(FiltersTest, strictInequalityIntegerBounds) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
          // Strict lower and upper bounds are adjusted for integers.
          {.condition = "n_nationkey > 2 AND n_nationkey < 22",
           .pushedDownFilter = velox::exec::between(3, 21),
           .expectedMin = 3,
           .expectedMax = 21},
          // Non-strict bounds are not adjusted.
          {.condition = "n_nationkey >= 2 AND n_nationkey <= 22",
           .pushedDownFilter = velox::exec::between(2, 22),
           .expectedMin = 2,
           .expectedMax = 22},
          // Mixed: strict lower, non-strict upper.
          {.condition = "n_nationkey > 2 AND n_nationkey <= 15",
           .pushedDownFilter = velox::exec::between(3, 15),
           .expectedMin = 3,
           .expectedMax = 15},
          // Mixed: non-strict lower, strict upper.
          {.condition = "n_nationkey >= 5 AND n_nationkey < 20",
           .pushedDownFilter = velox::exec::between(5, 19),
           .expectedMin = 5,
           .expectedMax = 19},
          // Only strict lower bound.
          {.condition = "n_nationkey > 10",
           .pushedDownFilter = velox::exec::greaterThan(10),
           .expectedMin = 11,
           .expectedMax = 24},
          // Only strict upper bound.
          {.condition = "n_nationkey < 10",
           .pushedDownFilter = velox::exec::lessThan(10),
           .expectedMin = 0,
           .expectedMax = 9},
          // Strict lower bound at column minimum (> 0 on column with min=0).
          {.condition = "n_nationkey > 0",
           .pushedDownFilter = velox::exec::greaterThan(0),
           .expectedMin = 1,
           .expectedMax = 24},
      });
}

TEST_F(FiltersTest, varcharRangeSelectivity) {
  // String range selectivity uses a first-character approximation. n_name
  // spans 22 characters from A through V.
  std::vector<StringFilterTestCase> testCases = {
      // Strict inequalities do not adjust string bounds.
      {
          .name = "exclusive range",
          .condition = "n_name > 'B' AND n_name < 'P'",
          .pushedDownFilter = velox::exec::betweenExclusive("B", "P"),
          .expectedMin = "B",
          .expectedMax = "P",
      },
      // Different first characters: 6 letters in ['B', 'G'] out of 22.
      {
          .name = "overlapping ranges",
          .condition =
              "n_name >= 'B' AND n_name <= 'P' AND n_name >= 'A' AND n_name <= 'G'",
          .pushedDownFilter = velox::exec::between("B", "G"),
          .expectedSelectivity = 6.0 / 22,
          .expectedMin = "B",
          .expectedMax = "G",
          .expectedCardinality = 6.8f,
          .cardinalityTolerance = 1.0,
      },
      // Same first character. The first-character approximation would give 0,
      // but the floor ensures a small positive estimate.
      {
          .name = "same-prefix range",
          .condition = "n_name >= 'BRAZIL' AND n_name <= 'BRITAIN'",
          .pushedDownFilter = velox::exec::between("BRAZIL", "BRITAIN"),
          .expectedSelectivity = 1.0 / 22,
          .expectedMin = "BRAZIL",
          .expectedMax = "BRITAIN",
          .expectedCardinality = 1.0f,
          .cardinalityTolerance = 1.0,
      },
  };

  verifyFilterTestCases("nation", testCases);

  testConnector_->addTable("t", ROW("c", VARCHAR()))
      ->setStats(1'000, {{"c", {.nonNull = true, .numDistinct = 100}}});
  verifyFilterTestCases(
      "t",
      std::vector<StringFilterTestCase>{
          {
              .name = "missing bounds",
              .condition = "c BETWEEN 'a' AND 'z'",
              .pushedDownFilter = velox::exec::between("a", "z"),
              .expectedSelectivity = Selectivity::kNoRange,
              .expectedMin = "a",
              .expectedMax = "z",
              .expectedCardinality = 10,
          },
      },
      kTestConnectorId);
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
      std::vector<IntegerFilterTestCase>{
          // The disjoint ranges contain 5 + 4 values.
          {.condition = "n_nationkey < 5 OR n_nationkey > 20",
           .expectedSelectivity = 9.0 / 25},
          // P(= 10) = 1/25, P(> 20) = 4/25.
          {.condition = "n_nationkey = 10 OR n_nationkey > 20",
           .expectedSelectivity = 1.0 - (1.0 - 1.0 / 25) * (1.0 - 4.0 / 25)},
          // The ranges overlap and cover the full column domain.
          {.condition = "n_nationkey > 5 OR n_nationkey < 20",
           .expectedSelectivity = 1.0},
          // The value outside the column domain is discarded.
          {.condition = "n_nationkey = 3 OR n_nationkey = 100",
           .expectedSelectivity = 1.0 / 25},
      });
}

TEST_F(FiltersTest, notSelectivity) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
          // NOT(P(= 10)) = 1 - 1/25.
          {
              .condition = "NOT(n_nationkey = 10)",
              .pushedDownFilter =
                  std::make_unique<velox::common::NegatedBigintRange>(
                      10, 10, /*nullAllowed=*/false),
              .expectedSelectivity = 1.0 - 1.0 / 25,
          },
          // NOT(P(= 100)) = 1 - likelyZero (100 is out of range).
          {
              .condition = "NOT(n_nationkey = 100)",
              .pushedDownFilter =
                  std::make_unique<velox::common::NegatedBigintRange>(
                      100, 100, /*nullAllowed=*/false),
              .expectedSelectivity = 1.0 - Selectivity::kLikelyZero,
          },
          // NOT IN with 3 in-range values (2, 7, 8) and 1 out-of-range (100).
          {
              .condition = "n_nationkey NOT IN (2, 7, 8, 100)",
              .pushedDownFilter = velox::common::createNegatedBigintValues(
                  {2, 7, 8, 100}, /*nullAllowed=*/false),
              .expectedSelectivity = 1.0 - 3.0 / 25,
          },
          // The same 3 in-range values with a far out-of-range one, which the
          // pushed-down filter stores in a hash table rather than a bitmask.
          {
              .condition = "n_nationkey NOT IN (2, 7, 8, 1000000)",
              .pushedDownFilter = velox::common::createNegatedBigintValues(
                  {2, 7, 8, 1'000'000}, /*nullAllowed=*/false),
              .expectedSelectivity = 1.0 - 3.0 / 25,
          },
      });

  // n_name is VARCHAR with cardinality=25. NOT IN with 2 values.
  verifyFilterTestCases(
      "nation",
      std::vector<StringFilterTestCase>{
          {
              .condition = "n_name NOT IN ('FRANCE', 'EGYPT')",
              .pushedDownFilter =
                  std::make_unique<velox::common::NegatedBytesValues>(
                      std::vector<std::string>{"FRANCE", "EGYPT"},
                      /*nullAllowed=*/false),
              .expectedSelectivity = 1.0 - 2.0 / 25,
          },
      });
}

// A negated string range keeps the non-null rows its positive range rejects.
TEST_F(FiltersTest, negatedBytesRange) {
  withContext([&]() {
    constexpr double kNullFraction = 0.1;
    const auto value = makeValue<std::string>(100, "a", "z", kNullFraction);
    const velox::common::BytesRange positive{
        "c", false, false, "m", false, false, /*nullAllowed=*/false};
    const velox::common::NegatedBytesRange negated{
        "c", false, false, "m", false, false, /*nullAllowed=*/false};

    double positiveSelectivity{0};
    verifyPushedDownFilter(
        value, positive, [&](double selectivity, const Value&) {
          positiveSelectivity = selectivity;
        });
    ASSERT_GT(positiveSelectivity, 0.0);
    verifyPushedDownFilter(
        value, negated, [&](double selectivity, const Value&) {
          EXPECT_NEAR(
              selectivity,
              1.0 - kNullFraction - positiveSelectivity,
              kTolerance);
        });
  });
}

// A negated filter that passes nulls keeps every null row plus the non-null
// rows outside the rejected value, and the refined statistics report the null
// share of the rows that pass.
TEST_F(FiltersTest, negatedFilterPassingNulls) {
  withContext([&]() {
    constexpr double kNullFraction = 0.2;
    const auto value = makeValue<int64_t>(25, 0, 24, kNullFraction);
    const velox::common::NegatedBigintRange filter{
        10, 10, /*nullAllowed=*/true};

    const double expected =
        (1.0 - kNullFraction) * (1.0 - 1.0 / 25) + kNullFraction;
    verifyPushedDownFilter(
        value, filter, [&](double selectivity, const Value& refined) {
          EXPECT_NEAR(selectivity, expected, kTolerance);
          EXPECT_NEAR(
              refined.nullFraction.value(),
              kNullFraction / expected,
              kTolerance);
        });
  });
}

TEST_F(FiltersTest, unsupportedExpressions) {
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
          // The <> operator is not normalized to NOT(eq) and is treated as an
          // unknown function, receiving default selectivity kLikelyTrue. This
          // differs from NOT(n_nationkey = 10) which correctly computes
          // 1 - 1/25 = 0.96.
          {.condition = "n_nationkey <> 10",
           .expectedSelectivity = Selectivity::kLikelyTrue},
          {.condition = "n_nationkey BETWEEN n_regionkey AND 10",
           .expectedSelectivity = Selectivity::kLikelyTrue},
      });
}

TEST_F(FiltersTest, multiColumnConjuncts) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
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
  verifyTranslation(
      "SELECT n_regionkey FROM nation WHERE n_regionkey = n_nationkey",
      [](NodeCP root) {
        auto allFilters = getAllFilters(root, "nation");
        ASSERT_EQ(allFilters.size(), 1);

        auto constraints = makeSchemaConstraints(allFilters);
        auto selectivity = exprSelectivity(constraints, allFilters[0], true);
        ASSERT_TRUE(selectivity.has_value());

        EXPECT_GE(selectivity->trueFraction, 0.0);
        EXPECT_LE(selectivity->trueFraction, 1.0);

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
  verifyTranslation(
      "SELECT * FROM nation WHERE n_comment IS NULL", [](NodeCP root) {
        auto allFilters = getAllFilters(root, "nation");
        ASSERT_FALSE(allFilters.empty());

        auto constraints = makeSchemaConstraints(allFilters);
        auto selectivity = conjunctsSelectivity(constraints, allFilters, true);
        ASSERT_TRUE(selectivity.has_value());

        // ISNULL: trueFraction = argument's nullFraction (0 for non-nullable
        // columns, > 0 for nullable columns).
        EXPECT_GE(selectivity->trueFraction, 0.0);
        EXPECT_LE(selectivity->trueFraction, 1.0);
        // nullFraction of IS NULL is 0 (the result is always TRUE or FALSE,
        // never NULL).
        EXPECT_NEAR(selectivity->nullFraction, 0.0, kTolerance);
      });
}

TEST_F(FiltersTest, inListWithRangeBounds) {
  // n_nationkey is BIGINT with min=0, max=24, cardinality=25.
  // IN list combined with range bounds prunes the list to values within
  // the range, then computes selectivity from the pruned list size.
  verifyFilterTestCases(
      "nation",
      std::vector<IntegerFilterTestCase>{
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
                  "n_nationkey IN (1, 5, 10, 20) AND n_nationkey BETWEEN 4 AND 14",
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
      std::vector<IntegerFilterTestCase>{
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
      std::vector<IntegerFilterTestCase>{
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
              .condition = "n_nationkey = 10 AND n_nationkey BETWEEN 5 AND 20",
              .expectedSelectivity = 1.0 / 25,
              .expectedMin = 10,
              .expectedMax = 10,
              .expectedCardinality = 1.0,
          },
      });
}

TEST_F(FiltersTest, cardinalityBasedSelectivity) {
  // Missing min/max statistics use NDV-based selectivity.
  constexpr int64_t kCardinality = 100;
  constexpr float kNullPct = 10.0;
  constexpr float kNullFraction = kNullPct / 100.0;
  // P(either null) = P(a null) + P(b null) - P(a null) * P(b null).
  constexpr double kCombinedNull =
      kNullFraction + kNullFraction - kNullFraction * kNullFraction;

  constexpr uint64_t kNumRows = 1000;

  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          kNumRows,
          {
              {"a", {.nullPct = kNullPct, .numDistinct = kCardinality}},
              {"b", {.nullPct = kNullPct, .numDistinct = kCardinality}},
          });

  auto verify = [&](std::string_view condition,
                    double expectedSelectivity,
                    double expectedNullFraction) {
    SCOPED_TRACE(condition);
    auto sql = fmt::format("SELECT * FROM t WHERE {}", condition);
    verifyTranslation(
        sql,
        [&](NodeCP root) {
          auto allFilters = getAllFilters(root, "t");
          ASSERT_EQ(1, allFilters.size());

          auto constraints = makeSchemaConstraints(allFilters);
          auto selectivity =
              conjunctsSelectivity(constraints, allFilters, true);
          ASSERT_TRUE(selectivity.has_value());

          EXPECT_NEAR(
              selectivity->trueFraction, expectedSelectivity, kTolerance);
          EXPECT_NEAR(
              selectivity->nullFraction, expectedNullFraction, kTolerance);
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
  // coalesce(a, 10) produces a non-null result for every row, so
  // nullFraction = 0 for the expression. The selectivity is noRange applied
  // to the full row set (no null discount).
  verify("coalesce(a, 10) > 5", Selectivity::kNoRange, 0.0);
}

} // namespace
} // namespace facebook::axiom::optimizer
