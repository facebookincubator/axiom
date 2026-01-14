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
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;

class FiltersTest : public HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();

    // Initialize context for tests that don't use optimize()
    allocator_ = std::make_unique<HashStringAllocator>(optimizerPool_.get());
    context_ = std::make_unique<optimizer::QueryGraphContext>(*allocator_);
    optimizerQueryCtx_ = context_.get();
    optimizer::queryCtx() = optimizerQueryCtx_;
  }

  void TearDown() override {
    // Reset optimization and dependencies
    optimization_.reset();
    session_.reset();
    allocator_.reset();
    context_.reset();

    // Clean up queryCtx
    optimizer::queryCtx() = nullptr;
    optimizerQueryCtx_ = nullptr;

    HiveQueriesTestBase::TearDown();
  }

  /// Helper to find a BaseTable by name in a DerivedTable.
  /// Fails the test if the table is not found.
  const BaseTable* findBaseTable(
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
    ADD_FAILURE() << "Table '" << tableName << "' not found in DerivedTable";
    return nullptr;
  }

  /// Helper to get all filters from a table by name and reset column values
  /// to schema values. Gets the root derived table, finds the base table,
  /// resets all column values to their schema values, and combines all
  /// filters. Fails the test if the table is not found.
  ExprVector getAllFiltersAndResetToSchema(std::string_view tableName) {
    auto* rootDt = optimization_->rootDt();
    EXPECT_NE(rootDt, nullptr) << "Root derived table is null";

    const BaseTable* table = findBaseTable(rootDt, tableName);
    EXPECT_NE(table, nullptr) << "Table '" << tableName << "' not found";

    // Reset all column values to their schema table values
    for (auto* column : table->columns) {
      auto* schemaColumn = column->schemaColumn();
      if (schemaColumn) {
        // Assign the schema column's value to this column's value
        const_cast<Value&>(const_cast<Column*>(column)->value()) =
            schemaColumn->value();
      }
    }

    ExprVector allFilters;
    allFilters.insert(
        allFilters.end(),
        table->columnFilters.begin(),
        table->columnFilters.end());
    allFilters.insert(
        allFilters.end(), table->filter.begin(), table->filter.end());
    return allFilters;
  }
};

TEST_F(FiltersTest, basic) {
  // SQL query with filters on lineitem
  optimize(
      "select l_shipdate, l_partkey, l_suppkey "
      "from lineitem "
      "where l_shipdate between cast('1995-01-01' as date) and cast('1995-06-01' as date) "
      "and l_partkey < 10000 and l_suppkey > 10",
      velox::exec::test::kHiveConnectorId);

  // Get all filters from the table
  ExprVector allFilters = getAllFiltersAndResetToSchema("lineitem");
  ASSERT_EQ(4, allFilters.size());

  // Create a temporary PlanState
  auto* rootDt = optimization_->rootDt();
  PlanState state(*optimization_, rootDt);

  ConstraintMap constraints;

  Selectivity selectivity =
      conjunctsSelectivity(state, allFilters, true, constraints);

  // Selectivity should be in valid range [0, 1]
  EXPECT_GE(selectivity.trueFraction, 0.0);
  EXPECT_LE(selectivity.trueFraction, 1.0);
  EXPECT_GE(selectivity.nullFraction, 0.0);
  EXPECT_LE(selectivity.nullFraction, 1.0);
}

TEST_F(FiltersTest, combineRanges) {
  // SQL query with overlapping between clauses on nation table
  optimize(
      "select * from nation "
      "where n_nationkey between 2 and 15 "
      "and n_nationkey between 5 and 20",
      velox::exec::test::kHiveConnectorId);

  // Get all filters from the table
  ExprVector allFilters = getAllFiltersAndResetToSchema("nation");
  ASSERT_FALSE(allFilters.empty()) << "Expected at least one filter expression";

  // Create a temporary PlanState
  auto* rootDt = optimization_->rootDt();
  PlanState state(*optimization_, rootDt);

  // Test conjunctsSelectivity on combined filters with constraint tightening
  ConstraintMap constraints;
  Selectivity selectivity =
      conjunctsSelectivity(state, allFilters, true, constraints);

  // Selectivity should be 10/25 = 0.8 within 1%
  float expectedSelectivity = 10.0f / 24.0f;
  EXPECT_NEAR(selectivity.trueFraction, expectedSelectivity, 0.01f)
      << "Expected selectivity around " << expectedSelectivity << " but got "
      << selectivity.trueFraction;

  // Check that there is exactly one constraint for n_nationkey
  ASSERT_EQ(constraints.size(), 1)
      << "Expected exactly one constraint for n_nationkey";

  // Get the constraint value
  const auto& constraintValue = constraints.begin()->second;

  // Check min = 5
  ASSERT_NE(constraintValue.min, nullptr) << "Expected min to be set";
  auto minValue = constraintValue.min->value<velox::TypeKind::BIGINT>();
  EXPECT_EQ(minValue, 5) << "Expected min to be 5, got " << minValue;

  // Check max = 15
  ASSERT_NE(constraintValue.max, nullptr) << "Expected max to be set";
  auto maxValue = constraintValue.max->value<velox::TypeKind::BIGINT>();
  EXPECT_EQ(maxValue, 15) << "Expected max to be 15, got " << maxValue;

  // Check cardinality is between 10 and 11
  EXPECT_GE(constraintValue.cardinality, 10.0f)
      << "Expected cardinality to be at least 10, got "
      << constraintValue.cardinality;
  EXPECT_LE(constraintValue.cardinality, 11.0f)
      << "Expected cardinality to be at most 11, got "
      << constraintValue.cardinality;
}

TEST_F(FiltersTest, combineStringRanges) {
  // Test string range combining with n_name column
  optimize(
      "select * from nation "
      "where n_name between 'B' and 'P' "
      "and n_name between 'A' and 'G'",
      velox::exec::test::kHiveConnectorId);

  // Get all filters from the table
  ExprVector stringFilters = getAllFiltersAndResetToSchema("nation");
  ASSERT_FALSE(stringFilters.empty())
      << "Expected at least one filter expression for string case";

  // Create a new PlanState for the string test
  auto* rootDt2 = optimization_->rootDt();
  PlanState stringState(*optimization_, rootDt2);

  // Test conjunctsSelectivity on combined string filters
  ConstraintMap stringConstraints;
  Selectivity stringSelectivity =
      conjunctsSelectivity(stringState, stringFilters, true, stringConstraints);

  EXPECT_GE(stringSelectivity.trueFraction, 0.2f);
  EXPECT_LE(stringSelectivity.trueFraction, 0.3f);

  // Check that there is exactly one constraint for n_name
  ASSERT_EQ(stringConstraints.size(), 1)
      << "Expected exactly one constraint for n_name";

  // Get the constraint value
  const auto& stringConstraintValue = stringConstraints.begin()->second;

  // Check min = 'B'
  ASSERT_NE(stringConstraintValue.min, nullptr)
      << "Expected min to be set for string";
  auto minStringValue =
      stringConstraintValue.min->value<velox::TypeKind::VARCHAR>();
  EXPECT_EQ(minStringValue, "B")
      << "Expected min to be 'B', got '" << minStringValue << "'";

  // Check max = 'G'
  ASSERT_NE(stringConstraintValue.max, nullptr)
      << "Expected max to be set for string";
  auto maxStringValue =
      stringConstraintValue.max->value<velox::TypeKind::VARCHAR>();
  EXPECT_EQ(maxStringValue, "G")
      << "Expected max to be 'G', got '" << maxStringValue << "'";
  EXPECT_LT(5, stringConstraintValue.cardinality);
  EXPECT_GT(6, stringConstraintValue.cardinality);
}

TEST_F(FiltersTest, empty) {
  // Test various filter conditions that should produce empty results
  std::vector<std::string> emptyConditions = {
      "n_nationkey = 1 and n_nationkey = 2",
      "n_nationkey = 3 and n_nationkey > 10",
      "n_nationkey = 100",
      "n_nationkey in (1, 2, 3) and n_nationkey = 10",
      "n_nationkey in (1, 2, 3) and n_nationkey in (10, 11, 12)",
      "n_nationkey in (50, 100)",
      "n_nationkey > 10 and n_nationkey < 5",
      "n_nationkey between 1 and 3 and n_nationkey between 10 and 13"};

  for (const auto& condition : emptyConditions) {
    SCOPED_TRACE("Testing condition: " + condition);

    // Build and optimize the SQL query
    std::string sql = "select * from nation where " + condition;
    optimize(sql, velox::exec::test::kHiveConnectorId);

    // Get all filters from the table
    ExprVector allFilters = getAllFiltersAndResetToSchema("nation");
    ASSERT_FALSE(allFilters.empty())
        << "Expected at least one filter expression for condition: "
        << condition;

    // Create a temporary PlanState
    auto* rootDt = optimization_->rootDt();
    PlanState state(*optimization_, rootDt);

    // Test conjunctsSelectivity - should return 0 for guaranteed empty result
    ConstraintMap constraints;
    Selectivity selectivity =
        conjunctsSelectivity(state, allFilters, true, constraints);

    EXPECT_EQ(selectivity.trueFraction, 0)
        << "Expected 0 (" << 0 << ") for empty condition, but got "
        << selectivity.trueFraction << " for: " << condition;
  }
}

TEST_F(FiltersTest, combinations) {
  // Test various filter combinations with expected selectivities
  std::vector<std::pair<std::string, float>> conditionsWithExpected = {
      {"n_nationkey < 5 or n_nationkey > 20", 0.340278f},
      {"n_nationkey = 10 or n_nationkey > 20", 0.2f},
      {"not(n_nationkey = 10)", 0.96f},
      {"n_nationkey > 10 and n_name > 'M'", 0.25f},
      {"n_nationkey not in (2, 7, 8, 100)", 0.88f},
      {"n_nationkey > 5 or n_nationkey < 20", 0.965278f},
      {"(n_nationkey = 1 and n_regionkey = 2 and n_name = 'FRANCE') or (n_regionkey = 2 and n_name = 'EGYPT' and n_nationkey = 11)",
       0.000639498f},
      {"n_nationkey = 3 or n_nationkey = 100", 0.04f},
      {"not(n_nationkey = 100)", 1.0f}};

  struct TestResult {
    std::string condition;
    float expected;
    float actual;
    bool passed;
  };

  std::vector<TestResult> results;
  bool allPassed = true;

  for (const auto& [condition, expectedSelectivity] : conditionsWithExpected) {
    SCOPED_TRACE("Testing condition: " + condition);

    // Build and optimize the SQL query
    std::string sql = "select * from nation where " + condition;
    optimize(sql, velox::exec::test::kHiveConnectorId);

    // Get all filters from the table
    ExprVector allFilters = getAllFiltersAndResetToSchema("nation");
    ASSERT_FALSE(allFilters.empty())
        << "Expected at least one filter expression for condition: "
        << condition;

    // Create a temporary PlanState
    auto* rootDt = optimization_->rootDt();
    PlanState state(*optimization_, rootDt);

    // Test conjunctsSelectivity
    ConstraintMap constraints;
    Selectivity selectivity =
        conjunctsSelectivity(state, allFilters, true, constraints);

    // Check if within 5% of expected
    float tolerance = expectedSelectivity * 0.05f;
    bool passed =
        std::abs(selectivity.trueFraction - expectedSelectivity) <= tolerance;

    if (!passed) {
      allPassed = false;
    }

    results.push_back(
        {condition,
         expectedSelectivity,
         (float)selectivity.trueFraction,
         passed});
  }

  // If any test failed, log all results in a format that can be pasted back
  if (!allPassed) {
    LOG(ERROR) << "Some conditions did not produce expected selectivity:";
    LOG(ERROR)
        << "std::vector<std::pair<std::string, float>> conditionsWithExpected = {";
    for (size_t i = 0; i < results.size(); ++i) {
      const auto& result = results[i];
      LOG(ERROR) << "    {\"" << result.condition << "\", " << result.actual
                 << "f}" << (i < results.size() - 1 ? "," : "};");
    }
  }

  // Fail the test if any condition didn't match expected
  for (const auto& result : results) {
    EXPECT_TRUE(result.passed)
        << "Condition '" << result.condition << "' expected selectivity "
        << result.expected << " but got " << result.actual;
  }
}

TEST_F(FiltersTest, combineConjunctsBasic) {
  // Test basic AND combinations
  std::vector<Selectivity> conjuncts = {
      {0.8, 0.0}, // 80% true, 0% null, 20% false
      {0.6, 0.0} // 60% true, 0% null, 40% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = 0.8 * 0.6 = 0.48
  EXPECT_NEAR(result.trueFraction, 0.48, 0.001);
  // P(NULL) = (0.8 + 0.0) * (0.6 + 0.0) - 0.48 = 0.48 - 0.48 = 0.0
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
  // P(FALSE) = 1 - 0.48 - 0.0 = 0.52
}

TEST_F(FiltersTest, combineConjunctsWithNulls) {
  // Test AND with NULL values
  std::vector<Selectivity> conjuncts = {
      {0.5, 0.2}, // 50% true, 20% null, 30% false
      {0.6, 0.1} // 60% true, 10% null, 30% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = 0.5 * 0.6 = 0.30
  EXPECT_NEAR(result.trueFraction, 0.30, 0.001);
  // P(NULL) = (0.5 + 0.2) * (0.6 + 0.1) - 0.30 = 0.7 * 0.7 - 0.30 = 0.49 - 0.30
  // = 0.19
  EXPECT_NEAR(result.nullFraction, 0.19, 0.001);
  // P(FALSE) = 1 - 0.30 - 0.19 = 0.51
}

TEST_F(FiltersTest, combineConjunctsMultiple) {
  // Test AND with three operands
  std::vector<Selectivity> conjuncts = {
      {0.8, 0.1}, // 80% true, 10% null, 10% false
      {0.7, 0.0}, // 70% true, 0% null, 30% false
      {0.9, 0.05} // 90% true, 5% null, 5% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = 0.8 * 0.7 * 0.9 = 0.504
  EXPECT_NEAR(result.trueFraction, 0.504, 0.001);
  // P(NULL) = (0.9 * 0.7 * 0.95) - 0.504 = 0.5985 - 0.504 = 0.0945
  EXPECT_NEAR(result.nullFraction, 0.0945, 0.001);
}

TEST_F(FiltersTest, combineConjunctsEmpty) {
  // Empty span should return TRUE with no NULLs
  std::vector<Selectivity> empty;
  Selectivity result = combineConjuncts(empty);
  EXPECT_NEAR(result.trueFraction, 1.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, combineDisjunctsBasic) {
  // Test basic OR combinations
  std::vector<Selectivity> disjuncts = {
      {0.3, 0.0}, // 30% true, 0% null, 70% false
      {0.4, 0.0} // 40% true, 0% null, 60% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - (1 - 0.3) * (1 - 0.4) = 1 - 0.7 * 0.6 = 1 - 0.42 = 0.58
  EXPECT_NEAR(result.trueFraction, 0.58, 0.001);
  // P(FALSE) = 0.7 * 0.6 = 0.42
  // P(NULL) = 1 - 0.58 - 0.42 = 0.0
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, combineDisjunctsWithNulls) {
  // Test OR with NULL values
  std::vector<Selectivity> disjuncts = {
      {0.3, 0.2}, // 30% true, 20% null, 50% false
      {0.4, 0.1} // 40% true, 10% null, 50% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - (1 - 0.3) * (1 - 0.4) = 1 - 0.7 * 0.6 = 0.58
  EXPECT_NEAR(result.trueFraction, 0.58, 0.001);
  // P(FALSE) = 0.5 * 0.5 = 0.25
  // P(NULL) = 1 - 0.58 - 0.25 = 0.17
  EXPECT_NEAR(result.nullFraction, 0.17, 0.001);
}

TEST_F(FiltersTest, combineDisjunctsMultiple) {
  // Test OR with three operands
  std::vector<Selectivity> disjuncts = {
      {0.2, 0.1}, // 20% true, 10% null, 70% false
      {0.3, 0.0}, // 30% true, 0% null, 70% false
      {0.1, 0.05} // 10% true, 5% null, 85% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - (1-0.2) * (1-0.3) * (1-0.1) = 1 - 0.8 * 0.7 * 0.9 = 1 - 0.504
  // = 0.496
  EXPECT_NEAR(result.trueFraction, 0.496, 0.001);
  // P(FALSE) = 0.7 * 0.7 * 0.85 = 0.4165
  // P(NULL) = 1 - 0.496 - 0.4165 = 0.0875
  EXPECT_NEAR(result.nullFraction, 0.0875, 0.001);
}

TEST_F(FiltersTest, combineDisjunctsEmpty) {
  // Empty span should return FALSE with no NULLs
  std::vector<Selectivity> empty;
  Selectivity result = combineDisjuncts(empty);
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, combineConjunctsAllNull) {
  // Test when all inputs are NULL
  std::vector<Selectivity> conjuncts = {
      {0.0, 1.0}, // 0% true, 100% null, 0% false
      {0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineConjuncts(conjuncts);

  // P(TRUE) = 0 * 0 = 0
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  // P(NULL) = (0 + 1) * (0 + 1) - 0 = 1
  EXPECT_NEAR(result.nullFraction, 1.0, 0.001);
}

TEST_F(FiltersTest, combineDisjunctsAllNull) {
  // Test when all inputs are NULL
  std::vector<Selectivity> disjuncts = {
      {0.0, 1.0}, // 0% true, 100% null, 0% false
      {0.0, 1.0} // 0% true, 100% null, 0% false
  };

  Selectivity result = combineDisjuncts(disjuncts);

  // P(TRUE) = 1 - (1-0) * (1-0) = 0
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  // P(FALSE) = 0 * 0 = 0
  // P(NULL) = 1 - 0 - 0 = 1
  EXPECT_NEAR(result.nullFraction, 1.0, 0.001);
}

TEST_F(FiltersTest, columnComparisonNoOverlapBelow) {
  // Test a < b when ranges don't overlap and a is entirely below b
  // a: [100, 200], cardinality 50
  // b: [300, 400], cardinality 50
  ConstraintMap emptyConstraints;
  Value leftValue(velox::BIGINT().get(), 50);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 50);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(300));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(400));
  rightValue.nullFraction = 0.0;

  // P(a < b) should be 1.0 (a always less than b)
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 1.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a > b) should be 0.0
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a = b) should be 0.0 (no overlap)
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("eq"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, columnComparisonNoOverlapAbove) {
  ConstraintMap emptyConstraints;
  // Test a > b when ranges don't overlap and a is entirely above b
  // a: [300, 400], cardinality 50
  // b: [100, 200], cardinality 50
  Value leftValue(velox::BIGINT().get(), 50);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(300));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(400));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 50);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  rightValue.nullFraction = 0.0;

  // P(a > b) should be 1.0 (a always greater than b)
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 1.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a < b) should be 0.0
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.0, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, columnComparisonPartialOverlap) {
  ConstraintMap emptyConstraints;
  // Test with partial overlap as in the problem statement
  // a: [1000, 2000], cardinality 100
  // b: [1500, 2500], cardinality 100
  Value leftValue(velox::BIGINT().get(), 100);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(1000));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(2000));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 100);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(1500));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(2500));
  rightValue.nullFraction = 0.0;

  // Overlap is [1500, 2000], size 500
  // range_a = 1000, range_b = 1000
  // n_a = 100 * 500/1000 = 50
  // n_b = 100 * 500/1000 = 50
  // P(a = b) = min(50, 50) / (100 * 100) = 50 / 10000 = 0.005
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("eq"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.005, 0.0001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a < b): below_b = min(2000, 1500) - 1000 = 500
  // overlap_integral = 500 * (2*2500 - 2000 - 1500) / (2 * 1000)
  //                  = 500 * 1500 / 2000 = 375
  // P(a < b) = (500 + 375) / 1000 = 0.875
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.875, 0.01);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a > b): below_a = min(2500, 1000) - 1500 = 0 (b doesn't go below a)
  // Actually b goes from 1500-2500, a goes from 1000-2000
  // below_a should be 0 since min(bh=2500, al=1000) = 1000, 1000-bl=1500 =
  // -500, max(0,-500)=0 overlap_integral = 500 * (2*2000 - 2000 - 1500) / (2 *
  // 1000)
  //                  = 500 * 500 / 2000 = 125
  // P(a > b) = (0 + 125) / 1000 = 0.125
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.125, 0.01);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, columnComparisonFullOverlap) {
  ConstraintMap emptyConstraints;
  // Test with identical ranges
  // a: [100, 200], cardinality 50
  // b: [100, 200], cardinality 50
  Value leftValue(velox::BIGINT().get(), 50);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 50);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  rightValue.nullFraction = 0.0;

  // P(a = b) = 50 / (50 * 50) = 0.02
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("eq"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.02, 0.001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);

  // P(a < b) and P(a > b) should be symmetric and around 0.5
  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  EXPECT_GT(result.trueFraction, 0.4);
  EXPECT_LT(result.trueFraction, 0.6);

  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  EXPECT_GT(result.trueFraction, 0.4);
  EXPECT_LT(result.trueFraction, 0.6);
}

TEST_F(FiltersTest, columnComparisonWithNulls) {
  ConstraintMap emptyConstraints;
  // Test NULL propagation
  // a: [100, 200], cardinality 50, nullFraction 0.1
  // b: [150, 250], cardinality 50, nullFraction 0.2
  Value leftValue(velox::BIGINT().get(), 50);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  leftValue.nullFraction = 0.1;

  Value rightValue(velox::BIGINT().get(), 50);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(150));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(250));
  rightValue.nullFraction = 0.2;

  // P(either null) = 0.1 + 0.2 - 0.1*0.2 = 0.28
  // All true fractions should be scaled by (1 - 0.28) = 0.72
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("eq"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.nullFraction, 0.28, 0.001);
  EXPECT_GT(result.trueFraction, 0.0);
  EXPECT_LT(result.trueFraction, 0.72); // Should be less than max scaling

  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.nullFraction, 0.28, 0.001);
  EXPECT_GT(result.trueFraction, 0.0);
  EXPECT_LT(result.trueFraction, 0.72);

  result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.nullFraction, 0.28, 0.001);
  EXPECT_GT(result.trueFraction, 0.0);
  EXPECT_LT(result.trueFraction, 0.72);
}

TEST_F(FiltersTest, columnComparisonDifferentCardinalities) {
  ConstraintMap emptyConstraints;
  // Test with different cardinalities
  // a: [100, 200], cardinality 10
  // b: [150, 250], cardinality 100
  Value leftValue(velox::BIGINT().get(), 10);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 100);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(150));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(250));
  rightValue.nullFraction = 0.0;

  // Overlap is [150, 200], size 50
  // n_a = 10 * 50/100 = 5
  // n_b = 100 * 50/100 = 50
  // P(a = b) = min(5, 50) / (10 * 100) = 5 / 1000 = 0.005
  Selectivity result = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("eq"),
      false,
      emptyConstraints);
  EXPECT_NEAR(result.trueFraction, 0.005, 0.0001);
  EXPECT_NEAR(result.nullFraction, 0.0, 0.001);
}

TEST_F(FiltersTest, columnComparisonLteGte) {
  ConstraintMap emptyConstraints;
  // Test that lte and gte work the same as lt and gt
  Value leftValue(velox::BIGINT().get(), 50);
  leftValue.min = registerVariant(velox::Variant::create<int64_t>(100));
  leftValue.max = registerVariant(velox::Variant::create<int64_t>(200));
  leftValue.nullFraction = 0.0;

  Value rightValue(velox::BIGINT().get(), 50);
  rightValue.min = registerVariant(velox::Variant::create<int64_t>(300));
  rightValue.max = registerVariant(velox::Variant::create<int64_t>(400));
  rightValue.nullFraction = 0.0;

  Selectivity resultLt = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lt"),
      false,
      emptyConstraints);
  Selectivity resultLte = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("lte"),
      false,
      emptyConstraints);
  EXPECT_NEAR(resultLt.trueFraction, resultLte.trueFraction, 0.001);

  Selectivity resultGt = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gt"),
      false,
      emptyConstraints);
  Selectivity resultGte = columnComparisonSelectivity(
      nullptr,
      nullptr,
      leftValue,
      rightValue,
      toName("gte"),
      false,
      emptyConstraints);
  EXPECT_NEAR(resultGt.trueFraction, resultGte.trueFraction, 0.001);
}

TEST_F(FiltersTest, equalityConstraintPropagation) {
  // Test constraint propagation for n_regionkey = n_nationkey
  // When two columns are equated, we should propagate constraints
  // based on the intersection of their ranges
  optimize(
      "select n_regionkey from nation where n_regionkey = n_nationkey",
      velox::exec::test::kHiveConnectorId);

  // Get all filters from the nation table
  ExprVector allFilters = getAllFiltersAndResetToSchema("nation");
  ASSERT_EQ(1, allFilters.size()) << "Expected exactly one filter expression";

  // Create a temporary PlanState
  auto* rootDt = optimization_->rootDt();
  PlanState state(*optimization_, rootDt);

  // Call comparisonSelectivity with updateConstraints=true
  ConstraintMap constraints;
  Selectivity selectivity =
      exprSelectivity(state, allFilters[0], true, constraints);

  // Verify selectivity is valid
  EXPECT_GE(selectivity.trueFraction, 0.0);
  EXPECT_LE(selectivity.trueFraction, 1.0);

  // Check that 2 constraints were added (one for n_regionkey, one for
  // n_nationkey)
  ASSERT_EQ(2, constraints.size())
      << "Expected exactly 2 constraints (one for n_regionkey, one for n_nationkey)";

  // Both constraints should have the same values
  // Based on actual data: n_regionkey is [0, 4] with cardinality 5
  // n_nationkey is [0, 24] with cardinality 25
  // Intersection is [0, 4]
  for (const auto& [id, constraintValue] : constraints) {
    // Check cardinality - should be around 5 (actual value may vary slightly)
    EXPECT_GE(constraintValue.cardinality, 4.0f)
        << "Expected cardinality to be at least 4, got "
        << constraintValue.cardinality;
    EXPECT_LE(constraintValue.cardinality, 5.5f)
        << "Expected cardinality to be at most 5.5, got "
        << constraintValue.cardinality;

    // Check min = 0
    ASSERT_NE(constraintValue.min, nullptr) << "Expected min to be set";
    auto minValue = constraintValue.min->value<velox::TypeKind::BIGINT>();
    EXPECT_EQ(minValue, 0) << "Expected min to be 0, got " << minValue;

    // Check max = 4
    ASSERT_NE(constraintValue.max, nullptr) << "Expected max to be set";
    auto maxValue = constraintValue.max->value<velox::TypeKind::BIGINT>();
    EXPECT_EQ(maxValue, 4) << "Expected max to be 4, got " << maxValue;
  }
}

} // namespace facebook::axiom::optimizer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
