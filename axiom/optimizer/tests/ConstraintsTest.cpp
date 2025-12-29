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
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;

class ConstraintsTest : public HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
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

  /// Helper to find a column by name in the plan's constraints.
  /// Returns the Value from the ConstraintMap for the column.
  template <typename ColumnVec>
  std::optional<Value> findColumnConstraint(
      const ConstraintMap& constraints,
      const ColumnVec& columns,
      std::string_view columnName) {
    for (auto* col : columns) {
      if (col->name() == columnName) {
        auto it = constraints.find(col->id());
        if (it != constraints.end()) {
          return it->second;
        }
      }
    }
    return std::nullopt;
  }
};

TEST_F(ConstraintsTest, scanEquality) {
  // SQL: select n_nationkey, r_regionkey from nation, region
  //      where n_nationkey = r_regionkey
  optimize(
      "select n_nationkey, r_regionkey from nation, region "
      "where n_nationkey = r_regionkey",
      velox::exec::test::kHiveConnectorId);

  // Get the best plan
  auto* plan = optimization_->bestPlan();
  ASSERT_NE(plan, nullptr) << "Best plan should not be null";

  // Get the output columns from the plan
  const auto& columns = plan->op->columns();
  ASSERT_FALSE(columns.empty()) << "Plan should have output columns";

  // Check n_nationkey constraints
  auto n_nationkey_constraint =
      findColumnConstraint(*plan->constraints, columns, "n_nationkey");
  ASSERT_TRUE(n_nationkey_constraint.has_value())
      << "Should have constraint for n_nationkey";

  // Check range is [0, 4]
  ASSERT_NE(n_nationkey_constraint->min, nullptr)
      << "n_nationkey min should be set";
  ASSERT_NE(n_nationkey_constraint->max, nullptr)
      << "n_nationkey max should be set";
  auto minValue = n_nationkey_constraint->min->value<velox::TypeKind::BIGINT>();
  auto maxValue = n_nationkey_constraint->max->value<velox::TypeKind::BIGINT>();
  EXPECT_EQ(minValue, 0) << "n_nationkey min should be 0";
  EXPECT_EQ(maxValue, 4) << "n_nationkey max should be 4";

  // Check cardinality is between 4 and 5
  EXPECT_GE(n_nationkey_constraint->cardinality, 4.0f)
      << "n_nationkey cardinality should be at least 4";
  EXPECT_LE(n_nationkey_constraint->cardinality, 5.0f)
      << "n_nationkey cardinality should be at most 5";

  // Check r_regionkey constraints
  auto r_regionkey_constraint =
      findColumnConstraint(*plan->constraints, columns, "r_regionkey");
  ASSERT_TRUE(r_regionkey_constraint.has_value())
      << "Should have constraint for r_regionkey";

  // Check range is [0, 4]
  ASSERT_NE(r_regionkey_constraint->min, nullptr)
      << "r_regionkey min should be set";
  ASSERT_NE(r_regionkey_constraint->max, nullptr)
      << "r_regionkey max should be set";
  minValue = r_regionkey_constraint->min->value<velox::TypeKind::BIGINT>();
  maxValue = r_regionkey_constraint->max->value<velox::TypeKind::BIGINT>();
  EXPECT_EQ(minValue, 0) << "r_regionkey min should be 0";
  EXPECT_EQ(maxValue, 4) << "r_regionkey max should be 4";

  // Check cardinality is between 4 and 5
  EXPECT_GE(r_regionkey_constraint->cardinality, 4.0f)
      << "r_regionkey cardinality should be at least 4";
  EXPECT_LE(r_regionkey_constraint->cardinality, 5.0f)
      << "r_regionkey cardinality should be at most 5";
}

TEST_F(ConstraintsTest, aggregateConstraint) {
  // SQL: select n_regionkey, count(*), max(n_name) from nation
  //      group by n_regionkey
  optimize(
      "select n_regionkey, count(*), max(n_name) from nation "
      "group by n_regionkey",
      velox::exec::test::kHiveConnectorId);

  // Get the best plan
  auto* plan = optimization_->bestPlan();
  ASSERT_NE(plan, nullptr) << "Best plan should not be null";

  // Get the output columns from the plan
  const auto& columns = plan->op->columns();
  ASSERT_FALSE(columns.empty()) << "Plan should have output columns";

  // All three output columns should have cardinality between 4 and 5
  // (since there are 5 regions)
  ASSERT_EQ(columns.size(), 3) << "Should have exactly 3 output columns";

  for (size_t i = 0; i < columns.size(); ++i) {
    auto* col = columns[i];
    auto it = plan->constraints->find(col->id());
    if (it != plan->constraints->end()) {
      EXPECT_GE(it->second.cardinality, 4.0f)
          << "Column " << col->name() << " should have cardinality at least 4";
      EXPECT_LE(it->second.cardinality, 5.0f)
          << "Column " << col->name() << " should have cardinality at most 5";
    }
  }
}

TEST_F(ConstraintsTest, projectConstraint) {
  // SQL: select n_nationkey + 1, n_regionkey + 1, n_nationkey + n_regionkey
  //      from nation
  optimize(
      "select n_nationkey + 1, n_regionkey + 1, n_nationkey + n_regionkey "
      "from nation",
      velox::exec::test::kHiveConnectorId);

  // Get the best plan
  auto* plan = optimization_->bestPlan();
  ASSERT_NE(plan, nullptr) << "Best plan should not be null";

  // Get the output columns from the plan
  const auto& columns = plan->op->columns();
  ASSERT_FALSE(columns.empty()) << "Plan should have output columns";

  ASSERT_EQ(columns.size(), 3) << "Should have exactly 3 output columns";

  // Expected cardinalities: 25, 5, 25
  std::vector<float> expectedCardinalities = {25.0f, 5.0f, 25.0f};

  for (size_t i = 0; i < columns.size(); ++i) {
    auto* col = columns[i];
    auto it = plan->constraints->find(col->id());
    if (it != plan->constraints->end()) {
      EXPECT_EQ(it->second.cardinality, expectedCardinalities[i])
          << "Column " << i << " (" << col->name()
          << ") should have cardinality " << expectedCardinalities[i];
    }
  }
}

TEST_F(ConstraintsTest, outer) {
  // SQL: select n_nationkey, r_regionkey, n_name, r_name
  //      from nation left join region on n_nationkey = r_regionkey
  optimize(
      "select n_nationkey, r_regionkey, n_name, r_name "
      "from nation left join region on n_nationkey = r_regionkey",
      velox::exec::test::kHiveConnectorId);

  // Get the best plan
  auto* plan = optimization_->bestPlan();
  ASSERT_NE(plan, nullptr) << "Best plan should not be null";

  // Get the output columns from the plan
  const auto& columns = plan->op->columns();
  ASSERT_FALSE(columns.empty()) << "Plan should have output columns";

  // Check n_nationkey constraints (left side, non-optional)
  auto n_nationkey_constraint =
      findColumnConstraint(*plan->constraints, columns, "n_nationkey");
  ASSERT_TRUE(n_nationkey_constraint.has_value())
      << "Should have constraint for n_nationkey";

  // n_nationkey should have cardinality 24 (25 nations, only 5 match regions)
  EXPECT_NEAR(n_nationkey_constraint->cardinality, 24.0f, 1.0f)
      << "n_nationkey cardinality should be close to 24";

  // n_nationkey should have nullFraction 0 (left side is not optional)
  EXPECT_EQ(n_nationkey_constraint->nullFraction, 0.0f)
      << "n_nationkey nullFraction should be 0";

  // Check n_name constraints (left side, non-optional, non-key)
  auto n_name_constraint =
      findColumnConstraint(*plan->constraints, columns, "n_name");
  ASSERT_TRUE(n_name_constraint.has_value())
      << "Should have constraint for n_name";

  // n_name should have nullFraction 0 (left side is not optional)
  EXPECT_EQ(n_name_constraint->nullFraction, 0.0f)
      << "n_name nullFraction should be 0";

  // Check r_regionkey constraints (right side, optional)
  auto r_regionkey_constraint =
      findColumnConstraint(*plan->constraints, columns, "r_regionkey");
  ASSERT_TRUE(r_regionkey_constraint.has_value())
      << "Should have constraint for r_regionkey";

  // r_regionkey should have cardinality 5
  EXPECT_NEAR(r_regionkey_constraint->cardinality, 5.0f, 1.0f)
      << "r_regionkey cardinality should be close to 5";

  // r_regionkey should have nullFraction close to 0.8 (20 out of 25 nations
  // don't match)
  EXPECT_NEAR(r_regionkey_constraint->nullFraction, 0.8f, 0.1f)
      << "r_regionkey nullFraction should be close to 0.8";

  // Check r_name constraints (right side, optional, non-key)
  auto r_name_constraint =
      findColumnConstraint(*plan->constraints, columns, "r_name");
  ASSERT_TRUE(r_name_constraint.has_value())
      << "Should have constraint for r_name";

  // r_name should have nullFraction close to 0.8 (20 out of 25 nations don't
  // match)
  EXPECT_NEAR(r_name_constraint->nullFraction, 0.8f, 0.1f)
      << "r_name nullFraction should be close to 0.8";
}

TEST_F(ConstraintsTest, bitwiseAnd) {
  // Register the bitwise_and function with a constraint function
  auto metadata = std::make_unique<FunctionMetadata>();
  metadata->functionConstraint = [](ExprCP expr,
                                    PlanState& state) -> std::optional<Value> {
    auto* call = expr->as<Call>();
    if (!call || call->args().size() != 2) {
      return std::nullopt;
    }

    // Get constraints for both arguments
    Value arg0Value = exprConstraint(call->args()[0], state);
    Value arg1Value = exprConstraint(call->args()[1], state);

    // The result range is [0, min(arg0.max, arg1.max)]
    Value result = expr->value();
    result.min = registerVariant(velox::Variant::create<int64_t>(0));

    // Find the minimum of the two maxes
    const velox::variant* max1 = arg0Value.max;
    const velox::variant* max2 = arg1Value.max;

    // If both have a max, use the minimum of the two
    if (max1 && !max1->isNull() && max2 && !max2->isNull()) {
      result.max = (max1->value<velox::TypeKind::BIGINT>() <
                    max2->value<velox::TypeKind::BIGINT>())
          ? max1
          : max2;
    }
    // If only one has a max, use that one
    else if (max1 && !max1->isNull()) {
      result.max = max1;
    } else if (max2 && !max2->isNull()) {
      result.max = max2;
    }
    // If neither has a max, result.max is nullptr
    else {
      result.max = nullptr;
    }

    // Cardinality is the lesser of: max cardinality of args, and (max + 1)
    float maxCardinality =
        std::max(arg0Value.cardinality, arg1Value.cardinality);
    if (result.max && !result.max->isNull()) {
      int64_t maxValue = result.max->value<velox::TypeKind::BIGINT>();
      result.cardinality =
          std::min(maxCardinality, static_cast<float>(maxValue + 1));
    } else {
      result.cardinality = maxCardinality;
    }

    return result;
  };

  FunctionRegistry::instance()->registerFunction(
      "bitwise_and", std::move(metadata));

  optimize(
      "select ps_partkey from partsupp where bitwise_and(ps_partkey, 1023) < 2000",
      velox::exec::test::kHiveConnectorId);

  // Get the best plan
  auto* plan = optimization_->bestPlan();
  ASSERT_NE(plan, nullptr) << "Best plan should not be null";

  // Find the TableScan in the plan to check filterSelectivity
  auto* tableScanOp = findInPlan(plan->op.get(), RelType::kTableScan);
  ASSERT_NE(tableScanOp, nullptr) << "Should find a TableScan in the plan";

  auto* tableScan = tableScanOp->as<TableScan>();
  auto* baseTable = tableScan->baseTable;
  ASSERT_NE(baseTable, nullptr) << "Should have a BaseTable in the TableScan";

  // filterSelectivity should be 1.0 (all filtered rows pass)
  EXPECT_EQ(baseTable->filterSelectivity, 1.0f)
      << "filterSelectivity should be 1.0";
}

} // namespace facebook::axiom::optimizer::test
