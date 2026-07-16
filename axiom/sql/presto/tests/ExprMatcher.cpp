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

#include "axiom/sql/presto/tests/ExprMatcher.h"
#include <gtest/gtest.h>
#include <algorithm>
#include "axiom/logical_plan/ExprApi.h"
#include "velox/parse/Expressions.h"

namespace facebook::axiom::logical_plan::test {
namespace {

#define AXIOM_RETURN_IF_FAILURE                \
  if (::testing::Test::HasNonfatalFailure()) { \
    return;                                    \
  }

#define AXIOM_RETURN_FALSE_IF_FAILURE          \
  if (::testing::Test::HasNonfatalFailure()) { \
    return false;                              \
  }

bool isWildcard(const velox::core::ExprPtr& expr) {
  if (!expr->is(velox::core::IExpr::Kind::kCall)) {
    return false;
  }
  const auto* call = expr->as<velox::core::CallExpr>();
  return call->name() == ExprMatcher::kWildcard && call->inputs().empty();
}

bool isExistsWildcard(const velox::core::ExprPtr& expr) {
  if (!expr->is(velox::core::IExpr::Kind::kCall)) {
    return false;
  }
  const auto* call = expr->as<velox::core::CallExpr>();
  return call->name() == ExprMatcher::kExistsWildcard && call->inputs().empty();
}

// Forward declaration for mutual recursion.
bool matchImpl(const ExprPtr& actual, const velox::core::ExprPtr& expected);

void matchChildren(
    const std::vector<ExprPtr>& actualInputs,
    const std::vector<velox::core::ExprPtr>& expectedInputs) {
  EXPECT_EQ(actualInputs.size(), expectedInputs.size());
  AXIOM_RETURN_IF_FAILURE

  for (size_t i = 0; i < actualInputs.size(); ++i) {
    SCOPED_TRACE("child " + std::to_string(i));
    matchImpl(actualInputs[i], expectedInputs[i]);
    AXIOM_RETURN_IF_FAILURE
  }
}

void matchConstant(
    const ConstantExpr& actual,
    const velox::core::ConstantExpr& expected) {
  EXPECT_EQ(actual.isNull(), expected.value().isNull());
  AXIOM_RETURN_IF_FAILURE

  if (actual.isNull()) {
    return;
  }

  if (actual.value()->equalsWithEpsilon(expected.value())) {
    return;
  }

  // Band-aid: some tests specify expected SQL with bare integer literals
  // parsed as BIGINT (per parseIntegerAsBigint option), while the plan uses
  // INTEGER. Many existing tests rely on this tolerance.
  if (actual.type()->isInteger() && expected.type()->isBigint()) {
    EXPECT_EQ(
        static_cast<int64_t>(actual.value()->value<int32_t>()),
        expected.value().value<int64_t>());
    return;
  }

  if (actual.type()->isBigint() && expected.type()->isInteger()) {
    EXPECT_EQ(
        actual.value()->value<int64_t>(),
        static_cast<int64_t>(expected.value().value<int32_t>()));
    return;
  }

  ADD_FAILURE() << "Constant mismatch.";
}

// Returns the lowercase IExpr call name for a SpecialForm.
std::string specialFormToCallName(SpecialForm form) {
  auto name = std::string(SpecialFormName::toName(form));
  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
  return name;
}

void matchDereference(
    const SpecialFormExpr& actual,
    const velox::core::ExprPtr& expected) {
  VELOX_CHECK_EQ(actual.inputs().size(), 2);

  if (expected->is(velox::core::IExpr::Kind::kFieldAccess)) {
    const auto* field = expected->as<velox::core::FieldAccessExpr>();
    EXPECT_FALSE(field->isRootColumn()) << "Expected non-root FieldAccessExpr.";
    AXIOM_RETURN_IF_FAILURE

    // The second input is a constant with the field name or index.
    const auto* fieldId = actual.inputAt(1)->as<ConstantExpr>();
    VELOX_CHECK_NOT_NULL(fieldId);

    std::string actualFieldName;
    if (fieldId->type()->isVarchar()) {
      actualFieldName =
          std::string(fieldId->value()->value<velox::TypeKind::VARCHAR>());
    } else {
      // Integer index — look up field name from the struct type.
      VELOX_CHECK(actual.inputAt(0)->type()->isRow());
      auto index = fieldId->value()->value<velox::TypeKind::INTEGER>();
      actualFieldName = actual.inputAt(0)->type()->asRow().nameOf(index);
    }

    EXPECT_EQ(actualFieldName, field->name());
    AXIOM_RETURN_IF_FAILURE

    matchImpl(actual.inputAt(0), field->inputs()[0]);
    return;
  }

  if (expected->is(velox::core::IExpr::Kind::kCall)) {
    const auto* call = expected->as<velox::core::CallExpr>();
    if (call->name() == "subscript") {
      EXPECT_EQ(2, call->inputs().size());
      AXIOM_RETURN_IF_FAILURE

      EXPECT_TRUE(call->inputs()[1]->is(velox::core::IExpr::Kind::kConstant))
          << "Expected constant subscript index.";
      AXIOM_RETURN_IF_FAILURE

      const auto* indexExpr =
          call->inputs()[1]->as<velox::core::ConstantExpr>();
      int64_t expectedIndex = 0;
      if (indexExpr->type()->kind() == velox::TypeKind::INTEGER) {
        expectedIndex = indexExpr->value().value<velox::TypeKind::INTEGER>();
      } else if (indexExpr->type()->kind() == velox::TypeKind::BIGINT) {
        expectedIndex = indexExpr->value().value<velox::TypeKind::BIGINT>();
      } else {
        FAIL() << "Expected integer subscript index, got "
               << indexExpr->type()->toString() << ".";
      }

      // Logical plan dereference uses 0-based index.
      const auto* actualIndexExpr = actual.inputAt(1)->as<ConstantExpr>();
      VELOX_CHECK_NOT_NULL(actualIndexExpr);
      int64_t actualIndex = 0;
      if (actualIndexExpr->type()->kind() == velox::TypeKind::INTEGER) {
        actualIndex =
            actualIndexExpr->value()->value<velox::TypeKind::INTEGER>();
      } else if (actualIndexExpr->type()->kind() == velox::TypeKind::BIGINT) {
        actualIndex =
            actualIndexExpr->value()->value<velox::TypeKind::BIGINT>();
      }

      EXPECT_EQ(actualIndex + 1, expectedIndex)
          << "Subscript index mismatch (0-based vs 1-based).";
      AXIOM_RETURN_IF_FAILURE

      matchImpl(actual.inputAt(0), call->inputs()[0]);
      return;
    }
  }

  ADD_FAILURE() << "Expected FieldAccessExpr or subscript() for dereference.";
}

// Matches sort ordering fields.
void matchOrdering(
    const std::vector<SortingField>& actualOrdering,
    const std::vector<velox::core::SortKey>& expectedOrdering) {
  EXPECT_EQ(actualOrdering.size(), expectedOrdering.size());
  AXIOM_RETURN_IF_FAILURE

  for (size_t i = 0; i < actualOrdering.size(); ++i) {
    SCOPED_TRACE("ordering " + std::to_string(i));
    matchImpl(actualOrdering[i].expression, expectedOrdering[i].expr);
    AXIOM_RETURN_IF_FAILURE

    EXPECT_EQ(
        actualOrdering[i].order.isAscending(), expectedOrdering[i].ascending)
        << "Sort direction mismatch.";
    AXIOM_RETURN_IF_FAILURE

    EXPECT_EQ(
        actualOrdering[i].order.isNullsFirst(), expectedOrdering[i].nullsFirst)
        << "Nulls-first mismatch.";
    AXIOM_RETURN_IF_FAILURE
  }
}

bool matchWindowType(
    WindowExpr::WindowType actual,
    velox::core::WindowCallExpr::WindowType expected) {
  switch (actual) {
    case WindowExpr::WindowType::kRange:
      return expected == velox::core::WindowCallExpr::WindowType::kRange;
    case WindowExpr::WindowType::kRows:
      return expected == velox::core::WindowCallExpr::WindowType::kRows;
    case WindowExpr::WindowType::kGroups:
      return expected == velox::core::WindowCallExpr::WindowType::kGroups;
  }
  return false;
}

bool matchBoundType(
    WindowExpr::BoundType actual,
    velox::core::WindowCallExpr::BoundType expected) {
  switch (actual) {
    case WindowExpr::BoundType::kUnboundedPreceding:
      return expected ==
          velox::core::WindowCallExpr::BoundType::kUnboundedPreceding;
    case WindowExpr::BoundType::kPreceding:
      return expected == velox::core::WindowCallExpr::BoundType::kPreceding;
    case WindowExpr::BoundType::kCurrentRow:
      return expected == velox::core::WindowCallExpr::BoundType::kCurrentRow;
    case WindowExpr::BoundType::kFollowing:
      return expected == velox::core::WindowCallExpr::BoundType::kFollowing;
    case WindowExpr::BoundType::kUnboundedFollowing:
      return expected ==
          velox::core::WindowCallExpr::BoundType::kUnboundedFollowing;
  }
  return false;
}

void matchWindowFrame(
    const WindowExpr::Frame& actual,
    const velox::core::WindowCallExpr::Frame& expected) {
  EXPECT_TRUE(matchWindowType(actual.type, expected.type))
      << "Window frame type mismatch.";
  AXIOM_RETURN_IF_FAILURE

  EXPECT_TRUE(matchBoundType(actual.startType, expected.startType))
      << "Start bound type mismatch.";
  AXIOM_RETURN_IF_FAILURE

  EXPECT_TRUE(matchBoundType(actual.endType, expected.endType))
      << "End bound type mismatch.";
  AXIOM_RETURN_IF_FAILURE

  if (expected.startValue != nullptr) {
    EXPECT_NE(actual.startValue, nullptr) << "Expected start bound value.";
    AXIOM_RETURN_IF_FAILURE
    matchImpl(actual.startValue, expected.startValue);
    AXIOM_RETURN_IF_FAILURE
  } else {
    EXPECT_EQ(actual.startValue, nullptr) << "Unexpected start bound value.";
    AXIOM_RETURN_IF_FAILURE
  }

  if (expected.endValue != nullptr) {
    EXPECT_NE(actual.endValue, nullptr) << "Expected end bound value.";
    AXIOM_RETURN_IF_FAILURE
    matchImpl(actual.endValue, expected.endValue);
    AXIOM_RETURN_IF_FAILURE
  } else {
    EXPECT_EQ(actual.endValue, nullptr) << "Unexpected end bound value.";
    AXIOM_RETURN_IF_FAILURE
  }
}

void matchWindow(
    const WindowExpr& actual,
    const velox::core::WindowCallExpr& expected) {
  EXPECT_EQ(actual.name(), expected.name());
  AXIOM_RETURN_IF_FAILURE

  matchChildren(actual.inputs(), expected.inputs());
  AXIOM_RETURN_IF_FAILURE

  EXPECT_EQ(actual.partitionKeys().size(), expected.partitionKeys().size());
  AXIOM_RETURN_IF_FAILURE

  for (size_t i = 0; i < actual.partitionKeys().size(); ++i) {
    SCOPED_TRACE("partition key " + std::to_string(i));
    matchImpl(actual.partitionKeys()[i], expected.partitionKeys()[i]);
    AXIOM_RETURN_IF_FAILURE
  }

  matchOrdering(actual.ordering(), expected.orderByKeys());
  AXIOM_RETURN_IF_FAILURE

  EXPECT_TRUE(expected.frame().has_value()) << "Expected window frame.";
  AXIOM_RETURN_IF_FAILURE

  matchWindowFrame(actual.frame(), expected.frame().value());
  AXIOM_RETURN_IF_FAILURE

  EXPECT_EQ(actual.ignoreNulls(), expected.isIgnoreNulls())
      << "IGNORE NULLS mismatch.";
}

void matchAggregate(
    const AggregateExpr& actual,
    const velox::core::AggregateCallExpr& expected) {
  EXPECT_EQ(actual.name(), expected.name());
  AXIOM_RETURN_IF_FAILURE

  EXPECT_EQ(actual.isDistinct(), expected.isDistinct()) << "DISTINCT mismatch.";
  AXIOM_RETURN_IF_FAILURE

  matchChildren(actual.inputs(), expected.inputs());
  AXIOM_RETURN_IF_FAILURE

  matchOrdering(actual.ordering(), expected.orderBy());
  AXIOM_RETURN_IF_FAILURE

  if (expected.filter() != nullptr) {
    EXPECT_NE(actual.filter(), nullptr) << "Expected aggregate filter.";
    AXIOM_RETURN_IF_FAILURE
    matchImpl(actual.filter(), expected.filter());
  } else {
    EXPECT_EQ(actual.filter(), nullptr) << "Unexpected aggregate filter.";
  }
}

void matchSpecialForm(
    const SpecialFormExpr& actual,
    const velox::core::ExprPtr& expected) {
  const auto form = actual.form();

  // Cast/TryCast: expect IExpr::kCast.
  if (form == SpecialForm::kCast || form == SpecialForm::kTryCast) {
    EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kCast))
        << "Expected CastExpr.";
    AXIOM_RETURN_IF_FAILURE

    const auto* cast = expected->as<velox::core::CastExpr>();
    EXPECT_EQ(form == SpecialForm::kTryCast, cast->isTryCast())
        << "CAST vs TRY_CAST mismatch.";
    AXIOM_RETURN_IF_FAILURE

    EXPECT_TRUE(actual.type()->equivalent(*cast->type()))
        << "Cast target type: actual " << actual.type()->toString()
        << ", expected " << cast->type()->toString() << ".";
    AXIOM_RETURN_IF_FAILURE

    VELOX_CHECK_EQ(actual.inputs().size(), 1);
    matchImpl(actual.inputAt(0), cast->input());
    return;
  }

  // Dereference: expect FieldAccessExpr or subscript call.
  if (form == SpecialForm::kDereference) {
    matchDereference(actual, expected);
    return;
  }

  // NullIf: the logical plan has 3 inputs (value, comparand, null literal),
  // but IExpr "nullif" call has only 2 inputs (value, comparand).
  if (form == SpecialForm::kNullIf) {
    EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kCall))
        << "Expected nullif CallExpr.";
    AXIOM_RETURN_IF_FAILURE

    const auto* expectedCall = expected->as<velox::core::CallExpr>();
    EXPECT_EQ("nullif", expectedCall->name());
    AXIOM_RETURN_IF_FAILURE

    EXPECT_GE(actual.inputs().size(), 2);
    EXPECT_EQ(expectedCall->inputs().size(), 2);
    AXIOM_RETURN_IF_FAILURE

    for (int32_t i = 0; i < 2; ++i) {
      SCOPED_TRACE("child " + std::to_string(i));
      matchImpl(actual.inputAt(i), expectedCall->inputAt(i));
      AXIOM_RETURN_IF_FAILURE
    }
    return;
  }

  // All other special forms: expect kCall with matching lowercase name.
  EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kCall))
      << "Expected CallExpr for special form " << SpecialFormName::toName(form)
      << ".";
  AXIOM_RETURN_IF_FAILURE

  const auto callName = specialFormToCallName(form);

  const auto* expectedCall = expected->as<velox::core::CallExpr>();
  EXPECT_EQ(callName, expectedCall->name());
  AXIOM_RETURN_IF_FAILURE

  matchChildren(actual.inputs(), expectedCall->inputs());
}

bool matchImpl(const ExprPtr& actual, const velox::core::ExprPtr& expected) {
  VELOX_CHECK_NOT_NULL(actual);
  VELOX_CHECK_NOT_NULL(expected);

  if (isWildcard(expected)) {
    return true;
  }

  if (isExistsWildcard(expected)) {
    if (actual->kind() != ExprKind::kSpecialForm) {
      ADD_FAILURE() << "Expected EXISTS (any_exists() wildcard), got "
                    << actual->kindName() << ".";
      return false;
    }
    const auto form = actual->as<SpecialFormExpr>()->form();
    if (form != SpecialForm::kExists) {
      ADD_FAILURE() << "Expected EXISTS special form, got "
                    << SpecialFormName::toName(form) << ".";
      return false;
    }
    return true;
  }

  SCOPED_TRACE(
      "actual: '" + actual->toString() + "', expected: '" +
      expected->toString() + "'");

  switch (actual->kind()) {
    case ExprKind::kInputReference: {
      const auto* inputRef = actual->as<InputReferenceExpr>();

      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kFieldAccess))
          << "Expected FieldAccessExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* field = expected->as<velox::core::FieldAccessExpr>();
      EXPECT_TRUE(field->isRootColumn()) << "Expected root column.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      EXPECT_EQ(inputRef->name(), field->name());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kConstant: {
      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kConstant))
          << "Expected ConstantExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      matchConstant(
          *actual->as<ConstantExpr>(),
          *expected->as<velox::core::ConstantExpr>());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kCall: {
      const auto* call = actual->as<CallExpr>();

      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kCall))
          << "Expected CallExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* expectedCall = expected->as<velox::core::CallExpr>();
      EXPECT_EQ(call->name(), expectedCall->name());
      AXIOM_RETURN_FALSE_IF_FAILURE

      matchChildren(call->inputs(), expectedCall->inputs());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kSpecialForm: {
      matchSpecialForm(*actual->as<SpecialFormExpr>(), expected);
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kAggregate: {
      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kAggregate))
          << "Expected AggregateCallExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      matchAggregate(
          *actual->as<AggregateExpr>(),
          *expected->as<velox::core::AggregateCallExpr>());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kSpecialFormAgg: {
      const auto* expectedCall =
          dynamic_cast<const velox::core::SpecialFormAggCallExpr*>(
              expected.get());
      EXPECT_NE(expectedCall, nullptr) << "Expected SpecialFormAggCallExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* aggregate = actual->as<SpecialFormAggExpr>();
      EXPECT_EQ(aggregate->kind(), expectedCall->specialAggregateKind());
      AXIOM_RETURN_FALSE_IF_FAILURE

      matchChildren(aggregate->inputs(), expectedCall->inputs());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kWindow: {
      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kWindow))
          << "Expected WindowCallExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      matchWindow(
          *actual->as<WindowExpr>(),
          *expected->as<velox::core::WindowCallExpr>());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kLambda: {
      const auto* lambda = actual->as<LambdaExpr>();

      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kLambda))
          << "Expected LambdaExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* expectedLambda = expected->as<velox::core::LambdaExpr>();
      const auto& actualNames = lambda->signature()->names();
      const auto& expectedNames = expectedLambda->arguments();
      EXPECT_EQ(actualNames.size(), expectedNames.size());
      AXIOM_RETURN_FALSE_IF_FAILURE

      for (size_t i = 0; i < actualNames.size(); ++i) {
        EXPECT_EQ(actualNames[i], expectedNames[i])
            << "Lambda parameter mismatch at index " << i << ".";
        AXIOM_RETURN_FALSE_IF_FAILURE
      }

      matchImpl(lambda->body(), expectedLambda->body());
      return !::testing::Test::HasNonfatalFailure();
    }

    case ExprKind::kSubquery: {
      EXPECT_TRUE(expected->is(velox::core::IExpr::Kind::kSubquery))
          << "Expected SubqueryExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* actualSubquery = actual->as<SubqueryExpr>();
      const auto* expectedSubquery = expected->as<velox::core::SubqueryExpr>();
      EXPECT_EQ(actualSubquery->subquery(), expectedSubquery->subquery());
      return !::testing::Test::HasNonfatalFailure();
    }
  }

  ADD_FAILURE() << "Unsupported expression kind: " << actual->kindName() << ".";
  return false;
}

#undef AXIOM_RETURN_IF_FAILURE
#undef AXIOM_RETURN_FALSE_IF_FAILURE

} // namespace

bool ExprMatcher::match(
    const ExprPtr& actual,
    const velox::core::ExprPtr& expected) {
  return matchImpl(actual, expected);
}

} // namespace facebook::axiom::logical_plan::test
