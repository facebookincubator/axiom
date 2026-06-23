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

#include "axiom/optimizer/tests/ExprMatcher.h"
#include <gtest/gtest.h>
#include "velox/core/Expressions.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::core {
namespace {

#define AXIOM_RETURN_IF_FAILURE                \
  if (::testing::Test::HasNonfatalFailure()) { \
    return;                                    \
  }

#define AXIOM_RETURN_FALSE_IF_FAILURE          \
  if (::testing::Test::HasNonfatalFailure()) { \
    return false;                              \
  }

bool isNullConstant(const TypedExprPtr& expr) {
  return expr->isConstantKind() &&
      expr->asUnchecked<ConstantTypedExpr>()->isNull();
}

bool isWildcard(const ExprPtr& expr) {
  if (!expr->is(IExpr::Kind::kCall)) {
    return false;
  }
  const auto* call = expr->as<CallExpr>();
  return call->name() == ExprMatcher::kWildcard && call->inputs().empty();
}

void matchConstant(
    const ConstantTypedExpr& actual,
    const ConstantExpr& expected) {
  EXPECT_EQ(actual.isNull(), expected.value().isNull());
  AXIOM_RETURN_IF_FAILURE

  if (actual.isNull()) {
    return;
  }

  if (actual.hasValueVector()) {
    EXPECT_EQ(actual.toString(), expected.toString());
    return;
  }

  if (actual.value().equalsWithEpsilon(expected.value())) {
    return;
  }

  // Band-aid: some tests specify expected SQL with bare integer literals
  // parsed as BIGINT (per parseIntegerAsBigint option), while the plan uses
  // INTEGER. Many existing tests rely on this tolerance.
  if (actual.type()->isInteger() && expected.type()->isBigint()) {
    EXPECT_EQ(
        static_cast<int64_t>(actual.value().value<int32_t>()),
        expected.value().value<int64_t>());
    return;
  }

  if (actual.type()->isBigint() && expected.type()->isInteger()) {
    EXPECT_EQ(
        actual.value().value<int64_t>(),
        static_cast<int64_t>(expected.value().value<int32_t>()));
    return;
  }

  ADD_FAILURE() << "Constant mismatch.";
}

bool matchImpl(const TypedExprPtr& actual, const ExprPtr& expected);

void matchChildren(
    const std::vector<TypedExprPtr>& actualInputs,
    const std::vector<ExprPtr>& expectedInputs) {
  EXPECT_EQ(actualInputs.size(), expectedInputs.size());
  AXIOM_RETURN_IF_FAILURE

  for (size_t i = 0; i < actualInputs.size(); ++i) {
    SCOPED_TRACE("child " + std::to_string(i));
    matchImpl(actualInputs[i], expectedInputs[i]);
    AXIOM_RETURN_IF_FAILURE
  }
}

void matchFieldAccess(
    const std::string& actualName,
    const TypedExprPtr& actualInput,
    const FieldAccessExpr& expected) {
  EXPECT_FALSE(expected.isRootColumn()) << "Expected non-root FieldAccessExpr.";
  AXIOM_RETURN_IF_FAILURE

  EXPECT_EQ(actualName, expected.name());
  AXIOM_RETURN_IF_FAILURE

  matchImpl(actualInput, expected.inputs()[0]);
}

void matchSubscript(
    const DereferenceTypedExpr& actual,
    const CallExpr& expected) {
  EXPECT_EQ(2, expected.inputs().size());
  AXIOM_RETURN_IF_FAILURE

  EXPECT_TRUE(expected.inputs()[1]->is(IExpr::Kind::kConstant))
      << "Expected constant subscript index.";
  AXIOM_RETURN_IF_FAILURE

  const auto* indexExpr = expected.inputs()[1]->as<ConstantExpr>();
  int64_t expectedIndex = 0;
  if (indexExpr->type()->kind() == TypeKind::INTEGER) {
    expectedIndex = indexExpr->value().value<TypeKind::INTEGER>();
  } else if (indexExpr->type()->kind() == TypeKind::BIGINT) {
    expectedIndex = indexExpr->value().value<TypeKind::BIGINT>();
  } else {
    FAIL() << "Expected integer subscript index, got "
           << indexExpr->type()->toString() << ".";
    return;
  }

  EXPECT_EQ(static_cast<int64_t>(actual.index() + 1), expectedIndex)
      << "Subscript index mismatch (0-based vs 1-based).";
  AXIOM_RETURN_IF_FAILURE

  matchImpl(actual.inputs()[0], expected.inputs()[0]);
}

bool matchImpl(const TypedExprPtr& actual, const ExprPtr& expected) {
  VELOX_CHECK_NOT_NULL(actual);
  VELOX_CHECK_NOT_NULL(expected);

  if (isWildcard(expected)) {
    return true;
  }

  SCOPED_TRACE(
      "actual: '" + actual->toString() + "', expected: '" +
      expected->toString() + "'");

  // InputTypedExpr.
  if (actual->isInputKind()) {
    EXPECT_TRUE(expected->is(IExpr::Kind::kInput)) << "Expected InputExpr.";
    return !::testing::Test::HasNonfatalFailure();
  }

  // FieldAccessTypedExpr.
  if (actual->isFieldAccessKind()) {
    const auto* field = actual->asUnchecked<FieldAccessTypedExpr>();

    if (field->isInputColumn()) {
      EXPECT_TRUE(expected->is(IExpr::Kind::kFieldAccess))
          << "Expected FieldAccessExpr.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      const auto* expectedField = expected->as<FieldAccessExpr>();
      EXPECT_TRUE(expectedField->isRootColumn()) << "Expected root column.";
      AXIOM_RETURN_FALSE_IF_FAILURE

      EXPECT_EQ(field->name(), expectedField->name());
      return !::testing::Test::HasNonfatalFailure();
    }

    // Struct field access by name.
    EXPECT_TRUE(expected->is(IExpr::Kind::kFieldAccess))
        << "Expected FieldAccessExpr for struct field.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    VELOX_CHECK_EQ(field->inputs().size(), 1);
    matchFieldAccess(
        field->name(), field->inputs()[0], *expected->as<FieldAccessExpr>());
    return !::testing::Test::HasNonfatalFailure();
  }

  // DereferenceTypedExpr.
  if (actual->isDereferenceKind()) {
    const auto* deref = actual->asUnchecked<DereferenceTypedExpr>();
    VELOX_CHECK_EQ(deref->inputs().size(), 1);

    if (expected->is(IExpr::Kind::kFieldAccess)) {
      matchFieldAccess(
          deref->name(), deref->inputs()[0], *expected->as<FieldAccessExpr>());
      return !::testing::Test::HasNonfatalFailure();
    }

    if (expected->is(IExpr::Kind::kCall)) {
      const auto* expectedCall = expected->as<CallExpr>();
      if (expectedCall->name() == "subscript") {
        matchSubscript(*deref, *expectedCall);
        return !::testing::Test::HasNonfatalFailure();
      }
    }

    ADD_FAILURE() << "Expected FieldAccessExpr or subscript() for dereference.";
    return false;
  }

  // CallTypedExpr.
  if (actual->isCallKind()) {
    const auto* call = actual->asUnchecked<CallTypedExpr>();

    EXPECT_TRUE(expected->is(IExpr::Kind::kCall)) << "Expected CallExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    const auto* expectedCall = expected->as<CallExpr>();
    EXPECT_EQ(call->name(), expectedCall->name());
    AXIOM_RETURN_FALSE_IF_FAILURE

    // A 2-arg `if(cond, then)` is semantically `if(cond, then, null)`, so it
    // matches a plan's 3-arg `if` with a typed-null else.
    if (call->name() == "if" && call->inputs().size() == 3 &&
        expectedCall->inputs().size() == 2 &&
        isNullConstant(call->inputs()[2])) {
      matchChildren(
          {call->inputs()[0], call->inputs()[1]}, expectedCall->inputs());
      return !::testing::Test::HasNonfatalFailure();
    }

    matchChildren(call->inputs(), expectedCall->inputs());
    return !::testing::Test::HasNonfatalFailure();
  }

  // ConstantTypedExpr.
  if (actual->isConstantKind()) {
    EXPECT_TRUE(expected->is(IExpr::Kind::kConstant))
        << "Expected ConstantExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    matchConstant(
        *actual->asUnchecked<ConstantTypedExpr>(),
        *expected->as<ConstantExpr>());
    return !::testing::Test::HasNonfatalFailure();
  }

  // CastTypedExpr.
  if (actual->isCastKind()) {
    const auto* cast = actual->asUnchecked<CastTypedExpr>();

    EXPECT_TRUE(expected->is(IExpr::Kind::kCast)) << "Expected CastExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    const auto* expectedCast = expected->as<CastExpr>();
    EXPECT_EQ(cast->isTryCast(), expectedCast->isTryCast());
    AXIOM_RETURN_FALSE_IF_FAILURE

    EXPECT_TRUE(cast->type()->equivalent(*expectedCast->type()))
        << "Cast target type: actual " << cast->type()->toString()
        << ", expected " << expectedCast->type()->toString() << ".";
    AXIOM_RETURN_FALSE_IF_FAILURE

    VELOX_CHECK_EQ(cast->inputs().size(), 1);
    matchImpl(cast->inputs()[0], expectedCast->input());
    return !::testing::Test::HasNonfatalFailure();
  }

  // ConcatTypedExpr.
  if (actual->isConcatKind()) {
    EXPECT_TRUE(expected->is(IExpr::Kind::kCall))
        << "Expected row_constructor CallExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    const auto* expectedCall = expected->as<CallExpr>();
    EXPECT_EQ("row_constructor", expectedCall->name());
    AXIOM_RETURN_FALSE_IF_FAILURE

    matchChildren(actual->inputs(), expectedCall->inputs());
    return !::testing::Test::HasNonfatalFailure();
  }

  // LambdaTypedExpr.
  if (actual->isLambdaKind()) {
    const auto* lambda = actual->asUnchecked<LambdaTypedExpr>();

    EXPECT_TRUE(expected->is(IExpr::Kind::kLambda)) << "Expected LambdaExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    const auto* expectedLambda = expected->as<LambdaExpr>();
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

  // NullIfTypedExpr.
  if (actual->isNullIfKind()) {
    EXPECT_TRUE(expected->is(IExpr::Kind::kCall))
        << "Expected nullif CallExpr.";
    AXIOM_RETURN_FALSE_IF_FAILURE

    const auto* expectedCall = expected->as<CallExpr>();
    EXPECT_EQ("nullif", expectedCall->name());
    AXIOM_RETURN_FALSE_IF_FAILURE

    matchChildren(actual->inputs(), expectedCall->inputs());
    return !::testing::Test::HasNonfatalFailure();
  }

  ADD_FAILURE() << "Unsupported typed expression kind.";
  return false;
}

#undef AXIOM_RETURN_IF_FAILURE
#undef AXIOM_RETURN_FALSE_IF_FAILURE

} // namespace

bool ExprMatcher::match(const TypedExprPtr& actual, const ExprPtr& expected) {
  return matchImpl(actual, expected);
}

} // namespace facebook::velox::core
