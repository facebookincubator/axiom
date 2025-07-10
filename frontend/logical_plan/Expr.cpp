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

#include "velox/frontend/logical_plan/Expr.h"
#include <boost/algorithm/string.hpp>
#include "velox/frontend/logical_plan/ExprVisitor.h"
#include "velox/frontend/logical_plan/LogicalPlanNode.h"

namespace facebook::velox::logical_plan {

void InputReferenceExpr::accept(
    const ExprVisitor& visitor,
    ExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

void ConstantExpr::accept(
    const ExprVisitor& visitor,
    ExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

void CallExpr::accept(const ExprVisitor& visitor, ExprVisitorContext& context)
    const {
  visitor.visit(*this, context);
}

void SpecialFormExpr::accept(
    const ExprVisitor& visitor,
    ExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

void AggregateExpr::accept(
    const ExprVisitor& visitor,
    ExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

void WindowExpr::accept(const ExprVisitor& visitor, ExprVisitorContext& context)
    const {
  visitor.visit(*this, context);
}

void LambdaExpr::accept(const ExprVisitor& visitor, ExprVisitorContext& context)
    const {
  visitor.visit(*this, context);
}

void SubqueryExpr::accept(
    const ExprVisitor& visitor,
    ExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
const SortOrder SortOrder::kAscNullsFirst{true, true};

// static
const SortOrder SortOrder::kAscNullsLast{true, false};

// static
const SortOrder SortOrder::kDescNullsFirst{false, true};

// static
const SortOrder SortOrder::kDescNullsLast{false, false};

namespace {
folly::F14FastMap<SpecialForm, std::string> specialFormNames() {
  return {
      {SpecialForm::kAnd, "AND"},
      {SpecialForm::kOr, "OR"},
      {SpecialForm::kCast, "CAST"},
      {SpecialForm::kTryCast, "TRY_CAST"},
      {SpecialForm::kTry, "TRY"},
      {SpecialForm::kDereference, "DEREFERENCE"},
      {SpecialForm::kCoalesce, "COALESCE"},
      {SpecialForm::kIf, "IF"},
      {SpecialForm::kSwitch, "SWITCH"},
  };
}
} // namespace

VELOX_DEFINE_ENUM_NAME(SpecialForm, specialFormNames)

namespace {
void validateDereferenceInputs(
    const TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_EQ(
      inputs.size(), 2, "DEREFERENCE must have exactly two inputs");

  VELOX_USER_CHECK_EQ(
      inputs.at(1)->type()->kind(),
      TypeKind::VARCHAR,
      "Second input to DEREFERENCE must be a constant string");

  VELOX_USER_CHECK(
      inputs.at(1)->isConstant(),
      "Second input to DEREFERENCE must be a constant string");

  const auto* fieldNameExpr = inputs.at(1)->asUnchecked<ConstantExpr>();
  VELOX_USER_CHECK(
      !fieldNameExpr->isNull(), "Second input to DEREFERENCE must not be null");

  const auto& fieldName = fieldNameExpr->value().value<TypeKind::VARCHAR>();
  VELOX_USER_CHECK(
      !fieldName.empty(),
      "Second input to DEREFERENCE must not be emtpy string");

  VELOX_USER_CHECK(
      inputs.at(0)->type()->isRow(),
      "First input to DEREFERENCE must not be a struct");

  const auto& rowType = inputs.at(0)->type()->asRow();
  const auto index = rowType.getChildIdxIfExists(fieldName);
  VELOX_USER_CHECK(
      index.has_value(),
      "Field name specified in DEREFERENCE is not found in the struct: {} not in {}",
      fieldName,
      folly::join(", ", rowType.names()));

  VELOX_USER_CHECK(
      type->equivalent(*rowType.childAt(index.value())),
      "Result type of DEREFERENCE must be the same as the type of the field: {} vs {}",
      type->toString(),
      rowType.childAt(index.value())->toString());
}

void validateIfInputs(const TypePtr& type, const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_GE(
      inputs.size(), 2, "IF must have exactly either two or three inputs");
  VELOX_USER_CHECK_LE(
      inputs.size(), 3, "IF must have exactly either two or three inputs");

  VELOX_USER_CHECK_EQ(
      inputs.at(0)->type()->kind(),
      TypeKind::BOOLEAN,
      "First input to IF must be boolean");

  const auto& thenType = inputs.at(1)->type();
  VELOX_USER_CHECK(
      type->equivalent(*thenType),
      "Second input to IF must have the same type as the result of the expression: {} vs {}",
      type->toString(),
      thenType->toString());

  if (inputs.size() == 3) {
    const auto& elseType = inputs.at(2)->type();
    VELOX_USER_CHECK(
        type->equivalent(*elseType),
        "Third input to IF must have the same type as the result of the expression: {} vs {}",
        type->toString(),
        elseType->toString());
  }
}

void validateSwitchInputs(
    const TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_GE(inputs.size(), 2, "SWITCH must have at least two inputs");

  const auto numCases = inputs.size() / 2;

  for (auto i = 0; i < numCases; ++i) {
    const auto& condition = inputs.at(2 * i);
    VELOX_USER_CHECK_EQ(
        condition->type()->kind(),
        TypeKind::BOOLEAN,
        "SWITCH conditions must be boolean");

    const auto& thenClause = inputs.at(2 * i + 1);

    VELOX_USER_CHECK(
        type->equivalent(*thenClause->type()),
        "Then clauses in SWITCH must have the same type as the output of the SWITCH: {} vs. {}",
        type->toString(),
        thenClause->type()->toString());
  }

  if (inputs.size() % 2 == 1) {
    const auto& elseClause = inputs.back();
    VELOX_USER_CHECK(
        type->equivalent(*elseClause->type()),
        "Else clause in SWITCH must have the same type as the output of the SWITCH: {} vs. {}",
        type->toString(),
        elseClause->type()->toString());
  }
}
} // namespace

SpecialFormExpr::SpecialFormExpr(
    const TypePtr& type,
    SpecialForm form,
    const std::vector<ExprPtr>& inputs)
    : Expr(ExprKind::kSpecialForm, type, inputs), form_{form} {
  switch (form) {
    case SpecialForm::kAnd:
    case SpecialForm::kOr:
      VELOX_USER_CHECK_GE(
          inputs.size(),
          2,
          "{} must have at least two inputs",
          SpecialFormName::toName(form));

      for (auto& input : inputs) {
        VELOX_USER_CHECK_EQ(
            input->type()->kind(),
            TypeKind::BOOLEAN,
            "All inputs to AND and OR must be boolean");
      }
      break;
    case SpecialForm::kCast:
    case SpecialForm::kTryCast:
    case SpecialForm::kTry:
      VELOX_USER_CHECK_EQ(
          inputs.size(),
          1,
          "{} must have exactly one input",
          SpecialFormName::toName(form));
      break;
    case SpecialForm::kDereference:
      validateDereferenceInputs(type, inputs);
      break;
    case SpecialForm::kCoalesce: {
      VELOX_USER_CHECK_GE(
          inputs.size(), 2, "COALESCE must have at least two inputs");

      const auto& firstType = inputs.at(0)->type();
      for (auto& input : inputs) {
        VELOX_USER_CHECK(
            firstType->equivalent(*input->type()),
            "All inputs to COALESCE must have the same type: {} vs {}",
            firstType->toString(),
            input->type()->toString());
      }
      break;
    }
    case SpecialForm::kIf:
      validateIfInputs(type, inputs);
      break;
    case SpecialForm::kSwitch:
      validateSwitchInputs(type, inputs);
      break;
  }
}

CallExpr::CallExpr(
    const TypePtr& type,
    const std::string& name,
    const std::vector<ExprPtr>& inputs)
    : Expr(ExprKind::kCall, type, inputs), name_{name} {
  VELOX_USER_CHECK(!name.empty());

  static const auto kReservedNames = Enums::invertMap(specialFormNames());

  VELOX_USER_CHECK(
      !kReservedNames.contains(boost::algorithm::to_upper_copy(name)),
      "Function name cannot match special for name: {}",
      name);
}

namespace {
folly::F14FastMap<WindowExpr::WindowType, std::string> windowTypeNames() {
  return {
      {WindowExpr::WindowType::kRows, "ROWS"},
      {WindowExpr::WindowType::kRange, "RANGE"},
  };
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowExpr, WindowType, windowTypeNames)

namespace {
folly::F14FastMap<WindowExpr::BoundType, std::string> boundTypeNames() {
  return {
      {WindowExpr::BoundType::kCurrentRow, "CURRENT ROW"},
      {WindowExpr::BoundType::kPreceding, "PRECEDING"},
      {WindowExpr::BoundType::kFollowing, "FOLLOWING"},
      {WindowExpr::BoundType::kUnboundedPreceding, "UNBOUNDED PRECEDING"},
      {WindowExpr::BoundType::kUnboundedFollowing, "UNBOUNDED FOLLOWING"},
  };
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowExpr, BoundType, boundTypeNames)

SubqueryExpr::SubqueryExpr(const LogicalPlanNodePtr& subquery)
    : Expr(ExprKind::kSubquery, subquery->outputType()->childAt(0), {}),
      subquery_{subquery} {
  VELOX_USER_CHECK_EQ(
      1,
      subquery->outputType()->size(),
      "Scalar subquery must produce exactly one column");
}

} // namespace facebook::velox::logical_plan
