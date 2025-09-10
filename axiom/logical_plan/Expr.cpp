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

#include <boost/algorithm/string.hpp>

#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/logical_plan/LogicalPlanNode.h"

namespace facebook::axiom::logical_plan {

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
const auto& specialFormNames() {
  static const folly::F14FastMap<SpecialForm, std::string_view> kNames = {
      {SpecialForm::kAnd, "AND"},
      {SpecialForm::kOr, "OR"},
      {SpecialForm::kCast, "CAST"},
      {SpecialForm::kTryCast, "TRY_CAST"},
      {SpecialForm::kTry, "TRY"},
      {SpecialForm::kDereference, "DEREFERENCE"},
      {SpecialForm::kCoalesce, "COALESCE"},
      {SpecialForm::kIf, "IF"},
      {SpecialForm::kSwitch, "SWITCH"},
      {SpecialForm::kStar, "STAR"},
      {SpecialForm::kIn, "IN"},
      {SpecialForm::kExists, "EXISTS"},
  };
  return kNames;
}
} // namespace

VELOX_DEFINE_ENUM_NAME(SpecialForm, specialFormNames)

namespace {
void validateDereferenceInputs(
    const velox::TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_EQ(
      inputs.size(), 2, "DEREFERENCE must have exactly two inputs");

  VELOX_USER_CHECK(
      inputs[0]->type()->isRow(),
      "First input to DEREFERENCE must be a struct");

  const auto& rowType = inputs[0]->type()->asRow();

  VELOX_USER_CHECK(
      inputs[1]->type()->kind() == velox::TypeKind::VARCHAR ||
          inputs[1]->type()->kind() == velox::TypeKind::INTEGER,
      "Second input to DEREFERENCE must be a constant string or integer. Got: {}",
      inputs[1]->type()->toString());

  VELOX_USER_CHECK(
      inputs[1]->isConstant(),
      "Second input to DEREFERENCE must be a constant");

  const auto* fieldExpr = inputs[1]->asUnchecked<ConstantExpr>();
  VELOX_USER_CHECK(
      !fieldExpr->isNull(), "Second input to DEREFERENCE must not be null");

  if (fieldExpr->type()->kind() == velox::TypeKind::VARCHAR) {
    const auto& fieldName =
        fieldExpr->value()->value<velox::TypeKind::VARCHAR>();
    VELOX_USER_CHECK(
        !fieldName.empty(),
        "Second input to DEREFERENCE must not be emtpy string");

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
  } else {
    const auto index = fieldExpr->value()->value<int32_t>();

    VELOX_USER_CHECK_GE(index, 0, "Field index must be >= 0");
    VELOX_USER_CHECK_LT(
        index, rowType.size(), "Field index must be < size of the row type");

    VELOX_USER_CHECK(
        type->equivalent(*rowType.childAt(index)),
        "Result type of DEREFERENCE must be the same as the type of the field: {} vs {}",
        type->toString(),
        rowType.childAt(index)->toString());
  }
}

void validateIfInputs(
    const velox::TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_GE(
      inputs.size(), 2, "IF must have exactly either two or three inputs");
  VELOX_USER_CHECK_LE(
      inputs.size(), 3, "IF must have exactly either two or three inputs");

  VELOX_USER_CHECK_EQ(
      inputs[0]->type()->kind(),
      velox::TypeKind::BOOLEAN,
      "First input to IF must be boolean");

  const auto& thenType = inputs[1]->type();
  VELOX_USER_CHECK(
      type->equivalent(*thenType),
      "Second input to IF must have the same type as the result of the expression: {} vs {}",
      type->toString(),
      thenType->toString());

  if (inputs.size() == 3) {
    const auto& elseType = inputs[2]->type();
    VELOX_USER_CHECK(
        type->equivalent(*elseType),
        "Third input to IF must have the same type as the result of the expression: {} vs {}",
        type->toString(),
        elseType->toString());
  }
}

void validateSwitchInputs(
    const velox::TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_GE(inputs.size(), 2, "SWITCH must have at least two inputs");

  const auto numCases = inputs.size() / 2;

  for (size_t i = 0; i < numCases; ++i) {
    const auto& condition = inputs[2 * i];
    VELOX_USER_CHECK_EQ(
        condition->type()->kind(),
        velox::TypeKind::BOOLEAN,
        "SWITCH conditions must be boolean");

    const auto& thenClause = inputs[2 * i + 1];

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

void validateInInputs(
    const velox::TypePtr& type,
    const std::vector<ExprPtr>& inputs) {
  VELOX_USER_CHECK_EQ(
      type->kind(),
      velox::TypeKind::BOOLEAN,
      "IN expression must return boolean type");
  VELOX_USER_CHECK_GE(inputs.size(), 2, "IN must have at least two inputs");
  if (inputs[1]->isSubquery()) {
    VELOX_USER_CHECK_EQ(inputs.size(), 2, "IN subquery must have two inputs");
    auto subquery = inputs[1]->asUnchecked<SubqueryExpr>();
    VELOX_USER_CHECK_EQ(
        subquery->subquery()->outputType()->size(),
        1,
        "Subquery must return one column");
    VELOX_USER_CHECK(
        subquery->subquery()->outputType()->childAt(0)->equivalent(
            *inputs[0]->type()),
        "IN subquery must return the same type as the left operand");
  } else {
    auto leftType = inputs[0]->type();
    for (size_t i = 1; i < inputs.size(); ++i) {
      VELOX_USER_CHECK(
          inputs[i]->type()->equivalent(*leftType),
          "All inputs to IN must have the same type as the left operand: {} vs {}",
          inputs[i]->type()->toString(),
          leftType->toString());
    }
  }
}
} // namespace

SpecialFormExpr::SpecialFormExpr(
    velox::TypePtr type,
    SpecialForm form,
    std::vector<ExprPtr> inputs)
    : Expr{ExprKind::kSpecialForm, std::move(type), std::move(inputs)},
      form_{form} {
  switch (form) {
    case SpecialForm::kAnd:
    case SpecialForm::kOr:
      VELOX_USER_CHECK_GE(
          inputs_.size(),
          2,
          "{} must have at least two inputs",
          SpecialFormName::toName(form));

      for (const auto& input : inputs_) {
        VELOX_USER_CHECK_EQ(
            input->type()->kind(),
            velox::TypeKind::BOOLEAN,
            "All inputs to AND and OR must be boolean");
      }
      break;
    case SpecialForm::kCast:
    case SpecialForm::kTryCast:
    case SpecialForm::kTry:
      VELOX_USER_CHECK_EQ(
          inputs_.size(),
          1,
          "{} must have exactly one input",
          SpecialFormName::toName(form));
      break;
    case SpecialForm::kDereference:
      validateDereferenceInputs(type_, inputs_);
      break;
    case SpecialForm::kCoalesce:
      VELOX_USER_CHECK_GE(
          inputs_.size(), 2, "COALESCE must have at least two inputs");

      for (const auto& firstType = inputs_[0]->type();
           const auto& input : inputs_) {
        VELOX_USER_CHECK(
            firstType->equivalent(*input->type()),
            "All inputs to COALESCE must have the same type: {} vs {}",
            firstType->toString(),
            input->type()->toString());
      }
      break;
    case SpecialForm::kIf:
      validateIfInputs(type_, inputs_);
      break;
    case SpecialForm::kSwitch:
      validateSwitchInputs(type_, inputs_);
      break;
    case SpecialForm::kStar:
      VELOX_USER_CHECK_GE(
          inputs_.size(), 0, "'*' expression cannot not have any inputs");
      break;
    case SpecialForm::kIn:
      validateInInputs(type_, inputs_);
      break;
    case SpecialForm::kExists:
      VELOX_USER_CHECK_EQ(inputs_.size(), 1, "EXISTS must have one input");
      VELOX_USER_CHECK(
          inputs_[0]->isSubquery(),
          "EXISTS input must be a subquery expression");
      break;
  }
}

CallExpr::CallExpr(
    velox::TypePtr type,
    std::string name,
    std::vector<ExprPtr> inputs)
    : Expr{ExprKind::kCall, std::move(type), std::move(inputs)},
      name_{std::move(name)} {
  VELOX_USER_CHECK(!name_.empty());
  VELOX_USER_CHECK(
      !SpecialFormName::tryToSpecialForm(
          boost::algorithm::to_upper_copy(name_)),
      "Function name cannot match special form name: {}",
      name_);
}

namespace {
const auto& windowTypeNames() {
  static const folly::F14FastMap<WindowExpr::WindowType, std::string_view>
      kNames = {
          {WindowExpr::WindowType::kRows, "ROWS"},
          {WindowExpr::WindowType::kRange, "RANGE"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowExpr, WindowType, windowTypeNames)

namespace {
const auto& boundTypeNames() {
  static const folly::F14FastMap<WindowExpr::BoundType, std::string_view>
      kNames = {
          {WindowExpr::BoundType::kCurrentRow, "CURRENT ROW"},
          {WindowExpr::BoundType::kPreceding, "PRECEDING"},
          {WindowExpr::BoundType::kFollowing, "FOLLOWING"},
          {WindowExpr::BoundType::kUnboundedPreceding, "UNBOUNDED PRECEDING"},
          {WindowExpr::BoundType::kUnboundedFollowing, "UNBOUNDED FOLLOWING"},
      };
  return kNames;
}
} // namespace

VELOX_DEFINE_EMBEDDED_ENUM_NAME(WindowExpr, BoundType, boundTypeNames)

SubqueryExpr::SubqueryExpr(LogicalPlanNodePtr subquery)
    : Expr{ExprKind::kSubquery, subquery->outputType()->childAt(0), {}},
      subquery_{std::move(subquery)} {
  VELOX_USER_CHECK_LE(
      1,
      subquery_->outputType()->size(),
      "Subquery must produce at least one column");
}

} // namespace facebook::axiom::logical_plan
