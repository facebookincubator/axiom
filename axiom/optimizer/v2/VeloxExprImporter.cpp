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

#include "axiom/optimizer/v2/VeloxExprImporter.h"

#include <folly/ScopeGuard.h>

#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::v2 {

VeloxExprImporter::VeloxExprImporter(
    Builder& builder,
    folly::F14FastMap<std::string_view, ColumnCP> columns)
    : builder_{builder}, exprFactory_{builder}, columns_{std::move(columns)} {
  for (const auto& [form, veloxName] :
       queryCtx()->functionRegistry().specialForms()) {
    specialFormNames_.emplace(
        veloxName, SpecialFormCallNames::toCallName(form));
  }
}

ExprCP VeloxExprImporter::importExpr(const velox::core::TypedExprPtr& expr) {
  switch (expr->kind()) {
    case velox::core::ExprKind::kInput: {
      const auto& inputType = expr->type()->asRow();
      ExprVector args;
      args.reserve(inputType.size());
      for (const auto& name : inputType.names()) {
        const auto it = columns_.find(name);
        VELOX_CHECK(
            it != columns_.end(),
            "Expression reads a column outside its bindings: {}",
            name);
        args.push_back(it->second);
      }
      return exprFactory_.makeRowConstructor(
          std::move(args), toType(expr->type()));
    }
    case velox::core::ExprKind::kFieldAccess: {
      const auto* field =
          expr->asUnchecked<velox::core::FieldAccessTypedExpr>();
      if (field->isInputColumn()) {
        const auto it = columns_.find(field->name());
        VELOX_CHECK(
            it != columns_.end(),
            "Expression reads a column outside its bindings: {}",
            field->name());
        return it->second;
      }
      VELOX_CHECK_EQ(field->inputs().size(), 1);
      return make<Field>(
          toType(field->type()),
          importExpr(field->inputs().front()),
          toName(field->name()));
    }
    case velox::core::ExprKind::kDereference: {
      const auto* field =
          expr->asUnchecked<velox::core::DereferenceTypedExpr>();
      VELOX_CHECK_EQ(field->inputs().size(), 1);
      return make<Field>(
          toType(field->type()),
          importExpr(field->inputs().front()),
          static_cast<int32_t>(field->index()));
    }
    case velox::core::ExprKind::kConstant: {
      const auto* constant =
          expr->asUnchecked<velox::core::ConstantTypedExpr>();
      velox::Variant value = constant->hasValueVector()
          ? constant->valueVector()->variantAt(0)
          : constant->value();
      return exprFactory_.makeLiteral(std::move(value), toType(expr->type()));
    }
    case velox::core::ExprKind::kCall: {
      const auto* call = expr->asUnchecked<velox::core::CallTypedExpr>();
      ExprVector args;
      args.reserve(call->inputs().size());
      for (const auto& input : call->inputs()) {
        args.push_back(importExpr(input));
      }
      const auto specialForm = specialFormNames_.find(call->name());
      const bool isSpecialForm = specialForm != specialFormNames_.end();
      const Name name =
          isSpecialForm ? specialForm->second : toName(call->name());
      return exprFactory_.makeCall(
          name, Value(toType(call->type())), std::move(args), isSpecialForm);
    }
    case velox::core::ExprKind::kCast: {
      const auto* cast = expr->asUnchecked<velox::core::CastTypedExpr>();
      VELOX_CHECK_EQ(cast->inputs().size(), 1);
      ExprVector args{importExpr(cast->inputs().front())};
      const Name name = cast->isTryCast() ? SpecialFormCallNames::kTryCast
                                          : SpecialFormCallNames::kCast;
      return exprFactory_.makeCall(
          name,
          Value(toType(cast->type())),
          std::move(args),
          /*specialForm=*/true);
    }
    case velox::core::ExprKind::kConcat: {
      ExprVector args;
      args.reserve(expr->inputs().size());
      for (const auto& input : expr->inputs()) {
        args.push_back(importExpr(input));
      }
      return exprFactory_.makeRowConstructor(
          std::move(args), toType(expr->type()));
    }
    case velox::core::ExprKind::kLambda: {
      const auto* lambda = expr->asUnchecked<velox::core::LambdaTypedExpr>();
      auto outerColumns = columns_;
      SCOPE_EXIT {
        columns_ = std::move(outerColumns);
      };
      ColumnVector args;
      args.reserve(lambda->signature()->size());
      for (uint32_t i = 0; i < lambda->signature()->size(); ++i) {
        const auto& name = lambda->signature()->nameOf(i);
        ColumnCP column = Column::create(
            toName(name), Value(toType(lambda->signature()->childAt(i)), 1));
        args.push_back(column);
        columns_.insert_or_assign(name, column);
      }
      return make<Lambda>(
          std::move(args), toType(lambda->type()), importExpr(lambda->body()));
    }
    case velox::core::ExprKind::kNullIf: {
      const auto* nullIf = expr->asUnchecked<velox::core::NullIfTypedExpr>();
      VELOX_CHECK_EQ(nullIf->inputs().size(), 2);
      ExprVector args{
          importExpr(nullIf->inputs()[0]),
          importExpr(nullIf->inputs()[1]),
          builder_.makeNull(toType(nullIf->commonType()))};
      return exprFactory_.makeCall(
          SpecialFormCallNames::kNullIf,
          Value(toType(nullIf->type())),
          std::move(args),
          /*specialForm=*/true);
    }
  }
  VELOX_UNREACHABLE();
}

} // namespace facebook::axiom::optimizer::v2
