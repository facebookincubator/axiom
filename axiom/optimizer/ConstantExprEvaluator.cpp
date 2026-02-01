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

#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {
namespace {

class ExprTranslatorContext : public lp::ExprVisitorContext {
 public:
  velox::core::TypedExprPtr veloxExpr;
};

class ExprTranslator : public lp::ExprVisitor {
 public:
  void visit(
      const lp::InputReferenceExpr& expr,
      lp::ExprVisitorContext& context) const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    myCtx.veloxExpr = std::make_shared<velox::core::FieldAccessTypedExpr>(
        expr.type(), expr.name());
  }

  void visit(const lp::CallExpr& expr, lp::ExprVisitorContext& context) const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    auto inputs = makeTypedInputs(expr.inputs(), context);

    myCtx.veloxExpr = std::make_shared<velox::core::CallTypedExpr>(
        expr.type(), inputs, expr.name());
  }

  void visit(const lp::SpecialFormExpr& expr, lp::ExprVisitorContext& context)
      const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    auto inputs = makeTypedInputs(expr.inputs(), context);

    switch (expr.form()) {
      case lp::SpecialForm::kCast:
      case lp::SpecialForm::kTryCast: {
        const bool isTryCast = expr.form() == lp::SpecialForm::kTryCast;
        myCtx.veloxExpr = std::make_shared<velox::core::CastTypedExpr>(
            expr.type(), std::move(inputs), isTryCast);
      } break;
      case lp::SpecialForm::kExists:
      case lp::SpecialForm::kStar:
        VELOX_NYI();
      default:
        myCtx.veloxExpr = std::make_shared<velox::core::CallTypedExpr>(
            expr.type(), std::move(inputs), specialForm(expr.form()));
    }
  }

  void visit(const lp::AggregateExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::WindowExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::ConstantExpr& expr, lp::ExprVisitorContext& context)
      const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    myCtx.veloxExpr = std::make_shared<velox::core::ConstantTypedExpr>(
        expr.type(), *expr.value());
  }

  void visit(const lp::LambdaExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::SubqueryExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

 private:
  std::vector<velox::core::TypedExprPtr> makeTypedInputs(
      const std::vector<lp::ExprPtr>& inputs,
      lp::ExprVisitorContext& context) const {
    std::vector<velox::core::TypedExprPtr> veloxInputs;
    veloxInputs.reserve(inputs.size());
    for (const auto& input : inputs) {
      input->accept(*this, context);
      auto& myCtx = static_cast<ExprTranslatorContext&>(context);
      veloxInputs.push_back(myCtx.veloxExpr);
    }
    return veloxInputs;
  }
};

} // namespace

// static
velox::Variant ConstantExprEvaluator::evaluateConstantExpr(
    const lp::Expr& expr) {
  ExprTranslatorContext context;
  ExprTranslator translator;
  expr.accept(translator, context);

  auto result = velox::exec::tryEvaluateConstantExpression(
      context.veloxExpr,
      &velox::memory::deprecatedSharedLeafPool(),
      velox::core::QueryCtx::create());

  VELOX_CHECK_NOT_NULL(result);

  return result->variantAt(0);
}
} // namespace facebook::axiom::optimizer
