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

#include "axiom/logical_plan/Utils.h"
#include <velox/common/base/Exceptions.h>

namespace facebook::axiom::logical_plan {

namespace {

/// Recursive visitor that automatically visits all children of expressions.
class RecursiveExprVisitor : public ExprVisitor {
 public:
  void visit(std::span<const ExprPtr> exprs, ExprVisitorContext& ctx) const {
    for (const auto& expr : exprs) {
      visit(expr, ctx);
    }
  }

  void visit(
      std::span<const SortingField> sortingFields,
      ExprVisitorContext& ctx) const {
    for (const auto& sortField : sortingFields) {
      visit(sortField.expression, ctx);
    }
  }

 private:
  void visit(const InputReferenceExpr& expr, ExprVisitorContext& context)
      const override {
    // Leaf node - no children to visit
  }

  void visit(const ConstantExpr& expr, ExprVisitorContext& context)
      const override {
    // Leaf node - no children to visit
  }

  void visit(const CallExpr& expr, ExprVisitorContext& context) const override {
    visitInputs(expr, context);
  }

  void visit(const SpecialFormExpr& expr, ExprVisitorContext& context)
      const override {
    visitInputs(expr, context);
  }

  void visit(const AggregateExpr& expr, ExprVisitorContext& context)
      const override {
    visitInputs(expr, context);
    visit(expr.filter(), context);
    visit(expr.ordering(), context);
  }

  void visit(const WindowExpr& expr, ExprVisitorContext& context)
      const override {
    visitInputs(expr, context);
    visit(expr.partitionKeys(), context);
    visit(expr.ordering(), context);

    const auto& frame = expr.frame();
    visit(frame.startValue, context);
    visit(frame.endValue, context);
  }

  void visit(const LambdaExpr& expr, ExprVisitorContext& context)
      const override {
    visit(expr.body(), context);
  }

  void visit(const SubqueryExpr& expr, ExprVisitorContext& context)
      const override {
    // All usages of the class doesn't need to visit the subquery.
  }

  void visitInputs(const Expr& expr, ExprVisitorContext& ctx) const {
    for (const auto& input : expr.inputs()) {
      visit(input, ctx);
    }
  }

  void visit(const ExprPtr& expr, ExprVisitorContext& ctx) const {
    if (!expr) {
      return;
    }

    auto& castedCtx = static_cast<RecursiveExprVisitorContext&>(ctx);
    if (castedCtx.preExprVisitor && !castedCtx.preExprVisitor(*expr)) {
      return;
    }
    expr->accept(*this, ctx);
    if (castedCtx.postExprVisitor) {
      castedCtx.postExprVisitor(*expr);
    }
  }
};

} // namespace

void visitExprsRecursively(
    std::span<const ExprPtr> exprs,
    RecursiveExprVisitorContext& ctx) {
  RecursiveExprVisitor().visit(exprs, ctx);
}

void visitExprsRecursively(
    std::span<const SortingField> exprs,
    RecursiveExprVisitorContext& ctx) {
  RecursiveExprVisitor().visit(exprs, ctx);
}

} // namespace facebook::axiom::logical_plan
