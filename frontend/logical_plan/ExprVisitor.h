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
#pragma once

#include "logical_plan/Expr.h" //@manual

namespace facebook::velox::logical_plan {

class ExprVisitorContext {
 public:
  virtual ~ExprVisitorContext() = default;
};

class ExprVisitor {
 public:
  virtual ~ExprVisitor() = default;

  virtual void visit(
      const InputReferenceExpr& expr,
      ExprVisitorContext& context) const = 0;

  virtual void visit(const CallExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const SpecialFormExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const AggregateExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const WindowExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const ConstantExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const LambdaExpr& expr, ExprVisitorContext& context)
      const = 0;

  virtual void visit(const SubqueryExpr& expr, ExprVisitorContext& context)
      const = 0;

 protected:
  void visitInputs(const Expr& expr, ExprVisitorContext& ctx) const {
    for (const auto& input : expr.inputs()) {
      input->accept(*this, ctx);
    }
  }
};

} // namespace facebook::velox::logical_plan
