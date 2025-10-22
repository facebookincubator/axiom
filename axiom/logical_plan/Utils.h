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

#include <functional>
#include <span>
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprVisitor.h"

namespace facebook::axiom::logical_plan {

class RecursiveExprVisitorContext : public ExprVisitorContext {
 public:
  std::function<bool(const Expr&)>
      preExprVisitor; // Return false to not visit children
  std::function<void(const Expr&)> postExprVisitor;
};

/// Utility function to visit expressions recursively with pre and post visitor
/// callbacks.
void visitExprsRecursively(
    std::span<const ExprPtr> exprs,
    RecursiveExprVisitorContext& ctx);

void visitExprsRecursively(
    std::span<const SortingField> exprs,
    RecursiveExprVisitorContext& ctx);

} // namespace facebook::axiom::logical_plan
