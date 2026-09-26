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

#include "axiom/optimizer/v2/PrecomputeProjections.h"

#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"

namespace facebook::axiom::optimizer::v2 {

void PrecomputeProjections::inlineInputProject(
    NodeCP& input,
    ExprVector& exprs,
    Builder& builder) {
  if (input->is(NodeType::kProject)) {
    const auto* child = input->as<Project>();
    bool foldable = child->isDeterministic();
    if (!foldable) {
      PlanObjectSet read;
      foldable = true;
      for (ExprCP expr : exprs) {
        if (!expr->is(PlanType::kColumnExpr) || read.contains(expr)) {
          foldable = false;
          break;
        }
        read.add(expr);
      }
    }
    if (foldable) {
      exprs = ExprFactory(builder).substitute(
          exprs, child->outputColumns(), child->exprs());
      input = child->input();
    }
  }
}

NodeCP PrecomputeProjections::makeProject(
    NodeCP input,
    ExprVector exprs,
    ColumnVector outColumns,
    Builder& builder,
    ExprSimplifier& simplifier) {
  inlineInputProject(input, exprs, builder);
  for (ExprCP& expr : exprs) {
    expr = simplifier.simplify(expr);
  }
  return builder.make<Project>(
      {input, std::move(exprs), std::move(outColumns)});
}

std::pair<NodeCP, ExprVector> PrecomputeProjections::materializeKeys(
    NodeCP input,
    const ExprVector& keys,
    Builder& builder,
    ExprSimplifier& simplifier,
    const ColumnVector& aliases) {
  if (!aliases.empty()) {
    VELOX_CHECK_EQ(aliases.size(), keys.size());
  }
  PrecomputeProjections precompute{input, builder, simplifier};
  ExprVector columnKeys;
  columnKeys.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    columnKeys.push_back(
        precompute.toColumn(keys[i], aliases.empty() ? nullptr : aliases[i]));
  }
  return {std::move(precompute).node(), std::move(columnKeys)};
}

PrecomputeProjections::PrecomputeProjections(
    NodeCP input,
    Builder& builder,
    ExprSimplifier& simplifier,
    bool projectAllInputs)
    : input_(input),
      builder_(builder),
      simplifier_(simplifier),
      projectAllInputs_(projectAllInputs) {
  if (!projectAllInputs_) {
    return;
  }
  const auto& inputColumns = input->outputColumns();
  outColumns_.reserve(inputColumns.size());
  outExprs_.reserve(inputColumns.size());
  for (ColumnCP column : inputColumns) {
    addToProject(column, column);
  }
}

ExprCP PrecomputeProjections::toColumn(
    ExprCP expr,
    ColumnCP alias,
    bool allowConstant) {
  if (allowConstant && expr->is(PlanType::kLiteralExpr)) {
    return expr;
  }

  if (expr->is(PlanType::kColumnExpr)) {
    const auto* column = expr->as<Column>();
    if (alias != nullptr && alias->outputName() != column->outputName()) {
      if (auto it = seen_.find(expr); it != seen_.end()) {
        if (it->second == alias) {
          return alias;
        }
        seen_.erase(it);
      }
      addToProject(expr, alias);
      needsProject_ = true;
      return alias;
    }

    // In narrowing mode the project is not seeded with the input columns, so a
    // referenced passthrough column must be added explicitly. This is not a
    // lifted expression, so it does not by itself require a project.
    if (!projectAllInputs_ && !seen_.contains(expr)) {
      addToProject(expr, column);
    }
    return expr;
  }

  // Lambdas are consumed by their parent higher-order function directly
  // and cannot be evaluated by a Project node.
  if (expr->is(PlanType::kLambdaExpr)) {
    return expr;
  }

  if (auto it = seen_.find(expr); it != seen_.end()) {
    return it->second;
  }

  // The input may already compute this expression -- a name the query
  // projects, lifted here for an operator that reads the same value. Read that
  // column instead of computing it a second time.
  if (alias == nullptr && input_->is(NodeType::kProject)) {
    const auto* project = input_->as<Project>();
    for (size_t i = 0; i < project->exprs().size(); ++i) {
      if (project->exprs()[i] == expr) {
        ColumnCP column = project->outputColumns()[i];
        if (!projectAllInputs_ && !seen_.contains(column)) {
          addToProject(column, column);
        }
        seen_.emplace(expr, column);
        return column;
      }
    }
  }

  if (alias != nullptr) {
    addToProject(expr, alias);
    needsProject_ = true;
    return alias;
  }

  ColumnCP column = Column::create("__p", expr->value());
  addToProject(expr, column);
  needsProject_ = true;
  return column;
}

NodeCP PrecomputeProjections::node() && {
  if (!needsProject_) {
    return input_;
  }
  return PrecomputeProjections::makeProject(
      input_,
      std::move(outExprs_),
      std::move(outColumns_),
      builder_,
      simplifier_);
}

void PrecomputeProjections::addToProject(ExprCP expr, ColumnCP column) {
  VELOX_DCHECK(!seen_.contains(expr));
  seen_.emplace(expr, column);
  outColumns_.emplace_back(column);
  outExprs_.emplace_back(expr);
}

} // namespace facebook::axiom::optimizer::v2
