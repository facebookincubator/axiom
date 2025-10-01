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

#include "axiom/optimizer/PrecomputeProjection.h"

namespace facebook::axiom::optimizer {

PrecomputeProjection::PrecomputeProjection(
    const RelationOpPtr& input,
    DerivedTableCP dt,
    bool projectAllInputs)
    : input_{input}, dt_{dt} {
  if (projectAllInputs) {
    projectColumns_.reserve(input->columns().size());
    projectExprs_.reserve(input->columns().size());
    for (const auto* column : input->columns()) {
      addToProject(column, column);
    }
  }
}

ExprCP PrecomputeProjection::toColumn(
    ExprCP expr,
    ColumnCP alias,
    bool preserveLiterals) {
  if (preserveLiterals && expr->is(PlanType::kLiteralExpr)) {
    return expr;
  }

  auto it = projections_.find(expr);
  if (it != projections_.end()) {
    return it->second;
  }

  if (expr->is(PlanType::kColumnExpr) &&
      (alias == nullptr ||
       expr->as<Column>()->outputName() == alias->outputName())) {
    addToProject(expr, expr->as<Column>());
    return expr;
  }

  auto* column = make<Column>(
      toName(fmt::format("__p{}", expr->id())),
      dt_,
      expr->value(),
      alias != nullptr ? alias->name() : nullptr);

  addToProject(expr, column);
  needsProject_ = true;
  return column;
}

ExprVector PrecomputeProjection::toColumns(
    const ExprVector& exprs,
    const ColumnVector* aliases,
    bool preserveLiterals) {
  ExprVector columns;
  columns.reserve(exprs.size());

  for (auto i = 0; i < exprs.size(); ++i) {
    columns.emplace_back(toColumn(
        exprs[i],
        aliases != nullptr ? (*aliases)[i] : nullptr,
        preserveLiterals));
  }

  return columns;
}

void PrecomputeProjection::addToProject(ExprCP expr, ColumnCP column) {
  projectExprs_.emplace_back(expr);
  projectColumns_.emplace_back(column);
  projections_.emplace(expr, column);
}
} // namespace facebook::axiom::optimizer
