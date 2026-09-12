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

#include "axiom/optimizer/v2/JoinFilterRewriter.h"

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/ExprFactory.h"

namespace facebook::axiom::optimizer::v2 {

ExprVector JoinFilterRewriter::rewrite(const ExprVector& filter) {
  ExprVector result;
  result.reserve(filter.size());
  for (ExprCP conjunct : filter) {
    result.push_back(rewrite(conjunct));
  }
  return result;
}

ExprCP JoinFilterRewriter::rewrite(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kCallExpr:
      return rewriteCall(expr->as<Call>());
    case PlanType::kFieldExpr:
      return rewriteField(expr->as<Field>());
    case PlanType::kColumnExpr:
      keep(expr->as<Column>());
      return expr;
    case PlanType::kLiteralExpr:
      // No columns to keep and nothing to compute.
      return expr;
    default:
      VELOX_UNREACHABLE(
          "Unexpected expression in a join filter: {}", expr->toString());
  }
}

ExprCP JoinFilterRewriter::rewriteCall(const Call* call) {
  if (ExprCP column = tryPrecompute(call)) {
    return column;
  }

  const Name name = call->name();

  // `try` catches the error its argument raises; an input would raise it where
  // nothing catches it.
  if (name == SpecialFormCallNames::kTry) {
    keepAll(call);
    return call;
  }

  // `if`, `switch` and `coalesce` evaluate their first argument for every row
  // and the rest only for the rows that one selects.
  if (name == SpecialFormCallNames::kIf ||
      name == SpecialFormCallNames::kSwitch ||
      name == SpecialFormCallNames::kCoalesce) {
    ExprVector newArgs = call->args();
    newArgs[0] = rewrite(newArgs[0]);
    for (size_t i = 1; i < newArgs.size(); ++i) {
      keepAll(newArgs[i]);
    }
    if (newArgs[0] == call->args()[0]) {
      return call;
    }
    return ExprFactory(builder_).rebuildCall(call, std::move(newArgs));
  }

  ExprVector newArgs;
  newArgs.reserve(call->args().size());
  bool changed = false;
  for (ExprCP arg : call->args()) {
    if (arg->is(PlanType::kLambdaExpr)) {
      // A Project cannot evaluate a Lambda on its own, so it stays with the
      // call that binds its arguments. Its columns are the outer ones the body
      // reads -- `Lambda` excludes the bound arguments from that set -- and
      // those still have to reach the join.
      keepAll(arg);
      newArgs.push_back(arg);
      continue;
    }
    ExprCP newArg = rewrite(arg);
    changed |= newArg != arg;
    newArgs.push_back(newArg);
  }
  if (!changed) {
    return call;
  }
  return ExprFactory(builder_).rebuildCall(call, std::move(newArgs));
}

ExprCP JoinFilterRewriter::rewriteField(const Field* field) {
  if (ExprCP column = tryPrecompute(field)) {
    return column;
  }
  ExprCP newBase = rewrite(field->base());
  if (newBase == field->base()) {
    return field;
  }
  return ExprFactory(builder_).rebuildField(field, newBase);
}

ExprCP JoinFilterRewriter::tryPrecompute(ExprCP expr) {
  // A non-deterministic expression has to produce a new value per pair.
  if (expr->containsNonDeterministic()) {
    return nullptr;
  }
  // A constant is the same for every row; projecting it would only add a
  // column.
  if (expr->columns().empty()) {
    return nullptr;
  }
  if (leftColumns_.containsColumns(expr)) {
    return leftPrecompute_.toColumn(expr);
  }
  if (rightColumns_.containsColumns(expr)) {
    return rightPrecompute_.toColumn(expr);
  }
  // 'expr' reads both inputs.
  return nullptr;
}

void JoinFilterRewriter::keep(ColumnCP column) {
  if (leftColumns_.contains(column)) {
    leftPrecompute_.toColumn(column);
  } else {
    VELOX_CHECK(
        rightColumns_.contains(column),
        "Join filter reads a column neither input produces: {}",
        column->toString());
    rightPrecompute_.toColumn(column);
  }
}

} // namespace facebook::axiom::optimizer::v2
