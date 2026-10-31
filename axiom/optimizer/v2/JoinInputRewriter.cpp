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

#include "axiom/optimizer/v2/JoinInputRewriter.h"

#include <algorithm>

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/ExprFactory.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// The depth of complex types in 'type': 0 for a primitive, 1 for an array of
// primitives.
int32_t nesting(const velox::Type& type) {
  int32_t childNesting = -1;
  for (uint32_t i = 0; i < type.size(); ++i) {
    childNesting = std::max(childNesting, nesting(*type.childAt(i)));
  }
  return childNesting + 1;
}

// True when 'expr' produces a value no wider than what it reads: a fixed-width
// primitive, or a value no more nested than a complex column it reads.
bool isNarrowing(ExprCP expr) {
  const velox::Type& type = *expr->value().type;
  if (type.isPrimitiveType() && type.isFixedWidth()) {
    return true;
  }
  int32_t inputNesting = 0;
  expr->columns().forEach<Column>([&](ColumnCP column) {
    inputNesting = std::max(inputNesting, nesting(*column->value().type));
  });
  return inputNesting > 0 && nesting(type) <= inputNesting;
}

} // namespace

ExprVector JoinInputRewriter::rewrite(const ExprVector& exprs) {
  ExprVector result;
  result.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    result.push_back(rewrite(expr));
  }
  return result;
}

ExprCP JoinInputRewriter::rewrite(ExprCP expr) {
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
    case PlanType::kLambdaExpr:
      // An aggregate's lambda argument stays with the aggregate, as a call's
      // stays with the call.
      keepAll(expr);
      return expr;
    default:
      VELOX_UNREACHABLE(
          "Unexpected expression over a join: {}", expr->toString());
  }
}

ExprCP JoinInputRewriter::rewriteCall(const Call* call) {
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

ExprCP JoinInputRewriter::rewriteField(const Field* field) {
  if (ExprCP column = tryPrecompute(field)) {
    return column;
  }
  ExprCP newBase = rewrite(field->base());
  if (newBase == field->base()) {
    return field;
  }
  return ExprFactory(builder_).rebuildField(field, newBase);
}

ExprCP JoinInputRewriter::tryPrecompute(ExprCP expr) {
  // A non-deterministic expression has to produce a new value per evaluation.
  if (expr->containsNonDeterministic()) {
    return nullptr;
  }
  // A constant is the same for every row; projecting it would only add a
  // column.
  if (expr->columns().empty()) {
    return nullptr;
  }
  if (onlyNarrowing_ && !isNarrowing(expr)) {
    return nullptr;
  }
  for (const Input& input : inputs_) {
    if (input.columns->containsColumns(expr)) {
      ExprCP column = input.precompute->toColumn(expr);
      moved_.addIfAbsent(expr, column);
      return column;
    }
  }
  // 'expr' reads more than one input, or a column no input produces.
  return nullptr;
}

void JoinInputRewriter::keep(ColumnCP column) {
  for (const Input& input : inputs_) {
    if (input.columns->contains(column)) {
      input.precompute->toColumn(column);
      return;
    }
  }
}

} // namespace facebook::axiom::optimizer::v2
