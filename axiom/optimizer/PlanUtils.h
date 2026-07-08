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

#include <folly/Range.h>
#include "axiom/logical_plan/Expr.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

/// Prints a number with precision' digits followed by a scale letter (n, u, m,
/// k, M, G T, P).
std::string succinctNumber(double value, int32_t precision = 2);

/// Returns the sum of the sizes of 'exprs'.
template <typename V>
float byteSize(const V& exprs) {
  float size = 0;
  for (auto& expr : exprs) {
    size += expr->value().byteSize();
  }
  return size;
}

template <typename Target, typename V, typename Func>
Target transform(const V& set, Func func) {
  Target result;
  for (auto& elt : set) {
    result.push_back(func(elt));
  }
  return result;
}

/// Returns the integer value of 'variant'. The variant must be non-null and
/// hold an integer type. Throws otherwise.
int64_t integerValue(const velox::Variant* variant);

/// Returns the integer value of 'expr' if the type is an integer,
/// std::nullopt otherwise.
std::optional<int64_t> maybeIntegerLiteral(
    const logical_plan::ConstantExpr* expr);

/// Returns true if 'expr' points to an instance of 'form'.
bool isSpecialForm(
    const logical_plan::ExprPtr& expr,
    logical_plan::SpecialForm form);

/// Extracts field access information from a DEREFERENCE expression. Throws if
/// 'expr' is not a DEREFERENCE.
/// @param expr The DEREFERENCE expression.
/// @return A Step with kField kind, field name, and field index. For anonymous
/// structs, the field name will be an empty string.
Step extractDereferenceStep(const logical_plan::ExprPtr& expr);

/// Returns true if 'expr' is a boolean literal with the given value.
inline bool isConstantBool(ExprCP expr, bool expected) {
  if (expr->isNot(PlanType::kLiteralExpr)) {
    return false;
  }
  const auto& variant = expr->as<Literal>()->literal();
  return variant.kind() == velox::TypeKind::BOOLEAN && !variant.isNull() &&
      variant.value<bool>() == expected;
}

/// Returns true if 'expr' is a boolean literal with value true.
inline bool isConstantTrue(ExprCP expr) {
  return isConstantBool(expr, true);
}

/// Returns true if 'expr' is a boolean literal with value false.
inline bool isConstantFalse(ExprCP expr) {
  return isConstantBool(expr, false);
}

std::string conjunctsToString(const ExprVector& conjuncts);

std::string orderByToString(
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes);

std::string columnsToString(const ColumnVector& columns);

std::string exprsToString(const ExprVector& exprs);

} // namespace facebook::axiom::optimizer
