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

#include "axiom/optimizer/v2/Builder.h"

namespace facebook::axiom::optimizer::v2 {

/// Constructors for the recurring `Call` expression shapes that
/// translate, decorrelate, and downstream rewrites need. Holds a
/// `Builder&` so hash-cons construction goes through the right cache;
/// construct one per logical "rewrite context" (typically once per
/// pass / per `Translator`).
///
/// Column synthesis lives on the `Column` class itself
/// (`Column::create(prefix, value)`) since `Column` is arena-allocated,
/// not hash-consed via `Builder`.
///
/// New combinators belong here, not as free functions scattered across
/// caller files. Each method enforces the shared invariants (correct
/// `FunctionSet` propagation via `Call::unionArgFunctions`, function
/// names sourced from `FunctionRegistry`, cardinality conventions for
/// boolean results) so call sites can't get them wrong.
///
/// Combinators do not null-handle their operands; callers that pass
/// optional predicates should null-check before delegating. Methods
/// that source their function name from `FunctionRegistry` throw
/// `VELOX_USER_CHECK` if the active dialect did not register that
/// function.
class ExprFactory {
 public:
  explicit ExprFactory(Builder& builder) : builder_(builder) {}

  /// Flattens an AND tree into conjuncts. `expr` may be an AND call
  /// (nested arbitrarily) or any other expression (appended as a
  /// single conjunct).
  static ExprVector flattenAnd(ExprCP expr);
  static void flattenAnd(ExprCP expr, ExprVector& conjuncts);

  /// Flattens an OR tree into disjuncts. `expr` may be an OR call
  /// (nested arbitrarily) or any other expression (appended as a
  /// single disjunct).
  static ExprVector flattenOr(ExprCP expr);
  static void flattenOr(ExprCP expr, ExprVector& disjuncts);

  /// Builds `lhs AND rhs`.
  ExprCP makeAnd(ExprCP lhs, ExprCP rhs);

  /// Builds `lhs OR rhs`.
  ExprCP makeOr(ExprCP lhs, ExprCP rhs);

  /// Builds `not(arg)`. Preserves three-valued logic: `not(NULL)` is
  /// NULL. Used to turn a semi-project mark into the antijoin predicate.
  ExprCP makeNot(ExprCP arg);

  /// Builds `lhs = rhs`.
  ExprCP makeEq(ExprCP lhs, ExprCP rhs);

  /// Builds `lhs <= rhs`.
  ExprCP makeLessThanOrEqual(ExprCP lhs, ExprCP rhs);

  /// Builds `is_null(arg)`. Result is BOOLEAN and is never NULL —
  /// returns `true` when 'arg' is NULL and `false` otherwise.
  ExprCP makeIsNull(ExprCP arg);

  /// Builds `coalesce(value, fallback)`. Both operands must be of the
  /// same type per Velox's COALESCE rules; the factory does not verify.
  ExprCP makeCoalesce(ExprCP value, ExprCP fallback);

  /// Builds `if(condition, thenExpr, elseExpr)`. `thenExpr` and
  /// `elseExpr` must be the same type per Velox's IF rules; the factory
  /// does not verify.
  ExprCP makeIf(ExprCP condition, ExprCP thenExpr, ExprCP elseExpr);

  /// Substitutes the i-th source `Column*` with the i-th `target`
  /// Expr in `expr`, returning a rewritten Expr. Handles
  /// Column/Literal/Call/Field/Lambda. Columns not in `sources` pass
  /// through unchanged. `Call`s with at least one substituted child
  /// are rebuilt through `Builder::makeCall` so hash-consing remains
  /// canonical; Field/Lambda are rebuilt through `make`
  /// (arena allocator, no dedup table). For Lambda, bound args are
  /// removed from the source mapping before recursing into the body
  /// — callers don't need to filter them out.
  ///
  /// `kAggregateExpr` substitution is not implemented (aggregates only
  /// live inside `Aggregate` nodes, not embedded in scalar expressions
  /// we substitute through); throws `VELOX_NYI` if encountered.
  ExprCP substitute(
      ExprCP expr,
      const ColumnVector& sources,
      const ExprVector& targets);

  /// Applies `substitute` to each element of `exprs`, returning the
  /// rewritten vector in the same order.
  ExprVector substitute(
      const ExprVector& exprs,
      const ColumnVector& sources,
      const ExprVector& targets);

  /// Left-folds a non-empty sequence of boolean predicates with
  /// `makeAnd`: `[p1, p2, p3]` → `AND(AND(p1, p2), p3)`. Single-element
  /// input returns the predicate unchanged. The empty case is a
  /// caller error (no neutral element is constructed).
  template <typename Range>
  ExprCP andAll(const Range& predicates) {
    auto it = std::begin(predicates);
    auto end = std::end(predicates);
    VELOX_CHECK(it != end, "andAll requires a non-empty sequence");
    ExprCP result = *it++;
    for (; it != end; ++it) {
      result = makeAnd(result, *it);
    }
    return result;
  }

  /// Left-folds a non-empty sequence of boolean predicates with
  /// `makeOr`. Single-element input returns the predicate unchanged.
  /// The empty case is a caller error.
  template <typename Range>
  ExprCP orAll(const Range& predicates) {
    auto it = std::begin(predicates);
    auto end = std::end(predicates);
    VELOX_CHECK(it != end, "orAll requires a non-empty sequence");
    ExprCP result = *it++;
    for (; it != end; ++it) {
      result = makeOr(result, *it);
    }
    return result;
  }

 private:
  // Builds a BOOLEAN-typed (cardinality 2) call named `name` over
  // `arguments`, seeding the `FunctionSet` from the function's own bits
  // (`specialForm` selects special-form vs registered-function lookup)
  // OR'd with each argument's.
  ExprCP makeBooleanCall(Name name, ExprVector arguments, bool specialForm);

  Builder& builder_;
};

} // namespace facebook::axiom::optimizer::v2
