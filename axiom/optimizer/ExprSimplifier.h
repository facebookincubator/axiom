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

#include <folly/container/F14Map.h>

#include "axiom/optimizer/Builder.h"
#include "velox/core/ExpressionEvaluator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::optimizer {

/// Reduces expressions to simpler equivalent forms using constant folding and
/// contextual facts supplied by a plan pass.
///
/// `evaluator` must outlive this `ExprSimplifier` and any IR it
/// produces: folded constants register `Variant`s in the
/// `QueryGraphContext`, but vectors produced during compilation are
/// allocated on the evaluator's pool and may be referenced by Velox
/// plan nodes that emit later constructs from the same IR.
class ExprSimplifier {
 public:
  ExprSimplifier(Builder& builder, velox::core::ExpressionEvaluator& evaluator)
      : builder_(builder), evaluator_(evaluator) {}

  /// Returns `expr` simplified bottom-up, or `expr` itself when no rule
  /// applies. Rules:
  ///  - IF and SWITCH drop the branches whose literal conditions cannot select
  ///    them, in evaluation order: IF(true, a, 1 / 0) -> a.
  ///  - COALESCE drops NULL literal arguments, and a single remaining argument
  ///    replaces the call: coalesce(NULL, a) -> a.
  ///  - A call with default null behavior and a NULL literal argument becomes
  ///    NULL: a + NULL -> NULL.
  ///  - A call that reads no columns and that Velox compiles to a constant
  ///    becomes that literal: 1 + 2 -> 3. A call whose evaluation fails is
  ///    kept, so the error surfaces at execution.
  ///  - AND and OR drop boolean literals that do not decide them and become
  ///    the literal that does: a AND true -> a, a OR true -> true.
  ///  - Equality between a row constructor and a non-NULL row literal compares
  ///    field by field: ROW(a, b) = ROW(1, 2) -> a = 1 AND b = 2.
  ///  - A field of a row constructor becomes the argument that builds it:
  ///    ROW(a, b).field0 -> a.
  ExprCP simplify(ExprCP expr);

  /// Returns whether `expr` cannot produce NULL when `nonNullColumns` are
  /// known to be non-null.
  bool isKnownNonNull(ExprCP expr, const PlanObjectSet& nonNullColumns) const;

  /// Simplifies `expr` using columns known to be non-null: COALESCE stops at
  /// its first non-null argument, `x IS NULL` is false for a non-null `x`, and
  /// `x = x` is true for a non-null, deterministic `x` of a primitive type.
  ExprCP simplify(ExprCP expr, const PlanObjectSet& nonNullColumns) const;

  /// Evaluates a column-free `expr` to a single value. Places no determinism
  /// or constant-ness requirement on `expr`: for contexts like VALUES where an
  /// expression is evaluated exactly once. `expr` must not reference any
  /// columns.
  velox::Variant evaluate(ExprCP expr);

  /// Splits a filter `predicate` into conjuncts (AND-flattened),
  /// simplifies each, and applies filter semantics: a literal `true`
  /// conjunct is dropped; a literal `false` or `NULL` conjunct
  /// short-circuits (filter passes only on `TRUE`, so `NULL` is
  /// equivalent to `FALSE`).
  ///
  /// Returns true iff the filter is statically known to drop every
  /// row; on true return `into` is unmodified. Returns false
  /// otherwise and appends the surviving (flattened, simplified,
  /// non-trivial) conjuncts to `into`. An empty `into` after a false
  /// return means `predicate` is statically `true` and the filter can
  /// be removed.
  bool simplifyFilter(ExprCP predicate, ExprVector& into);

 private:
  // Simplifies IF and SWITCH in evaluation order, skipping result expressions
  // whose literal conditions cannot select them.
  // For example, IF(true, a, 1 / 0) simplifies to 'a'.
  ExprCP simplifyConditional(const Call* call);

  // Simplifies an AND or OR call with boolean literal arguments. Returns
  // `expr` unchanged for any other call. Its arguments are already simplified.
  ExprCP tryFoldConjunct(ExprCP expr);

  // Expands `ROW(a, b) = <row literal>` into `a = <field 0> AND b = <field 1>`.
  // Returns nullptr for any other expression.
  ExprCP tryExpandRowEquality(ExprCP expr);

  // Folds field `i` of `ROW(a0, a1, ...)` to `ai`. Returns nullptr for any
  // other expression.
  ExprCP tryFoldRowField(ExprCP expr);

  // Folds `expr` to a `Literal` when it has no column refs and the
  // evaluator produces a single constant value. Otherwise returns
  // `expr` unchanged.
  ExprCP tryFoldConstant(ExprCP expr);

  Builder& builder_;
  velox::core::ExpressionEvaluator& evaluator_;
  folly::F14FastMap<ExprCP, ExprCP> simplified_;

  // Reusable single empty row, the input for evaluate().
  velox::RowVectorPtr emptyInput_;
};

} // namespace facebook::axiom::optimizer
