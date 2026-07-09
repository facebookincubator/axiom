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
#include "velox/core/ExpressionEvaluator.h"

namespace facebook::axiom::optimizer::v2 {

/// Reduces expressions to simpler equivalent forms — today constant
/// folding of column-free expressions; algebraic and boolean identities
/// (`a + 0 → a`, `true AND f → f`, …) plug in as the rule set grows.
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

  /// Returns the simplified `expr`, or `expr` unchanged if no rule
  /// applies.
  ExprCP simplify(ExprCP expr);

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
  // Folds `expr` to a `Literal` when it has no column refs and the
  // evaluator produces a single constant value. Otherwise returns
  // `expr` unchanged.
  ExprCP tryFoldConstant(ExprCP expr);

  Builder& builder_;
  velox::core::ExpressionEvaluator& evaluator_;
};

} // namespace facebook::axiom::optimizer::v2
