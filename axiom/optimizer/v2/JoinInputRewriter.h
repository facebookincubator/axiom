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

#include "axiom/optimizer/v2/PlanSubstitutions.h"
#include "axiom/optimizer/v2/PrecomputeProjections.h"

namespace facebook::axiom::optimizer::v2 {

/// Moves the parts of expressions over a join that read a single input into
/// that input, where each is evaluated once per row of the input.
///
/// A cross join evaluates its filter once for every pair of input rows, while a
/// part of the filter reading only one side has the same value for every pair
/// built from a given row of that side:
///
///   SELECT ... FROM big, small WHERE big.threshold < length(small.name)
///
///   Join[INNER, filter: threshold < length(name)]
///     Scan big
///     Scan small
///
///   becomes
///
///   Join[INNER, filter: threshold < __p]
///     Scan big
///     Project __p := length(name)
///       Scan small
///
/// `length(name)` is now evaluated once per row of `small` instead of once per
/// pair, and the broadcast of `small` carries `__p` in place of `name`.
///
/// A projection or aggregation over a join evaluates its expressions once per
/// output row. Join planning computes a part reading only one relation of the
/// join at that relation when the relation has fewer rows than the join:
///
///   SELECT t.a + array_max(arr) FROM t, (SELECT array_agg(b) AS arr FROM u)
///
///   Project t.a + array_max(arr)       Project t.a + __p
///     Join[INNER]                 ->     Join[INNER]
///       Scan t                             Scan t
///       Aggregate arr                      Project __p := array_max(arr)
///                                            Aggregate arr
///
/// This moves an expression across an operator, so it is bound by the rules in
/// docs/ExpressionEvaluation.md: only a deterministic expression may move, an
/// argument a special form may skip never does, and what moves gives up the
/// error masking the filter gave it.
///
/// Within those rules, a subexpression moves when it reads at least one column
/// and every column it reads comes from one input. The highest such node moves
/// as a whole, since the walk does not descend into it, and the walk stops at
/// the branches of `if`, `switch` and `coalesce` and at anything inside a `try`
/// -- though each of these still moves as a unit when one input supplies all
/// of its columns.
///
/// TODO: Extend the filter's move to equi joins. Their filter is evaluated only
/// on the pairs that matched on the keys, and there can be fewer of those than
/// either input has rows, so the move pays only when the estimated number of
/// pairs reaching the filter exceeds the rows on the side the expression reads.
class JoinInputRewriter {
 public:
  /// An input that computes the parts of the expressions that read only its
  /// columns.
  struct Input {
    /// Takes the moved parts, and the columns the rewritten expressions read
    /// from this input, into the projection over the input.
    PrecomputeProjections* precompute;
    /// The input's output columns.
    const PlanObjectSet* columns;
  };

  /// 'inputs' are disjoint in their columns. A column the expressions read
  /// that no input produces stays as it is.
  JoinInputRewriter(std::vector<Input> inputs, Builder& builder)
      : inputs_{std::move(inputs)}, builder_{builder} {}

  /// Moves only the parts whose result is no wider than what they read: a
  /// fixed-width primitive, or a value no more nested than a complex column
  /// the part reads. A moved part's result travels through every join and
  /// exchange above its input, so a part that builds a wide value from a
  /// narrow one, such as `sequence(1, n)`, moves more data than it saves.
  void moveOnlyNarrowing() {
    onlyNarrowing_ = true;
  }

  /// Returns 'exprs' with each maximal single-input subexpression replaced by
  /// the column it was moved to. Every input column the result still reads is
  /// added to its input's projection, so the caller need not keep those columns
  /// alive separately.
  ExprVector rewrite(const ExprVector& exprs);

  /// Maps each subexpression 'rewrite' moved to the column it became.
  const PlanSubstitutions& moved() const {
    return moved_;
  }

  /// Adds 'column' to the projection of the input that produces it, if any.
  void keep(ColumnCP column);

  /// Adds every column 'expr' reads to the projection of the input that
  /// produces it.
  void keepAll(ExprCP expr) {
    expr->columns().forEach<Column>([&](ColumnCP column) { keep(column); });
  }

 private:
  ExprCP rewrite(ExprCP expr);
  ExprCP rewriteCall(const Call* call);
  ExprCP rewriteField(const Field* field);

  // Computes 'expr' in the input that supplies all of its columns and returns
  // the column it became there. Returns nullptr, leaving 'expr' in place, when
  // no single input supplies its columns or 'expr' is non-deterministic.
  ExprCP tryPrecompute(ExprCP expr);

  // The inputs a part of the expressions may move into.
  const std::vector<Input> inputs_;
  Builder& builder_;
  bool onlyNarrowing_{false};
  // Maps each part moved so far to the column it became.
  PlanSubstitutions moved_;
};

} // namespace facebook::axiom::optimizer::v2
