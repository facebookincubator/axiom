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

#include "axiom/optimizer/v2/PrecomputeProjections.h"

namespace facebook::axiom::optimizer::v2 {

/// Moves the single-side parts of a cross join's filter into its inputs.
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
/// This moves an expression across an operator, so it is bound by the rules in
/// docs/ExpressionEvaluation.md: only a deterministic expression may move, an
/// argument a special form may skip never does, and what moves gives up the
/// error masking the filter gave it.
///
/// Within those rules, a subexpression moves when it reads at least one column
/// and reads no column from the other side. The highest such node moves as a
/// whole, since the walk does not descend into it, and the walk stops at the
/// branches of `if`, `switch` and `coalesce` and at anything inside a `try` --
/// though each of these still moves as a unit when one side supplies all of its
/// columns.
///
/// TODO: Extend this to equi joins. Their filter is evaluated only on the pairs
/// that matched on the keys, and there can be fewer of those than either input
/// has rows, so the move pays only when the estimated number of pairs reaching
/// the filter exceeds the rows on the side the expression reads.
class JoinFilterRewriter {
 public:
  JoinFilterRewriter(
      PrecomputeProjections& leftPrecompute,
      PrecomputeProjections& rightPrecompute,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      Builder& builder)
      : leftPrecompute_{leftPrecompute},
        rightPrecompute_{rightPrecompute},
        leftColumns_{leftColumns},
        rightColumns_{rightColumns},
        builder_{builder} {}

  // Returns 'filter' with each maximal single-side subexpression replaced by
  // the column it was moved to. Every column the result still reads is added to
  // its side's projection, so the caller need not keep filter columns alive
  // separately.
  ExprVector rewrite(const ExprVector& filter);

 private:
  ExprCP rewrite(ExprCP expr);
  ExprCP rewriteCall(const Call* call);
  ExprCP rewriteField(const Field* field);

  // Computes 'expr' in the input that supplies all of its columns and returns
  // the column it became there. Returns nullptr, leaving 'expr' in the filter,
  // when no single input supplies its columns or 'expr' is non-deterministic.
  ExprCP tryPrecompute(ExprCP expr);

  // Adds 'column' to the projection of the input that produces it.
  void keep(ColumnCP column);

  // Adds every column 'expr' reads to the projection of the input that
  // produces it.
  void keepAll(ExprCP expr) {
    expr->columns().forEach<Column>([&](ColumnCP column) { keep(column); });
  }

  PrecomputeProjections& leftPrecompute_;
  PrecomputeProjections& rightPrecompute_;
  const PlanObjectSet& leftColumns_;
  const PlanObjectSet& rightColumns_;
  Builder& builder_;
};

} // namespace facebook::axiom::optimizer::v2
