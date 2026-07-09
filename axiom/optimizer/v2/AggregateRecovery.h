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
#include "axiom/optimizer/v2/ExprFactory.h"

namespace facebook::axiom::optimizer::v2 {

/// Substrate for the decorrelate pass's Apply-over-Aggregate recovery
/// skeleton. The recovery rewrites a correlated Apply whose body
/// includes an Aggregate into a single linear chain
/// `AssignUniqueId → LEFT JOIN → Aggregate`, where the LEFT JOIN's
/// right side carries the `_include` marker synthesized by the Apply's
/// terminus. This class collects the single-purpose transformations
/// the chain needs; each one is independently testable and free of
/// dispatch on Apply kind or body shape.
class AggregateRecovery {
 public:
  AggregateRecovery(Builder& builder, ExprFactory& exprFactory)
      : builder_(builder), exprFactory_(exprFactory) {}

  /// Returns a new aggregate vector with `count(*)` rewritten to
  /// `count() FILTER (WHERE marker)`. Other aggregates pass through
  /// unchanged — their NULL handling already matches the
  /// scalar-subquery-empty semantics; only `count(*)` treats the
  /// padded NULL row as a real row (size = 1) instead of empty
  /// (size = 0), so it's the only one needing the marker FILTER.
  ///
  /// If a `count(*)` already carries a FILTER condition (e.g. from a
  /// HAVING-like rewrite), the marker is AND-combined with the
  /// existing condition.
  ///
  /// `count(*)` is identified structurally as `name == "count"` with
  /// empty `args()` — Velox uses the same Call (no args) for
  /// `COUNT(*)`.
  AggregateCallVector rewriteCountStar(
      const AggregateCallVector& aggregates,
      ColumnCP marker);

  /// Returns a new aggregate vector with `predicate` AND-combined into
  /// every aggregate's existing FILTER condition (or set as the
  /// condition if the aggregate didn't have one). Used to inject an
  /// eligibility marker (the LEFT JOIN pad-present predicate) so that
  /// padded rows don't contribute to the recovery Aggregate's results.
  AggregateCallVector addFilterCondition(
      const AggregateCallVector& aggregates,
      ExprCP predicate);

  /// Throws `VELOX_USER_FAIL` if any aggregate's args reference only
  /// outer-correlation columns. After the recovery's LEFT JOIN, the
  /// padded row still carries outer column values; an aggregate like
  /// `sum(L.col)` would include them once per outer row, producing
  /// wrong results.
  ///
  /// Aggregates with no args (e.g. `count(*)`) are accepted; the
  /// count-bug recovery handles them via `rewriteCountStar`.
  static void validateAggregateArgs(
      const AggregateCallVector& aggregates,
      const ColumnVector& outerCorrelations);

  /// Constructs a `bool_or(arg)` aggregate, used by the semi recovery
  /// to collapse per-group eligibility booleans to a single mark per
  /// outer row.
  optimizer::AggregateCP makeBoolOr(ExprCP arg);

 private:
  // Returns a copy of `aggregate` with the FILTER condition replaced by
  // `condition`. Other fields (name, args, distinct, orderKeys, etc.)
  // are preserved.
  optimizer::AggregateCP replaceCondition(
      optimizer::AggregateCP aggregate,
      ExprCP condition);

  Builder& builder_;
  ExprFactory& exprFactory_;
};

} // namespace facebook::axiom::optimizer::v2
