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

#include "axiom/optimizer/Filters.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer {

/// Cardinality estimator for equi-joins.
class JoinFanout {
 public:
  /// Returns the estimated number of rows produced by the equi-join
  /// `leftKeys == rightKeys` of 'leftCardinality' left rows against
  /// 'rightCardinality' right rows. Each side's joint key NDV is the product
  /// of its per-key NDVs, capped at that side's row count (a side cannot have
  /// more distinct key tuples than rows); the larger of the two joint NDVs
  /// determines selectivity. Orientation-invariant: swapping the two sides
  /// yields the same result. 'leftKeys' and 'rightKeys' are aligned by
  /// position. Each key's NDV is read from 'constraints' (falling back to
  /// `expr->value()` for keys absent from the map), so propagated post-filter /
  /// post-join statistics drive the estimate instead of base-table NDV. Returns
  /// nullopt if any cardinality or key NDV is unknown, 0 if any key NDV is 0.
  static std::optional<float> estimateJoinCardinality(
      std::optional<float> leftCardinality,
      std::optional<float> rightCardinality,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      const ConstraintMap& constraints);

  /// Output cardinality of a join that admits 'matched' row pairs before
  /// 'filter' is applied. The filter is part of the match condition, so
  /// it reduces the matches for every join type; the type then decides the
  /// output:
  ///   - inner: matched.
  ///   - left / right: matched, and at least one row per left / right row.
  ///   - full: both sides minus the matched overlap.
  ///   - left/right semi filter: left / right rows that matched.
  ///   - left/right semi project: every left / right row (a mark is added).
  ///   - anti: left rows with no match.
  ///
  /// 'leftCardinality' and 'rightCardinality' are the join's operands in the
  /// order 'joinType' names them. 'constraints' is refined by the filter, as
  /// for a `Filter` node. Unknown inputs propagate to nullopt.
  static std::optional<float> outputCardinality(
      velox::core::JoinType joinType,
      std::optional<float> leftCardinality,
      std::optional<float> rightCardinality,
      std::optional<float> matched,
      const ExprVector& filter,
      ConstraintMap& constraints);
};

} // namespace facebook::axiom::optimizer
