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
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Moves expressions a consumer references into a `Project` over its input.
class PrecomputeProjectionsPass {
 public:
  /// Lifts grouping expressions, aggregate arguments, filters, and ordering
  /// keys of 'aggregate' into a Project where required. Aggregate arguments
  /// comes from 'rewrittenInput' provided by caller in replacement for
  /// `aggregate->input()`. Returns a new equivalent Aggregate node with lifted
  /// fields.
  static AggregateCP prepareAggregateInputs(
      const Aggregate* aggregate,
      NodeCP rewrittenInput,
      Builder& builder);

  /// Returns the tree rooted at 'node' rewritten so the expressions listed
  /// below are computed by a `Project` inserted between the consumer and its
  /// input, with the consumer rebuilt to reference the projected column.
  ///
  /// Most positions are moved because Velox demands a `FieldAccessTypedExpr`
  /// (or, where allowed, a constant) there:
  ///   - Aggregate: grouping keys, aggregate args, FILTER mask, ORDER BY keys
  ///   - Window: partition keys, order keys, function args, frame bounds
  ///   - Sort, TopN: order keys
  ///   - RowNumber: partition keys
  ///   - TopNRowNumber: partition keys, order keys
  ///   - Unnest: unnest expressions
  ///   - Join: join keys
  ///
  /// A join filter is one exception: Velox accepts any expression there, so
  /// the move is an optimization rather than a requirement. A `UnionAll` is
  /// another: its legs are aligned to the union's output columns because
  /// Velox's `LocalPartition` requires one shared output `RowType`.
  ///
  /// A join with no equi keys evaluates its filter once for every pair of
  /// input rows. A subexpression of the filter that reads only one side has
  /// the same value for every pair built from a given row of that side, so
  /// computing it in that side's input costs one evaluation per row instead of
  /// one per pair. Each maximal such subexpression is moved. A
  /// non-deterministic one is not: it has to produce a new value per pair.
  ///
  /// A join with equi keys is left alone, because its filter runs only on the
  /// pairs that match on the keys, and there can be far fewer of those than
  /// either input has rows.
  ///
  /// TODO: Decide from the estimated number of pairs reaching the filter
  /// rather than from the absence of equi keys.
  ///
  /// Moving a subexpression out of a filter takes it out of the filter's error
  /// masking, where an error from one conjunct is discarded for a row that
  /// another conjunct evaluates to false. `a <> 0 AND 1000 / a > x` does not
  /// fail as a filter, but does once `1000 / a` is computed by a `Project`.
  /// SQL does not define an evaluation order, so that masking is not a
  /// property the optimizer preserves.
  ///
  /// Returns the original tree unchanged when nothing needs to move.
  static NodeCP run(NodeCP node, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
