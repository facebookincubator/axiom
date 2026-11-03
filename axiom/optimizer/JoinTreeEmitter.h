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

#include <vector>

#include <folly/Function.h>

#include "axiom/optimizer/Builder.h"
#include "axiom/optimizer/DPhyp.h"
#include "axiom/optimizer/JoinHypergraph.h"
#include "axiom/optimizer/Node.h"

namespace facebook::axiom::optimizer {

class ExprSimplifier;

/// Translates the `MemoOp` tree DPhyp produced back into an IR `Join` tree by
/// walking bottom-up: each `LeafOp` resolves to the relation's IR node; each
/// `JoinOp` constructs a Join from the edge's keys/filter and the chosen child
/// plans.
///
/// Intermediate Joins carry the lossless union of their children's
/// columns. The root Join carries exactly `rootOutputColumns` so
/// upstream consumers see the same schema the original cluster root
/// exposed, regardless of how the chosen tree shape rearranges
/// relations underneath.
class JoinTreeEmitter {
 public:
  /// Constructs one emitted join. A caller may add physical distribution while
  /// constructing the node.
  using JoinFactory = folly::FunctionRef<NodeCP(Join::Key)>;

  /// Emits the tree-IR for the join tree DPhyp chose, rooted at the memo
  /// entry `root`. `rootOutputColumns` are the columns the result must expose
  /// (see class comment); `graph` supplies each memo op's relation and edge,
  /// and `builder` hash-conses the emitted nodes. `numWorkers` coarsens the
  /// bucketing of a relation the plan reads grouped (see `GroupedRead`).
  /// `oneDriverPerTask` says whether each task runs one driver, which every
  /// exchange the tree adds states (see `Exchange::placementAfter`).
  static NodeCP emit(
      MemoOpCP root,
      const JoinHypergraph& graph,
      const ColumnVector& rootOutputColumns,
      Builder& builder,
      ExprSimplifier& simplifier,
      int32_t numWorkers,
      bool oneDriverPerTask);

  /// Emits the tree using `joinFactory` to construct each Join step.
  static NodeCP emit(
      MemoOpCP root,
      const JoinHypergraph& graph,
      const ColumnVector& rootOutputColumns,
      Builder& builder,
      ExprSimplifier& simplifier,
      int32_t numWorkers,
      bool oneDriverPerTask,
      JoinFactory joinFactory);

  /// Emits a cross-product combine of independently planned connected
  /// components. Each component subtree is emitted, then the components
  /// are folded left-deep into keyless inner `Join` nodes (Velox lowers
  /// these to NestedLoopJoin): the largest component (by estimated
  /// cardinality) drives as the bottom-left probe, the rest join as
  /// builds in ascending cardinality. Cross-component filter conjuncts
  /// are placed at the lowest fold whose cover satisfies them. The root
  /// carries `rootOutputColumns`. Requires at least two components.
  /// `numWorkers > 1` broadcasts each cross-product build so a single-task
  /// (Values / global-aggregate) or scan component is not co-located with
  /// another component in one fragment.
  static NodeCP emitComponents(
      const std::vector<MemoOpCP>& componentRoots,
      const JoinHypergraph& graph,
      const ColumnVector& rootOutputColumns,
      Builder& builder,
      ExprSimplifier& simplifier,
      int32_t numWorkers,
      bool oneDriverPerTask);

  /// Emits the component trees using `joinFactory` within each component and
  /// `crossJoinFactory` to combine components.
  static NodeCP emitComponents(
      const std::vector<MemoOpCP>& componentRoots,
      const JoinHypergraph& graph,
      const ColumnVector& rootOutputColumns,
      Builder& builder,
      ExprSimplifier& simplifier,
      int32_t numWorkers,
      bool oneDriverPerTask,
      JoinFactory joinFactory,
      JoinFactory crossJoinFactory);
};

} // namespace facebook::axiom::optimizer
