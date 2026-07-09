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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/DPhyp.h"
#include "axiom/optimizer/v2/JoinHypergraph.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Translates the `MemoOp` tree DPhyp produced back into an IR
/// `Join` tree by walking bottom-up: each `LeafOp` resolves to the
/// relation's IR node; each `JoinOp` calls `Builder::make<Join>` with
/// the edge's keys/filter and the chosen child plans.
///
/// Intermediate Joins carry the lossless union of their children's
/// columns. The root Join carries exactly `rootOutputColumns` so
/// upstream consumers see the same schema the original cluster root
/// exposed, regardless of how the chosen tree shape rearranges
/// relations underneath.
class JoinTreeEmitter {
 public:
  /// Emits the tree-IR for the join tree DPhyp chose, rooted at the memo
  /// entry `root`. `rootOutputColumns` are the columns the result must expose
  /// (see class comment); `graph` supplies each memo op's relation and edge,
  /// and `builder` hash-conses the emitted nodes.
  static NodeCP emit(
      MemoOpCP root,
      const JoinHypergraph& graph,
      const ColumnVector& rootOutputColumns,
      Builder& builder);

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
      int32_t numWorkers);
};

} // namespace facebook::axiom::optimizer::v2
