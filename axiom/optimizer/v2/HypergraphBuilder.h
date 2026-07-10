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

#include "axiom/optimizer/v2/JoinCluster.h"
#include "axiom/optimizer/v2/JoinHypergraph.h"

namespace facebook::axiom::optimizer::v2 {

class EstimateProvider;

/// Converts a `JoinCluster` into the `JoinHypergraph` consumed by
/// `DPhyp`: one `Relation` per cluster leaf, one `JoinEdge` per
/// cluster Join.
class HypergraphBuilder {
 public:
  /// `cluster.leaves` carries the original pointers found in
  /// `cluster.root`'s tree (used to walk the cluster topology);
  /// `rewrittenLeaves` carries the corresponding post-rewrite leaf
  /// pointers (used to populate hypergraph relations so the emitted
  /// plan reflects sub-cluster rewrites). `rewrittenLeaves[i]` must
  /// align 1:1 with `cluster.leaves[i]` and output the same logical
  /// columns.
  ///
  /// Throws if the cluster has more leaves than
  /// `RelationSet::kMaxRelations`, or if any cluster Join references
  /// a join key that does not resolve to a leaf column.
  static JoinHypergraph build(
      const JoinCluster& cluster,
      const std::vector<NodeCP>& rewrittenLeaves,
      EstimateProvider& estimateProvider);
};

} // namespace facebook::axiom::optimizer::v2
