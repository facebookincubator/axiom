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

#include <utility>
#include <vector>

#include <folly/Function.h>
#include <folly/container/F14Map.h>

#include "axiom/optimizer/Node.h"

namespace facebook::axiom::optimizer {

/// A maximal subtree of reorderable equi-Joins discovered in the IR.
///
/// `root` is the topmost Join. `leaves` are the non-Join input nodes
/// at the cluster's frontier; their pointer identity is preserved so
/// downstream emit can stitch the rewritten join tree back together
/// with surrounding IR. `joins` lists every Join in the cluster,
/// `root` included.
///
/// `unnests` records Unnest IR nodes the cluster collection descended through.
/// Classification removes the ones placed above the cluster root; each
/// remaining Unnest becomes a hypergraph relation connected to its input
/// relations by an unnest edge. Order is post order, so an input Unnest appears
/// before any Unnest that consumes its output.
struct JoinCluster {
  JoinCP root;
  std::vector<NodeCP> leaves;
  std::vector<JoinCP> joins;
  std::vector<UnnestCP> unnests;

  /// Unnests whose row multiplication is observed by a non-inner join above
  /// them in the written tree. These cannot move above that join.
  std::vector<UnnestCP> unnestBarriers;

  /// Columns produced by Unnests placed above the cluster root.
  PlanObjectSet freeUnnestColumns;

  /// Predicates of the `Filter` nodes the cluster spans. Each becomes a graph
  /// conjunct, applied wherever the relations it reads are covered.
  ExprVector filterPredicates;

  /// Predicates of the `Filter` directly above `root` that join planning takes
  /// in. Each becomes a graph conjunct placed like a `filterPredicates` one.
  ExprVector absorbedPredicates;

  /// The fresh outputs of the cluster's joins. A join that null-extends an
  /// input gives each of its columns a fresh output, and a join above can copy
  /// that output again.
  class FreshOutputs {
   public:
    explicit FreshOutputs(const JoinCluster& cluster);

    /// Returns the column that 'column' is made from, following a fresh output
    /// of a fresh output back to the first, or 'column' when no join of the
    /// cluster makes it. Calls 'visit' with each join on the way, nearest
    /// first.
    ColumnCP source(ColumnCP column, folly::FunctionRef<void(JoinCP)> visit)
        const;

   private:
    // The join that makes each fresh output, and the column it is made from.
    folly::F14FastMap<ColumnCP, std::pair<JoinCP, ColumnCP>> copies_;
  };
};

} // namespace facebook::axiom::optimizer
