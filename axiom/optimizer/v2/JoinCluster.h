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

#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// A maximal subtree of reorderable equi-Joins discovered in the IR.
///
/// `root` is the topmost Join. `leaves` are the non-Join input nodes
/// at the cluster's frontier; their pointer identity is preserved so
/// downstream emit can stitch the rewritten join tree back together
/// with surrounding IR. `joins` lists every Join in the cluster,
/// `root` included.
///
/// `unnests` records Unnest IR nodes the cluster collection
/// descended through. Each becomes its own relation in the
/// hypergraph, connected to the leaves contributed by its input
/// subtree via a directed cross-join-unnest edge. Order is descent
/// order (innermost first) so dependent chains (`UNNEST(u1.x) u2`)
/// have `u1`'s cardinality computed before `u2`'s.
struct JoinCluster {
  JoinCP root;
  std::vector<NodeCP> leaves;
  std::vector<JoinCP> joins;
  std::vector<UnnestCP> unnests;
};

} // namespace facebook::axiom::optimizer::v2
