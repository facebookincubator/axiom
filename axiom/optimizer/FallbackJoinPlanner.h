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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <folly/container/F14Set.h>

#include "axiom/optimizer/JoinHypergraph.h"
#include "axiom/optimizer/MemoOp.h"
#include "axiom/optimizer/RelationSet.h"

namespace facebook::axiom::optimizer {

/// Builds an uncosted join order when missing statistics prevent DPhyp from
/// producing a plan. The fallback retains relation order except that it takes
/// an equi-joinable relation before introducing a cross join.
///
/// Invariants:
///   - `graph` outlives this planner.
class FallbackJoinPlanner {
 public:
  explicit FallbackJoinPlanner(const JoinHypergraph& graph) : graph_{graph} {}

  /// Returns one left-deep MemoOp root for each all-inner component. Returns an
  /// empty vector when no relation needs to move or the graph's ordering must
  /// remain unchanged. May be called once; returned nodes remain valid for this
  /// object's lifetime.
  std::vector<MemoOpCP> build(const std::vector<RelationSet>& components);

 private:
  MemoOpCP makeLeaf(int32_t relationId);

  // Builds a left-deep tree from the component's first relation outward.
  MemoOpCP buildComponent(const RelationSet& component);

  const JoinHypergraph& graph_;

  // Owns every MemoOp returned directly or transitively by `build`.
  std::vector<std::unique_ptr<MemoOp>> memoOps_;

  // Tracks equalities enforced by the emitted joins and filters.
  folly::F14FastSet<size_t> appliedEdges_;

  // True when a connected relation moved ahead of an earlier cross join.
  bool reordered_{false};
};

} // namespace facebook::axiom::optimizer
