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

#include "axiom/optimizer/v2/JoinHypergraph.h"
#include "axiom/optimizer/v2/MemoOp.h"
#include "axiom/optimizer/v2/PlanSet.h"
#include "axiom/optimizer/v2/RelationSet.h"
#include "folly/container/F14Map.h"

namespace facebook::axiom::optimizer::v2 {

class CostModel;

/// Bottom-up DP join-order enumeration over a `JoinHypergraph`
/// (Moerkotte/Neumann 2006 DPhyp: connected-subgraph + complement
/// enumeration).
///
/// Invariants:
///   - The hypergraph passed to the constructor must outlive the
///     `DPhyp` instance.
///   - The hypergraph must contain at least two relations.
class DPhyp {
 public:
  /// `enumerationBudget` caps the number of connected-subgraph/complement pairs
  /// the DP evaluates before falling back to greedy (GOO) ordering; <= 0 means
  /// unlimited. See `OptimizerOptions::dphypEnumerationBudget`. `numWorkers` is
  /// the target task count: when > 1 the enumeration generates remote-exchange
  /// (repartition / broadcast) candidates; at 1 it stays single-fragment.
  /// `broadcastSizeLimit` caps the estimated build size a broadcast candidate
  /// may replicate to every task (bytes); <= 0 disables the limit. See
  /// `OptimizerOptions::broadcastSizeLimit`.
  DPhyp(
      const JoinHypergraph& graph,
      const CostModel& costModel,
      int64_t enumerationBudget,
      int32_t numWorkers,
      int64_t broadcastSizeLimit);

  /// Runs the enumeration and returns the root `MemoOp` for the full
  /// relation set, or nullptr when no costable plan exists (some relation
  /// or edge had unknown cost). A nullptr return tells the caller to fall
  /// back to the query's syntactic join order.
  MemoOpCP enumerate();

  /// Runs enumeration once and returns the optimal plan for each
  /// connected component in `components`. Each entry must be a connected
  /// relation set and together they must cover all relations. Use when
  /// the hypergraph is disconnected (genuine cross products); the caller
  /// combines the per-component plans with cross joins. Returns an empty
  /// vector when any component has no costable plan (some relation or edge
  /// had unknown cost), telling the caller to fall back to syntactic order.
  std::vector<MemoOpCP> enumerate(const std::vector<RelationSet>& components);

 private:
  const JoinHypergraph& graph_;
  const CostModel& costModel_;
  const int64_t enumerationBudget_;
  const int32_t numWorkers_;
  const int64_t broadcastSizeLimit_;
  folly::F14NodeMap<RelationSet, PlanSet> memo_;

  // Owns the remote-exchange (repartition / broadcast) MemoOps the enumeration
  // creates to enforce a join's required input partitioning. They are children
  // of join candidates, not entries of any relation set's PlanSet, so they live
  // here for the optimizer's lifetime rather than in the memo map.
  std::vector<std::unique_ptr<MemoOp>> enforcementOps_;
};

} // namespace facebook::axiom::optimizer::v2
