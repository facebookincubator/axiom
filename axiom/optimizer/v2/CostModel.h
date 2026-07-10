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

#include <folly/container/F14Map.h>

#include "axiom/optimizer/v2/Cost.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/JoinHypergraph.h"
#include "axiom/optimizer/v2/MemoOp.h"
#include "axiom/optimizer/v2/RelationSet.h"

namespace facebook::axiom::optimizer::v2 {

/// Pluggable cost source: given a candidate `MemoOp` plan, returns its cost.
/// A node's children have their `cost` fields populated before the node is
/// costed, so an implementation may read them without recomputing. Also prices
/// the distributed-exchange costs (shuffle, broadcast), so a custom model
/// controls the whole cost surface, not just per-op cost.
///
/// A model may cache per-cover estimates for the single hypergraph it is
/// costing, so it is not required to be thread-safe: construct one per
/// (single-threaded) planning run.
class CostModel {
 public:
  virtual ~CostModel() = default;

  /// Returns the cost (and output cardinality) of executing the plan
  /// rooted at `op`. `graph` provides access to the relations and
  /// edges referenced by leaves and joins.
  virtual Cost cost(MemoOpCP op, const JoinHypergraph& graph) const = 0;

  /// Cost of repartitioning `op`'s output with a remote (across-task) exchange.
  /// Returns nullopt when the output cardinality is unknown.
  virtual std::optional<float> shuffleCost(
      MemoOpCP op,
      const JoinHypergraph& graph) const = 0;

  /// Cost of broadcasting `op`'s output to every one of `numWorkers` tasks,
  /// each of which receives a full copy. Returns nullopt when the output
  /// cardinality is unknown.
  virtual std::optional<float> broadcastCost(
      MemoOpCP op,
      const JoinHypergraph& graph,
      int32_t numWorkers) const = 0;

  /// True if `op`'s estimated output fits `limitBytes`, so each task can hold a
  /// full broadcast copy. Unknown cardinality is not broadcastable. A
  /// non-positive `limitBytes` disables the limit (always fits).
  virtual bool broadcastFits(
      MemoOpCP op,
      const JoinHypergraph& graph,
      int64_t limitBytes) const = 0;
};

/// Default cost model used when callers don't supply one. Costs leaves at their
/// cardinality and costs joins with hash-join formulas (build asymmetric
/// in the build-side cardinality, probe asymmetric in the probe-side
/// cardinality).
///
/// Carries a per-cover estimate memo (`bySet_`): as DPhyp costs candidates
/// bottom-up, each cover's cardinality is computed once — for an inner cover,
/// as the product of the base cardinalities and the per-edge selectivities,
/// which is the same regardless of the order the joins are applied — and reused
/// by every later candidate for the same cover. So the cardinality DPhyp costs
/// on does not depend on which join order reached the cover. Leaf estimates are
/// seeded from `estimateProvider`. `RelationSet` ids are cluster-local, so
/// construct one model per cluster.
///
/// Future work: pull row width and column counts from the relations covered by
/// each MemoOp; add `cost()` dispatch for non-join MemoOp kinds (aggregate, the
/// `ExchangeOp` node itself, ...) as those land.
class DefaultCostModel : public CostModel {
 public:
  /// `estimateProvider` supplies the per-node cardinality and column-NDV
  /// estimates used to seed each leaf cover. It is shared across the query's
  /// clusters and must outlive this model.
  explicit DefaultCostModel(EstimateProvider& estimateProvider);

  Cost cost(MemoOpCP op, const JoinHypergraph& graph) const override;

  /// Prices the exchange as `cardinality × output row bytes ×
  /// Costs::kByteShuffleCost`.
  std::optional<float> shuffleCost(MemoOpCP op, const JoinHypergraph& graph)
      const override;

  /// `shuffleCost(op)` times `numWorkers`.
  std::optional<float> broadcastCost(
      MemoOpCP op,
      const JoinHypergraph& graph,
      int32_t numWorkers) const override;

  /// True when `cardinality × output row bytes` is within `limitBytes`.
  bool broadcastFits(
      MemoOpCP op,
      const JoinHypergraph& graph,
      int64_t limitBytes) const override;

 private:
  EstimateProvider& estimateProvider_;

  // Per-cover estimate (see class doc): the cover's cardinality and its refined
  // per-column NDVs, so a join key that is a post-filter column or an aggregate
  // output is costed from a derived NDV rather than the unknown base value.
  // Mutable because `cost` is const; safe under the single-threaded planning
  // assumption.
  mutable folly::F14FastMap<RelationSet, Estimate> bySet_;
};

} // namespace facebook::axiom::optimizer::v2
