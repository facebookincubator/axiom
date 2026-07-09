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

#include <memory>
#include <optional>
#include <vector>

#include "axiom/optimizer/v2/MemoOp.h"
#include "axiom/optimizer/v2/PhysicalProperties.h"

namespace facebook::axiom::optimizer::v2 {

class CostModel;
class JoinHypergraph;

/// Physical properties a memo consumer requires of the plan it selects from a
/// `PlanSet`. A nullopt field means the consumer accepts any value for it.
///
/// Carries only global partitioning today. Ordering will join it when its first
/// consumer (merge join / merging gather) lands; until then no consumer
/// requires an input order — a top-level `ORDER BY` is a structural `Sort`, and
/// streaming aggregation reads the emitted node's local property — so the slot
/// is omitted rather than left unused.
struct RequiredProperties {
  /// Required global (across-tasks) partitioning, or nullopt for "any".
  std::optional<Partitioning> globalPartition;
};

/// The value the property-aware memo keeps per relation set: a set of physical
/// plans (each a `MemoOp`) that are *non-dominated* on global partitioning. A
/// plan that is pricier on cost survives if it is already partitioned a way a
/// downstream consumer needs, so converting another plan to that partitioning
/// (a remote shuffle) would cost more than the difference.
///
/// At a single worker, candidates carry no partitioning, so the set holds one
/// plan and behaves as the single-best memo. At `numWorkers > 1`, exchange
/// candidate generation creates plans of differing partitioning, which is what
/// exercises the cross-partitioning paths (`addPlan` dominance across classes,
/// `best` enforcement).
///
/// Usage:
///   PlanSet& set = memo[relationSet];
///   set.addPlan(std::move(candidate), graph, costModel);   // shuffle-aware
///   auto [plan, cost] =
///       set.best({.globalPartition = keys}, graph, costModel);   // select
class PlanSet {
 public:
  /// A selected plan together with its cost after any enforcement (a shuffle to
  /// meet the requirement) folded in. `plan` is null when the set is empty.
  struct Selection {
    MemoOpCP plan{nullptr};
    Cost cost;
  };

  /// Inserts `candidate`, keeping the set non-dominated: drops `candidate` if
  /// an existing plan dominates it (cheaper even after a shuffle to convert
  /// into `candidate`'s partitioning), and drops any existing plan `candidate`
  /// dominates. Takes ownership; a dropped `candidate` is freed on return.
  void addPlan(
      std::unique_ptr<MemoOp> candidate,
      const JoinHypergraph& graph,
      const CostModel& costModel);

  /// Selects the plan with the least cost after folding in any shuffle needed
  /// to meet `required` — a plan already in the required partitioning pays
  /// nothing, one in another partitioning pays a remote shuffle. With no
  /// requirement, the cheapest plan. Returns a null `plan` when the set is
  /// empty.
  Selection best(
      const RequiredProperties& required,
      const JoinHypergraph& graph,
      const CostModel& costModel) const;

  /// Cheapest plan regardless of partitioning, or nullptr when empty.
  MemoOpCP cheapest() const;

  /// Cheapest plan whose output is connector-bucketed (`kPartitioned` with a
  /// non-null `partitionType`) on exactly `keys`, or nullptr when none. Used by
  /// the co-bucketed join strategy to find an input that can join co-located
  /// without a shuffle.
  MemoOpCP bestBucketed(const ExprVector& keys) const;

  /// Output cardinality of this relation set — a relation-set fact, identical
  /// across the set's plans. nullopt when empty or unknown.
  std::optional<float> cardinality() const;

  bool empty() const {
    return plans_.empty();
  }

 private:
  std::vector<std::unique_ptr<MemoOp>> plans_;
};

} // namespace facebook::axiom::optimizer::v2
