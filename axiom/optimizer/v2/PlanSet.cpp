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

#include "axiom/optimizer/v2/PlanSet.h"

#include <tuple>

#include "axiom/optimizer/v2/CostModel.h"
#include "axiom/optimizer/v2/JoinHypergraph.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Total order over plans so dominance and selection are independent of memo
// insertion order. Plans in one set cover the same relation set; the tuple
// distinguishes a join, an exchange wrapping a plan, or a lone leaf.
using TieKey = std::tuple<uint64_t, uint64_t, size_t, int, int>;
TieKey tieKey(MemoOpCP op) {
  if (op->is(MemoOpKind::kJoin)) {
    const auto* join = op->as<JoinOp>();
    return {
        join->left->cover().bits(),
        join->right->cover().bits(),
        join->edgeIndex,
        static_cast<int>(join->joinType),
        static_cast<int>(join->reversedAnti)};
  }
  if (op->is(MemoOpKind::kExchange)) {
    const auto* exchange = op->as<ExchangeOp>();
    const auto& partitioning = exchange->outputPartitioning();
    return {
        exchange->input->cover().bits(),
        0,
        partitioning.keys.size(),
        static_cast<int>(partitioning.kind),
        1};
  }
  return {static_cast<uint64_t>(op->as<LeafOp>()->relationId), 0, 0, 0, 0};
}

// Cost of producing `target` partitioning from `op`: `op`'s own cost when it is
// already in `target`, else plus a remote shuffle. nullopt when the shuffle
// cost is unknown (so the conversion cannot be ranked). `op`'s cost is known by
// the PlanSet precondition (uncostable candidates never enter the set).
std::optional<float> costAsPartitioning(
    MemoOpCP op,
    const Partitioning& target,
    const JoinHypergraph& graph,
    const CostModel& costModel) {
  const float base = *op->cost.cost;
  if (op->outputPartitioning().sameClassAs(target)) {
    return base;
  }
  const std::optional<float> shuffle = costModel.shuffleCost(op, graph);
  if (!shuffle.has_value()) {
    return std::nullopt;
  }
  return base + *shuffle;
}

} // namespace

void PlanSet::addPlan(
    std::unique_ptr<MemoOp> candidate,
    const JoinHypergraph& graph,
    const CostModel& costModel) {
  // An uncostable plan cannot be ranked. Only a leaf with unknown cardinality
  // reaches here (uncostable joins are dropped before insertion), and a
  // relation has exactly one leaf, so keep it as the lone incomparable entry;
  // downstream cardinality checks then bail on the unknown cost.
  if (!candidate->cost.cost.has_value()) {
    plans_.push_back(std::move(candidate));
    return;
  }

  const Partitioning& candidatePartition = candidate->outputPartitioning();
  const float candidateCost = *candidate->cost.cost;
  const TieKey candidateKey = tieKey(candidate.get());

  // Drop the candidate if an existing plan reaches the candidate's partitioning
  // at no greater cost (cheaper, or equal with a smaller tiebreak).
  for (const auto& existing : plans_) {
    const std::optional<float> asCandidate = costAsPartitioning(
        existing.get(), candidatePartition, graph, costModel);
    if (!asCandidate.has_value()) {
      continue;
    }
    if (*asCandidate < candidateCost ||
        (*asCandidate == candidateCost &&
         tieKey(existing.get()) <= candidateKey)) {
      return;
    }
  }

  // The candidate survives; drop every existing plan it now dominates.
  std::erase_if(plans_, [&](const std::unique_ptr<MemoOp>& existing) {
    const std::optional<float> asExisting = costAsPartitioning(
        candidate.get(), existing->outputPartitioning(), graph, costModel);
    if (!asExisting.has_value()) {
      return false;
    }
    const float existingCost = *existing->cost.cost;
    return *asExisting < existingCost ||
        (*asExisting == existingCost && candidateKey < tieKey(existing.get()));
  });

  plans_.push_back(std::move(candidate));
}

PlanSet::Selection PlanSet::best(
    const RequiredProperties& required,
    const JoinHypergraph& graph,
    const CostModel& costModel) const {
  Selection result;
  std::optional<float> bestCost;
  TieKey bestKey;
  for (const auto& plan : plans_) {
    std::optional<float> effective;
    if (!required.globalPartition.has_value() ||
        plan->outputPartitioning().sameClassAs(*required.globalPartition)) {
      effective = plan->cost.cost;
    } else if (plan->cost.cost.has_value()) {
      const std::optional<float> shuffle =
          costModel.shuffleCost(plan.get(), graph);
      if (shuffle.has_value()) {
        effective = *plan->cost.cost + *shuffle;
      }
    }
    if (!effective.has_value()) {
      continue;
    }
    const TieKey key = tieKey(plan.get());
    if (result.plan == nullptr || *effective < *bestCost ||
        (*effective == *bestCost && key < bestKey)) {
      bestCost = effective;
      bestKey = key;
      result.plan = plan.get();
      result.cost = plan->cost;
      result.cost.cost = effective;
    }
  }
  return result;
}

MemoOpCP PlanSet::cheapest() const {
  // Hot path: until exchange candidates create differing partitionings every
  // set holds one plan, so skip the ranking scan. Returns the lone plan even if
  // uncostable (a leaf with unknown cardinality), matching the all-uncostable
  // fallback below.
  if (plans_.size() == 1) {
    return plans_.front().get();
  }
  MemoOpCP result = nullptr;
  std::optional<float> bestCost;
  TieKey bestKey;
  for (const auto& plan : plans_) {
    if (!plan->cost.cost.has_value()) {
      continue;
    }
    const float cost = *plan->cost.cost;
    const TieKey key = tieKey(plan.get());
    if (result == nullptr || cost < *bestCost ||
        (cost == *bestCost && key < bestKey)) {
      bestCost = cost;
      bestKey = key;
      result = plan.get();
    }
  }
  // All entries uncostable: a lone leaf with unknown cardinality. Return it so
  // callers can build on it and drop the result as uncostable.
  if (result == nullptr && !plans_.empty()) {
    return plans_.front().get();
  }
  return result;
}

MemoOpCP PlanSet::bestBucketed(const ExprVector& keys) const {
  MemoOpCP result{nullptr};
  for (const auto& plan : plans_) {
    if (!plan->cost.cost.has_value() ||
        !plan->outputPartitioning().isBucketedOn(keys)) {
      continue;
    }
    if (result == nullptr || *plan->cost.cost < *result->cost.cost) {
      result = plan.get();
    }
  }
  return result;
}

std::optional<float> PlanSet::cardinality() const {
  if (plans_.empty()) {
    return std::nullopt;
  }
  return plans_.front()->cost.cardinality;
}

} // namespace facebook::axiom::optimizer::v2
