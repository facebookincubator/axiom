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

#include "axiom/optimizer/v2/DPhyp.h"

#include <algorithm>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/small_vector.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/CostModel.h"
#include "axiom/optimizer/v2/GroupedRead.h"
#include "axiom/optimizer/v2/JoinPartitioning.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

DPhyp::DPhyp(
    const JoinHypergraph& graph,
    const CostModel& costModel,
    Builder& builder,
    int64_t enumerationBudget,
    int32_t numWorkers,
    int32_t hashStageTasks,
    int64_t broadcastSizeLimit)
    : graph_{graph},
      costModel_{costModel},
      builder_{builder},
      enumerationBudget_{enumerationBudget},
      numWorkers_{numWorkers},
      hashStageTasks_{hashStageTasks},
      broadcastSizeLimit_{broadcastSizeLimit} {
  VELOX_CHECK_GE(
      graph_.relations().size(),
      2,
      "DPhyp requires at least two relations to enumerate join orders");
}

namespace {

// True for the join types whose output keeps both sides' columns, so an inner
// equality applied above the join can still read them. Semi and anti keep only
// one side, so a co-crossing inner edge has nowhere to go.
bool keepsBothInputs(velox::core::JoinType joinType) {
  return joinType == velox::core::JoinType::kInner ||
      joinType == velox::core::JoinType::kLeft ||
      joinType == velox::core::JoinType::kRight ||
      joinType == velox::core::JoinType::kFull;
}

// Returns the minimum relation id from each reachable opposite hyperedge side,
// provided the whole side is outside both `set` and `forbidden`. The minimum is
// DPhyp's canonical seed for growing that side without emitting duplicates.
RelationSet neighborhood(
    const JoinHypergraph& graph,
    RelationSet set,
    RelationSet forbidden) {
  RelationSet result;
  for (const auto& edge : graph.edges()) {
    RelationSet other;
    if (edge.leftEligibility().isSubset(set) &&
        !edge.rightEligibility().hasIntersection(set)) {
      other = edge.rightEligibility();
    } else if (
        edge.rightEligibility().isSubset(set) &&
        !edge.leftEligibility().hasIntersection(set)) {
      other = edge.leftEligibility();
    } else {
      continue;
    }
    if (other.hasIntersection(forbidden)) {
      continue;
    }
    result.add(other.min());
  }
  return result;
}

// True if the edge's right (build-on-the-preserved-side) orientation is a
// valid Velox plan. Both the kRightSemiProject build-side flip of a
// kLeftSemiProject and the reversed-anti lowering (kRightSemiProject +
// Filter(not mark)) put the edge's residual filter on a kRightSemiProject,
// which Velox forbids for null-aware joins (PlanNode.h). So a null-aware
// edge carrying a filter is single-orientation.
bool hasRightProjectVariant(const JoinEdge& edge) {
  return !(edge.nullAware() && !edge.filter().empty());
}

// Caps DPhyp's connected-subgraph/complement enumeration. DPhyp is polynomial
// in the number of csg/cmp pairs for sparse graphs but exponential for dense
// ones (e.g. a same-key N-way join the transitive-equality closure turns into a
// clique), so the bound caps worst-case planning time; sparse real queries stay
// far below it. A cap <= 0 is unlimited. Once the count exceeds the cap the
// budget trips and stays tripped, so the recursion unwinds and the caller falls
// back to greedy ordering.
class EnumerationBudget {
 public:
  // A cap <= 0 disables the budget (unlimited enumeration).
  explicit EnumerationBudget(int64_t cap) : cap_{cap}, enforced_{cap > 0} {}

  // Counts one csg/cmp pair and returns true once the budget is exceeded (and
  // on every later call), signalling the caller to stop. Always false while
  // disabled or unlimited.
  bool consume() {
    if (!enforced_) {
      return false;
    }
    if (!exceeded_ && ++count_ > cap_) {
      exceeded_ = true;
    }
    return exceeded_;
  }

  // True once the budget has been exceeded; guards the recursion.
  bool exceeded() const {
    return exceeded_;
  }

  // Restarts the count for a fresh (per-component) enumeration.
  void reset() {
    count_ = 0;
    exceeded_ = false;
    enforced_ = cap_ > 0;
  }

  // Stops enforcement; greedy's polynomial GOO loop must run to completion.
  void disable() {
    enforced_ = false;
  }

 private:
  // Pair cap; <= 0 when unlimited (then `enforced_` is false and this is
  // unused).
  const int64_t cap_;
  int64_t count_{0};
  bool exceeded_{false};
  bool enforced_;
};

// Moerkotte/Neumann 2008 DPhyp (csg/cmp recursion).
class Enumerator {
 public:
  // `enumerationBudget` caps the csg/cmp pairs evaluated before falling back to
  // greedy ordering; <= 0 means unlimited. See `EnumerationBudget`.
  Enumerator(
      const JoinHypergraph& graph,
      const CostModel& costModel,
      Builder& builder,
      folly::F14NodeMap<RelationSet, PlanSet>& memo,
      int64_t enumerationBudget,
      int32_t numWorkers,
      int32_t hashStageTasks,
      int64_t broadcastSizeLimit,
      std::vector<std::unique_ptr<MemoOp>>& enforcementOps)
      : graph_{graph},
        costModel_{costModel},
        builder_{builder},
        memo_{memo},
        budget_{enumerationBudget},
        numWorkers_{numWorkers},
        hashStageTasks_{hashStageTasks},
        broadcastSizeLimit_{broadcastSizeLimit},
        enforcementOps_{enforcementOps} {}

  // Runs the DP enumeration over `component` (a connected relation set).
  // Returns true if it gave up after exceeding the enumeration budget (the
  // component is too dense); the caller then plans the component greedily.
  // Resets the budget counters so each component is enumerated — and its budget
  // judged — independently.
  bool solveComponent(RelationSet component) {
    budget_.reset();
    initLeaves(component);
    folly::small_vector<int32_t, 16> ids;
    component.forEach([&](int32_t id) { ids.push_back(id); });
    // Descending id: each (subgraph, complement) pair is emitted once.
    for (auto it = ids.rbegin(); it != ids.rend(); ++it) {
      // A relation an Unnest expands is never the smallest of a set that has a
      // plan, so seeding from it would enumerate only unbuildable sets — and
      // spend enumeration budget doing it.
      if (graph_.expandedRelationIds().contains(*it)) {
        continue;
      }
      const RelationSet subgraph = RelationSet::singleton(*it);
      emitCsg(subgraph);
      enumerateCsgRec(subgraph, RelationSet::prefixThrough(*it));
    }
    return budget_.exceeded();
  }

  // Runs the DP enumeration over the whole graph (a single connected
  // component).
  bool solve() {
    return solveComponent(
        RelationSet::prefixThrough(graph_.relations().back().id()));
  }

  // Greedy operator ordering (GOO) for `component`: repeatedly merge the
  // connected pair of subplans with the smallest join-output cardinality until
  // one remains. Polynomial, used when `solve` overflows the enumeration
  // budget. Cardinality (not plan cost) drives which pair to merge — the
  // classic GOO heuristic of keeping intermediate results small; the
  // build/probe orientation within a chosen pair is still cost-optimal
  // (cardinality is orientation-invariant). Returns the component's root plan,
  // or nullptr if a relation's cardinality is unknown. Reuses any matching
  // subplans already in the memo (all memoized subplans are valid), so a
  // partially-filled memo from an overflowed `solveComponent` is fine.
  MemoOpCP greedy(RelationSet component) {
    budget_.disable();
    initLeaves(component);
    // A join touching a relation of unknown cardinality has unknown cardinality
    // too, so a component with any such relation can never be assembled. Bail
    // before doing merge work; the caller selects a fallback plan.
    bool allCardinalitiesKnown = true;
    component.forEach([&](int32_t id) {
      if (graph_.expandedRelationIds().contains(id)) {
        return;
      }
      const auto it = memo_.find(RelationSet::singleton(id));
      if (it == memo_.end() || !it->second.cardinality().has_value()) {
        allCardinalitiesKnown = false;
      }
    });
    if (!allCardinalitiesKnown) {
      return nullptr;
    }
    // An expanded relation is never a fragment of its own: it is applied to
    // the fragment whose rows it expands.
    std::vector<RelationSet> fragments;
    component.forEach([&](int32_t id) {
      if (!graph_.expandedRelationIds().contains(id)) {
        fragments.push_back(RelationSet::singleton(id));
      }
    });
    while (fragments.size() > 1) {
      std::optional<float> bestCardinality;
      size_t bestLeft = 0;
      size_t bestRight = 0;
      RelationSet bestCombined;
      for (size_t i = 0; i < fragments.size(); ++i) {
        for (size_t j = i + 1; j < fragments.size(); ++j) {
          RelationSet combined{fragments[i]};
          combined.unionSet(fragments[j]);
          emitCsgCmp(fragments[i], fragments[j]);
          const auto it = memo_.find(combined);
          // emitCsgCmp records a pair only when it is valid and costable —
          // considerCandidate drops uncostable plans — so a present entry
          // always has a known cardinality.
          if (it == memo_.end()) {
            continue;
          }
          const float cardinality = *it->second.cardinality();
          if (!bestCardinality.has_value() || cardinality < *bestCardinality) {
            bestCardinality = cardinality;
            bestLeft = i;
            bestRight = j;
            bestCombined = combined;
          }
        }
      }
      if (!bestCardinality.has_value()) {
        // An edge may read columns an Unnest has yet to produce, and until it
        // does nothing connects across it. Expand one fragment and retry.
        if (expandOneFragment(fragments)) {
          continue;
        }
        // No remaining fragment pair produces a valid costable join. The
        // caller selects a fallback plan.
        return nullptr;
      }
      fragments[bestLeft] = bestCombined;
      fragments.erase(fragments.begin() + bestRight);
    }
    // Apply any Unnest no join step consumed on top of the finished tree. An
    // Unnest chain roots at a relation nothing expands, so a fragment remains.
    VELOX_DCHECK_EQ(fragments.size(), 1);
    expandThroughUnnests(fragments.front());
    const auto it = memo_.find(component);
    return it == memo_.end() ? nullptr : it->second.cheapest();
  }

 private:
  // Relations `set` would gain by applying every Unnest whose input it covers,
  // transitively. Answers what an expansion would produce without recording
  // one.
  RelationSet expandableRelations(RelationSet set) const {
    RelationSet gained;
    bool grown{true};
    while (grown) {
      grown = false;
      for (const auto& edge : graph_.edges()) {
        RelationSet reached{set};
        reached.unionSet(gained);
        if (!edge.isUnnest() || !edge.leftEligibility().isSubset(reached) ||
            edge.rightEligibility().isSubset(reached)) {
          continue;
        }
        gained.unionSet(edge.rightEligibility());
        grown = true;
      }
    }
    return gained;
  }

  // Expands the fragment an edge is waiting on: one whose expansion produces
  // relations some edge reads to reach beyond that fragment. Nothing else is
  // expanded, so the rows an expansion multiplies pass through as few joins as
  // possible. Returns true only when a fragment grew, so the caller's retry
  // always follows progress; false says no expansion would connect anything, or
  // none could be costed. Fragments are visited in index order, so the choice
  // is deterministic.
  bool expandOneFragment(std::vector<RelationSet>& fragments) {
    for (auto& fragment : fragments) {
      const RelationSet gained = expandableRelations(fragment);
      if (gained.empty()) {
        continue;
      }
      RelationSet reached{fragment};
      reached.unionSet(gained);
      for (const auto& edge : graph_.edges()) {
        if (edge.isUnnest()) {
          continue;
        }
        const RelationSet edgeTes = edge.totalEligibility();
        // An edge wholly inside the expanded fragment joins nothing new; only
        // one reaching outside can merge this fragment with another.
        if (!edgeTes.hasIntersection(gained) || edgeTes.isSubset(reached)) {
          continue;
        }
        const RelationSet before = fragment;
        expandThroughUnnests(fragment);
        if (fragment != before) {
          return true;
        }
        // The expansion has no valid costable plan. Leave this fragment alone
        // and look for another; expanding it again would not get any further.
        break;
      }
    }
    return false;
  }

  // Applies every Unnest whose input `set` covers, repeatedly, since one
  // expansion can make another's input ready. Grows `set` by the relations
  // expanded and records a plan for each step.
  void expandThroughUnnests(RelationSet& set) {
    bool grown{true};
    while (grown) {
      grown = false;
      for (size_t edgeIndex{0}; edgeIndex < graph_.edges().size();
           ++edgeIndex) {
        const auto& edge = graph_.edges()[edgeIndex];
        if (!edge.isUnnest() || !edge.leftEligibility().isSubset(set) ||
            edge.rightEligibility().isSubset(set)) {
          continue;
        }
        RelationSet expanded{set};
        expanded.unionSet(edge.rightEligibility());
        emitUnnest(set, edgeIndex, expanded, {});
        if (cheapestPlan(expanded) == nullptr) {
          continue;
        }
        set = expanded;
        grown = true;
      }
    }
  }

  // Adds a LeafOp for every relation in `relations` not already in the memo. A
  // relation an Unnest edge expands gets none: it enters a plan only as the
  // expansion of the rows it multiplies.
  void initLeaves(RelationSet relations) {
    relations.forEach([&](int32_t id) {
      if (graph_.expandedRelationIds().contains(id)) {
        return;
      }
      const RelationSet singleton = RelationSet::singleton(id);
      if (memo_.contains(singleton)) {
        return;
      }
      auto leaf = std::make_unique<LeafOp>(
          Cost{},
          static_cast<int8_t>(id),
          graph_.relation(id).node()->physicalProperties().globalPartition);
      leaf->cost = costModel_.cost(leaf.get(), graph_);
      memo_[singleton].addPlan(std::move(leaf), graph_, costModel_);
    });
  }

  void emitCsg(RelationSet subgraph) {
    if (budget_.exceeded()) {
      return;
    }
    const RelationSet forbidden = RelationSet::prefixThrough(subgraph.min());
    const RelationSet neighbors = neighborhood(graph_, subgraph, forbidden);
    // Descending id ensures each (subgraph, complement) pair is emitted once.
    std::vector<int8_t> orderedNeighbors;
    orderedNeighbors.reserve(neighbors.size());
    neighbors.forEach([&](int32_t id) {
      orderedNeighbors.push_back(static_cast<int8_t>(id));
    });
    std::sort(orderedNeighbors.rbegin(), orderedNeighbors.rend());
    for (int8_t vertexId : orderedNeighbors) {
      const RelationSet complement = RelationSet::singleton(vertexId);
      emitCsgCmp(subgraph, complement);
      // Complement must stay disjoint from subgraph; `forbidden`
      // alone does not cover subgraph when emitCsg is invoked with
      // a non-singleton subgraph.
      RelationSet cmpForbidden{forbidden};
      cmpForbidden.unionSet(subgraph);
      RelationSet vertexTail = RelationSet::prefixThrough(vertexId);
      vertexTail.intersect(neighbors);
      cmpForbidden.unionSet(vertexTail);
      enumerateCmpRec(subgraph, complement, cmpForbidden);
    }
  }

  // Cheapest plan for `set`, or null when it has none: a set holding an Unnest
  // relation without the relations it expands is never built, and enumeration
  // walks such sets.
  MemoOpCP cheapestPlan(RelationSet set) const {
    const auto it = memo_.find(set);
    return it == memo_.end() ? nullptr : it->second.cheapest();
  }

  void emitCsgCmp(RelationSet subgraph, RelationSet complement) {
    if (budget_.consume()) {
      return;
    }
    auto crossing = graph_.crossingEdges(subgraph, complement);
    if (!crossing.has_value()) {
      return;
    }
    if (crossing->joinEdges.empty()) {
      return;
    }
    RelationSet combined{subgraph};
    combined.unionSet(complement);
    auto filterEdges = std::move(crossing->filterEdges);

    // Filter edges are evaluated above the primary operator and may reference
    // either input. A semi or anti join discards one input's columns, so its
    // output cannot safely evaluate them.
    const auto canApplyFilterEdges = [&](size_t edgeIndex) {
      const auto& edge = graph_.edges()[edgeIndex];
      return filterEdges.empty() || edge.isUnnest() ||
          keepsBothInputs(edge.joinType());
    };
    if (crossing->joinEdges.size() == 1) {
      if (!canApplyFilterEdges(crossing->joinEdges[0].index)) {
        return;
      }
      applyCrossingEdge(
          crossing->joinEdges[0].index,
          crossing->joinEdges[0].leftToRight,
          subgraph,
          complement,
          combined,
          {},
          std::move(filterEdges));
      return;
    }

    // At most one crossing edge can be other than a plain inner edge. An
    // Unnest expands the rows and an outer join null-pads them; either way the
    // remaining edges can only constrain what it produced, so they filter its
    // output rather than joining its condition — which would null-pad the rows
    // they reject instead of dropping them, and is what applying an inner join
    // above means. Two such edges have no single well-defined step and fall
    // through to the check below.
    std::optional<size_t> specialPosition;
    for (size_t i = 0; i < crossing->joinEdges.size(); ++i) {
      const auto& edge = graph_.edges()[crossing->joinEdges[i].index];
      if (!edge.isUnnest() &&
          edge.joinType() == velox::core::JoinType::kInner) {
        continue;
      }
      if (specialPosition.has_value()) {
        specialPosition.reset();
        break;
      }
      specialPosition = i;
    }

    if (specialPosition.has_value()) {
      const size_t specialEdge = crossing->joinEdges[*specialPosition].index;
      filterEdges.reserve(filterEdges.size() + crossing->joinEdges.size() - 1);
      for (size_t i = 0; i < crossing->joinEdges.size(); ++i) {
        if (i != *specialPosition) {
          filterEdges.push_back(crossing->joinEdges[i].index);
        }
      }
      if (!canApplyFilterEdges(specialEdge)) {
        return;
      }
      applyCrossingEdge(
          specialEdge,
          crossing->joinEdges[*specialPosition].leftToRight,
          subgraph,
          complement,
          combined,
          {},
          std::move(filterEdges));
      return;
    }

    // Several inner edges cross: apply them as one inner join over all their
    // keys. Reaching here with an Unnest or a non-inner edge means two or more
    // of them cross, which has no single well-defined step; fail rather than
    // drop a predicate.
    for (const auto& crossingEdge : crossing->joinEdges) {
      const size_t edgeIndex = crossingEdge.index;
      const auto& edge = graph_.edges()[edgeIndex];
      VELOX_CHECK(
          edge.joinType() == velox::core::JoinType::kInner && !edge.isUnnest(),
          "Two or more edges crossing one join partition are not plain inner joins: {}, unnest: {}",
          edge.joinTypeName(),
          edge.isUnnest());
    }
    MemoOpCP leftPlan = cheapestPlan(subgraph);
    MemoOpCP rightPlan = cheapestPlan(complement);
    if (leftPlan == nullptr || rightPlan == nullptr) {
      return;
    }

    std::vector<size_t> edgeIndices;
    edgeIndices.reserve(crossing->joinEdges.size());
    for (const auto& crossingEdge : crossing->joinEdges) {
      edgeIndices.push_back(crossingEdge.index);
    }
    edgeIndices = graph_.canonicalKeyEdges(std::move(edgeIndices));
    const size_t primary{edgeIndices.front()};
    std::vector<size_t> keyEdges{edgeIndices.begin() + 1, edgeIndices.end()};
    considerCandidate(
        leftPlan,
        rightPlan,
        primary,
        velox::core::JoinType::kInner,
        combined,
        /*reversedAnti=*/false,
        keyEdges,
        filterEdges);
    considerCandidate(
        rightPlan,
        leftPlan,
        primary,
        velox::core::JoinType::kInner,
        combined,
        /*reversedAnti=*/false,
        keyEdges,
        filterEdges);
  }

  // Applies the edge crossing this partition: an Unnest expands the side
  // holding its input, anything else joins the two sides' plans. `forward` is
  // true when the edge's left operand is `subgraph`.
  void applyCrossingEdge(
      size_t edgeIndex,
      bool forward,
      RelationSet subgraph,
      RelationSet complement,
      RelationSet combined,
      std::vector<size_t> keyEdges,
      std::vector<size_t> filterEdges) {
    const auto& edge = graph_.edges()[edgeIndex];
    if (edge.isUnnest()) {
      // Enumeration never seeds a subgraph from the Unnest relation and grows
      // subgraphs from their seed, so the relation reaches a partition only as
      // the complement, alone. A complement holding anything else is a set the
      // enumeration cannot build; expanding it would record a plan under a
      // cover it never assembled.
      if (complement != edge.rightEligibility()) {
        return;
      }
      VELOX_DCHECK(keyEdges.empty());
      emitUnnest(subgraph, edgeIndex, combined, std::move(filterEdges));
      return;
    }

    MemoOpCP leftPlan = cheapestPlan(subgraph);
    MemoOpCP rightPlan = cheapestPlan(complement);
    if (leftPlan == nullptr || rightPlan == nullptr) {
      return;
    }
    emitSingleEdge(
        edgeIndex,
        forward,
        leftPlan,
        rightPlan,
        combined,
        std::move(keyEdges),
        std::move(filterEdges));
  }

  // Adds the candidate that expands `inputSet`'s plan by the Unnest relation
  // of `edgeIndex`. Offered for every input the expansion may sit on, so cost
  // decides where it goes.
  void emitUnnest(
      RelationSet inputSet,
      size_t edgeIndex,
      RelationSet combined,
      std::vector<size_t> filterEdges) {
    MemoOpCP input = cheapestPlan(inputSet);
    if (input == nullptr) {
      return;
    }
    budget_.consume();
    auto candidate = std::make_unique<UnnestOp>(
        Cost{},
        input,
        edgeIndex,
        graph_.edges()[edgeIndex].rightEndpoints(),
        std::move(filterEdges));
    // The memo is keyed by cover, so a candidate filed under a set it does not
    // cover would claim relations no operator below it reads.
    VELOX_DCHECK_EQ(candidate->cover().bits(), combined.bits());
    candidate->cost = costModel_.cost(candidate.get(), graph_);
    if (!candidate->cost.cost.has_value()) {
      return;
    }
    memo_[combined].addPlan(std::move(candidate), graph_, costModel_);
  }

  // Emits join candidates for a single edge crossing the partition,
  // preserving orientation rules (native-only for anti / null-aware
  // semi-project; both orientations otherwise). `keyEdges` become additional
  // join keys; `filterEdges` are applied as a filter above the join.
  void emitSingleEdge(
      size_t edgeIndex,
      bool forward,
      MemoOpCP leftPlan,
      MemoOpCP rightPlan,
      RelationSet combined,
      std::vector<size_t> keyEdges,
      std::vector<size_t> filterEdges) {
    const auto& edge = graph_.edges()[edgeIndex];
    // An additional key edge contributes hash keys and a filter edge becomes
    // a filter above the join. Either way the join must keep every column the
    // equalities read, which inner and outer joins do. Semi and anti keep one
    // side only.
    VELOX_CHECK(
        keyEdges.empty() || edge.joinType() == velox::core::JoinType::kInner,
        "An additional join-key edge alongside a {} edge is not supported",
        edge.joinTypeName());
    VELOX_CHECK(
        filterEdges.empty() || keepsBothInputs(edge.joinType()),
        "A filter edge alongside a {} edge is not supported",
        edge.joinTypeName());
    // `probeOnLeft` plays the edge's left operand; `probeOnRight` its right.
    MemoOpCP probeOnLeft = forward ? leftPlan : rightPlan;
    MemoOpCP probeOnRight = forward ? rightPlan : leftPlan;

    const bool nativeOnly = edge.joinType() == velox::core::JoinType::kAnti ||
        (edge.joinType() == velox::core::JoinType::kLeftSemiProject &&
         !hasRightProjectVariant(edge));
    if (nativeOnly) {
      considerCandidate(
          probeOnLeft,
          probeOnRight,
          edgeIndex,
          edge.joinType(),
          combined,
          /*reversedAnti=*/false,
          std::move(keyEdges),
          std::move(filterEdges));
      // Antijoin can additionally build on the preserved (left) side by
      // probing with the right input (the reversed orientation).
      if (edge.joinType() == velox::core::JoinType::kAnti &&
          hasRightProjectVariant(edge)) {
        considerCandidate(
            probeOnRight,
            probeOnLeft,
            edgeIndex,
            edge.joinType(),
            combined,
            /*reversedAnti=*/true);
      }
      return;
    }

    // Flip the joinType when the subgraph plays the edge's right operand. The
    // Filter edges are equalities evaluated above the output, so they are
    // carried unchanged into either orientation.
    const auto leftTypeForSubgraph =
        forward ? edge.joinType() : Join::swapType(edge.joinType());
    considerCandidate(
        leftPlan,
        rightPlan,
        edgeIndex,
        leftTypeForSubgraph,
        combined,
        /*reversedAnti=*/false,
        keyEdges,
        filterEdges);
    considerCandidate(
        rightPlan,
        leftPlan,
        edgeIndex,
        Join::swapType(leftTypeForSubgraph),
        combined,
        /*reversedAnti=*/false,
        std::move(keyEdges),
        std::move(filterEdges));
  }

  // Records an enforcement exchange and returns a stable pointer to it.
  MemoOpCP makeExchange(MemoOpCP child, Partitioning partitioning, Cost cost) {
    auto op =
        std::make_unique<ExchangeOp>(cost, child, std::move(partitioning));
    MemoOpCP ptr = op.get();
    enforcementOps_.push_back(std::move(op));
    return ptr;
  }

  // Cheapest plan for `cover` in `required`: an already-matching plan if one
  // exists, else the cheapest wrapped in a repartition exchange to `required`.
  // Null when `cover` has no costable plan (e.g. a relation with no stats), so
  // the caller drops the strategy rather than building on an unrankable child.
  MemoOpCP bestOnPartitioning(RelationSet cover, Partitioning required) {
    const auto it = memo_.find(cover);
    if (it == memo_.end()) {
      return nullptr;
    }
    const auto selection =
        it->second.best({.globalPartition = required}, graph_, costModel_);
    if (selection.plan == nullptr) {
      return nullptr;
    }
    if (selection.plan->outputPartitioning().sameClassAs(required)) {
      return selection.plan;
    }
    return makeExchange(selection.plan, std::move(required), selection.cost);
  }

  // Cheapest plan for `cover` standard-hash partitioned on `keys`.
  MemoOpCP repartitioned(
      RelationSet cover,
      const ExprVector& keys,
      bool replicateNullsAndAny) {
    return bestOnPartitioning(
        cover,
        Partitioning::hash(
            PropertyScope::kGlobal,
            keysInCoverSchema(keys, cover),
            replicateNullsAndAny));
  }

  // Cheapest plan for `cover` repartitioned on `keys` using `targetType`
  // (a connector partitioning), so it co-locates with a side already bucketed
  // that way.
  MemoOpCP repartitionedTo(
      RelationSet cover,
      const ExprVector& keys,
      const connector::PartitionType* targetType) {
    return bestOnPartitioning(
        cover,
        Partitioning::connectorHash(
            PropertyScope::kGlobal,
            keysInCoverSchema(keys, cover),
            targetType));
  }

  // Plans for `cover` partitioned on a subset of `keys`. May add a grouped
  // scan when the memo has no connector-bucketed alternative.
  PlanSet::PartitioningAlternatives partitioningsOn(
      RelationSet cover,
      const ExprVector& keys) {
    const auto it = memo_.find(cover);
    if (it == memo_.end()) {
      return {};
    }
    const ExprVector coverKeys = keysInCoverSchema(keys, cover);
    auto alternatives = it->second.partitioningsOnSubsets(coverKeys);
    if (!alternatives.connectorBucketed.empty()) {
      return alternatives;
    }

    // Nothing in the memo is bucketed on these keys, but a single relation
    // whose bucketed tables, read grouped, co-locate them can be read that way.
    // Reading it so costs no more and saves the shuffle, so it is preferred
    // rather than offered for costing. A grouped read only pays off against a
    // shuffle, and considerCandidate takes the single-fragment path before it
    // asks for a bucketed side, so this is never reached with one worker.
    VELOX_DCHECK_GT(numWorkers_, 1);
    if (cover.size() != 1) {
      return alternatives;
    }
    const int32_t relationId = cover.min();
    const Partitioning& groupedPartition = groupedRelationPartition(relationId);
    if (!groupedPartition.coLocates(coverKeys)) {
      return alternatives;
    }

    auto grouped = std::make_unique<LeafOp>(
        Cost{}, static_cast<int8_t>(relationId), groupedPartition);
    grouped->cost = costModel_.cost(grouped.get(), graph_);
    it->second.addPlan(std::move(grouped), graph_, costModel_);
    // Read back rather than keeping the pointer: addPlan drops a plan an
    // existing one dominates, and enumeration asks for this cover repeatedly.
    return it->second.partitioningsOnSubsets(coverKeys);
  }

  // Partitioning relation 'relationId' has when its bucketed tables are read
  // grouped. Computed once per relation: enumeration asks for the same
  // relation under many key sets.
  const Partitioning& groupedRelationPartition(int32_t relationId) {
    auto [it, inserted] = groupedPartitions_.try_emplace(relationId);
    if (inserted) {
      it->second = GroupedRead::partitioning(
          graph_.relation(relationId).node(), numWorkers_, builder_);
    }
    return it->second;
  }

  // Tasks in the stage whose rows are 'partitioning': a bucketed stage runs one
  // per bucket group, a stage fed by a hash exchange runs 'hashStageTasks_',
  // and a gathered stage runs one. Anything else is a scan stage, which runs
  // as many tasks as its splits allow, up to the worker count.
  int32_t stageTasks(const Partitioning& partitioning) const {
    switch (partitioning.kind) {
      case PartitionKind::kPartitioned:
        return partitioning.partitionType != nullptr
            ? partitioning.partitionType->numPartitions()
            : hashStageTasks_;
      case PartitionKind::kGather:
        return 1;
      case PartitionKind::kUnspecified:
      case PartitionKind::kBroadcast:
      case PartitionKind::kArbitrary:
        return numWorkers_;
    }
    VELOX_UNREACHABLE();
  }

  // Cheapest plan for `cover` copied to every task of the stage the probe
  // runs in, whose rows are 'probePartitioning'. A stage on one task receives
  // it by a gather, or by no exchange when the plan is already there. Null
  // when `cover` has no costable plan, or when its estimated size exceeds the
  // broadcast limit (it would not fit in each task's memory) — the caller then
  // relies on the co-partition candidate.
  MemoOpCP replicatedChild(
      RelationSet cover,
      const Partitioning& probePartitioning) {
    const auto it = memo_.find(cover);
    if (it == memo_.end()) {
      return nullptr;
    }
    MemoOpCP base = it->second.cheapest();
    if (base == nullptr) {
      return nullptr;
    }
    if (!CostModel::broadcastSizeIfFits(base, graph_, broadcastSizeLimit_)
             .has_value()) {
      return nullptr;
    }
    const Partitioning replicated =
        Partitioning::globalReplicatedTo(probePartitioning);
    if (replicated.is(PartitionKind::kGather)) {
      return bestOnPartitioning(cover, replicated);
    }
    Cost cost = base->cost;
    cost.cost = add(
        base->cost.cost,
        costModel_.broadcastCost(base, graph_, stageTasks(probePartitioning)));
    return makeExchange(base, Partitioning::globalBroadcast(), cost);
  }

  // Equi-keys oriented to the physical (left, right) children across the
  // primary edge and additional key edges.
  std::pair<ExprVector, ExprVector> orientedKeys(
      MemoOpCP left,
      size_t edgeIndex,
      const std::vector<size_t>& keyEdges) {
    ExprVector leftKeys;
    ExprVector rightKeys;
    const auto collect = [&](const JoinEdge& edge) {
      const bool leftIsLeft = edge.leftEligibility().isSubset(left->cover());
      const auto& edgeLeft = leftIsLeft ? edge.leftKeys() : edge.rightKeys();
      const auto& edgeRight = leftIsLeft ? edge.rightKeys() : edge.leftKeys();
      leftKeys.insert(leftKeys.end(), edgeLeft.begin(), edgeLeft.end());
      rightKeys.insert(rightKeys.end(), edgeRight.begin(), edgeRight.end());
    };
    collect(graph_.edges()[edgeIndex]);
    for (size_t keyEdgeIndex : keyEdges) {
      collect(graph_.edges()[keyEdgeIndex]);
    }
    return {std::move(leftKeys), std::move(rightKeys)};
  }

  // Rewrites each partition key to the representative of its equivalence group
  // within 'cover' — the column that survives in a subplan over 'cover'.
  // Partition keys are taken from the raw edge keys, but an equi-join collapses
  // each equated group to that one representative in its output; without this
  // rewrite a key could name a collapsed-away column the output no longer
  // carries.
  ExprVector keysInCoverSchema(
      const ExprVector& keys,
      const RelationSet& cover) {
    const auto reps = graph_.coverColumnReps(cover);
    ExprVector result;
    result.reserve(keys.size());
    for (ExprCP key : keys) {
      const auto it =
          key->isColumn() ? reps.find(key->as<Column>()) : reps.end();
      result.push_back(it != reps.end() ? it->second : key);
    }
    return result;
  }

  // Output partitioning of a candidate joining these already
  // distribution-enforced children. Each side's keys are resolved in that
  // child's schema, the one its partitioning keys are stated in.
  Partitioning candidateOutputPartitioning(
      MemoOpCP left,
      MemoOpCP right,
      velox::core::JoinType joinType,
      bool reversedAnti,
      size_t edgeIndex,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      RelationSet combined) {
    const ExprVector leftCoverKeys = keysInCoverSchema(leftKeys, left->cover());
    const ExprVector rightCoverKeys =
        keysInCoverSchema(rightKeys, right->cover());
    const auto& edge = graph_.edges()[edgeIndex];
    return Join::outputPartitioning(
        JoinOp::emittedJoinType(joinType, reversedAnti),
        left->outputPartitioning(),
        right->outputPartitioning(),
        leftCoverKeys,
        rightCoverKeys,
        graph_.coverOutputColumns(combined),
        edge.outputColumns(),
        edge.sourceColumns(),
        builder_);
  }

  // Builds, costs, and (if costable) inserts one join candidate with the given
  // output partitioning. The children are already distribution-enforced.
  void addJoinCandidate(
      MemoOpCP left,
      MemoOpCP right,
      size_t edgeIndex,
      velox::core::JoinType joinType,
      RelationSet combined,
      bool reversedAnti,
      std::vector<size_t> keyEdges,
      std::vector<size_t> filterEdges,
      Partitioning outputPartitioning) {
    // One budget unit per candidate, not per csg/cmp pair: at numWorkers>1 a
    // pair fans out into several distribution variants, so candidate count is
    // the work that explodes on a clique.
    budget_.consume();
    auto candidate = std::make_unique<JoinOp>(
        Cost{},
        left,
        right,
        edgeIndex,
        joinType,
        reversedAnti,
        std::move(keyEdges),
        std::move(filterEdges),
        std::move(outputPartitioning));
    candidate->cost = costModel_.cost(candidate.get(), graph_);
    // An uncostable subplan cannot be ranked or built on; drop it.
    if (!candidate->cost.cost.has_value()) {
      return;
    }
    memo_[combined].addPlan(std::move(candidate), graph_, costModel_);
  }

  // Adds a distributed candidate and derives the partitioning its join
  // exposes from its enforced children.
  void addDistributedJoinCandidate(
      MemoOpCP left,
      MemoOpCP right,
      size_t edgeIndex,
      velox::core::JoinType joinType,
      RelationSet combined,
      bool reversedAnti,
      const std::vector<size_t>& keyEdges,
      const std::vector<size_t>& filterEdges,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys) {
    addJoinCandidate(
        left,
        right,
        edgeIndex,
        joinType,
        combined,
        reversedAnti,
        keyEdges,
        filterEdges,
        candidateOutputPartitioning(
            left,
            right,
            joinType,
            reversedAnti,
            edgeIndex,
            leftKeys,
            rightKeys,
            combined));
  }

  // Memo plan and the positions of its partition keys among the join keys.
  struct MemoPartitioningOffer {
    // Memo plan providing the partitioning.
    MemoOpCP plan;

    // The plan's partitioning expressed in join-key positions.
    JoinPartitioning partitioning;
  };

  // Cheapest memo offers for each distinct reusable partitioning.
  struct MemoPartitioningOffers {
    // Cheapest plan for each reusable standard-hash partitioning.
    std::vector<MemoPartitioningOffer> hash;

    // Cheapest plan for each reusable connector partitioning.
    std::vector<MemoPartitioningOffer> connector;

    bool empty() const {
      return hash.empty() && connector.empty();
    }
  };

  // Collects the distinct partitionings that can be retained for this join
  // side. Multiple memo plans with the same partitioning contribute only the
  // cheapest offer.
  MemoPartitioningOffers partitioningOffers(
      MemoOpCP plan,
      const ExprVector& keys) {
    const auto alternatives = partitioningsOn(plan->cover(), keys);
    const ExprVector coverKeys = keysInCoverSchema(keys, plan->cover());
    const auto makeOffers = [&](const std::vector<MemoOpCP>& plans) {
      std::vector<MemoPartitioningOffer> result;
      result.reserve(plans.size());
      for (MemoOpCP candidate : plans) {
        auto partitioning =
            JoinPartitioning::from(candidate->outputPartitioning(), coverKeys);
        VELOX_CHECK(
            partitioning.has_value(),
            "A reusable partitioning must map to the join keys");
        const auto same =
            std::find_if(result.begin(), result.end(), [&](const auto& offer) {
              return offer.partitioning.keyPositions ==
                  partitioning->keyPositions &&
                  offer.partitioning.partitionType ==
                  partitioning->partitionType;
            });
        if (same == result.end()) {
          result.push_back({candidate, std::move(*partitioning)});
        } else if (*candidate->cost.cost < *same->plan->cost.cost) {
          *same = {candidate, std::move(*partitioning)};
        }
      }
      return result;
    };
    return {
        makeOffers(alternatives.standardHash),
        makeOffers(alternatives.connectorBucketed),
    };
  }

  // Adds candidates that retain a side's compatible partitioning and align
  // only the other side. Hash offers cover a subset or permutation of the join
  // keys; connector offers additionally require compatible bucket functions
  // and widths before both can be kept.
  void addCoPartitionedCandidate(
      MemoOpCP left,
      MemoOpCP right,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      size_t edgeIndex,
      velox::core::JoinType joinType,
      RelationSet combined,
      bool reversedAnti,
      const std::vector<size_t>& keyEdges,
      const std::vector<size_t>& filterEdges,
      const std::vector<MemoPartitioningOffer>& leftOffers,
      const std::vector<MemoPartitioningOffer>& rightOffers) {
    if (leftOffers.empty() && rightOffers.empty()) {
      return;
    }

    const auto add = [&](MemoOpCP leftChild, MemoOpCP rightChild) {
      addDistributedJoinCandidate(
          leftChild,
          rightChild,
          edgeIndex,
          joinType,
          combined,
          reversedAnti,
          keyEdges,
          filterEdges,
          leftKeys,
          rightKeys);
    };

    for (const auto& leftOffer : leftOffers) {
      for (const auto& rightOffer : rightOffers) {
        if (leftOffer.partitioning.coPartitionsWith(rightOffer.partitioning)) {
          add(leftOffer.plan, rightOffer.plan);
        }
      }
    }

    const auto addAligned = [&](bool keepLeft,
                                const MemoPartitioningOffer& keptOffer) {
      const ExprVector& movedKeys = keepLeft ? rightKeys : leftKeys;
      const ExprVector alignedKeys =
          keptOffer.partitioning.correspondingKeys(movedKeys);
      const RelationSet movedCover = keepLeft ? right->cover() : left->cover();
      const auto* partitionType =
          keptOffer.plan->outputPartitioning().partitionType;
      MemoOpCP aligned = partitionType == nullptr
          ? repartitioned(movedCover, alignedKeys, false)
          : repartitionedTo(movedCover, alignedKeys, partitionType);
      if (aligned != nullptr) {
        add(keepLeft ? keptOffer.plan : aligned,
            keepLeft ? aligned : keptOffer.plan);
      }
    };
    for (const auto& leftOffer : leftOffers) {
      addAligned(true, leftOffer);
    }
    for (const auto& rightOffer : rightOffers) {
      addAligned(false, rightOffer);
    }
  }

  void considerCandidate(
      MemoOpCP left,
      MemoOpCP right,
      size_t edgeIndex,
      velox::core::JoinType joinType,
      RelationSet combined,
      bool reversedAnti = false,
      std::vector<size_t> keyEdges = {},
      std::vector<size_t> filterEdges = {}) {
    // Single-fragment: children as selected, no exchange, no output
    // partitioning.
    if (numWorkers_ == 1) {
      addJoinCandidate(
          left,
          right,
          edgeIndex,
          joinType,
          combined,
          reversedAnti,
          std::move(keyEdges),
          std::move(filterEdges),
          {});
      return;
    }

    const auto [leftKeys, rightKeys] = orientedKeys(left, edgeIndex, keyEdges);

    // Both inputs already sit on one task, so joining them moves no rows and
    // the result stays there. Offered next to the partitioned strategies below,
    // which would shuffle two single-task inputs apart; cost decides.
    if (left->outputPartitioning().is(PartitionKind::kGather) &&
        right->outputPartitioning().is(PartitionKind::kGather)) {
      addDistributedJoinCandidate(
          left,
          right,
          edgeIndex,
          joinType,
          combined,
          reversedAnti,
          keyEdges,
          filterEdges,
          leftKeys,
          rightKeys);
    }

    if (!leftKeys.empty()) {
      // Partition strategy: co-partition both inputs on the join keys. A join
      // with a preserved side exposes that side's keys for same-key reuse
      // above; a full join exposes the canonical coalesce of each key pair. A
      // null-aware anti/semi join (NOT IN / IN) needs the existence side's null
      // keys on every probe partition; the existence side is the edge's right
      // operand, which may be either physical child depending on orientation.
      const auto& edge = graph_.edges()[edgeIndex];
      const auto addPartitioned = [&](MemoOpCP leftPart, MemoOpCP rightPart) {
        if (leftPart == nullptr || rightPart == nullptr) {
          return;
        }
        addDistributedJoinCandidate(
            leftPart,
            rightPart,
            edgeIndex,
            joinType,
            combined,
            reversedAnti,
            keyEdges,
            filterEdges,
            leftKeys,
            rightKeys);
      };
      if (edge.nullAware()) {
        // Reusing connector bucketing would confine an existence-side null to
        // one bucket, so probe rows in other buckets would miss it.
        const bool existenceOnLeft =
            edge.rightEligibility().isSubset(left->cover());
        const bool existenceOnRight =
            edge.rightEligibility().isSubset(right->cover());
        MemoOpCP leftPart =
            repartitioned(left->cover(), leftKeys, existenceOnLeft);
        MemoOpCP rightPart =
            repartitioned(right->cover(), rightKeys, existenceOnRight);
        addPartitioned(leftPart, rightPart);
      } else {
        const auto leftOffers = partitioningOffers(left, leftKeys);
        const auto rightOffers = partitioningOffers(right, rightKeys);
        addCoPartitionedCandidate(
            left,
            right,
            leftKeys,
            rightKeys,
            edgeIndex,
            joinType,
            combined,
            reversedAnti,
            keyEdges,
            filterEdges,
            leftOffers.hash,
            rightOffers.hash);
        addCoPartitionedCandidate(
            left,
            right,
            leftKeys,
            rightKeys,
            edgeIndex,
            joinType,
            combined,
            reversedAnti,
            keyEdges,
            filterEdges,
            leftOffers.connector,
            rightOffers.connector);

        // Under the current cost model, retaining an offer cannot cost more
        // than shuffling that side, while the other side pays the same shuffle
        // in either candidate.
        // TODO: Restore the shuffle-both candidate when costing accounts for
        // skew from reusing a subset of the join keys.
        if (leftOffers.empty() && rightOffers.empty()) {
          MemoOpCP leftPart = repartitioned(left->cover(), leftKeys, false);
          MemoOpCP rightPart = repartitioned(right->cover(), rightKeys, false);
          addPartitioned(leftPart, rightPart);
        }
      }
    }

    // Replicate the build (right); the probe (left) keeps its partitioning.
    // Only when the join does not preserve the build — replicating a preserved
    // side would emit its rows on every task. A reversed anti join builds on
    // its preserved (left) side, so its build is preserved and cannot be
    // replicated.
    if (Join::canBroadcastBuild(joinType) && !reversedAnti) {
      MemoOpCP replicatedBuild =
          replicatedChild(right->cover(), left->outputPartitioning());
      if (replicatedBuild != nullptr) {
        addDistributedJoinCandidate(
            left,
            replicatedBuild,
            edgeIndex,
            joinType,
            combined,
            reversedAnti,
            keyEdges,
            filterEdges,
            leftKeys,
            rightKeys);
      }
    }
  }

  // Returns subsets of `neighborBits` in popcount-ascending order,
  // breaking same-popcount ties by bit value so the result is
  // fully deterministic. Inline storage covers up to 4 neighbors
  // (2^4 = 16 subsets) without a heap allocation; larger
  // neighborhoods spill to heap transparently.
  using SubsetVector = folly::small_vector<uint64_t, 16>;
  SubsetVector subsetsBySize(uint64_t neighborBits) {
    SubsetVector subsets;
    for (uint64_t subset = neighborBits; subset > 0;
         subset = (subset - 1) & neighborBits) {
      subsets.push_back(subset);
    }
    std::sort(subsets.begin(), subsets.end(), [](uint64_t a, uint64_t b) {
      const int popA = __builtin_popcountll(a);
      const int popB = __builtin_popcountll(b);
      return std::tie(popA, a) < std::tie(popB, b);
    });
    return subsets;
  }

  void enumerateCsgRec(RelationSet subgraph, RelationSet forbidden) {
    if (budget_.exceeded()) {
      return;
    }

    const RelationSet neighbors = neighborhood(graph_, subgraph, forbidden);
    if (neighbors.empty()) {
      return;
    }

    const auto subsets = subsetsBySize(neighbors.bits());
    for (uint64_t subset : subsets) {
      const RelationSet combined = RelationSet{subgraph.bits() | subset};
      if (memo_.find(combined) != memo_.end()) {
        emitCsg(combined);
      }
    }

    RelationSet newForbidden{forbidden};
    newForbidden.unionSet(neighbors);
    for (uint64_t subset : subsets) {
      const RelationSet combined = RelationSet{subgraph.bits() | subset};
      enumerateCsgRec(combined, newForbidden);
    }
  }

  void enumerateCmpRec(
      RelationSet subgraph,
      RelationSet complement,
      RelationSet forbidden) {
    if (budget_.exceeded()) {
      return;
    }

    const RelationSet neighbors = neighborhood(graph_, complement, forbidden);
    if (neighbors.empty()) {
      return;
    }

    const auto subsets = subsetsBySize(neighbors.bits());
    for (uint64_t subset : subsets) {
      const RelationSet grown = RelationSet{complement.bits() | subset};
      if (memo_.find(grown) != memo_.end()) {
        emitCsgCmp(subgraph, grown);
      }
    }

    RelationSet newForbidden{forbidden};
    newForbidden.unionSet(neighbors);
    for (uint64_t subset : subsets) {
      const RelationSet grown = RelationSet{complement.bits() | subset};
      enumerateCmpRec(subgraph, grown, newForbidden);
    }
  }

  const JoinHypergraph& graph_;
  const CostModel& costModel_;
  // Interns the partition keys a candidate's output partitioning names.
  Builder& builder_;
  folly::F14NodeMap<RelationSet, PlanSet>& memo_;
  // Pair budget; <= 0 means unlimited.
  EnumerationBudget budget_;
  // Target task count; > 1 enables remote-exchange candidate generation.
  const int32_t numWorkers_;
  // Task count of a hash-partitioned stage.
  const int32_t hashStageTasks_;
  // Max estimated build size a broadcast candidate may replicate; <= 0
  // disables.
  const int64_t broadcastSizeLimit_;
  // Owner of enforcement exchanges created during candidate generation.
  std::vector<std::unique_ptr<MemoOp>>& enforcementOps_;
  // Grouped-read partitioning per relation; see groupedRelationPartition.
  folly::F14FastMap<int32_t, Partitioning> groupedPartitions_;
};

// Collects the edges `plan` applies.
void collectAppliedEdges(MemoOpCP plan, folly::F14FastSet<size_t>& applied) {
  switch (plan->kind()) {
    case MemoOpKind::kLeaf:
      return;
    case MemoOpKind::kJoin: {
      const auto* join = plan->as<JoinOp>();
      applied.insert(join->edgeIndex);
      applied.insert(join->keyEdges.begin(), join->keyEdges.end());
      applied.insert(join->filterEdges.begin(), join->filterEdges.end());
      collectAppliedEdges(join->left, applied);
      collectAppliedEdges(join->right, applied);
      return;
    }
    case MemoOpKind::kUnnest: {
      const auto* unnest = plan->as<UnnestOp>();
      applied.insert(unnest->edgeIndex);
      applied.insert(unnest->filterEdges.begin(), unnest->filterEdges.end());
      collectAppliedEdges(unnest->input, applied);
      return;
    }
    case MemoOpKind::kExchange:
      collectAppliedEdges(plan->as<ExchangeOp>()->input, applied);
      return;
  }
}

// Throws unless `plan` enforces every equality the query states.
void checkEdgesEnforced(const JoinHypergraph& graph, MemoOpCP plan) {
  if (plan == nullptr) {
    return;
  }
  folly::F14FastSet<size_t> applied;
  collectAppliedEdges(plan, applied);
  graph.checkEdgesEnforced(applied);
}

} // namespace

const MemoOp* DPhyp::enumerate() {
#ifndef NDEBUG
  // The single-component caller requires a connected graph; the disconnected
  // case goes through enumerate(components).
  graph_.checkConsistency();
#endif
  memo_.clear();
  Enumerator enumerator{
      graph_,
      costModel_,
      builder_,
      memo_,
      enumerationBudget_,
      numWorkers_,
      hashStageTasks_,
      broadcastSizeLimit_,
      enforcementOps_};
  const bool budgetExceeded = enumerator.solve();

  RelationSet allRelations;
  for (const auto& relation : graph_.relations()) {
    allRelations.add(relation.id());
  }

  if (budgetExceeded) {
    // The graph is too dense for exhaustive DP; restart greedily.
    memo_.clear();
    MemoOpCP greedyRoot = enumerator.greedy(allRelations);
    checkEdgesEnforced(graph_, greedyRoot);
    return greedyRoot;
  }

  const auto rootIt = memo_.find(allRelations);
  if (rootIt == memo_.end()) {
    // No valid costable plan for the full set. The caller falls back to the
    // query's syntactic join order.
    return nullptr;
  }
  MemoOpCP root = rootIt->second.cheapest();
  checkEdgesEnforced(graph_, root);
  return root;
}

std::vector<MemoOpCP> DPhyp::enumerate(
    const std::vector<RelationSet>& components) {
  memo_.clear();
  Enumerator enumerator{
      graph_,
      costModel_,
      builder_,
      memo_,
      enumerationBudget_,
      numWorkers_,
      hashStageTasks_,
      broadcastSizeLimit_,
      enforcementOps_};

  std::vector<MemoOpCP> roots;
  roots.reserve(components.size());
  for (const RelationSet& component : components) {
    // Enumerate and budget each component independently, so a single dense
    // component falls back to greedy without forcing greedy on the others. The
    // shared memo holds only valid subplans, so greedy reuses whatever DP
    // already produced for the overflowed component.
    MemoOpCP root;
    if (enumerator.solveComponent(component)) {
      root = enumerator.greedy(component);
    } else {
      const auto it = memo_.find(component);
      root = it == memo_.end() ? nullptr : it->second.cheapest();
    }
    if (root == nullptr) {
      // A component has no valid costable plan. Signal whole-cluster fallback
      // by returning empty.
      return {};
    }
    roots.push_back(root);
  }

  // Components partition the relations, so every edge lies inside one of them
  // and the roots together must apply all of them.
  folly::F14FastSet<size_t> applied;
  for (MemoOpCP root : roots) {
    collectAppliedEdges(root, applied);
  }
  graph_.checkEdgesEnforced(applied);

  return roots;
}

} // namespace facebook::axiom::optimizer::v2
