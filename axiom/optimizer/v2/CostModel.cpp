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

#include "axiom/optimizer/v2/CostModel.h"

#include <folly/container/F14Set.h>

#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/JoinFanout.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

using optimizer::add;
using optimizer::maxOf;
using optimizer::mul;

using BySetMap = folly::F14FastMap<RelationSet, Estimate>;

// Reads the memoized estimate for a child cover. DPhyp costs children before
// their parents, so a join's child covers are always present when the join is
// costed.
const Estimate& coverEstimate(const RelationSet& cover, const BySetMap& bySet) {
  const auto it = bySet.find(cover);
  VELOX_CHECK(
      it != bySet.end(), "Child cover estimate missing during join costing");
  return it->second;
}

// Merges 'from' into 'into', keeping existing entries. Sources have disjoint
// column-id spaces (a join's two child covers, or an unnest's single input), so
// a collision would only re-insert an equal Value.
void mergeConstraints(const ConstraintMap& from, ConstraintMap& into) {
  for (const auto& [id, value] : from) {
    into.emplace(id, value);
  }
}

// Row count of one side of an edge: the product of the cardinalities of the
// leaf relations the side spans. Fixed per relation (leaf estimates), so it is
// independent of join order. Unknown if any relation's cardinality is unknown.
std::optional<float> sideRowCount(
    const RelationSet& side,
    const JoinHypergraph& graph) {
  std::optional<float> rows{1.0f};
  side.forEach(
      [&](int32_t id) { rows = mul(rows, graph.relation(id).cardinality()); });
  return rows;
}

// Selectivity of an inner equi-edge relative to the cross product of its
// operands: 1 / max(jointNdvLeft, jointNdvRight), where each side's joint key
// NDV is the product of its per-key NDVs capped at the side's row count (a side
// cannot have more distinct key tuples than rows — the cap keeps a correlated
// composite key from being over-counted). The cap is the leaf relations' row
// count, a fixed per-relation value, so a cover's cardinality — the product of
// its operands' cardinalities and its internal edges' selectivities — stays
// independent of the order the edges are applied. Key NDVs come from
// `constraints` (the cover's refined per-column Values — post-filter, or the
// derived NDV of an aggregate output) with the column's base `value()` as
// fallback. An unknown key NDV makes it unknown; a zero-NDV key makes it 0
// (empty join).
std::optional<float> innerEdgeSelectivity(
    const JoinEdge& edge,
    const JoinHypergraph& graph,
    const ConstraintMap& constraints) {
  const auto& leftKeys = edge.leftKeys();
  const auto& rightKeys = edge.rightKeys();
  float jointNdvLeft{1};
  float jointNdvRight{1};
  for (size_t i = 0; i < leftKeys.size(); ++i) {
    const std::optional<float> leftNdv =
        value(constraints, leftKeys[i]).cardinality;
    const std::optional<float> rightNdv =
        value(constraints, rightKeys[i]).cardinality;
    if (!leftNdv.has_value() || !rightNdv.has_value()) {
      return std::nullopt;
    }
    if (*leftNdv == 0 || *rightNdv == 0) {
      return 0.0f;
    }
    jointNdvLeft *= *leftNdv;
    jointNdvRight *= *rightNdv;
  }
  if (const auto leftRows = sideRowCount(edge.left(), graph)) {
    jointNdvLeft = std::min(jointNdvLeft, *leftRows);
  }
  if (const auto rightRows = sideRowCount(edge.right(), graph)) {
    jointNdvRight = std::min(jointNdvRight, *rightRows);
  }
  return 1.0f / std::max(jointNdvLeft, jointNdvRight);
}

// Product of the operand cardinalities and the selectivities of the edges
// crossing the join (its primary edge plus any inner extra edges of a cyclic
// cover). This is the inner-join match count, and — for an all-inner cover —
// the cover's output cardinality.
std::optional<float> innerMatchCardinality(
    const JoinOp& join,
    const JoinHypergraph& graph,
    const ConstraintMap& constraints,
    std::optional<float> leftCardinality,
    std::optional<float> rightCardinality) {
  std::optional<float> matched = mul(leftCardinality, rightCardinality);
  matched = mul(
      matched,
      innerEdgeSelectivity(graph.edges()[join.edgeIndex], graph, constraints));
  for (size_t extraIndex : join.extraEdges) {
    matched = mul(
        matched,
        innerEdgeSelectivity(graph.edges()[extraIndex], graph, constraints));
  }
  return matched;
}

// Estimated output row width of a subplan covering `cover`: the sum of the byte
// widths (`Value::byteSize`) of `coverOutputColumns`. That set
// already carries one representative per equivalence group equated within the
// cover, so equal columns are counted once — and exactly as the emitter
// materializes them.
float coveredRowBytes(const RelationSet& cover, const JoinHypergraph& graph) {
  float bytes{0};
  graph.coverOutputColumns(cover).forEach<Column>(
      [&](const Column* column) { bytes += column->value().byteSize(); });
  return bytes;
}

// Records the leaf's estimate: its subtree cardinality and post-filter column
// NDVs.
const Estimate& leafEstimate(
    const LeafOp& leaf,
    const JoinHypergraph& graph,
    EstimateProvider& estimateProvider,
    BySetMap& bySet) {
  const RelationSet cover = leaf.cover();
  const auto it = bySet.find(cover);
  if (it != bySet.end()) {
    return it->second;
  }
  const auto* node = graph.relation(leaf.relationId).node();
  return bySet.emplace(cover, estimateProvider.estimate(node)).first->second;
}

Cost leafCost(
    const LeafOp& leaf,
    const JoinHypergraph& graph,
    EstimateProvider& estimateProvider,
    BySetMap& bySet) {
  const auto& estimate = leafEstimate(leaf, graph, estimateProvider, bySet);
  Cost out;
  out.cardinality = estimate.cardinality;
  // A leaf's cost is its cardinality; an unknown cardinality is an unknown
  // cost.
  out.cost = estimate.cardinality;
  return out;
}

// Records the estimate for a cross-join-unnest cover: the input cover's
// constraints pass through (the unnest adds columns but does not change the
// input columns' NDVs) and its cardinality scales by the unnest fanout.
const Estimate& unnestEstimate(const JoinOp& join, BySetMap& bySet) {
  const RelationSet cover = join.cover();
  const auto it = bySet.find(cover);
  if (it != bySet.end()) {
    return it->second;
  }
  const Estimate& input = coverEstimate(join.left->cover(), bySet);
  Estimate result;
  mergeConstraints(input.constraints, result.constraints);
  result.cardinality =
      maxOf(1.0f, mul(input.cardinality, kDefaultUnnestFanout));
  return bySet.emplace(cover, std::move(result)).first->second;
}

// Cost for a JoinOp wrapping a directed cross-join-unnest edge.
// `join->left` is the input-side plan (the orientation enforcement in
// DPhyp guarantees this); cardinality scales by the unnest fanout.
Cost unnestJoinCost(
    const JoinOp& join,
    const JoinHypergraph& graph,
    BySetMap& bySet) {
  using optimizer::Costs;
  const std::optional<float> inputCardinality = join.left->cost.cardinality;
  const float rowBytes = coveredRowBytes(join.cover(), graph);
  Cost out;
  out.cardinality = unnestEstimate(join, bySet).cardinality;
  // The per-row cost depends on the output cardinality; when that is unknown
  // the whole cost is unknown (propagated, not fabricated).
  const std::optional<float> perRowCost = out.cardinality.has_value()
      ? std::optional<float>{Costs::hashRowCost(*out.cardinality, rowBytes)}
      : std::nullopt;
  out.cost =
      add(add(join.left->cost.cost, mul(inputCardinality, perRowCost)),
          mul(out.cardinality, perRowCost));
  return out;
}

// Records the output cardinality for an equi-join cover. For an inner cover
// this is the product of the operand cardinalities and the crossing edges'
// selectivities, which is the same regardless of the order the joins are
// applied. For outer/semi/anti the per-join-type formula is applied to the
// preserved side.
//
// TODO: For covers of three or more relations whose join is outer/semi/anti,
// the preserved side and the "last" edge depend on how the candidate splits the
// cover, so the estimate is not yet independent of that split; computing it
// from a fixed decomposition of the cover would remove the dependence.
const Estimate&
joinEstimate(const JoinOp& join, const JoinHypergraph& graph, BySetMap& bySet) {
  const RelationSet cover = join.cover();
  const auto it = bySet.find(cover);
  if (it != bySet.end()) {
    return it->second;
  }
  const Estimate& left = coverEstimate(join.left->cover(), bySet);
  const Estimate& right = coverEstimate(join.right->cover(), bySet);
  const auto& edge = graph.edges()[join.edgeIndex];

  Estimate result;
  // Combine the child covers' refined constraints so a join key that is an
  // aggregate output (or a post-filter column) is costed from its derived NDV
  // rather than the unknown base value.
  mergeConstraints(left.constraints, result.constraints);
  mergeConstraints(right.constraints, result.constraints);

  const std::optional<float> matched = innerMatchCardinality(
      join, graph, result.constraints, left.cardinality, right.cardinality);
  if (edge.joinType() == velox::core::JoinType::kInner) {
    result.cardinality = maxOf(1.0f, matched);
  } else {
    const bool edgeLeftIsPhysicalLeft =
        edge.left().isSubset(join.left->cover());
    result.cardinality = maxOf(
        1.0f,
        JoinFanout::outputCardinality(
            edge.joinType(),
            edgeLeftIsPhysicalLeft ? left.cardinality : right.cardinality,
            edgeLeftIsPhysicalLeft ? right.cardinality : left.cardinality,
            matched));
  }
  return bySet.emplace(cover, std::move(result)).first->second;
}

Cost hashJoinCost(
    const JoinOp& join,
    const JoinHypergraph& graph,
    BySetMap& bySet) {
  using optimizer::Costs;
  const auto& edge = graph.edges()[join.edgeIndex];

  // A join may apply several inner edges at once (cyclic join graph). DPhyp has
  // already dropped edges made redundant by transitive equality, so every
  // remaining edge restricts further. Collect all equi-key pairs, oriented to
  // the physical left/right subplans so the joint-NDV cap uses the matching
  // side cardinality.
  ExprVector leftKeys;
  ExprVector rightKeys;
  const auto addEdgeKeys = [&](const JoinEdge& edgeToAdd) {
    const bool leftIsLeft = edgeToAdd.left().isSubset(join.left->cover());
    const auto& edgeLeftKeys =
        leftIsLeft ? edgeToAdd.leftKeys() : edgeToAdd.rightKeys();
    const auto& edgeRightKeys =
        leftIsLeft ? edgeToAdd.rightKeys() : edgeToAdd.leftKeys();
    leftKeys.insert(leftKeys.end(), edgeLeftKeys.begin(), edgeLeftKeys.end());
    rightKeys.insert(
        rightKeys.end(), edgeRightKeys.begin(), edgeRightKeys.end());
  };
  addEdgeKeys(edge);
  for (size_t extraIndex : join.extraEdges) {
    addEdgeKeys(graph.edges()[extraIndex]);
  }

  // Pure non-equi edges have no keys; treat them as if they had one
  // so the per-key cost terms still contribute.
  const auto numKeys = static_cast<float>(std::max<size_t>(1, leftKeys.size()));
  // Build-side (right) output row width.
  const float rowBytes = coveredRowBytes(join.right->cover(), graph);
  const std::optional<float> leftCard = join.left->cost.cardinality;
  const std::optional<float> rightCard = join.right->cost.cardinality;

  Cost out;
  out.cardinality = joinEstimate(join, graph, bySet).cardinality;

  // Per-row hash-build and probe cost formulas. Each term that
  // reads a cardinality is unknown when that cardinality is unknown.
  if (!rightCard.has_value()) {
    // Build and probe unit costs both depend on the right (build) side; an
    // unknown build cardinality makes the join cost unknown.
    out.cost = std::nullopt;
    return out;
  }
  const float buildUnitCost = Costs::hashBuildCost(*rightCard, rowBytes) +
      numKeys * 2 * Costs::kHashColumnCost + numKeys * Costs::kKeyCompareCost;
  // The probe key-compare term scales by min(1, output cardinality); when the
  // output cardinality is unknown that term — and so the probe unit cost — is
  // unknown.
  const std::optional<float> probeUnitCost = out.cardinality.has_value()
      ? std::optional<
            float>{Costs::hashTableCost(*rightCard) + numKeys * Costs::kHashColumnCost + numKeys * Costs::kKeyCompareCost * std::min<float>(1, *out.cardinality)}
      : std::nullopt;
  const float rowUnitCost = Costs::hashRowCost(*rightCard, rowBytes);

  // Left-to-right association matches the original flat sum bit-for-bit when
  // every term is known, so build-side selection is unchanged in that case.
  out.cost =
      add(add(add(add(join.left->cost.cost, join.right->cost.cost),
                  mul(*rightCard, buildUnitCost)),
              mul(leftCard, probeUnitCost)),
          mul(out.cardinality, rowUnitCost));
  return out;
}

} // namespace

std::optional<float> DefaultCostModel::shuffleCost(
    MemoOpCP op,
    const JoinHypergraph& graph) const {
  return mul(
      op->cost.cardinality,
      coveredRowBytes(op->cover(), graph) * Costs::kByteShuffleCost);
}

std::optional<float> DefaultCostModel::broadcastCost(
    MemoOpCP op,
    const JoinHypergraph& graph,
    int32_t numWorkers) const {
  return mul(shuffleCost(op, graph), static_cast<float>(numWorkers));
}

bool DefaultCostModel::broadcastFits(
    MemoOpCP op,
    const JoinHypergraph& graph,
    int64_t limitBytes) const {
  if (limitBytes <= 0) {
    return true;
  }
  if (!op->cost.cardinality.has_value()) {
    return false;
  }
  return *op->cost.cardinality * coveredRowBytes(op->cover(), graph) <=
      static_cast<float>(limitBytes);
}

DefaultCostModel::DefaultCostModel(EstimateProvider& estimateProvider)
    : estimateProvider_{estimateProvider} {}

Cost DefaultCostModel::cost(MemoOpCP op, const JoinHypergraph& graph) const {
  switch (op->kind()) {
    case MemoOpKind::kLeaf:
      return leafCost(*op->as<LeafOp>(), graph, estimateProvider_, bySet_);
    case MemoOpKind::kJoin: {
      const auto* join = op->as<JoinOp>();
      const auto& edge = graph.edges()[join->edgeIndex];
      if (edge.isUnnest()) {
        return unnestJoinCost(*join, graph, bySet_);
      }
      return hashJoinCost(*join, graph, bySet_);
    }
    case MemoOpKind::kExchange:
      VELOX_UNREACHABLE("ExchangeOp cost is set at construction");
  }
  VELOX_UNREACHABLE();
}

} // namespace facebook::axiom::optimizer::v2
