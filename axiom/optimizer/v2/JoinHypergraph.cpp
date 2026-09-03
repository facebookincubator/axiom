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

#include "axiom/optimizer/v2/JoinHypergraph.h"

#include <utility>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

int8_t JoinHypergraph::addRelation(
    NodeCP node,
    std::optional<float> cardinality,
    PlanObjectSet columns) {
  VELOX_CHECK_LT(
      relations_.size(),
      RelationSet::kMaxRelations,
      "Hypergraph cluster exceeds the per-hypergraph relation cap");
  const int8_t id{static_cast<int8_t>(relations_.size())};
  relations_.emplace_back(id, node, cardinality, std::move(columns));
  relationIds_.add(id);
  invalidateCoverCaches();
  return id;
}

void JoinHypergraph::setTargetColumns(PlanObjectSet targetColumns) {
  targetColumns_ = std::move(targetColumns);
  invalidateCoverCaches();
}

void JoinHypergraph::invalidateCoverCaches() {
  coverOutputColumnsCache_.clear();
}

int8_t JoinHypergraph::addUnnestRelation(
    NodeCP node,
    std::optional<float> cardinality,
    PlanObjectSet columns) {
  VELOX_CHECK_NOT_NULL(node);
  VELOX_CHECK(
      node->nodeType() == NodeType::kUnnest,
      "addUnnestRelation requires an Unnest node");
  const int8_t id = addRelation(node, cardinality, std::move(columns));
  unnestRelationIds_.add(id);
  return id;
}

RelationSet JoinHypergraph::connectedComponent(
    RelationSet seed,
    RelationSet bound) const {
  RelationSet reached{seed};
  bool changed{true};
  while (changed) {
    changed = false;
    for (const auto& edge : edges_) {
      const RelationSet edgeTes = edge.totalEligibility();
      if (!edgeTes.isSubset(bound)) {
        continue;
      }
      if (edgeTes.hasIntersection(reached) && !edgeTes.isSubset(reached)) {
        reached.unionSet(edgeTes);
        changed = true;
      }
    }
  }
  return reached;
}

void JoinHypergraph::checkConsistency() const {
  if (relations_.empty()) {
    return;
  }
  const RelationSet reached =
      connectedComponent(RelationSet::singleton(0), relationIds_);
  VELOX_CHECK_EQ(
      reached.size(), relations_.size(), "Hypergraph is not connected");

  // Every Unnest relation must be reachable through its unnest edge;
  // otherwise the directed-edge connectivity was never built.
  unnestRelationIds_.forEach([&](int32_t id) {
    // An Unnest of a constant has no input in the cluster and so no edge; the
    // connectivity check above covers it.
    if (relation(id).node()->onlyInput()->outputColumns().empty()) {
      return;
    }
    bool found = false;
    for (const auto& edge : edges_) {
      if (edge.isUnnest() && edge.rightEndpoints().contains(id)) {
        found = true;
        break;
      }
    }
    VELOX_CHECK(found, "Unnest relation has no unnest edge: {}", id);
  });
}

void JoinHypergraph::addFilterConjunct(FilterConjunct conjunct) {
  VELOX_CHECK_NOT_NULL(conjunct.expr);
  VELOX_CHECK(
      conjunct.relations.isSubset(relationIds_),
      "Filter conjunct references unknown relations");
  filterConjuncts_.push_back(std::move(conjunct));
  invalidateCoverCaches();
}

namespace {

// Whether a join of `joinType` proves its equi-key columns hold equal values
// on every row it emits, so downstream may substitute either for the other.
//
// Only a join that emits matched rows alone proves it, since an unmatched or
// null-padded row has unequal keys. Emitting one side is enough: the equal
// column it does not emit is recovered by renaming above it.
bool provesKeyEquality(velox::core::JoinType joinType) {
  using velox::core::JoinType;
  return joinType == JoinType::kInner ||
      joinType == JoinType::kCountingLeftSemiFilter;
}

} // namespace

PlanObjectSet JoinHypergraph::coverColumns(const RelationSet& cover) const {
  PlanObjectSet columns;
  cover.forEach([&](int32_t id) { columns.unionSet(relation(id).columns()); });
  return columns;
}

folly::F14FastMap<ColumnCP, ColumnCP> JoinHypergraph::coverColumnReps(
    const RelationSet& cover) const {
  folly::F14FastMap<ColumnCP, ColumnCP> parent;
  coverColumns(cover).forEach<Column>(
      [&](ColumnCP column) { parent[column] = column; });

  auto find = [&](ColumnCP column) {
    while (parent[column] != column) {
      parent[column] = parent[parent[column]];
      column = parent[column];
    }
    return column;
  };

  auto asColumn = [](ExprCP expr) -> ColumnCP {
    return expr->is(PlanType::kColumnExpr) ? expr->as<Column>() : nullptr;
  };

  // A target column is the better representative (its name must reach the
  // cluster root); ties break on column name, then id. Name is used rather than
  // id alone because column ids come from a process-global counter, so id order
  // within an equivalence class depends on allocation history — using it would
  // make the surviving column (and thus the plan) non-reproducible across runs
  // and query forms.
  auto betterRep = [&](ColumnCP a, ColumnCP b) {
    const bool aTarget = targetColumns_.contains(a);
    const bool bTarget = targetColumns_.contains(b);
    if (aTarget != bTarget) {
      return aTarget;
    }
    const std::string_view aName{a->name()};
    const std::string_view bName{b->name()};
    if (aName != bName) {
      return aName < bName;
    }
    return a->id() < b->id();
  };

  for (const auto& edge : edges_) {
    // Only a join that proves its keys equal on every emitted row lets the two
    // columns collapse. Where the join emits one side only, the representative
    // may be the column it does not produce; the emitter projects it back.
    if (!provesKeyEquality(edge.joinType())) {
      continue;
    }
    const RelationSet edgeTes = edge.totalEligibility();
    if (!edgeTes.isSubset(cover)) {
      continue;
    }
    const auto& leftKeys = edge.leftKeys();
    const auto& rightKeys = edge.rightKeys();
    for (size_t i = 0; i < leftKeys.size() && i < rightKeys.size(); ++i) {
      ColumnCP left = asColumn(leftKeys[i]);
      ColumnCP right = asColumn(rightKeys[i]);
      if (left == nullptr || right == nullptr || !parent.contains(left) ||
          !parent.contains(right)) {
        continue;
      }
      ColumnCP rootLeft = find(left);
      ColumnCP rootRight = find(right);
      if (rootLeft == rootRight) {
        continue;
      }
      if (betterRep(rootRight, rootLeft)) {
        std::swap(rootLeft, rootRight);
      }
      parent[rootRight] = rootLeft;
    }
  }

  folly::F14FastMap<ColumnCP, ColumnCP> reps;
  reps.reserve(parent.size());
  for (const auto& [column, _] : parent) {
    reps[column] = find(column);
  }
  return reps;
}

PlanObjectSet JoinHypergraph::coverOutputColumns(
    const RelationSet& cover) const {
  if (auto it = coverOutputColumnsCache_.find(cover);
      it != coverOutputColumnsCache_.end()) {
    return it->second;
  }
  PlanObjectSet neededAbove = targetColumns_;
  for (const auto& edge : edges_) {
    const RelationSet edgeTes = edge.totalEligibility();
    if (!edgeTes.isSubset(cover)) {
      neededAbove.unionColumns(edge.leftKeys());
      neededAbove.unionColumns(edge.rightKeys());
      neededAbove.unionColumns(edge.filter());
      if (edge.isUnnest()) {
        // An Unnest not yet applied reads what it expands from below.
        neededAbove.unionColumns(relation(edge.rightEndpoints().min())
                                     .node()
                                     ->as<Unnest>()
                                     ->unnestExpressions());
      }
    }
  }
  for (const auto& conjunct : filterConjuncts_) {
    if (!conjunct.relations.isSubset(cover)) {
      neededAbove.unionColumns(conjunct.expr);
    }
  }
  PlanObjectSet demand = coverColumns(cover);
  demand.intersect(neededAbove);

  // Collapse each demanded column to its equivalence representative, so a group
  // of provably-equal columns contributes a single output column.
  const auto reps = coverColumnReps(cover);
  PlanObjectSet columns;
  demand.forEach<Column>([&](ColumnCP column) {
    const auto it = reps.find(column);
    columns.add(it != reps.end() ? it->second : column);
  });
  return coverOutputColumnsCache_.emplace(cover, std::move(columns))
      .first->second;
}

void JoinHypergraph::addEdge(JoinEdge edge) {
  const RelationSet eligibility = edge.totalEligibility();
  VELOX_CHECK(
      eligibility.isSubset(relationIds_),
      "Edge eligibility must reference only relations already added: eligibility={}, relations={}",
      eligibility.bits(),
      relationIds_.bits());
  if (edge.isUnnest()) {
    // Relations are registered before the Unnests over them. Enumeration
    // relies on this: a set holding an expanded relation always holds a
    // smaller id, so such a set is never enumerated from the expanded
    // relation as its seed.
    VELOX_CHECK_LT(edge.leftEndpoints().min(), edge.rightEndpoints().min());
    expandedRelationIds_.unionSet(edge.rightEndpoints());
  }
  edges_.push_back(std::move(edge));
  invalidateCoverCaches();
}

} // namespace facebook::axiom::optimizer::v2
