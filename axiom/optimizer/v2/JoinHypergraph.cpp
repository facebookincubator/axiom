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
      RelationSet endpoints{edge.left()};
      endpoints.unionSet(edge.right());
      if (!endpoints.isSubset(bound)) {
        continue;
      }
      if (endpoints.hasIntersection(reached) && !endpoints.isSubset(reached)) {
        reached.unionSet(endpoints);
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

  // Every Unnest relation must be reachable through its cross-join-unnest
  // edge; otherwise the directed-edge connectivity was never built.
  unnestRelationIds_.forEach([&](int32_t id) {
    bool found = false;
    for (const auto& edge : edges_) {
      if (edge.isUnnest() && edge.right().contains(id)) {
        found = true;
        break;
      }
    }
    VELOX_CHECK(found, "Unnest relation has no cross-join-unnest edge: {}", id);
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
    // Only an inner join makes its key columns equal in the output: across an
    // outer join the null-padded side's key is NULL, not equal to the
    // preserved side, so those columns must not be collapsed.
    if (edge.joinType() != velox::core::JoinType::kInner) {
      continue;
    }
    RelationSet endpoints{edge.left()};
    endpoints.unionSet(edge.right());
    if (!endpoints.isSubset(cover)) {
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
    RelationSet endpoints{edge.left()};
    endpoints.unionSet(edge.right());
    if (!endpoints.isSubset(cover)) {
      neededAbove.unionColumns(edge.leftKeys());
      neededAbove.unionColumns(edge.rightKeys());
      neededAbove.unionColumns(edge.filter());
      neededAbove.unionColumns(edge.unnestExprs());
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

void JoinHypergraph::addEdge(JoinEdge edge, RelationSet tes) {
  RelationSet touched{edge.left()};
  touched.unionSet(edge.right());
  VELOX_CHECK(touched.isSubset(tes), "Edge endpoints must be a subset of TES");
  VELOX_CHECK(
      tes.isSubset(relationIds_),
      "TES must reference only relations already added");
  edges_.push_back(std::move(edge));
  tes_.push_back(tes);
  invalidateCoverCaches();
}

} // namespace facebook::axiom::optimizer::v2
