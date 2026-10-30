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

#include "axiom/optimizer/JoinHypergraph.h"

#include <algorithm>
#include <utility>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer {

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

std::optional<JoinHypergraph::CrossingEdges> JoinHypergraph::crossingEdges(
    const RelationSet& left,
    const RelationSet& right) const {
  RelationSet combined{left};
  combined.unionSet(right);
  CrossingEdges result;
  for (size_t edgeIndex{0}; edgeIndex < edges_.size(); ++edgeIndex) {
    const auto& edge = edges_[edgeIndex];
    const RelationSet edgeRelations = edge.totalEligibility();
    if (edgeRelations.isSubset(left) || edgeRelations.isSubset(right) ||
        !edgeRelations.isSubset(combined)) {
      continue;
    }

    const bool forward = edge.leftEligibility().isSubset(left) &&
        edge.rightEligibility().isSubset(right);
    const bool reverse = edge.leftEligibility().isSubset(right) &&
        edge.rightEligibility().isSubset(left);
    if (forward || reverse) {
      result.joinEdges.push_back({edgeIndex, forward});
    } else if (
        !edge.isUnnest() && edge.joinType() == velox::core::JoinType::kInner) {
      result.filterEdges.push_back(edgeIndex);
    } else {
      return std::nullopt;
    }
  }
  return result;
}

std::vector<size_t> JoinHypergraph::canonicalKeyEdges(
    std::vector<size_t> edgeIndices) const {
  std::ranges::sort(edgeIndices);
  folly::F14FastSet<EquivalenceP> seenClasses;
  const auto pairClass = [](const JoinEdge& edge,
                            size_t keyIndex) -> EquivalenceP {
    ExprCP left = edge.leftKeys()[keyIndex];
    ExprCP right = edge.rightKeys()[keyIndex];
    if (!left->isColumn() || !right->isColumn()) {
      return nullptr;
    }
    const EquivalenceP equivalenceClass = left->as<Column>()->equivalence();
    return equivalenceClass == right->as<Column>()->equivalence()
        ? equivalenceClass
        : nullptr;
  };

  std::vector<size_t> result;
  result.reserve(edgeIndices.size());
  for (size_t edgeIndex : edgeIndices) {
    const JoinEdge& edge = edges_[edgeIndex];
    VELOX_CHECK(
        edge.joinType() == velox::core::JoinType::kInner && !edge.isUnnest(),
        "Canonical key edge is not a plain inner join: {}",
        edge.joinTypeName());
    bool redundant{!edge.leftKeys().empty()};
    for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
      const EquivalenceP equivalenceClass = pairClass(edge, i);
      if (equivalenceClass == nullptr ||
          !seenClasses.contains(equivalenceClass)) {
        redundant = false;
      }
    }
    if (redundant) {
      continue;
    }
    result.push_back(edgeIndex);
    for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
      if (const EquivalenceP equivalenceClass = pairClass(edge, i)) {
        seenClasses.insert(equivalenceClass);
      }
    }
  }
  return result;
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

// Tracks the transitive closure of equalities over pointer-identity values.
// The comparator overload keeps the preferred representative when sets merge.
template <typename T>
class EquivalenceClosure {
 public:
  void add(T value) {
    parents_.try_emplace(value, value);
  }

  T find(T value) {
    add(value);
    T root = value;
    while (parents_[root] != root) {
      root = parents_[root];
    }
    while (value != root) {
      T next = parents_[value];
      parents_[value] = root;
      value = next;
    }
    return root;
  }

  void merge(T left, T right) {
    merge(left, right, [](T, T) { return false; });
  }

  template <typename Better>
  void merge(T left, T right, Better better) {
    left = find(left);
    right = find(right);
    if (left == right) {
      return;
    }
    if (better(right, left)) {
      std::swap(left, right);
    }
    parents_[right] = left;
  }

 private:
  folly::F14FastMap<T, T> parents_;
};

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
  for (const auto& edge : edges_) {
    if (!edge.totalEligibility().isSubset(cover)) {
      continue;
    }
    for (size_t i = 0; i < edge.outputColumns().size(); ++i) {
      if (edge.outputColumns()[i] != edge.sourceColumns()[i]) {
        columns.add(edge.outputColumns()[i]);
      }
    }
  }
  return columns;
}

folly::F14FastMap<ColumnCP, ColumnCP> JoinHypergraph::coverColumnReps(
    const RelationSet& cover) const {
  const PlanObjectSet columns = coverColumns(cover);
  EquivalenceClosure<ColumnCP> closure;
  columns.forEach<Column>([&](ColumnCP column) { closure.add(column); });

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
      if (left == nullptr || right == nullptr || !columns.contains(left) ||
          !columns.contains(right)) {
        continue;
      }
      closure.merge(left, right, betterRep);
    }
  }

  folly::F14FastMap<ColumnCP, ColumnCP> reps;
  reps.reserve(columns.size());
  columns.forEach<Column>(
      [&](ColumnCP column) { reps[column] = closure.find(column); });
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
      neededAbove.unionObjects(edge.sourceColumns());
      if (edge.isUnnest()) {
        // An Unnest not yet applied reads what it expands from below.
        neededAbove.unionColumns(relation(edge.rightEndpoints().min())
                                     .node()
                                     ->as<Unnest>()
                                     ->unnestExpressions());
      }
    }
  }
  // A demanded column produced at a value-changing join boundary requires
  // the corresponding source below that edge. Repeat because nested outer
  // joins can form a chain of output-to-source mappings.
  size_t previousSize;
  do {
    previousSize = neededAbove.size();
    for (const auto& edge : edges_) {
      for (size_t i = 0; i < edge.outputColumns().size(); ++i) {
        if (neededAbove.contains(edge.outputColumns()[i])) {
          neededAbove.add(edge.sourceColumns()[i]);
        }
      }
    }
  } while (neededAbove.size() != previousSize);
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
      "Edge eligibility must reference only relations already added");
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

void JoinHypergraph::checkEdgesEnforced(
    const folly::F14FastSet<size_t>& appliedEdges) const {
  EquivalenceClosure<ExprCP> closure;

  // A relation is an already-built input. Columns from one equivalence class
  // within it have therefore been made equal before any graph edge is applied.
  for (const auto& relation : relations_) {
    folly::F14FastMap<EquivalenceP, ColumnCP> representatives;
    relation.columns().forEach<Column>([&](ColumnCP column) {
      const EquivalenceP equivalence = column->equivalence();
      if (equivalence == nullptr) {
        return;
      }
      const auto [it, inserted] =
          representatives.try_emplace(equivalence, column);
      if (!inserted) {
        closure.merge(it->second, column);
      }
    });
  }

  for (size_t index : appliedEdges) {
    const JoinEdge& edge = edges_[index];
    if (edge.isUnnest() || !provesKeyEquality(edge.joinType())) {
      continue;
    }
    for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
      closure.merge(edge.leftKeys()[i], edge.rightKeys()[i]);
    }
  }

  for (size_t index = 0; index < edges_.size(); ++index) {
    const JoinEdge& edge = edges_[index];
    if (edge.isUnnest() || !provesKeyEquality(edge.joinType())) {
      // An Unnest produces the rows it expands, and an unmatched or
      // null-padded row has unequal keys, so nothing stands in for these.
      VELOX_CHECK(
          appliedEdges.contains(index),
          "Plan does not apply a join edge: {} {}",
          index,
          edge.joinTypeName());
      continue;
    }
    for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
      VELOX_CHECK(
          closure.find(edge.leftKeys()[i]) == closure.find(edge.rightKeys()[i]),
          "Plan does not enforce a join equality: {} = {}",
          edge.leftKeys()[i]->toString(),
          edge.rightKeys()[i]->toString());
    }
  }
}

} // namespace facebook::axiom::optimizer
