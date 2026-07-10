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

#include <cstdint>
#include <optional>
#include <vector>

#include <folly/container/F14Map.h>

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/JoinEdge.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/Relation.h"
#include "axiom/optimizer/v2/RelationSet.h"

namespace facebook::axiom::optimizer::v2 {

/// A single boolean predicate that must be evaluated somewhere in
/// the chosen join tree. `relations` is the set of cluster leaves
/// whose columns the predicate references; the emitter places the
/// conjunct at the lowest join whose cover ⊇ `relations`. `expr` is
/// required at construction; default-null is only the
/// aggregate-init placeholder. Only free (inner-join / WHERE) conjuncts
/// live here; outer-join ON-clause conjuncts are edge-pinned via
/// `JoinEdge::filter` instead.
struct FilterConjunct {
  ExprCP expr{nullptr};
  RelationSet relations;
};

/// The hypergraph DPhyp enumerates over: a set of relations
/// (atomic join inputs) connected by hyperedges (predicates).
///
/// Each edge carries a Total Eligibility Set (TES) — the relations that
/// must already be in the joined set before the edge can fire.
///
/// Typical use: call `addRelation` once for each base input, then
/// `addEdge` once for each predicate, then pass the result to
/// DPhyp for plan enumeration. Edges and their TES are stored in
/// parallel: `tes()[i]` is the TES of `edges()[i]`.
///
/// Invariants (maintained across construction):
///   - `relations()[i].id() == i`.
///   - Every relation id referenced by an edge is in
///     `[0, relations().size())`.
///   - `tes()[i]` is a superset of
///     `edges()[i].left() ∪ edges()[i].right()`.
class JoinHypergraph {
 public:
  /// Constructs a Relation with the next assigned id and adds it
  /// to the hypergraph. Returns the assigned id.
  int8_t addRelation(
      NodeCP node,
      std::optional<float> cardinality,
      PlanObjectSet columns);

  /// Registers an Unnest IR node as its own relation and tracks its id
  /// for connectivity checks. `node` must be an Unnest. Returns the
  /// assigned id.
  int8_t addUnnestRelation(
      NodeCP node,
      std::optional<float> cardinality,
      PlanObjectSet columns);

  /// Relation ids registered via `addUnnestRelation`.
  const RelationSet& unnestRelationIds() const {
    return unnestRelationIds_;
  }

  /// Appends an edge with its TES. `tes` must be a superset of
  /// `edge.left() ∪ edge.right()`, and every relation referenced
  /// by `edge` or `tes` must already be in `relations()`.
  void addEdge(JoinEdge edge, RelationSet tes);

  /// Appends a filter conjunct. `conjunct.relations` must be
  /// a subset of the relations already in `relations()`.
  void addFilterConjunct(FilterConjunct conjunct);

  const std::vector<FilterConjunct>& filterConjuncts() const {
    return filterConjuncts_;
  }

  /// Columns the cluster's plan must output (the cluster root's output
  /// columns). Used to decide which of a subplan's columns survive past it: a
  /// cover's output columns are its columns that are either targets or
  /// referenced by a not-yet-applied edge / filter.
  void setTargetColumns(PlanObjectSet targetColumns);

  const PlanObjectSet& targetColumns() const {
    return targetColumns_;
  }

  /// Union of the output columns of every relation in `cover` — the columns a
  /// subplan over `cover` can supply.
  PlanObjectSet coverColumns(const RelationSet& cover) const;

  /// Maps each column of `cover` to the representative of its equivalence group
  /// under the equi-join keys of the INNER-join edges lying entirely within
  /// `cover` — only an inner join makes its keys equal in the output (an outer
  /// join's null-padded key is not). Columns equated to nothing map to
  /// themselves. The representative is the group's target column if any, else
  /// the column with the smallest name, with id breaking final ties; name
  /// rather than id because ids come from a process-global counter, which would
  /// make the choice non-reproducible across runs and query forms. The order is
  /// total, so `best(A∪B) = best(best A, best B)`: a join's representative is
  /// always one of its children's representatives, hence present in a child's
  /// output, independent of join order. Used to collapse provably-equal columns
  /// to one, consistently for costing (row width) and emission (output columns
  /// and key/filter references).
  folly::F14FastMap<ColumnCP, ColumnCP> coverColumnReps(
      const RelationSet& cover) const;

  /// The columns a subplan over `cover` must output: its `coverColumns` that
  /// are either target columns or referenced by an edge / filter conjunct
  /// crossing the cover boundary. This demand depends only on `cover`'s
  /// boundary, not on the internal join order, so it is the precise output
  /// set both for costing a candidate (row width) and for setting the emitted
  /// join's output columns.
  PlanObjectSet coverOutputColumns(const RelationSet& cover) const;

  const std::vector<Relation>& relations() const {
    return relations_;
  }

  /// Looks up a relation by id.
  const Relation& relation(int32_t id) const {
    return relations_[id];
  }

  const std::vector<JoinEdge>& edges() const {
    return edges_;
  }

  /// Per-edge Total Eligibility Sets, aligned with `edges()`:
  /// `tes()[i]` is the TES of `edges()[i]`.
  const std::vector<RelationSet>& tes() const {
    return tes_;
  }

  /// Returns the set of relations reachable from `seed` by repeatedly
  /// following edges whose endpoints are entirely within `bound`.
  RelationSet connectedComponent(RelationSet seed, RelationSet bound) const;

  /// Checks that all relations are connected to each other via
  /// edges. DPhyp's main enumeration loop requires a connected
  /// hypergraph.
  void checkConsistency() const;

 private:
  // Empties the cover-derived memo caches. Called by every builder mutation so
  // the caches never outlive the graph state they were computed from.
  void invalidateCoverCaches();

  // Indexed by relation id.
  std::vector<Relation> relations_;
  std::vector<JoinEdge> edges_;
  // Parallel to `edges_`: `tes_[i]` is the TES of `edges_[i]`.
  std::vector<RelationSet> tes_;
  std::vector<FilterConjunct> filterConjuncts_;
  // Bitset of every relation id currently in the hypergraph.
  RelationSet relationIds_;
  // Bitset of relations registered via `addUnnestRelation`.
  RelationSet unnestRelationIds_;
  // Columns the cluster's plan must output. See setTargetColumns().
  PlanObjectSet targetColumns_;

  // Memoized coverOutputColumns() results, keyed by cover. coverOutputColumns
  // is a pure function of the frozen graph but the cost model queries it once
  // per join candidate during enumeration; caching collapses the per-candidate
  // recomputation (a union-find over all inner edges) that otherwise dominates
  // planning a many-relation same-key join. Invalidated by
  // invalidateCoverCaches(). Assumes single-threaded planning (the mutable
  // cache makes the const method non-thread-safe).
  mutable folly::F14FastMap<RelationSet, PlanObjectSet>
      coverOutputColumnsCache_;
};

} // namespace facebook::axiom::optimizer::v2
