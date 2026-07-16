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
#include <folly/container/F14Set.h>
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Hash-consing factory for tree-IR nodes and selected Expr leaves. Identical
/// sub-trees produce the same pointer, so identity equality is O(1) and side-
/// table lookups keyed by node identity hit one canonical entry per logical
/// sub-tree.
///
/// Everything is interned by pointer: each dedup set stores `const T*` and
/// recovers identity via `T::KeyHash` / `T::KeyEq`, so a cache hit allocates
/// nothing. Nodes are constructed from an owning `Key` (e.g. `Scan::Key`)
/// passed to `make<T>`; the `Expr` leaves `Literal`, `Call`, and
/// `optimizer::Aggregate` use `makeLiteral` / `makeCall` / `makeAggregate`
/// (their `KeyHash` / `KeyEq` live on the `Expr` classes themselves). `Column`
/// is intentionally NOT hash-consed: `Column::equivalence_` is mutable state
/// mutated by `Column::equals()`, and dedup would leak equivalence classes
/// across unrelated subtrees.
class Builder {
 public:
  /// Snapshots `queryCtx()` and the function registry; the resulting
  /// `Builder` is bound to that `QueryGraphContext` for its lifetime.
  Builder();

  /// Returns a canonical node `T` for 'key', constructing one if not already
  /// present. Interned by pointer: the dedup set stores `const T*` and recovers
  /// identity via `T::KeyHash` / `T::KeyEq`. Looks up by the in-flight `key`
  /// (transparent, no allocation on a hit); on a miss, moves the key into the
  /// newly constructed node. `Expr` leaves use `makeLiteral` / `makeCall` /
  /// `makeAggregate`.
  template <typename T>
  const T* make(typename T::Key key) {
    auto& dedup = setFor<T>();
    if (auto it = dedup.find(key); it != dedup.end()) {
      return *it;
    }
    if constexpr (std::is_same_v<T, Join>) {
      // Before construction: the Join ctor caches partitioning from these.
      addEquivalences(key);
    }
    const T* node = optimizer::make<T>(std::move(key));
    dedup.insert(node);
    return node;
  }

  /// Registers 'variant' in the `QueryGraphContext` arena and returns a
  /// canonical `Literal` of 'type' carrying it. Cardinality defaults to
  /// 1 (a single distinct value — appropriate for literal constants).
  const Literal*
  makeLiteral(velox::Variant&& variant, TypeCP type, float cardinality = 1);

  /// Returns a canonical `Literal` of 'value' carrying the already-registered
  /// 'variant' (owned by the `QueryGraphContext` arena).
  const Literal* makeLiteral(const Value& value, const velox::Variant* variant);

  /// Returns a canonical `Call`. Non-deterministic calls are never deduped —
  /// two textually identical `random()` invocations must stay distinct objects
  /// because each produces an independent value. Reversible binary calls (e.g.
  /// `lt`/`gt`, `eq`, `plus`) are canonicalized before dedup so equivalent
  /// forms share one `Call*` (e.g. `2 > a` is rewritten to `a < 2`).
  /// 'functions' is the call's own bits OR-ed with each arg's `functions()`.
  const Call* makeCall(
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions);

  /// Returns a canonical aggregate `Call`. Result type and 'intermediateType'
  /// are determined by (name, args), so identity excludes them. 'specialKind'
  /// and 'fallback' mark a metadata aggregate (see optimizer::Aggregate); they
  /// are also excluded from identity (determined by 'name').
  const optimizer::Aggregate* makeAggregate(
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions,
      bool isDistinct,
      ExprCP condition,
      TypeCP intermediateType,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      std::optional<logical_plan::SpecialAggregateKind> specialKind =
          std::nullopt,
      const optimizer::Aggregate* fallback = nullptr);

  /// Canonical `Literal` for boolean constant 'value'.
  const Literal* makeBoolean(bool value) {
    return makeLiteral(velox::Variant(value), toType(velox::BOOLEAN()));
  }

  /// Canonical `Literal` for SQL NULL of 'type'.
  const Literal* makeNull(TypeCP type) {
    return makeLiteral(velox::Variant::null(type->kind()), type);
  }

  /// `Values` node carrying zero rows with the given output schema.
  /// Used to replace subtrees a rewrite proved produce no rows.
  const Values* makeEmptyValues(ColumnVector outputColumns) {
    return make<Values>({/*source=*/nullptr,
                         /*rows=*/nullptr,
                         std::move(outputColumns)});
  }

  /// Interned `Name`s for well-known functions, snapshotted from the
  /// active `QueryGraphContext` at construction so callers can compare
  /// or build calls against well-known operator names by pointer
  /// equality, without repeated registry lookups.
  const FunctionNames& functionNames() const {
    return functionNames_;
  }

 private:
  // For a binary `Call` whose 'name' is in `reversibleFunctions_`,
  // swaps 'args' (and renames to the reverse) when `args[0]` should
  // come second per the canonical order: literal-on-right, lower-id
  // expr on left when neither is a literal. No-op otherwise.
  void canonicalizeCall(Name& name, ExprVector& args) const;

  // Adds an inner join's equi-key columns to one equivalence class (see
  // `Column::equals`): equated columns hold the same value on every output row,
  // so any surviving member partitions the output as the dropped key did.
  // No-op for non-inner joins (an outer join's build key may be null-padded, so
  // its columns are not equal on every row) and for non-column keys.
  static void addEquivalences(const Join::Key& key);

  template <typename T>
  auto& setFor() {
    if constexpr (std::is_same_v<T, Scan>) {
      return scans_;
    } else if constexpr (std::is_same_v<T, Filter>) {
      return filters_;
    } else if constexpr (std::is_same_v<T, Project>) {
      return projects_;
    } else if constexpr (std::is_same_v<T, Limit>) {
      return limits_;
    } else if constexpr (std::is_same_v<T, Sort>) {
      return sorts_;
    } else if constexpr (std::is_same_v<T, TopN>) {
      return topNs_;
    } else if constexpr (std::is_same_v<T, Aggregate>) {
      return aggregates_;
    } else if constexpr (std::is_same_v<T, GroupId>) {
      return groupIds_;
    } else if constexpr (std::is_same_v<T, MarkDistinct>) {
      return markDistincts_;
    } else if constexpr (std::is_same_v<T, Values>) {
      return values_;
    } else if constexpr (std::is_same_v<T, Unnest>) {
      return unnests_;
    } else if constexpr (std::is_same_v<T, UnionAll>) {
      return unions_;
    } else if constexpr (std::is_same_v<T, Join>) {
      return joins_;
    } else if constexpr (std::is_same_v<T, Window>) {
      return windows_;
    } else if constexpr (std::is_same_v<T, TopNRowNumber>) {
      return topNRowNumbers_;
    } else if constexpr (std::is_same_v<T, Apply>) {
      return applies_;
    } else if constexpr (std::is_same_v<T, EnforceSingleRow>) {
      return enforceSingleRows_;
    } else if constexpr (std::is_same_v<T, AssignUniqueId>) {
      return assignUniqueIds_;
    } else if constexpr (std::is_same_v<T, EnforceDistinct>) {
      return enforceDistincts_;
    } else if constexpr (std::is_same_v<T, Exchange>) {
      return exchanges_;
    } else if constexpr (std::is_same_v<T, TableWrite>) {
      return tableWrites_;
    } else {
      static_assert(sizeof(T) == 0, "No dedup map for this node type");
    }
  }

  const FunctionNames functionNames_;

  // Maps a reversible binary function name to its reverse name.
  // Symmetric reversibles (e.g. `eq`) map to themselves; asymmetric
  // ones (e.g. `lt` → `gt`) map across.
  folly::F14FastMap<Name, Name> reversibleFunctions_;

  // Dedup container for hash-consed `T`: stores canonical `const T*`, keyed by
  // the node/expr's own identity via transparent `T::KeyHash` / `T::KeyEq`.
  template <typename T>
  using DedupSet =
      folly::F14FastSet<const T*, typename T::KeyHash, typename T::KeyEq>;

  DedupSet<Scan> scans_;
  DedupSet<Filter> filters_;
  DedupSet<Project> projects_;
  DedupSet<Limit> limits_;
  DedupSet<Sort> sorts_;
  DedupSet<TopN> topNs_;
  DedupSet<Aggregate> aggregates_;
  DedupSet<GroupId> groupIds_;
  DedupSet<MarkDistinct> markDistincts_;
  DedupSet<Values> values_;
  DedupSet<Unnest> unnests_;
  DedupSet<UnionAll> unions_;
  DedupSet<Join> joins_;
  DedupSet<Window> windows_;
  DedupSet<TopNRowNumber> topNRowNumbers_;
  DedupSet<Apply> applies_;
  DedupSet<EnforceSingleRow> enforceSingleRows_;
  DedupSet<AssignUniqueId> assignUniqueIds_;
  DedupSet<EnforceDistinct> enforceDistincts_;
  DedupSet<Exchange> exchanges_;
  DedupSet<TableWrite> tableWrites_;

  // Expr leaves, interned via `makeLiteral` / `makeCall` / `makeAggregate`.
  DedupSet<Literal> literals_;
  DedupSet<Call> calls_;
  DedupSet<optimizer::Aggregate> aggregateCalls_;
};

} // namespace facebook::axiom::optimizer::v2
