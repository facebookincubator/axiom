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

#include "axiom/common/Enums.h"
#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"

/// Physical-property types for the optimizer: the per-operator output ordering,
/// grouping, uniqueness, and partitioning that cost-based planning derives on
/// each physical candidate and the property-aware memo keys on. This header
/// declares the data shapes only; per-operator derivation and the satisfaction
/// / shuffle-cost queries live alongside the memo that consumes them.
///
/// The model splits properties by how they behave under distribution. Three
/// categories:
///   - per-driver local: ordering and grouping (`LocalProperty`),
///   - distribution: two independent partitioning slots, global and driver
///     (`Partitioning` with a `PropertyScope`),
///   - relation-level: uniqueness (`UniqueKeySet`).
namespace facebook::axiom::optimizer::v2 {

/// Scope at which a property holds. The two runtime levels distributed
/// execution distinguishes: across tasks (`kGlobal`) and across drivers within
/// one task (`kDriver`).
///
/// The scope's algebra differs by property:
///   - Partitioning carries two *independent* slots, one per scope — global
///     partitioned does not imply driver partitioned (a remote hash exchange
///     feeds drivers round-robin), so both are tracked.
///   - Uniqueness is a *lattice*: global uniqueness implies driver (per-driver)
///     uniqueness but not the reverse, so a key-set stores the single widest
///     scope at which it holds.
enum class PropertyScope : uint8_t {
  kGlobal,
  kDriver,
};

AXIOM_DECLARE_ENUM_NAME(PropertyScope);

/// Kind of data partitioning.
enum class PartitionKind : uint8_t {
  /// No specific partitioning is guaranteed.
  kUnspecified,
  /// Partitioned on `keys` using `partitionType` (connector-specific) or
  /// standard Velox hash when `partitionType` is null.
  kPartitioned,
  /// All rows on one partition (single task, or single driver at driver scope).
  kGather,
  /// Every consumer receives a full copy.
  kBroadcast,
  /// Arbitrary / round-robin assignment.
  kArbitrary,
};

AXIOM_DECLARE_ENUM_NAME(PartitionKind);

/// Partitioning of a relation's rows at one scope. The same concept serves both
/// the global (across-tasks) and driver (across-drivers) levels; `scope` says
/// which. A `PhysicalProperties` carries one `Partitioning` per scope, and the
/// two are independent.
///
/// Invariants:
///   - `keys` is non-empty only when `kind == kPartitioned`.
///   - `orderKeys` / `orderTypes` are non-empty only for an order-preserving
///     `kGather` (see `globalGatherMerge`).
///   - `partitionType` is null for standard Velox hash partitioning.
///   - `keys` are expressions (not just columns) to match the partition
///     function's input.
struct Partitioning {
  PartitionKind kind{PartitionKind::kUnspecified};

  /// Connector-specific partition function, or null for standard Velox hash.
  const connector::PartitionType* partitionType{nullptr};

  /// Partition keys; empty unless `kind == kPartitioned`.
  ExprVector keys;

  /// Sort keys and their orders that an order-preserving gather merges on
  /// (lowered to a Velox `MergeExchange`). Non-empty only for a `kGather`
  /// produced by `globalGatherMerge`; empty for a plain gather.
  ExprVector orderKeys;
  OrderTypeVector orderTypes;

  PropertyScope scope{PropertyScope::kGlobal};

  /// When true, the partitioned output replicates null-keyed rows (and one
  /// arbitrary row) to every partition. Required on the existence side of a
  /// null-aware anti/semi join (NOT IN / IN) so a null key reaches all probe
  /// partitions; otherwise NOT IN wrongly admits rows.
  bool replicateNullsAndAny{false};

  bool operator==(const Partitioning&) const = default;

  bool is(PartitionKind other) const {
    return kind == other;
  }

  /// A copy without the merge order (`orderKeys` / `orderTypes`). The merge
  /// order belongs to the gather-merge exchange that produced it; an operator
  /// that inherits a distribution without being that exchange keeps the kind
  /// and keys but not the order (which, if preserved, is carried as a local
  /// property).
  Partitioning dropOrder() const {
    Partitioning result = *this;
    result.orderKeys = {};
    result.orderTypes = {};
    return result;
  }

  /// A standard global hash partitioning on `keys`. Set `replicateNullsAndAny`
  /// for the existence side of a null-aware anti/semi join.
  static Partitioning globalHash(
      const ExprVector& keys,
      bool replicateNullsAndAny = false);

  /// A global gather (all rows on one task) partitioning.
  static Partitioning globalGather();

  /// A global gather that preserves ordering by merging the sorted per-task
  /// streams (lowered to a Velox `MergeExchange`). `orderKeys` / `orderTypes`
  /// are the sort keys to merge on.
  static Partitioning globalGatherMerge(
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes);

  /// A global broadcast (every task receives a full copy) partitioning.
  static Partitioning globalBroadcast();

  /// A global arbitrary (round-robin / shared-pool) partitioning.
  static Partitioning globalArbitrary();

  /// True when this co-locates rows that agree on `columns` — i.e. it
  /// hash-partitions on a subset of `columns`, so every group of equal
  /// `columns` is wholly within one partition and an aggregation grouping on
  /// `columns` needs no repartition. Not a co-partition test for joins: those
  /// must align corresponding keys across both inputs, so use `sameClassAs`.
  bool coLocates(const ExprVector& columns) const;

  /// True when this is the same class as `other` — same kind, connector
  /// function, and keys in order. Like `coLocates`, a pure class test: scope is
  /// fixed by the containing slot, so it is not compared. Two `kUnspecified`
  /// partitionings are the same class.
  bool sameClassAs(const Partitioning& other) const;

  /// True when this is connector-bucketed (`kPartitioned` with a non-null
  /// `partitionType`) on exactly `otherKeys` — same expressions in the same
  /// order. False for empty `otherKeys`.
  bool isBucketedOn(const ExprVector& otherKeys) const;

  /// True when this is bucketed on `otherKeys` (see `isBucketedOn`) with a
  /// partitioning that copartitions with `targetType` at this bucketing's own
  /// partition count — i.e. its rows already reach a `targetType`-bucketed
  /// consumer or write target with no reshuffle.
  bool isBucketedCompatibleWith(
      const ExprVector& otherKeys,
      const connector::PartitionType& targetType) const;
};

/// Kind of a per-driver local property.
enum class LocalPropertyKind : uint8_t {
  /// Rows with equal `columns` are contiguous (not necessarily ordered) within
  /// a driver, and every such group is wholly within one driver. See
  /// `LocalProperty` for the confinement contract.
  kGrouped,
  /// Rows ordered by `columns`, outermost key first, with the direction of each
  /// given by the matching entry in `orders`.
  kSorted,
};

AXIOM_DECLARE_ENUM_NAME(LocalPropertyKind);

/// One entry in a relation's ordered list of per-driver local properties. The
/// list order captures nesting: `[Grouped(P), Sorted(o1, o2)]` means grouped by
/// the set `P`, and within each group ordered by `o1` then `o2`.
///
/// A `kGrouped` entry uses `columns` alone (the grouped set; order among its
/// members is irrelevant) and leaves `orders` empty. A `kSorted` entry uses
/// both in parallel: `columns` are the sort keys outermost first, and
/// `orders[i]` is the direction of `columns[i]`.
///
/// A local property carries driver-confinement as part of its contract, not
/// just contiguity: a `kGrouped` set of equal `columns` (or a `kSorted`
/// equal-key run) is both contiguous within a driver's stream AND wholly
/// confined to that one driver — no group spans drivers. Producers must attach
/// a local property only when both hold; a source with mere per-driver
/// contiguity but no confinement (e.g. a bucketed/sorted scan whose splits
/// round-robin across drivers) must not. Consumers that decide driver placement
/// (e.g. skipping a local repartition before a single-stage aggregation) rely
/// on this directly: a local grouping on the keys means the groups are already
/// driver-confined.
struct LocalProperty {
  LocalPropertyKind kind{LocalPropertyKind::kGrouped};
  ColumnVector columns;
  OrderTypeVector orders;
};

/// A relation's per-driver local properties, outermost first.
using LocalPropertyVector = QGVector<LocalProperty>;

/// A set of columns that is unique across the relation — i.e., functionally
/// determines the row — at `scope`. Stored minimal: a key-set whose columns are
/// a superset of another stored key-set is redundant and not kept, but
/// independent minimal key-sets (e.g. `{a}` and `{b}` both unique) are all
/// retained. `columns` is a set (a `PlanObjectSet`) so consumers can test
/// containment ("is some unique key-set ⊆ the columns I care about?") directly.
struct UniqueKeySet {
  PlanObjectSet columns;
  PropertyScope scope{PropertyScope::kGlobal};
};

/// Unique key-sets of a relation. May hold several independent minimal
/// key-sets, each at its own scope.
using UniqueKeySetVector = QGVector<UniqueKeySet>;

/// The physical properties of one relation — the output of per-operator
/// derivation and the value the property-aware memo compares. Bundles the three
/// property categories.
///
/// The two partitioning slots are independent (see `PropertyScope`):
/// `globalPartition.scope == kGlobal` and `driverPartition.scope == kDriver`.
struct PhysicalProperties {
  /// Partitioning across tasks; set by a remote exchange.
  Partitioning globalPartition{.scope = PropertyScope::kGlobal};

  /// Partitioning across drivers within a task; set by a local exchange.
  Partitioning driverPartition{.scope = PropertyScope::kDriver};

  /// Per-driver ordering and grouping, outermost first.
  LocalPropertyVector local;

  /// Unique key-sets, relation-level, each scope-tagged.
  UniqueKeySetVector unique;

  /// Checks the invariant that these properties are expressed over the node's
  /// own `outputColumns`. A partitioning's keys and merge order may be computed
  /// expressions (the partition function's input), but every column they depend
  /// on must be an output column, so the partitioning is evaluable here.
  /// Local-property columns and every unique key-set must themselves be output
  /// columns. Fails (`VELOX_CHECK`) on violation. Called by the `Node` base
  /// constructor.
  void checkReferencesColumns(const PlanObjectSet& outputColumns) const;
};

} // namespace facebook::axiom::optimizer::v2
