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
/// The model splits properties by how they behave under distribution:
///   - per-driver local: ordering and grouping (`LocalProperty`),
///   - distribution: two independent partitioning slots, global and driver
///     (`Partitioning` with a `PropertyScope`),
///   - relation-level: uniqueness.
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
///   - `keys` is non-empty exactly when `kind == kPartitioned`.
///   - `orderKeys` and `orderTypes` have equal size and are non-empty only for
///     an order-preserving `kGather` (see `gatherMerge`).
///   - `partitionType` is non-null only for connector `kPartitioned`; null
///     means standard Velox hash partitioning.
///   - `replicateNullsAndAny` is valid only for global `kPartitioned`.
///   - As a requirement a consumer states, `keys` and `orderKeys` may be any
///     expression. On the `Partitioning` an `Exchange` carries they must be
///     columns the exchange's input produces: whoever places the shuffle
///     materializes an expression key first, so the value is computed once
///     below the shuffle and read as a column above it (see
///     `PrecomputeProjections::materializeKeys`).
struct Partitioning {
  /// A partition function shared by compatible partitionings. A null
  /// `connectorType` denotes standard hash partitioning.
  struct CommonType {
    /// Connector partition function, or null for standard hash.
    const connector::PartitionType* connectorType;
  };

  PartitionKind kind{PartitionKind::kUnspecified};

  /// Connector-specific partition function, or null for standard Velox hash.
  /// Lifetime is managed externally: a layout-owned type lives for the
  /// connector's lifetime, one derived during planning (scaleDown, copartition)
  /// is owned by `QueryGraphContext`.
  const connector::PartitionType* partitionType{nullptr};

  /// Partition keys; empty unless `kind == kPartitioned`.
  ExprVector keys;

  /// Sort keys and their orders that an order-preserving gather merges on
  /// (lowered to a Velox `MergeExchange`). Non-empty only for a `kGather`
  /// produced by `gatherMerge`; empty for a plain gather.
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

  /// No partitioning guarantee at `scope`.
  static Partitioning unspecified(PropertyScope scope) {
    return Partitioning{.scope = scope};
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

  /// A standard hash partitioning on `keys` at `scope`. Set
  /// `replicateNullsAndAny` for the global existence side of a null-aware
  /// anti/semi join.
  static Partitioning hash(
      PropertyScope scope,
      const ExprVector& keys,
      bool replicateNullsAndAny = false);

  /// A hash partitioning on `keys` at `scope` using a connector partition
  /// function.
  static Partitioning connectorHash(
      PropertyScope scope,
      const ExprVector& keys,
      const connector::PartitionType* partitionType);

  /// Returns the partition function a union of `left` and `right` retains, or
  /// nullopt when the inputs cannot share a partitioning. The result may use a
  /// coarser width than either input.
  static std::optional<CommonType> commonUnionPartitionType(
      const connector::PartitionType* left,
      const connector::PartitionType* right);

  /// Returns the partition function two join inputs share at their current
  /// widths, or nullopt when keeping both inputs would not co-partition them.
  static std::optional<CommonType> commonJoinPartitionType(
      const connector::PartitionType* left,
      const connector::PartitionType* right);

  /// A gather at `scope` (all rows on one task or one driver).
  static Partitioning gather(PropertyScope scope);

  /// A gather at `scope` that merges sorted input streams.
  static Partitioning gatherMerge(
      PropertyScope scope,
      const ExprVector& orderKeys,
      const OrderTypeVector& orderTypes);

  /// A global broadcast (every task receives a full copy) partitioning.
  static Partitioning globalBroadcast();

  /// The partitioning that gives every task of the stage whose rows are
  /// `consumer` a full copy: a gather when that stage runs on one task, a
  /// broadcast otherwise.
  static Partitioning globalReplicatedTo(const Partitioning& consumer);

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
  /// partitioning whose rows can be written to an equal or finer `targetType`
  /// with exactly one writer per target bucket.
  bool satisfiesWritePartitioning(
      const ExprVector& otherKeys,
      const connector::PartitionType& targetType) const;

 private:
  /// Returns the cached partition function shared by `left` and `right`, or
  /// nullopt when they cannot be paired.
  static std::optional<CommonType> commonPartitionType(
      const connector::PartitionType* left,
      const connector::PartitionType* right);
};

/// Kind of a per-driver local property.
enum class LocalPropertyKind : uint8_t {
  /// Rows with equal `columns` are contiguous (not necessarily ordered) within
  /// a driver, and every such group is wholly within one driver. See
  /// `LocalProperty` for the full contract.
  kGrouped,
  /// Rows ordered within each driver by `columns`, outermost key first, with
  /// the direction of each given by the matching entry in `orders`. Equal keys
  /// may occur on multiple drivers.
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
/// A `kGrouped` property guarantees both contiguity and driver co-location: an
/// equal group does not span drivers. A `kSorted` property guarantees order
/// only within each driver. Consumers may use grouping, but not sorting, to
/// skip a driver exchange.
struct LocalProperty {
  LocalPropertyKind kind{LocalPropertyKind::kGrouped};
  ColumnVector columns;
  OrderTypeVector orders;
};

/// A relation's per-driver local properties, outermost first.
using LocalPropertyVector = QGVector<LocalProperty>;

/// A set of columns that is unique across the relation — i.e., functionally
/// determines the row — at `scope`. `columns` is a set (a `PlanObjectSet`) so
/// consumers can test containment ("is some unique key-set ⊆ the columns I
/// care about?") directly.
struct UniqueKeySet {
  PlanObjectSet columns;
  PropertyScope scope{PropertyScope::kGlobal};
};

/// Unique key-sets of a relation, each at its own scope.
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

  /// Returns true when driver partitioning, global uniqueness, or a Grouped
  /// local property guarantees equal `keys` occur on one driver. Empty `keys`
  /// asks whether all rows are on one driver.
  bool driverCoLocates(const ExprVector& keys) const;

  /// Checks the invariant that these properties are expressed over the node's
  /// own `outputColumns`. A partitioning's keys and merge order may be computed
  /// expressions (the partition function's input), but every column they depend
  /// on must be an output column, so the partitioning is evaluable here.
  /// Local-property columns and unique key-sets must themselves be output
  /// columns. Fails (`VELOX_CHECK`) on violation. Called by the `Node` base
  /// constructor.
  void checkReferencesColumns(const PlanObjectSet& outputColumns) const;
};

} // namespace facebook::axiom::optimizer::v2
