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

#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/PlanObject.h"

/// Schema representation for use in query planning. All objects are
/// arena allocated for the duration of planning the query. We do
/// not expect to keep a full schema in memory, rather we expect to
/// instantiate the relevant schema objects based on the query. The
/// arena for these can be different from that for the PlanObjects,
/// though, so that a schema cache can have its own lifetime.
namespace facebook::axiom::optimizer {

template <typename T>
using NameMap = QGF14FastMap<Name, T>;

using VariantCP = const velox::Variant*;
using TypeCP = const velox::Type*;

/// Represents constraints on a column value or intermediate result.
// TODO: Refactor to separate schema facts from statistical estimates:
//   struct Value {
//     TypeCP type;
//     bool nullable{false};  // schema fact
//     struct Estimates {
//       float cardinality{1};
//       float nullFraction{0};
//       float trueFraction{kUnknown};
//       VariantCP min{nullptr};
//       VariantCP max{nullptr};
//     } estimates;
//   };
struct Value {
  Value(TypeCP type, float cardinality)
      : type{type}, cardinality{cardinality} {}

  /// Default copy constructor.
  Value(const Value&) = default;

  /// Assignment operator that checks type equality and assigns other members.
  Value& operator=(const Value& other);

  /// Returns the average byte size of a value when it occurs as an intermediate
  /// result without dictionary or other encoding.
  float byteSize() const;

  /// Returns a string representation of this Value.
  std::string toString() const;

  TypeCP type;

  /// Minimum and maximum values. Applies to orderable types (integers, floats,
  /// dates, timestamps). Not set for booleans, strings, or complex types.
  /// nullptr means no bound known. Otherwise points to a Variant with an
  /// actual value — never a null-valued Variant, since NULL is not a value
  /// that bounds a range.
  VariantCP min{nullptr};
  VariantCP max{nullptr};

  /// Count of distinct values. Is not exact and is used for estimating
  /// cardinalities of group bys or joins.
  float cardinality{1};

  /// Sentinel value for unknown trueFraction.
  static constexpr float kUnknown = -1.0f;

  /// Estimate of true fraction for booleans. 0 means always
  /// false. This is an estimate and 1 or 0 do not allow pruning
  /// dependent code paths.
  float trueFraction{kUnknown};

  /// 0 means no nulls, 0.5 means half are null.
  float nullFraction{0};

  /// True if nulls may occur. 'false' means that plans that allow no nulls may
  /// be generated.
  bool nullable{true};
};

/// Describes order in an order by or index.
enum class OrderType {
  kAscNullsFirst,
  kAscNullsLast,
  kDescNullsFirst,
  kDescNullsLast
};

AXIOM_DECLARE_ENUM_NAME(OrderType);

using OrderTypeVector = QGVector<OrderType>;

/// Describes desired distribution of data.
///
/// TODO Allow specifying partitioning on specific keys, but no particular
/// partitioning function.
struct DesiredDistribution {
  /// Connector-specific partitioning function or nullptr for standard Velox
  /// hash partitioning.
  const connector::PartitionType* partitionType;

  /// One or more partitioning keys.
  ExprVector partitionKeys;
};

/// Describes output of relational operator. For most operators this reflects
/// how the data is laid out across tasks. For Repartition, it describes the
/// kind of exchange the Repartition emits. See RelationOp::distribution() for
/// the full semantics.
struct Distribution {
  /// Kind of distribution. Represents both the data layout for non-Repartition
  /// operators and the exchange kind for Repartition.
  enum class Kind {
    /// No specific distribution is guaranteed. Not valid on Repartition.
    kUnspecified,
    /// Partitioned on `partitionKeys` using `partitionType` (connector-
    /// specific) or standard Velox hash when `partitionType` is nullptr.
    kPartitioned,
    /// All rows on one task / one partition.
    kGather,
    /// Broadcast (every consumer task receives a full copy). Only valid on
    /// Repartition.
    kBroadcast,
    /// Arbitrary / round-robin (any consumer task count, on-demand pull).
    /// Only valid on Repartition.
    kArbitrary,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Kind);

  Distribution() = default;

  Distribution(
      Kind kind,
      const connector::PartitionType* partitionType,
      ExprVector partitionKeys,
      ExprVector orderKeys = {},
      OrderTypeVector orderTypes = {},
      int32_t numKeysUnique = 0,
      ExprVector clusterKeys = {})
      : kind_{kind},
        partitionType_{partitionType},
        partitionKeys_{std::move(partitionKeys)},
        orderKeys_{std::move(orderKeys)},
        orderTypes_{std::move(orderTypes)},
        numKeysUnique_{numKeysUnique},
        clusterKeys_{std::move(clusterKeys)} {
    VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
    if (kind_ == Kind::kGather) {
      VELOX_CHECK_EQ(partitionKeys_.size(), 0);
    }
  }

  /// Convenience constructor for hash partitioning.
  Distribution(
      const connector::PartitionType* partitionType,
      ExprVector partitionKeys,
      ExprVector orderKeys = {},
      OrderTypeVector orderTypes = {},
      int32_t numKeysUnique = 0,
      ExprVector clusterKeys = {})
      : Distribution{
            Kind::kPartitioned,
            partitionType,
            std::move(partitionKeys),
            std::move(orderKeys),
            std::move(orderTypes),
            numKeysUnique,
            std::move(clusterKeys)} {}

  /// Returns a Distribution for use in a broadcast shuffle.
  static Distribution broadcast() {
    return {Kind::kBroadcast, /*partitionType=*/nullptr, /*partitionKeys=*/{}};
  }

  /// Returns a Distribution for use in an arbitrary (round-robin) shuffle.
  static Distribution arbitrary() {
    return {Kind::kArbitrary, /*partitionType=*/nullptr, /*partitionKeys=*/{}};
  }

  /// Returns a distribution that gathers all rows to a single task. Used
  /// wherever an operator requires a single task (e.g., final query gather,
  /// LIMIT or ORDER BY in a subquery, global aggregation).
  static Distribution gather() {
    return {Kind::kGather, /*partitionType=*/nullptr, /*partitionKeys=*/{}};
  }

  /// Returns a distribution for an ordered gather. Creates a merging exchange
  /// when the Distribution occurs in a Repartition.
  static Distribution gather(ExprVector orderKeys, OrderTypeVector orderTypes) {
    VELOX_CHECK_EQ(orderKeys.size(), orderTypes.size());
    return {
        Kind::kGather,
        /*partitionType=*/nullptr,
        /*partitionKeys=*/{},
        std::move(orderKeys),
        std::move(orderTypes),
    };
  }

  /// True if 'this' and 'other' have the same number/type of keys and same
  /// distribution type via strict pointer equality on PartitionType.
  bool isSamePartition(const Distribution& other) const;

  /// True if 'this' has the same number/type of partitioning keys and uses
  /// the same partitioning function (strict pointer equality on
  /// PartitionType).
  bool isSamePartition(const DesiredDistribution& other) const;

  /// Looser variant of isSamePartition: same partition keys and
  /// PartitionTypes for which copartition() returns non-null. Returns false
  /// when both sides have empty partitionKeys.
  bool isCopartitionedWith(const DesiredDistribution& other) const;

  /// True if 'other' has the same ordering columns and order type.
  bool isSameOrder(const Distribution& other) const;

  Kind kind() const {
    return kind_;
  }

  bool isGather() const {
    return kind_ == Kind::kGather;
  }

  bool isBroadcast() const {
    return kind_ == Kind::kBroadcast;
  }

  bool isArbitrary() const {
    return kind_ == Kind::kArbitrary;
  }

  /// Connector-specific partitioning function, or nullptr if standard Velox
  /// hash partitioning (or no partitioning).
  const connector::PartitionType* partitionType() const {
    return partitionType_;
  }

  const ExprVector& partitionKeys() const {
    return partitionKeys_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  int32_t numKeysUnique() const {
    return numKeysUnique_;
  }

  const ExprVector& clusterKeys() const {
    return clusterKeys_;
  }

  Distribution rename(const ExprVector& exprs, const ColumnVector& names) const;

  std::string toString() const;

 private:
  Kind kind_{Kind::kUnspecified};

  // Connector-specific partition function. nullptr means standard Velox hash
  // (when kind_ is kPartitioned) or no partitioning otherwise.
  const connector::PartitionType* partitionType_{nullptr};

  // Partitioning columns. The values of these columns determine which
  // partition contains any given row. Used with `partitionType_` when
  // `kind_ == kPartitioned`.
  ExprVector partitionKeys_;

  // Ordering columns. Each partition is ordered by these. Specifies that
  // streaming group by or merge join are possible.
  ExprVector orderKeys_;

  // Corresponds 1:1 to 'order'. The size of this gives the number of leading
  // columns of 'order' on which the data is sorted.
  OrderTypeVector orderTypes_;

  // Number of leading elements of 'order' such that these uniquely identify a
  // row. 0 if there is no uniqueness. This can be non-0 also if data is not
  // sorted. This indicates a uniqueness for joining.
  int32_t numKeysUnique_{0};

  // Clustering columns. Rows with the same values in these columns are
  // contiguous but not necessarily ordered. Enables streaming group by
  // when clustering keys are a subset of grouping keys.
  ExprVector clusterKeys_;
};

struct SchemaTable;
using SchemaTableCP = const SchemaTable*;

/// Represents a stored collection of rows with part of or all columns
/// of a table. A ColumnGroup may have a uniqueness constraint over a
/// set of columns, a partitioning and an ordering plus a set of
/// payload columns. An index is a ColumnGroup that may not have all
/// columns but is organized to facilitate retrieval. We use the name
/// index for ColumnGroup when using it for lookup.
struct ColumnGroup {
  ColumnGroup(
      const SchemaTable& table,
      const connector::TableLayout& layout,
      Distribution distribution,
      ColumnVector columns)
      : table{&table},
        layout{&layout},
        distribution{std::move(distribution)},
        columns{std::move(columns)} {}

  SchemaTableCP table;
  const connector::TableLayout* layout;
  const Distribution distribution;
  const ColumnVector columns;

  /// Returns cost of next lookup when the hit is within 'range' rows
  /// of the previous hit. If lookups are not batched or not ordered,
  /// then 'range' should be the cardinality of the index.
  float lookupCost(float range) const;
};

using ColumnGroupCP = const ColumnGroup*;

/// Describes the number of rows to look at and the number of expected matches
/// given equality constraints for a set of columns. See
/// SchemaTable::indexInfo().
struct IndexInfo {
  /// Index chosen based on columns.
  ColumnGroupCP index;

  /// True if the column combination is unique. This can be true even if there
  /// is no key order in 'index'.
  bool unique{false};

  /// The number of rows selected after index lookup based on 'lookupKeys'. For
  /// empty 'lookupKeys', this is the cardinality of 'index'.
  float scanCardinality;

  /// The lookup columns that match 'index'. These match 1:1 the leading keys
  /// of 'index'. If 'index' has no ordering columns or if the lookup columns
  /// are not a prefix of these, this is empty.
  std::vector<ColumnCP> lookupKeys;

  /// The columns that were considered in 'scanCardinality'. This may be
  /// fewer columns than given to indexInfo() if the index does not cover
  /// some columns.
  PlanObjectSet coveredColumns;

  /// Returns the schema column for the BaseTable column 'column' or nullptr
  /// if not in the index.
  ColumnCP schemaColumn(ColumnCP keyValue) const;
};

/// A table in a schema. The table may have multiple differently ordered and
/// partitioned physical representations (ColumnGroups). Not all ColumnGroups
/// (aka indices) need to contain all columns.
struct SchemaTable {
  explicit SchemaTable(const connector::Table& connectorTable)
      : connectorTable{&connectorTable},
        cardinality{std::max<float>(1, connectorTable.numRows())} {}

  ColumnGroupCP addIndex(
      const connector::TableLayout& layout,
      Distribution distribution,
      ColumnVector columns);

  ColumnCP findColumn(Name name) const;

  /// True if 'columns' match no more than one row.
  bool isUnique(CPSpan<Column> columns) const;

  /// Returns uniqueness and cardinality information for a lookup on 'index'
  /// where 'columns' have an equality constraint.
  IndexInfo indexInfo(ColumnGroupCP index, CPSpan<Column> columns) const;

  const SchemaTableName& name() const {
    return connectorTable->name();
  }

  /// Returns the connector ID (catalog name) for this table.
  const std::string& connectorId() const {
    return columnGroups[0]->layout->connectorId();
  }

  /// Table description from external schema.
  /// This is the source-dependent representation from which 'this' was created.
  const connector::Table* const connectorTable;

  const float cardinality;

  /// Maps column name to schema column.
  NameMap<ColumnCP> columns;

  /// All indices. Must contain at least one.
  QGVector<ColumnGroupCP> columnGroups;
};

/// Represents a collection of tables. Normally filled in ad hoc given
/// the set of tables referenced by a query. The lifetime is a single
/// optimization run. The owned objects are from the optimizer
/// arena. Schema is owned by the application and is not from the
/// optimization arena.  Objects of different catalogs/schemas get
/// added to 'this' on first use. The Schema feeds from a
/// SchemaResolver which interfaces to a local/remote metadata
/// repository.
class Schema {
 public:
  /// Constructs a Schema for producing executable plans, backed by 'source'.
  explicit Schema(
      const connector::SchemaResolver& source,
      std::shared_ptr<QueryRuntimeStats> runtimeStats = nullptr)
      : source_{&source}, runtimeStats_{std::move(runtimeStats)} {}

  /// Returns the table with 'name' or nullptr if not found, using
  /// the connector specified by connectorId to perform table lookups.
  /// An error is thrown if no connector with the specified ID exists.
  SchemaTableCP findTable(
      std::string_view connectorId,
      const SchemaTableName& tableName) const;

 private:
  struct Table {
    connector::TablePtr connectorTable;
    SchemaTableCP schemaTable{nullptr};
  };

  // This map from connector ID to map of tables in that connector.
  // In the tables map, the key is the SchemaTableName and the value is
  // schema table (optimizer object) and connector table (connector object).
  using TableMap = folly::F14FastMap<SchemaTableName, Table>;
  using ConnectorMap = folly::F14FastMap<std::string_view, TableMap>;

  const connector::SchemaResolver* source_;
  std::shared_ptr<QueryRuntimeStats> runtimeStats_;
  mutable ConnectorMap connectorTables_;
};

} // namespace facebook::axiom::optimizer

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::optimizer::Distribution, Kind);
