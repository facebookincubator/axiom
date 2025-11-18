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

#include "axiom/common/Enums.h"
#include "axiom/connectors/ConnectorSession.h"
#include "axiom/connectors/ConnectorSplitManager.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/Connector.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {
// Forward declare because used in sampling and filtering APIs in
// abstract Connector. The abstract interface does not depend on
// core:: but implementations do.
class ITypedExpr;
using TypedExprPtr = std::shared_ptr<const ITypedExpr>;

class PartitionFunctionSpec;
using PartitionFunctionSpecPtr =
    std::shared_ptr<const core::PartitionFunctionSpec>;
} // namespace facebook::velox::core

/// Base classes for schema elements used in execution. A ConnectorMetadata
/// provides access to table information. A Table has a TableLayout for each of
/// its physical organizations, e.g. base table, index, column group, sorted
/// projection etc. A TableLayout has partitioning and ordering properties and a
/// set of Columns. A Column has ColumnStatistics. A TableLayout combined with
/// Column and Subfield selection and optional filters and lookup keys produces
/// a ConnectorTableHandle. A ConnectorTableHandle can be used to build a table
/// scan or index lookup PlanNode and for split enumeration. Derived classes of
/// the above connect to different metadata stores and provide different
/// metadata, e.g. order, partitioning, bucketing etc.
namespace facebook::axiom::connector {

/// Represents statistics of a column. The statistics may represent the column
/// across the table or may be calculated over a sample of a layout of the
/// table. All fields are optional.
struct ColumnStatistics {
  /// Empty for top level column. Struct member name or string of key for struct
  /// or flat map subfield.
  std::string name;

  /// If true, the column cannot have nulls.
  bool nonNull{false};

  /// Observed percentage of nulls. 0 does not mean that there are no nulls.
  float nullPct{0};

  /// Minimum observed value for comparable scalar column.
  std::optional<velox::Variant> min;

  /// Maximum observed value for a comparable scalar column.
  std::optional<velox::Variant> max;

  /// For string, varbinary, array and map, the maximum observed number of
  /// characters/bytes/elements/key-value pairs.
  std::optional<int32_t> maxLength;

  /// Percentage of values where the next row is > the previous. 50 for a random
  /// distribution, 0 for descending, 100 for ascending.
  std::optional<float> ascendingPct;

  std::optional<float> descendingPct;

  /// Average count of characters/bytes/elements/key-value pairs.
  std::optional<int32_t> avgLength;

  /// Estimated number of distinct values. Not specified for complex types.
  std::optional<int64_t> numDistinct;

  /// Count of non-nulls.
  int64_t numValues{0};

  /// For complex type columns, statistics of children. For array, contains one
  /// element describing the array elements. For struct, has one element for
  /// each member. For map, has an element for keys and one for values. For flat
  /// map, may have one element for each key. In all cases, stats may be
  /// missing.
  std::vector<ColumnStatistics> children;
};

/// Base class for column. The column's name and type are immutable but the
/// stats may be set multiple times.
class Column {
 public:
  virtual ~Column() = default;

  Column(std::string name, velox::TypePtr type)
      : name_{std::move(name)},
        type_{std::move(type)},
        defaultValue_{velox::Variant::null(type_->kind())} {}

  /// Default value can be specified to be used for table write.
  Column(std::string name, velox::TypePtr type, velox::Variant defaultValue)
      : name_{std::move(name)},
        type_{std::move(type)},
        defaultValue_{std::move(defaultValue)} {}

  const ColumnStatistics* stats() const {
    return latestStats_;
  }

  ColumnStatistics* mutableStats() {
    std::lock_guard<std::mutex> l(mutex_);
    if (!latestStats_) {
      allStats_.push_back(std::make_unique<ColumnStatistics>());
      latestStats_ = allStats_.back().get();
    }
    return latestStats_;
  }

  /// Sets statistics. May be called multiple times if table contents change.
  void setStats(std::unique_ptr<ColumnStatistics> stats) {
    std::lock_guard<std::mutex> l(mutex_);
    allStats_.push_back(std::move(stats));
    latestStats_ = allStats_.back().get();
  }

  const std::string& name() const {
    return name_;
  }

  const velox::TypePtr& type() const {
    return type_;
  }

  const velox::Variant& defaultValue() const {
    return defaultValue_;
  }

  /// Returns approximate number of distinct values. Returns 'defaultValue' if
  /// no information.
  int64_t approxNumDistinct(int64_t defaultValue = 1000) const {
    if (auto* s = stats()) {
      return s->numDistinct.value_or(defaultValue);
    }

    return defaultValue;
  }

 protected:
  const std::string name_;
  const velox::TypePtr type_;
  const velox::Variant defaultValue_;

  // The latest element added to 'allStats_'.
  velox::tsan_atomic<ColumnStatistics*> latestStats_{nullptr};

  // All statistics recorded for this column. Old values can be purged when the
  // containing Schema is not in use.
  std::vector<std::unique_ptr<ColumnStatistics>> allStats_;

 private:
  // Serializes changes to statistics.
  std::mutex mutex_;
};

class Table;

/// Represents sorting order. Duplicate of core::SortOrder.
struct SortOrder {
  bool isAscending{true};
  bool isNullsFirst{false};
};

/// Represents a partitioning function. Partitions can be copartitioned if the
/// types are compatible.
class PartitionType {
 public:
  virtual ~PartitionType() = default;

  /// Returns 'this' or '&other' if the partitions are compatible. Partitions
  /// are compatible if data in one partitioned dataset can only match data in
  /// the same partition of another dataset if joined on equality of partition
  /// keys. Compatibility is not strict equality in the case of e.g. Hive where
  /// a dataset partitioned 8 ways is compatible with one partitioned 16 ways if
  /// the function is the same. In such a case the partition to use is the 8 way
  /// one. On the 16 side data from partitions 0 and 1 match 0 on the 8 side and
  /// 2, 3 match 1 and so on.
  virtual const PartitionType* copartition(
      const PartitionType& other) const = 0;

  /// Returns a factory that makes partition functions. The function takes a
  /// RowVector and calculates a partition number from the columns identified by
  /// 'channels'. If channels[i] == kConstantChannel then the corresponding
  /// element of 'constants' is used. 'isLocal' differentiates between remote
  /// and local exchange.
  virtual velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<velox::column_index_t>& channels,
      const std::vector<velox::VectorPtr>& constants,
      bool isLocal) const = 0;

  virtual std::string toString() const = 0;

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }
};

// TODO Move to velox/type/Subfield.h
using SubfieldPtr = std::shared_ptr<const velox::common::Subfield>;

struct SubfieldPtrHasher {
  size_t operator()(const SubfieldPtr& subfield) const {
    return subfield->hash();
  }
};

struct SubfieldPtrComparer {
  bool operator()(const SubfieldPtr& lhs, const SubfieldPtr& rhs) const {
    return *lhs == *rhs;
  }
};

/// Subfield and default value for use in pushing down a complex type cast into
/// a ColumnHandle.
struct TargetSubfield {
  SubfieldPtr target;
  velox::Variant defaultValue;
};

using SubfieldMapping = folly::F14FastMap<
    SubfieldPtr,
    TargetSubfield,
    SubfieldPtrHasher,
    SubfieldPtrComparer>;

/// A set of lookup keys. Lookup keys can be specified for supporting
/// connector types when creating a ConnectorTableHandle. The corresponding
/// DataSource will then be used with a lookup API. The keys should match a
/// prefix of lookupKeys() of the TableLayout when making a
/// ConnectorTableHandle. The leading keys are compared with equality. A
/// trailing key part may be compared with range constraints. The flags have the
/// same meaning as in common::BigintRange and related.
struct LookupKeys {
  /// Columns with equality constraints. Must be a prefix of the lookupKeys() in
  /// TableLayout.
  std::vector<std::string> equalityColumns;

  /// Column on which a range condition is applied in lookup. Must be the
  /// immediately following key in lookupKeys() order after the last column in
  /// 'equalities'. If 'equalities' is empty, 'rangeColumn' must be the first in
  /// lookupKeys() order.
  std::optional<std::string> rangeColumn;

  // True if the lookup has no lower bound for 'rangeColumn'.
  bool lowerUnbounded{true};

  /// true if the lookup specifies no upper bound for 'rangeColumn'.
  bool upperUnbounded{true};

  /// True if rangeColumn > range lookup lower bound.
  bool lowerExclusive{false};

  /// True if rangeColum < upper range lookup value.
  bool upperExclusive{false};

  /// True if matches for a range lookup should be returned in ascending order
  /// of the range column. Some lookup sources may support descending order.
  bool isAscending{true};
};

/// Represents a physical manifestation of a table. There is at least
/// one layout but for tables that have multiple sort orders, partitionings,
/// indices, column groups, etc. there is a separate layout for each. The layout
/// represents data at rest. The ConnectorTableHandle represents the query's
/// constraints on the layout a scan or lookup is accessing.
class TableLayout {
 public:
  /// @param name Name of the layout (not table) for documentation. If there are
  /// multiple layouts, this is unique within the table.
  TableLayout(
      std::string name,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitionColumns,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      bool supportsScan);

  virtual ~TableLayout() = default;

  const std::string& name() const {
    return name_;
  }

  /// The Connector to use for generating ColumnHandles and TableHandles for
  /// operations against this layout.
  velox::connector::Connector* connector() const {
    return connector_;
  }

  const std::string& connectorId() const {
    return connector_->connectorId();
  }

  /// The containing Table.
  const Table& table() const {
    return *table_;
  }

  /// List of columns present in this layout.
  const std::vector<const Column*>& columns() const {
    return columns_;
  }

  /// Set of partitioning columns. The values in partitioning columns determine
  /// the location of the row. Joins on equality of partitioning columns are
  /// co-located.
  const std::vector<const Column*>& partitionColumns() const {
    return partitionColumns_;
  }

  /// Describes how the value in partitionColumns() determines a partition. The
  /// returned value is owned by 'this'. nullptr if 'partitionColumns_' is
  /// empty.
  virtual const PartitionType* partitionType() const {
    VELOX_CHECK(partitionColumns_.empty());
    return nullptr;
  }

  /// Columns on which content is ordered within the range of rows covered by a
  /// Split.
  const std::vector<const Column*>& orderColumns() const {
    return orderColumns_;
  }

  /// Sorting order. Corresponds 1:1 to orderColumns().
  const std::vector<SortOrder>& sortOrder() const {
    return sortOrder_;
  }

  /// Returns the key columns usable for index lookup. This is modeled
  /// separately from sortedness since some sorted files may not support lookup.
  /// An index lookup has 0 or more equalities followed by up to one range. The
  /// equalities need to be on contiguous, leading parts of the column list and
  /// the range must be on the next. This coresponds to a multipart key.
  const std::vector<const Column*>& lookupKeys() const {
    return lookupKeys_;
  }

  /// True if a full table scan is supported. Some lookup sources prohibit this.
  /// At the same time the dataset may be available in a scannable form in
  /// another layout.
  bool supportsScan() const {
    return supportsScan_;
  }

  /// The columns and their names as a RowType.
  const velox::RowTypePtr& rowType() const {
    return rowType_;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  /// Samples 'pct' percent of rows. Applies filters in 'handle' before
  /// sampling. Returns {count of sampled, count matching filters}.
  /// 'extraFilters' is a list of conjuncts to evaluate in addition to the
  /// filters in 'handle'. If 'statistics' is non-nullptr, fills it with
  /// post-filter statistics for the subfields in 'fields'. When sampling on
  /// demand, it is usually sufficient to look at a subset of all accessed
  /// columns, so we specify these instead of defaulting to the columns in
  /// 'handle'. 'allocator' is used for temporary memory in gathering
  /// statistics. 'outputType' can specify a cast from map to struct. Filter
  /// expressions see the 'outputType' and 'subfields' are relative to that.
  virtual std::pair<int64_t, int64_t> sample(
      const velox::connector::ConnectorTableHandlePtr& handle,
      float pct,
      const std::vector<velox::core::TypedExprPtr>& extraFilters,
      velox::RowTypePtr outputType = nullptr,
      const std::vector<velox::common::Subfield>& fields = {},
      velox::HashStringAllocator* allocator = nullptr,
      std::vector<ColumnStatistics>* statistics = nullptr) const = 0;

  /// Return a column with the matching name. Returns nullptr if not found.
  const Column* findColumn(std::string_view name) const;

  /// Creates a ColumnHandle for 'columnName'. If the type is a complex type,
  /// 'subfields' specifies which subfields need to be retrievd. Empty
  /// 'subfields' means all are returned. If 'castToType' is present, this can
  /// be a type that the column can be cast to. The set of supported casts
  /// depends on the connector. In specific, a map may be cast to a struct. For
  /// casts between complex types, 'subfieldMapping' maps from the subfield in
  /// the data to the subfield in 'castToType'. The defaultValue is produced if
  /// the key Subfield does not occur in the data. Subfields of 'castToType'
  /// that are not covered by 'subfieldMapping' are set to null if 'castToType'
  /// is a struct and are absent if 'castToType' is a map. See implementing
  /// Connector for exact set of cast and subfield semantics.
  virtual velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields = {},
      std::optional<velox::TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) const = 0;

  /// Returns a ConnectorTableHandle for use in createDataSource. 'filters' are
  /// pushed down into the DataSource. 'filters' are expressions involving
  /// literals and columns of 'layout'. The filters not supported by the target
  /// system are returned in 'rejectedFilters'. 'rejectedFilters' will
  /// have to be applied to the data returned by the DataSource.
  /// 'rejectedFilters' may or may not be a subset of 'filters' or
  /// subexpressions thereof. If 'lookupKeys' is present, these must match the
  /// lookupKeys() in 'layout'. If 'dataColumns' is given, it must have all the
  /// existing columns and may additionally specify casting from maps to structs
  /// by giving a struct in the place of a map.
  virtual velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns = nullptr,
      std::optional<LookupKeys> lookupKeys = std::nullopt) const = 0;

 private:
  const std::string name_;
  const Table* table_;
  velox::connector::Connector* connector_;
  const std::vector<const Column*> columns_;
  const std::vector<const Column*> partitionColumns_;
  const std::vector<const Column*> orderColumns_;
  const std::vector<SortOrder> sortOrder_;
  const std::vector<const Column*> lookupKeys_;
  const bool supportsScan_;
  const velox::RowTypePtr rowType_;
};

/// Base class for table. This is used for name resolution. A TableLayout is
/// used for accessing physical organization like partitioning and sort order.
/// The Table object maintains ownership over the objects it contains, including
/// the TableLayout and Columns contained in the Table.
class Table : public std::enable_shared_from_this<Table> {
 public:
  virtual ~Table() = default;

  Table(
      std::string name,
      velox::RowTypePtr type,
      folly::F14FastMap<std::string, velox::Variant> options = {})
      : name_(std::move(name)),
        type_(std::move(type)),
        options_(std::move(options)) {
    VELOX_CHECK(!name_.empty());
    VELOX_CHECK_NOT_NULL(type_);
  }

  const std::string& name() const {
    return name_;
  }

  /// Returns all columns as RowType.
  const velox::RowTypePtr& type() const {
    return type_;
  }

  /// Returns the mapping of columns keyed on column names as abstract,
  /// non-owned columns. Implementations may have different Column
  /// implementations with different options, so we do not return the
  /// implementation's columns but an abstract form.
  virtual const folly::F14FastMap<std::string, const Column*>& columnMap()
      const = 0;

  const Column* findColumn(std::string_view name) const {
    const auto& map = columnMap();
    auto it = map.find(name);
    return it == map.end() ? nullptr : it->second;
  }

  virtual const std::vector<const TableLayout*>& layouts() const = 0;

  /// Returns an estimate of the number of rows in 'this'.
  virtual uint64_t numRows() const = 0;

  virtual const folly::F14FastMap<std::string, velox::Variant>& options()
      const {
    return options_;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

 protected:
  const std::string name_;

  // Discovered from data. In the event of different types, we take the
  // latest (i.e. widest) table type.
  const velox::RowTypePtr type_;
  const folly::F14FastMap<std::string, velox::Variant> options_;
};

using TablePtr = std::shared_ptr<const Table>;

/// Contains the information for an in-progress write operation. This may
/// include insert, update, or delete of an existing table, or insertion into a
/// new table. The ConnectorWriteHandle is generated when a table write
/// operation is initiated in beginWrite and used to commit or abort any
/// completed write operations in finishWrite or abortWrite. Derived classes of
/// the write handle must contain all the information required by the connector
/// to finish or abort a write operation.
class ConnectorWriteHandle {
 public:
  explicit ConnectorWriteHandle(
      velox::connector::ConnectorInsertTableHandlePtr veloxHandle,
      velox::RowTypePtr resultType)
      : veloxHandle_{std::move(veloxHandle)},
        resultType_{std::move(resultType)} {
    VELOX_CHECK_NOT_NULL(veloxHandle_);
    VELOX_CHECK_NOT_NULL(resultType_);
  }

  virtual ~ConnectorWriteHandle() = default;

  const velox::connector::ConnectorInsertTableHandlePtr& veloxHandle() const {
    return veloxHandle_;
  }

  const velox::RowTypePtr& resultType() const {
    return resultType_;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

 private:
  const velox::connector::ConnectorInsertTableHandlePtr veloxHandle_;
  const velox::RowTypePtr resultType_;
};

using ConnectorWriteHandlePtr = std::shared_ptr<ConnectorWriteHandle>;

/// Specifies what type of write is intended when initiating or concluding a
/// write operation.
enum class WriteKind {
  /// A write operation to a new table which does not yet exist in the
  /// connector. Covers both creation of an empty table and create as select
  /// operations.
  kCreate = 1,

  /// Rows are added and all columns must be specified for the TableWriter.
  /// Covers insert, Hive partition replacement or any other operation which
  /// adds whole rows.
  kInsert = 2,

  /// Individual rows are deleted. Only row ids as per
  /// ConnectorMetadata::rowIdHandles() are passed to the TableWriter.
  kDelete = 3,

  /// Column values in individual rows are changed. The TableWriter
  /// gets first the row ids as per ConnectorMetadata::rowIdHandles()
  /// and then new values for the columns being changed. The new values
  /// may overlap with row ids if the row id is a set of primary key
  /// columns.
  kUpdate = 4,
};

AXIOM_DECLARE_ENUM_NAME(WriteKind);

using RowsFuture = folly::SemiFuture<int64_t>;

class ConnectorMetadata {
 public:
  /// Temporary APIs to assist in removing dependency on ConnectorMetadata from
  /// Velox.
  static ConnectorMetadata* metadata(std::string_view connectorId);
  static ConnectorMetadata* tryMetadata(std::string_view connectorId);
  static ConnectorMetadata* metadata(velox::connector::Connector* connector);
  static void registerMetadata(
      std::string_view connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);
  static void unregisterMetadata(std::string_view connectorId);

  virtual ~ConnectorMetadata() = default;

  /// Return a ConnectorTablePtr given the table name. Table name is provided
  /// without the connector ID prefix for the connector. The returned Table
  /// object is immutable. If updates to the Table object are required, the
  /// ConnectorMetadata is required to drop its reference to the existing
  /// Table and return a reference to a newly created Table object for
  /// subsequent calls to findTable. The ConnectorMetadata may drop its
  /// reference ot the Table object at any time, and callers are required
  /// to retain a reference to the Table to prevent it from being reclaimed
  /// in the case of Table removal by the ConnectorMetadata.
  virtual TablePtr findTable(std::string_view name) = 0;

  /// Returns a SplitManager for split enumeration for TableLayouts accessed
  /// through 'this'.
  virtual ConnectorSplitManager* splitManager() = 0;

  /// Creates a table. 'tableName' is a name with optional 'schema.' followed by
  /// table name. The connector gives the first part of the three part name. The
  /// table properties are in 'options'. All options must be understood by the
  /// connector. To create a table, first make a ConnectorSession in a connector
  /// dependent manner, then call createTable to retrieve a Table object. Any
  /// transaction semantics are connector-dependent, and the ConnectorSession
  /// may be null for connectors which do not require it. Throws an error if the
  /// table exists. finishWrite should be called to commit the new table and any
  /// writes even if no data is added. To create an empty table, call
  /// createTable, then beginWrite/finishWrite with the generated table object.
  /// To create the table with data, call createTable to generate a Table, call
  /// beginWrite with the Table object, perform writes against the table using
  /// the returned insert handle, then finishWrite to commit the changes. The
  /// table is not available via the findTable interface until after finishWrite
  /// completes.
  virtual TablePtr createTable(
      const ConnectorSessionPtr& session,
      const std::string& tableName,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, velox::Variant>& options) {
    VELOX_UNSUPPORTED();
  }

  /// Begins the process of a write operation by creating an associated write
  /// handle. This handle must contain a valid physical insert handle for use
  /// with Velox TableWriter. To perform a write operation, first make a
  /// ConnectorSession in a connector dependent manner, then call beginWrite to
  /// generate the write handle. Insert data using the insert handle provided by
  /// the write handle and call finishWrite. Transaction semantics are
  /// connector-dependent, and ConnectorSession may be null for connectors which
  /// do not require it.
  virtual ConnectorWriteHandlePtr beginWrite(
      const ConnectorSessionPtr& session,
      const TablePtr& table,
      WriteKind kind) {
    VELOX_UNSUPPORTED();
  }

  /// Finalizes the table write operation represented by the provided handle.
  /// This runs once after all the table writers have finished. The result sets
  /// from the table writer fragments are passed as 'writeResults'. Their
  /// format and meaning is connector-specific. The type of 'writeResults' must
  /// match ConnectorWriteHandle::resultType returned from beginWrite.
  /// finishWrite returns a ContinueFuture which must be waited for to finalize
  /// the commit. If the implementation is synchronous, finishWrite should
  /// return an already-fulfilled future to the caller. ConnectorSession may be
  /// null for connectors which do not require it.
  /// The returned future contains the number of rows "written".
  virtual RowsFuture finishWrite(
      const ConnectorSessionPtr& session,
      const ConnectorWriteHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writeResults) {
    VELOX_UNSUPPORTED();
  }

  /// Aborts an abandoned or failed write operation. Abort is not guaranteed to
  /// run in all failure cases. After abort is triggered for the write operation
  /// represented by ConnectorWriteHandle, this handle can no longer be used to
  /// commit a write operation with finishWrite. If this function is not
  /// implemented by a connector, abort will be a no-op. If the abort is a
  /// synchronous operation, the connector should perform the abort and return
  /// an already-fulfilled future.
  virtual velox::ContinueFuture abortWrite(
      const ConnectorSessionPtr& session,
      const ConnectorWriteHandlePtr& handle) noexcept {
    return {};
  }

  /// Drop table with the specified name. If table doesn't exist and 'ifExists'
  /// is false, raises an error. Otherwise, returns true if table was dropped
  /// and false if table didn't exist.
  virtual bool dropTable(
      const ConnectorSessionPtr& session,
      std::string_view tableName,
      bool ifExists) {
    VELOX_UNSUPPORTED();
  }

  /// Returns column handles whose value uniquely identifies a row for creating
  /// an update or delete record. These may be for example some connector
  /// specific opaque row id or primary key columns.
  virtual std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
      const Table& table,
      WriteKind kind) {
    VELOX_UNSUPPORTED();
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }
};

} // namespace facebook::axiom::connector

AXIOM_ENUM_FORMATTER(facebook::axiom::connector::WriteKind);
