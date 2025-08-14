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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/Connector.h"
#include "velox/core/QueryCtx.h"
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
} // namespace facebook::velox::core

/// Base classes for schema elements used in execution. A
/// ConnectorMetadata provides access to table information.  A Table has a
/// TableLayout for each of its physical organizations, e.g. base table, index,
/// column group, sorted projection etc. A TableLayout has partitioning and
/// ordering properties and a set of Columns. A Column has ColumnStatistics. A
/// TableLayout combined with Column and Subfield selection and
/// optional filters and lookup keys produces a ConnectorTableHandle. A
/// ConnectorTableHandle can be used to build a table scan or index
/// lookup PlanNode. A ConnectorTableHandle can be used for split
/// enumeration.  Derived classes of the above connect to different
/// metadata stores and provide different metadata, e.g. order,
/// partitioning, bucketing etc.
namespace facebook::velox::connector {

/// Represents statistics of a column. The statistics may represent the column
/// across the table or may be calculated over a sample of a layout of the
/// table. All fields are optional.
struct ColumnStatistics {
  /// Empty for top level  column. Struct member name or string of key for
  /// struct  or flat map subfield.
  std::string name;

  /// If true, the column cannot have nulls.
  bool nonNull{false};

  /// Observed percentage of nulls. 0 does not mean that there are no nulls.
  float nullPct{0};

  /// Minimum observed value for comparable scalar columns.
  std::optional<variant> min;

  /// Maximum observed value for a comparable scalar.
  std::optional<variant> max;

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

/// Options for StatisticsBuilder.
struct StatisticsBuilderOptions {
  int32_t maxStringLength{100};
  int32_t initialSize{0};
  bool countDistincts{false};
  HashStringAllocator* allocator{nullptr};
};

/// Abstract class for building statistics from samples.
class StatisticsBuilder {
 public:
  virtual ~StatisticsBuilder() = default;

  static std::unique_ptr<StatisticsBuilder> create(
      const TypePtr& type,
      const StatisticsBuilderOptions& opts);

  virtual TypePtr type() const = 0;

  /// Accumulates elements of 'vector' into stats.
  virtual void add(VectorPtr& data) = 0;

  /// Merges the statistics of 'other' into 'this'.
  virtual void merge(const StatisticsBuilder& other) = 0;

  /// Fills 'result' with the accumulated stats. Scales up counts by
  /// 'sampleFraction', e.g. 0.1 means 10x.
  virtual void build(ColumnStatistics& result, float sampleFraction = 1) = 0;

  static void updateStatsBuilders(
      const RowVectorPtr& data,
      std::vector<std::unique_ptr<StatisticsBuilder>>& builders);

  virtual int64_t numAsc() const = 0;
  virtual int64_t numRepeat() const = 0;
  virtual int64_t numDesc() const = 0;
};

/// Base class for column. The column's name and type are immutable but the
/// stats may be set multiple times.
class Column {
 public:
  virtual ~Column() = default;

  Column(const std::string& name, TypePtr type) : name_(name), type_(type) {}

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

  /// Sets statistics. May be called multipl times if table contents change.
  void setStats(std::unique_ptr<ColumnStatistics> stats) {
    std::lock_guard<std::mutex> l(mutex_);
    allStats_.push_back(std::move(stats));
    latestStats_ = allStats_.back().get();
  }

  const std::string& name() const {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

  /// Returns approximate number of distinct values. Returns 'deflt' if no
  /// information.
  int64_t approxNumDistinct(int64_t deflt = 1000) const {
    auto* s = stats();
    return s && s->numDistinct.has_value() ? s->numDistinct.value() : deflt;
  }

 protected:
  const std::string name_;
  const TypePtr type_;

  // The latest element added to 'allStats_'.
  tsan_atomic<ColumnStatistics*> latestStats_{nullptr};

  // All statistics recorded for this column. Old values can be purged when the
  // containing Schema is not in use.
  std::vector<std::unique_ptr<ColumnStatistics>> allStats_;

 private:
  // Serializes changes to statistics.
  std::mutex mutex_;
};

//// Describes the kind of table, e.g. durable vs. temporary.
enum class TableKind { kTable, kTempTable };

VELOX_DECLARE_ENUM_NAME(TableKind);

class Table;

/// Represents sorting order. Duplicate of core::SortOrder. Connectors
struct SortOrder {
  bool isAscending{true};
  bool isNullsFirst{true};
};

/// Represents a physical manifestation of a table. There is at least
/// one layout but for tables that have multiple sort orders,
/// partitionings, indices, column groups, etc, there is a separate
/// layout for each. The layout represents data at rest. The
/// ConnectorTableHandle represents the query's constraints on the layout a scan
/// or lookup is accessing.
class TableLayout {
 public:
  TableLayout(
      const std::string& name,
      const Table* table,
      connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitionColumns,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      bool supportsScan)
      : name_(name),
        table_(table),
        connector_(connector),
        columns_(std::move(columns)),
        partitionColumns_(std::move(partitionColumns)),
        orderColumns_(std::move(orderColumns)),
        sortOrder_(std::move(sortOrder)),
        lookupKeys_(lookupKeys),
        supportsScan_(supportsScan) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (auto& column : columns_) {
      names.push_back(column->name());
      types.push_back(column->type());
    }
    rowType_ = ROW(std::move(names), std::move(types));
  }

  virtual ~TableLayout() = default;

  /// Name for documentation. If there are multiple layouts, this is unique
  /// within the table.
  const std::string name() const {
    return name_;
  }

  /// Returns the Connector to use for generating ColumnHandles and TableHandles
  /// for operations against this layout.
  connector::Connector* connector() const {
    return connector_;
  }

  /// Returns the containing Table.
  const Table* table() const {
    return table_;
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
  /// separately from sortedness since some sorted files may not
  /// support lookup. An index lookup has 0 or more equalities
  /// followed by up to one range. The equalities need to be on
  /// contiguous, leading parts of the column list and the range must
  /// be on the next. This coresponds to a multipart key.
  const std::vector<const Column*>& lookupKeys() const {
    return lookupKeys_;
  }

  /// True if a full table scan is supported. Some lookup sources prohibit this.
  /// At the same time the dataset may be available in a scannable form in
  /// another layout.
  bool supportsScan() const {
    return supportsScan_;
  }

  /// Returns the columns and their names as a RowType.
  const RowTypePtr& rowType() const {
    return rowType_;
  }

  /// Samples 'pct' percent of rows. Applies filters in 'handle'
  /// before sampling. Returns {count of sampled, count matching
  /// filters}. 'extraFilters' is a list of conjuncts to evaluate in
  /// addition to the filters in 'handle'.  If 'statistics' is
  /// non-nullptr, fills it with post-filter statistics for the
  /// subfields in 'fields'. When sampling on demand, it is usually sufficient
  /// to look at a subset of all accessed columns, so we specify these instead
  /// of defaulting to the columns in 'handle'.  'allocator' is used for
  /// temporary memory in gathering statistics.
  /// 'outputType' can specify a cast from map to struct. Filter expressions see
  /// the 'outputType' and 'subfields' are relative to that.
  virtual std::pair<int64_t, int64_t> sample(
      const connector::ConnectorTableHandlePtr& handle,
      float pct,
      std::vector<core::TypedExprPtr> extraFilters,
      RowTypePtr outputType = nullptr,
      const std::vector<common::Subfield>& fields = {},
      HashStringAllocator* allocator = nullptr,
      std::vector<ColumnStatistics>* statistics = nullptr) const {
    VELOX_UNSUPPORTED("Table class does not support sampling.");
  }

  const Column* findColumn(const std::string& name) const {
    for (auto& column : columns_) {
      if (column->name() == name) {
        return column;
      }
    }
    return nullptr;
  }

 private:
  const std::string name_;
  const Table* table_;
  connector::Connector* connector_;
  std::vector<const Column*> columns_;
  const std::vector<const Column*> partitionColumns_;
  const std::vector<const Column*> orderColumns_;
  const std::vector<SortOrder> sortOrder_;
  const std::vector<const Column*> lookupKeys_;
  const bool supportsScan_;
  RowTypePtr rowType_;
};

class Schema;

/// Base class for table. This is used for name resolution. A TableLayout is
/// used for accessing physical organization like partitioning  and sort order.
class Table {
 public:
  virtual ~Table() = default;

  Table(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

  /// Returns all columns as RowType.
  const RowTypePtr& rowType() const {
    return type_;
  }

  /// Returns the set of columns as abstract, non-owned
  /// columns. Implementations may have different Column
  /// implementations with different options, so we do not return the
  /// implementation's columns but an abstract form.
  virtual const std::unordered_map<std::string, const Column*>& columnMap()
      const = 0;

  const Column* findColumn(const std::string& name) const {
    auto& map = columnMap();
    auto it = map.find(name);
    return it == map.end() ? nullptr : it->second;
  }

  virtual const std::vector<const TableLayout*>& layouts() const = 0;

  /// Returns an estimate of the number of rows in 'this'.
  virtual uint64_t numRows() const = 0;

  virtual const std::unordered_map<std::string, std::string>& options() const {
    return options_;
  }

  TableKind kind() const {
    return kind_;
  }

 protected:
  const std::string name_;

  // Discovered from data. In the event of different types, we take the
  // latest (i.e. widest) table type.
  RowTypePtr type_;

  TableKind kind_{TableKind::kTable};

  std::unordered_map<std::string, std::string> options_;
};

/// Describes a single partition of a TableLayout. A TableLayout has at least
/// one partition, even if it has no partitioning columns.
class PartitionHandle {
 public:
  virtual ~PartitionHandle() = default;
};

using PartitionHandlePtr = std::shared_ptr<const PartitionHandle>;

/// Enumerates splits. The table and partitions to cover are given to
/// ConnectorSplitManager.
class SplitSource {
 public:
  static constexpr uint32_t kUngroupedGroupId =
      std::numeric_limits<uint32_t>::max();

  /// Result of getSplits. Each split belongs to a group. A nullptr split for
  /// group means that there are no more splits for the group. In ungrouped
  /// execution, the group is always kUngroupedGroupId.
  struct SplitAndGroup {
    std::shared_ptr<ConnectorSplit> split;
    uint32_t group{kUngroupedGroupId};
  };

  virtual ~SplitSource() = default;

  /// Returns a set of splits that cover up to 'targetBytes' of data.
  virtual std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) = 0;
};

// Specifies  options for split generation.
struct SplitOptions {
  /// make no more than one split per file.
  bool wholeFile{false};

  /// If non-0, gives a minimum number of splits to generate, e.g. at least one
  /// for each driver of each worker.
  int32_t targetSplitCount{0};

  /// Target size of split.
  uint64_t fileBytesPerSplit{128LL << 20};
};

class ConnectorSplitManager {
 public:
  virtual ~ConnectorSplitManager() = default;

  /// Returns the list of all partitions that match the filters in
  /// 'tableHandle'. A non-partitioned table returns one partition.
  virtual std::vector<PartitionHandlePtr> listPartitions(
      const ConnectorTableHandlePtr& tableHandle) = 0;

  /// Returns a SplitSource that covers the contents of 'partitions'. The set of
  /// partitions is exposed separately so that the caller may process the
  /// partitions in a specific order or distribute them to specific nodes in a
  /// cluster.
  virtual std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorTableHandlePtr& tableHandle,
      std::vector<PartitionHandlePtr> partitions,
      SplitOptions = {}) = 0;
};

using SubfieldPtr = std::shared_ptr<const common::Subfield>;

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
  variant defaultValue;
};

using SubfieldMapping = std::unordered_map<
    SubfieldPtr,
    TargetSubfield,
    SubfieldPtrHasher,
    SubfieldPtrComparer>;

/// Describes a set of lookup keys. Lookup keys can be specified for
/// supporting connector types when creating a
/// ConnectorTableHandle. The corresponding DataSource will then be
/// used with a lookup API. The keys should match a prefix of
/// lookupKeys() of the TableLayout when making a
/// ConnectorTableHandle. The leading keys are compared with
/// equality. A trailing key part may be compared with range
/// constraints. The flags have the same meaning as in
/// common::BigintRange and related.
struct LookupKeys {
  /// Columns with equality constraints. Must be a prefix of the lookupKeys() in
  /// TableLayout.
  std::vector<std::string> equalityColumns;

  /// Column on which a range condition is applied in lookup. Must be the
  /// immediately following key in lookupKeys() order after the last column in
  /// 'equalities. If 'equalities' is empty, 'rangeColumn' must be the first in
  /// lookupKeys() order.
  std::optional<std::string> rangeColumn;

  // True if the lookup has no lower bound for 'rangeColumn'.
  bool lowerUnbounded{true};

  /// true if the  lookup specifies no upper bound for 'rangeColumn'.
  bool upperUnbounded{true};

  /// True if rangeColumn > range lookup lower bound.
  bool lowerExclusive{false};

  /// 'true' if rangeColum <  upper range lookup value.
  bool upperExclusive{false};

  /// true if matches for a range lookup should be returned in ascending order
  /// of the range column. Some lookup sources may support descending order.
  bool isAscending{true};
};

/// Describes how to repartition data before a TableWriter.
struct WritePartitionInfo {
  /// Columns for partitioning,. Names refer to the column names in the insert
  /// table handle. Empty if any worker can write any row.
  const std::vector<std::string> columns;

  /// Specifies the partition function. nullptr if 'columns' is empty.
  const std::shared_ptr<const core::PartitionFunctionSpec> partitionSpec;

  /// Maximum number of workers. For example, having more workers than there are
  /// partitions makes no sense.
  const int32_t maxWorkers;
};

/// Representts session status for update operations. May for
/// example encapsulate a transaction state. The minimal
/// implementation does nothing, which amounts to all write
/// operations being non-isolated and autocommitting. Connector
/// specific implementations have their specific transaction functions.
class ConnectorSession {
 public:
  virtual ~ConnectorSession() = default;
};

using ConnectorSessionPtr = std::shared_ptr<ConnectorSession>;

/// Specifies what type of write is intended when initiating or concluding a
/// write operation.
enum class WriteKind {
  // Rows are added and all columns must be specified for the TableWriter. This
  // covers insert, create table and replacing a Hive partition and any other
  // use that adds whole rows.
  kInsert,

  // Individual rows are deleted. Only row ids as per
  // ConnectorMetadata::rowIdHandles() are passed to the TableWriter.
  kDelete,

  // Column values in individual rows are changed. The TableWriter
  // gets first the row ids as per ConnectorMetadata::rowIdHandles()
  // and then new values for the columns being changed. The new values
  // may overlap with row ids if the row id is a set of primary key
  // columns.
  kUpdate
};

VELOX_DECLARE_ENUM_NAME(WriteKind);

class ConnectorMetadata {
 public:
  virtual ~ConnectorMetadata() = default;

  /// Post-construction initialization. This is called after adding
  /// the ConnectorMetadata to the connector so that Connector methods
  /// that refer to metadata are available.
  virtual void initialize() = 0;

  /// Creates a ColumnHandle for 'columnName'. If the type is a
  /// complex type, 'subfields' specifies which subfields need to be
  /// retrievd. empty 'subfields' means all are returned. If
  /// 'castToType' is present, this can be a type that the column can
  /// be cast to. The set of supported casts depends on the
  /// connector. In specific, a map may be cast to a struct. For casts
  /// between complex types, 'subfieldMapping' maps from the subfield
  /// in the data to the subfield in 'castToType'. The defaultValue is
  /// produced if the key Subfield does not occur in the
  /// data. Subfields of 'castToType that are not covered by
  /// 'subfieldMapping' are set to null if 'castToType' is a struct
  /// and are absent if 'castToType' is a map. See implementing
  /// Connector for exact set of cast and subfield semantics.
  virtual ColumnHandlePtr createColumnHandle(
      const TableLayout& layoutData,
      const std::string& columnName,
      std::vector<common::Subfield> subfields = {},
      std::optional<TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) {
    VELOX_UNSUPPORTED();
  }

  /// Returns a ConnectorTableHandle for use in
  /// createDataSource. 'filters' are pushed down into the
  /// DataSource. 'filters' are expressions involving literals and
  /// columns of 'layout'. The filters not supported by the target
  /// system are returned in 'rejectedFilters'. 'rejectedFilters' will
  /// have to be applied to the data returned by the
  /// DataSource. 'rejectedFilters' may or may not be a subset of
  /// 'filters' or subexpressions thereof. If 'lookupKeys' is present,
  /// these must match the lookupKeys() in 'layout'. If 'dataColumns' is given,
  /// it must have all the existing columns and may additionally specify casting
  /// from maps to structs by giving a struct in the place of a map.
  virtual ConnectorTableHandlePtr createTableHandle(
      const TableLayout& layout,
      std::vector<ColumnHandlePtr> columnHandles,
      core::ExpressionEvaluator& evaluator,
      std::vector<core::TypedExprPtr> filters,
      std::vector<core::TypedExprPtr>& rejectedFilters,
      RowTypePtr dataColumns = nullptr,
      std::optional<LookupKeys> = std::nullopt) {
    VELOX_UNSUPPORTED();
  }

  virtual const Table* findTable(const std::string& name) = 0;

  /// Returns a SplitManager for split enumeration for TableLayouts accessed
  /// through 'this'.
  virtual ConnectorSplitManager* splitManager() = 0;

  virtual std::shared_ptr<core::QueryCtx> makeQueryCtx(
      const std::string& queryId) {
    VELOX_UNSUPPORTED();
  }

  /// Creates a table. 'tableName' is a name with optional 'schema.'
  /// followed by table name. The connector gives the first part of
  /// the three part name. The table properties are in 'options'. All
  /// options must be understood by the connector. To create a table,
  /// first make a ConnectorSession in a connector dependent manner,
  /// then call createTable, then access the created layout(s) and
  /// make an insert table handle for writing each. Insert data into
  /// each layout and then call finishWrite on each. Normally a table
  /// has one layout but if many exist, as in secondary indices or
  /// materializations that are not transparently handled by an
  /// outside system, the optimizer is expected to make plans that
  /// write to all. In such cases the plan typically has a different
  /// table writer for each materialization. Any transaction semantics
  /// are connector dependent. Throws an error if the table exists,
  /// unless 'errorIfExists' is false, in which case the operation returns
  /// silently.  finishWrite should be called for all insert table handles
  /// to complete the write also if no data is added. To create an empty
  /// table, call createTable and then commit if the connector is
  /// transactional. to create the table with data, insert into all
  /// materializations, call finishWrite on each and then commit the whole
  /// transaction if the connector requires that.
  ///
  /// *** Rename to createTable after renaming conflicting createTable functions
  /// in derived classes ***
  virtual void createTableWithOptions(
      const std::string& tableName,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      const ConnectorSessionPtr& session,
      bool errorIfExists = true,
      TableKind tableKind = TableKind::kTable) {
    VELOX_UNSUPPORTED();
  }

  /// Creates an insert table handle for use with Velox TableWriter. '
  /// 'rowType' is the type of one row, including any partitioning or
  /// bucketing columns. The order may be significant, for example
  /// Hive needs partitioning columns to be last in column order. If
  /// the write is a delete or update the row will reflect this,
  /// starting with the columns identified by rowIdHandles().  The set
  /// of options and their meaning is connector dependent. A connector
  /// is expected to throw an error if it does not understand all
  /// options. if the connector has transaction support, sets up a
  /// transaction if one does not exist. The handle is created in one
  /// process, which is considered to initiate the transaction. If
  /// data is added to the table, finishWrite must be called after the
  /// last writer is finished. Whether this autocommits a transaction
  /// depends on the connector and session settings.
  virtual ConnectorInsertTableHandlePtr createInsertTableHandle(
      const TableLayout& layout,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      WriteKind kind,
      const ConnectorSessionPtr& session) {
    VELOX_UNSUPPORTED();
  }

  /// Returns specification for repartitioning data before the table writer
  /// stage.
  virtual WritePartitionInfo writePartitionInfo(
      const ConnectorInsertTableHandlePtr& handle) {
    VELOX_UNSUPPORTED();
  }

  /// Finalizes a table write. This runs once after all the table writers have
  /// finished. The result sets from the table writer fragments are passed as
  /// 'writerResults'. Their format and meaning is connector specific. the
  /// RowType is given by the outputType() of the TableWriter.
  virtual void finishWrite(
      const TableLayout& layout,
      const ConnectorInsertTableHandlePtr& handle,
      const std::vector<RowVectorPtr>& writerResult,
      WriteKind kind,
      const ConnectorSessionPtr& session) {
    VELOX_UNSUPPORTED();
  }

  /// Returns column handles whose value uniquely identifies a row for creating
  /// an update or delete record. These may be for example some connector
  /// specific opaque row id or primary key columns.
  virtual std::vector<ColumnHandlePtr> rowIdHandles(
      const TableLayout& layout,
      WriteKind kind) {
    VELOX_UNSUPPORTED();
  }
};

} // namespace facebook::velox::connector

template <>
struct fmt::formatter<facebook::velox::connector::TableKind>
    : fmt::formatter<string_view> {
  template <typename FormatContext>
  auto format(facebook::velox::connector::TableKind k, FormatContext& ctx)
      const {
    return formatter<string_view>::format(
        facebook::velox::connector::TableKindName::toName(k), ctx);
  }
};

template <>
struct fmt::formatter<facebook::velox::connector::WriteKind>
    : fmt::formatter<string_view> {
  template <typename FormatContext>
  auto format(facebook::velox::connector::WriteKind k, FormatContext& ctx)
      const {
    return formatter<string_view>::format(
        facebook::velox::connector::WriteKindName::toName(k), ctx);
  }
};
