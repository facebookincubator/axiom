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

#include <functional>
#include "axiom/common/Enums.h"
#include "axiom/common/SchemaFunctionName.h"
#include "axiom/common/SchemaTableName.h"
#include "axiom/common/SchemaTypeName.h"
#include "axiom/connectors/ConnectorSession.h"
#include "axiom/connectors/ConnectorSplitManager.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "folly/CppAttributes.h"
#include "folly/coro/Task.h"
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

class TableLayout;

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

  /// @param name Name of the column. Cannot be empty. Column names within a
  /// table must be unique.
  /// @param type Type of the column.
  /// @param hidden If true, the column doesn't appear in the output of SELECT *
  /// FROM t query. Still, the column can be accessed by name: SELECT ...,
  /// foo,... FROM t.
  Column(
      std::string name,
      velox::TypePtr type,
      bool hidden,
      bool includeInExplainIo = false)
      : name_{std::move(name)},
        type_{std::move(type)},
        hidden_{hidden},
        includeInExplainIo_{includeInExplainIo},
        defaultValue_{velox::Variant::null(type_->kind())} {
    VELOX_CHECK_NOT_NULL(type_);
    VELOX_CHECK(!name_.empty());
  }

  /// Default value can be specified to be used for table write.
  Column(
      std::string name,
      velox::TypePtr type,
      bool hidden,
      velox::Variant defaultValue,
      bool includeInExplainIo = false)
      : name_{std::move(name)},
        type_{std::move(type)},
        hidden_{hidden},
        includeInExplainIo_{includeInExplainIo},
        defaultValue_{std::move(defaultValue)} {
    VELOX_CHECK_NOT_NULL(type_);
    VELOX_CHECK(!name_.empty());
    VELOX_CHECK_EQ(type_->kind(), defaultValue_.kind());
  }

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

  bool hidden() const {
    return hidden_;
  }

  const velox::Variant& defaultValue() const {
    return defaultValue_;
  }

  /// Returns true if this column should appear in EXPLAIN IO output.
  /// Set by connectors for columns relevant to IO (e.g., partition columns).
  /// Only VARCHAR and integer types (TINYINT, SMALLINT, INTEGER, BIGINT) are
  /// supported.
  bool includeInExplainIo() const {
    return includeInExplainIo_;
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
  const bool hidden_;
  const bool includeInExplainIo_;
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

  /// Returns a PartitionType compatible with both 'this' and 'other', or
  /// nullptr if they are not compatible. Compatibility is not strict equality:
  /// for Hive, a table with 8 buckets is compatible with one bucketed 16 ways
  /// (same hash function); the result has 8 partitions. The returned value
  /// is always a freshly allocated owned shared_ptr.
  virtual std::shared_ptr<PartitionType> copartition(
      const PartitionType& other) const = 0;

  /// Returns a PartitionType compatible with this one but with at most
  /// 'maxPartitions' partitions. The returned partitioning groups together
  /// rows that this PartitionType would have placed in different partitions,
  /// so the resulting partition assignment is a coarsening of this one. The
  /// returned value is always a freshly allocated owned shared_ptr.
  virtual std::shared_ptr<PartitionType> scaleDown(
      int32_t maxPartitions) const = 0;

  /// Returns a factory that makes partition functions. The function takes a
  /// RowVector and calculates a partition number from the columns identified by
  /// 'channels'. If channels[i] == kConstantChannel then the corresponding
  /// element of 'constants' is used. 'isLocal' differentiates between remote
  /// and local exchange.
  virtual velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<velox::column_index_t>& channels,
      const std::vector<velox::VectorPtr>& constants,
      bool isLocal) const = 0;

  /// Number of partitions.
  virtual int32_t numPartitions() const = 0;

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

  /// True if the lookup specifies no upper bound for 'rangeColumn'.
  bool upperUnbounded{true};

  /// True if rangeColumn > range lookup lower bound.
  bool lowerExclusive{false};

  /// True if rangeColum < upper range lookup value.
  bool upperExclusive{false};

  /// True if matches for a range lookup should be returned in ascending order
  /// of the range column. Some lookup sources may support descending order.
  bool isAscending{true};
};

class DiscretePredicates {
 public:
  explicit DiscretePredicates(std::vector<const Column*> columns)
      : columns_(std::move(columns)) {
    VELOX_CHECK(!columns_.empty());
  }

  virtual ~DiscretePredicates() = default;

  const std::vector<const Column*>& columns() const {
    return columns_;
  }

  /// Returns the next batch of values. Returns empty vector if there are no
  /// more values. Each velox::Variant is of type 'row' and contains one value
  /// per column specified in the ctor in order.
  virtual std::vector<velox::Variant> next() = 0;

 private:
  const std::vector<const Column*> columns_;
};

/// Result of sampling a table layout. Both fields are non-negative and
/// numMatched <= numSampled. numSampled may be zero for empty tables; the
/// caller treats this as selectivity 1.0.
struct SampleResult {
  /// Number of rows sampled. Zero for empty tables.
  int64_t numSampled;

  /// Number of sampled rows matching the filters in 'handle'. At most
  /// numSampled.
  int64_t numMatched;
};

/// Result of estimating filtered table statistics from the connector.
struct FilteredTableStats {
  /// Estimated row count after applying filters.
  uint64_t numRows{0};

  /// Per-column statistics corresponding 1:1 to the 'columns' parameter of
  /// co_estimateStats. Either empty (no column stats available) or has the
  /// same size as 'columns', in the same order.
  std::vector<ColumnStatistics> columnStats;

  /// Indices into the 'filterConjuncts' parameter of co_estimateStats
  /// identifying conjuncts the connector could not account for when
  /// estimating numRows. The optimizer applies its own selectivity estimation
  /// for these on top of numRows and column constraints. Empty means all
  /// conjuncts were accounted for.
  std::vector<int32_t> rejectedFilterIndices;
};

/// One group of a metadata-derived count produced by co_metadataCounts. One
/// entry per distinct tuple of the grouping columns; a single entry with empty
/// 'key' for a global (ungrouped) count. Counts may be estimates; see
/// co_metadataCounts.
struct MetadataCountGroup {
  /// Grouping-column values for this group, in the order of the
  /// 'groupingColumns' argument of co_metadataCounts. Empty for a global count.
  std::vector<velox::Variant> key;

  /// Number of rows in this group, derived from table metadata.
  int64_t numRows{0};

  /// Null count per requested column, 1:1 with the 'columns' argument of
  /// co_metadataCounts and in the same order. Empty when no columns were
  /// requested. Each entry must be in [0, numRows] -- even for an estimate, a
  /// column cannot have fewer than zero or more nulls than there are rows.
  std::vector<int64_t> numNulls;

  /// Verifies the group's invariants: numRows is non-negative and every
  /// numNulls entry is in [0, numRows]. The connector must guarantee this,
  /// including after rescaling an estimate.
  void checkConsistency() const;
};

/// Represents a physical manifestation of a table. There is at least
/// one layout but for tables that have multiple sort orders, partitionings,
/// indices, column groups, etc. there is a separate layout for each. The layout
/// represents data at rest. The ConnectorTableHandle represents the query's
/// constraints on the layout a scan or lookup is accessing.
class TableLayout {
 public:
  /// @param label Label for the layout, unique within the table.
  TableLayout(
      std::string label,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitionColumns,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      bool supportsScan);

  virtual ~TableLayout() = default;

  const std::string& label() const {
    return label_;
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

  /// Describes how the value in partitionColumns() determines a partition.
  /// The returned shared_ptr is owned by 'this'; callers may copy it to
  /// extend lifetime as needed. nullptr if 'partitionColumns_' is empty.
  virtual std::shared_ptr<const PartitionType> partitionType() const {
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

  /// Set of columns that have discrete values that can be enumerated using
  /// 'discretePredicates' API.
  /// For example, Hive connector returns a list of partition keys.
  virtual std::span<const Column* const> discretePredicateColumns() const {
    return {};
  }

  /// Returns an iterator into the list of discrete values of the specified
  /// columns. The union of these values covers all rows of the table. Each
  /// value corresponds to at least one row in the table. If 'columns' doesn't
  /// contain all 'discretePredicateColumns', the results may contains duplicate
  /// values.
  ///
  /// @param columns A subset of 'discretePredicateColumns'. Must not be empty.
  /// Must not contain duplicates.
  virtual std::unique_ptr<DiscretePredicates> discretePredicates(
      [[maybe_unused]] const std::vector<const Column*>& columns) const {
    return nullptr;
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

  /// True if this layout supports sampling for cardinality estimation. If
  /// false, the optimizer skips sampling and falls back to default estimates.
  virtual bool supportsSampling() const {
    return false;
  }

  /// The columns and their names as a RowType.
  const velox::RowTypePtr& rowType() const {
    return rowType_;
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  /// Samples a fraction of rows and applies filters from 'handle'. Returns the
  /// number of rows sampled and the number matching the filters.
  virtual SampleResult sample(
      const velox::connector::ConnectorTableHandlePtr& /*handle*/) const {
    VELOX_UNSUPPORTED("Sampling is not supported for this layout");
  }

  /// Returns estimated statistics for a table scan with the given filters.
  /// Connectors that have access to partition-level metadata (e.g., Hive
  /// Metastore) can resolve matching partitions and aggregate their stats
  /// without reading data.
  ///
  /// @param session Connector session for the current query.
  /// @param tableHandle Table handle for the table.
  /// @param columns Names of table columns the optimizer is interested in.
  /// Column names correspond to actual table columns (not synthetic subfield
  /// projections). If the connector provides per-column statistics, it must
  /// return them for all requested columns in the same order (1:1), or
  /// return an empty columnStats vector if per-column stats are unavailable.
  /// @param filterConjuncts Filter conjuncts applied to the table. This may
  /// be a superset of filters encoded in the table handle. The connector
  /// should report indices of conjuncts it could not account for via
  /// rejectedFilterIndices. The conjuncts are in the same canonical form as
  /// the 'filters' argument of createTableHandle.
  ///
  /// The default implementation returns std::nullopt, meaning the connector
  /// does not support stats estimation. Connectors opt in by overriding.
  virtual folly::coro::Task<std::optional<FilteredTableStats>> co_estimateStats(
      ConnectorSessionPtr /*session*/,
      velox::connector::ConnectorTableHandlePtr /*tableHandle*/,
      std::vector<std::string> /*columns*/,
      std::vector<velox::core::TypedExprPtr> /*filterConjuncts*/) const {
    co_return std::nullopt;
  }

  /// Returns per-group row counts, and per-column null counts, derived from
  /// table metadata, computed WITHOUT reading data. Backs aggregates whose
  /// result is a metadata-derived count rather than a live scan (see
  /// SpecialAggregateKind).
  ///
  /// The connector is free to return an estimate rather than exact counts --
  /// for example, derived from a sampled subset of partitions and rescaled --
  /// so the caller must treat the result as approximate. Filter and grouping
  /// accounting remain all-or-nothing: a connector that cannot resolve every
  /// conjunct, or group by the requested columns, from metadata declines by
  /// returning std::nullopt. There is no rejectedFilterIndices equivalent.
  ///
  /// @param session Connector session for the current query.
  /// @param tableHandle Table handle for the table.
  /// @param groupingColumns Names of columns to group the counts by, in
  /// output-key order. Empty requests a single global count. The connector
  /// returns std::nullopt if it cannot group by these columns from metadata
  /// (e.g. a grouping column is not a partition column).
  /// @param columns Names of columns for which per-group null counts are
  /// requested. Empty when only a row count is needed.
  /// @param filterConjuncts Filter conjuncts applied to the table, in the same
  /// canonical form as the 'filters' argument of createTableHandle. The
  /// connector must account for every conjunct or return std::nullopt.
  ///
  /// @return One MetadataCountGroup per group, or std::nullopt if the connector
  /// cannot answer from metadata. The default implementation returns
  /// std::nullopt.
  virtual folly::coro::Task<std::optional<std::vector<MetadataCountGroup>>>
  co_metadataCounts(
      ConnectorSessionPtr /*session*/,
      velox::connector::ConnectorTableHandlePtr /*tableHandle*/,
      std::vector<std::string> /*groupingColumns*/,
      std::vector<std::string> /*columns*/,
      std::vector<velox::core::TypedExprPtr> /*filterConjuncts*/) const {
    co_return std::nullopt;
  }

  /// Return a column with the matching name. Returns nullptr if not found.
  const Column* FOLLY_NULLABLE findColumn(std::string_view name) const;

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
  ///
  /// The optimizer normalizes 'filters' into a canonical form a connector may
  /// rely on:
  /// - A top-level conjunction is flattened: 'filters' holds one entry per
  ///   conjunct, with no nested AND.
  /// - A comparison against a constant is written column-first, e.g.
  ///   eq(column, constant). The column is always the first argument and the
  ///   constant the second, for eq, lt, lte, gt and gte. A connector never
  ///   sees the constant on the left (e.g. eq(constant, column)).
  /// - An IN list over constants is a single in() over the column and the
  ///   constant values, with duplicate values removed. The values may be
  ///   encoded either as a constant array, in(column, ARRAY[...]), or as
  ///   variadic arguments, in(column, v1, v2, ...). A connector should handle
  ///   both forms.
  ///
  /// Predicates are NOT combined across conjuncts: 'filters' may contain
  /// several predicates on the same column (e.g. a = 1 and a = 2), which the
  /// connector must handle. The optimizer does not fold redundant or
  /// contradictory same-column predicates.
  ///
  /// TODO: Unify the two IN encodings into the varargs form and remove the
  /// ARRAY encoding.
  /// TODO: Fold a single-element IN list to an equality, and simplify
  /// redundant or contradictory same-column predicates, before pushdown.
  virtual velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns = nullptr,
      std::optional<LookupKeys> lookupKeys = std::nullopt) const = 0;

 private:
  const std::string label_;
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
  /// Table::rowIdHandles() are passed to the TableWriter.
  kDelete = 3,

  /// Column values in individual rows are changed. The TableWriter
  /// gets first the row ids as per Table::rowIdHandles()
  /// and then new values for the columns being changed. The new values
  /// may overlap with row ids if the row id is a set of primary key
  /// columns.
  kUpdate = 4,
};

AXIOM_DECLARE_ENUM_NAME(WriteKind);

using RowsFuture = folly::SemiFuture<int64_t>;

/// Base class for table. This is used for name resolution. A TableLayout is
/// used for accessing physical organization like partitioning and sort order.
/// The Table object maintains ownership over the objects it contains, including
/// the TableLayout and Columns contained in the Table.
class Table : public std::enable_shared_from_this<Table> {
 public:
  /// @param columns List of all columns, including hidden. Column names must be
  /// non-empty and unique.
  Table(
      SchemaTableName name,
      std::vector<std::unique_ptr<const Column>> columns,
      folly::F14FastMap<std::string, velox::Variant> options = {});

  /// Given table schema (a list of column names and types), creates a list
  /// 'Column' objects. Useful for connectors that do not use custom 'Column'
  /// objects and do not support hidden columns.
  static std::vector<std::unique_ptr<const Column>> makeColumns(
      const velox::RowTypePtr& rowType);

  virtual ~Table() = default;

  const SchemaTableName& name() const {
    return name_;
  }

  /// Returns all but hidden columns as RowType.
  const velox::RowTypePtr& type() const {
    return type_;
  }

  /// Returns all columns inluding hidden ones.
  const std::vector<const Column*>& allColumns() const {
    return columnPtrs_;
  }

  /// Returns the mapping of columns keyed on column names as abstract,
  /// non-owned columns. Implementations may have different Column
  /// implementations with different options, so we do not return the
  /// implementation's columns but an abstract form.
  const folly::F14FastMap<std::string, const Column*>& columnMap() const {
    return columnMap_;
  }

  const Column* FOLLY_NULLABLE findColumn(std::string_view name) const {
    const auto& map = columnMap();
    auto it = map.find(name);
    return it == map.end() ? nullptr : it->second;
  }

  virtual const std::vector<const TableLayout*>& layouts() const = 0;

  /// Returns an estimate of the number of rows in 'this'. `nullopt`
  /// if the connector has no estimate; downstream cost-based decisions
  /// fall back to safe defaults rather than a fabricated value.
  virtual std::optional<uint64_t> numRows() const = 0;

  /// Returns the table properties specified at creation time (e.g. format,
  /// partitioned_by, bucket_count). Connectors must store all properties
  /// passed to createTable so they can be retrieved here. Values are Variant
  /// scalars (VARCHAR, BIGINT) or arrays of VARCHAR.
  virtual const folly::F14FastMap<std::string, velox::Variant>& options()
      const {
    return options_;
  }

  /// Returns column handles whose value uniquely identifies a row for creating
  /// an update or delete record. These may be for example some connector
  /// specific opaque row id or primary key columns.
  virtual std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
      WriteKind /*kind*/) const {
    VELOX_UNSUPPORTED();
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

 private:
  const SchemaTableName name_;
  const velox::RowTypePtr type_;
  const std::vector<std::unique_ptr<const Column>> columns_;
  const std::vector<const Column*> columnPtrs_;
  const folly::F14FastMap<std::string, const Column*> columnMap_;
  const folly::F14FastMap<std::string, velox::Variant> options_;
};

using TablePtr = std::shared_ptr<const Table>;

class View {
 public:
  View(SchemaTableName name, velox::RowTypePtr type, std::string text)
      : name_(std::move(name)), type_(std::move(type)), text_(std::move(text)) {
    VELOX_CHECK(!name_.schema.empty());
    VELOX_CHECK(!name_.table.empty());

    VELOX_CHECK_NOT_NULL(type_);
    VELOX_CHECK_GT(type_->size(), 0);

    VELOX_CHECK(!text_.empty());
  }

  const SchemaTableName& name() const {
    return name_;
  }

  /// Returns all columns as RowType.
  const velox::RowTypePtr& type() const {
    return type_;
  }

  const std::string& text() const {
    return text_;
  }

  virtual ~View() = default;

 private:
  const SchemaTableName name_;
  const velox::RowTypePtr type_;
  const std::string text_;
};

using ViewPtr = std::shared_ptr<const View>;

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

  /// Returns the column names to use as grouping keys for column statistics
  /// collection. Stats are aggregated per group and passed to finishWrite
  /// alongside a grouping keys RowVector. Returns empty if stats should not
  /// be grouped (e.g. unpartitioned tables produce a single set of stats).
  virtual const std::vector<std::string>& statsGroupingKeys() const {
    static const std::vector<std::string> kEmpty;
    return kEmpty;
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

/// One disjoint root the connector has agreed to execute natively.
/// The optimizer materialises a scan from `table` in place of
/// evaluating `root`; the `LogicalPlan` itself is never rewritten.
/// `table` carries the connector's handle, column statistics, and
/// row-count estimate via the standard `connector::Table` channel.
///
/// EXPERIMENTAL. Negotiates over `LogicalPlan`, which is not normalized.
/// Expected to migrate to the optimizer's v2 IR once it lands; struct and
/// method shapes may change.
struct PushdownRoot {
  /// Non-owning pointer into the `LogicalPlan` being optimized.
  /// Valid for the duration of optimization.
  const logical_plan::LogicalPlanNode* root;

  /// Virtual table representing the pushed-down subtree's output.
  /// Owned by the optimizer for the duration of optimization.
  TablePtr table;
};

/// One overload of a SQL-invoked function resolved from a connector, e.g. a
/// portable UDF from an external function registry. The caller inlines the body
/// at the call site (the connector does not parse it), so the function leaves
/// no call node in the plan.
struct SqlFunctionDefinition {
  /// Declared result type. Carried rather than derived from 'body' because it
  /// may be wider than the body's inferred type: the caller casts (widens) the
  /// resolved body to it. The defining connector guarantees the body type is
  /// coercible to this type.
  velox::TypePtr returnType;

  /// Formal argument names, positionally aligned with 'argumentTypes'. Bind
  /// the call's arguments to matching references inside 'body'.
  std::vector<std::string> argumentNames;

  /// Formal argument types. Used to select among overloads.
  std::vector<velox::TypePtr> argumentTypes;

  /// SQL expression over 'argumentNames', in the dialect of the defining
  /// connector. Each connector documents the single SQL dialect it supports;
  /// callers parse 'body' according to that documentation.
  std::string body;

  /// Whether a null in any argument forces a null result without evaluating the
  /// body (Velox's defaultNullBehavior; SQL's RETURNS NULL ON NULL INPUT).
  /// Carried explicitly because it cannot be recovered from 'body': it is a
  /// property of the definition that dictates how the body is embedded. When
  /// true, the caller guards the inlined body to yield null on a null argument.
  bool defaultNullBehavior{true};
};

using SqlFunctionDefinitionPtr = std::shared_ptr<const SqlFunctionDefinition>;

class ConnectorMetadata {
 public:
  /// Return the metadata for a given connector ID. Throws if not registered.
  [[deprecated("Use ConnectorMetadataRegistry::tryGet() instead.")]]
  static ConnectorMetadata* metadata(std::string_view connectorId);

  /// Return the metadata for a given connector ID, or nullptr if not
  /// registered.
  [[deprecated("Use ConnectorMetadataRegistry::tryGet() instead.")]]
  static ConnectorMetadata* FOLLY_NULLABLE
  tryMetadata(std::string_view connectorId);

  /// Register metadata for a connector ID.
  ///
  /// NOT THREADSAFE: If two processes register the same connector ID, one may
  /// raise a VeloxError.
  [[deprecated("Use ConnectorMetadataRegistry::global().insert() instead.")]]
  static void registerMetadata(
      std::string_view connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Unregister metadata for a connector ID.
  [[deprecated("Use ConnectorMetadataRegistry::global().erase() instead.")]]
  static void unregisterMetadata(std::string_view connectorId);

  /// Unregister all metadata.
  [[deprecated("Use ConnectorMetadataRegistry::global().clear() instead.")]]
  static void unregisterAllMetadata();

  /// Return all registered connector IDs.
  [[deprecated("Use ConnectorMetadataRegistry::allMetadataIds() instead.")]]
  static std::vector<std::string> allMetadataIds();

  virtual ~ConnectorMetadata() = default;

  /// Return a TablePtr given the table name. The returned Table object is
  /// immutable. If updates to the Table object are required, the
  /// ConnectorMetadata is required to drop its reference to the existing Table
  /// and return a reference to a newly created Table object for subsequent
  /// calls to findTable. The ConnectorMetadata may drop its reference to the
  /// Table object at any time, and callers are required to retain a reference
  /// to the Table to prevent it from being reclaimed in the case of Table
  /// removal by the ConnectorMetadata.
  ///
  /// @return nullptr if table doesn't exist.
  virtual TablePtr findTable(const SchemaTableName& tableName) = 0;

  /// Return a ViewPtr given the view name.
  ///
  /// @return nullptr if view doesn't exist.
  virtual ViewPtr findView(const SchemaTableName& /*tableName*/) {
    return nullptr;
  }

  /// Finds a user-defined type by schema-qualified name. Connectors that serve
  /// user-defined types (e.g., enums from an external type registry) override
  /// this method.
  ///
  /// @return nullptr if the type is not found.
  virtual velox::TypePtr findType(const SchemaTypeName& /*typeName*/) {
    return nullptr;
  }

  /// Finds all overloads of a SQL-invoked function by schema-qualified name.
  /// Connectors that serve externally-defined SQL functions (e.g. portable
  /// UDFs from a function registry) override this. Callers select among the
  /// returned overloads by argument type and inline the chosen body.
  ///
  /// @return empty if the function is not found.
  virtual std::vector<SqlFunctionDefinitionPtr> findFunction(
      const SchemaFunctionName& /*functionName*/) {
    return {};
  }

  /// EXPERIMENTAL. Opt-in gate for connector pushdown. Default false.
  /// The pushdown pass skips connectors that return false without
  /// calling `co_pushdownPlan`.
  virtual bool isPushdownSupported() const {
    return false;
  }

  /// EXPERIMENTAL. Returns the disjoint roots this connector will
  /// execute natively. The optimizer calls this once per maximal
  /// subtree whose `TableScanNode`s all belong to this connector;
  /// returned roots must be within `plan`. Each root tells the
  /// optimizer "scan `table` in place of evaluating `root`." Pushing
  /// a bare `TableScanNode` will error.
  ///
  /// Default `VELOX_FAIL`s. Connectors opting in via
  /// `isPushdownSupported()` must override this method.
  ///
  /// See `PushdownRoot` for the v2-IR migration note.
  virtual folly::coro::Task<std::vector<PushdownRoot>> co_pushdownPlan(
      const logical_plan::LogicalPlanNode& /*plan*/) const {
    VELOX_FAIL(
        "Connector opted into pushdown via isPushdownSupported() but did not override co_pushdownPlan()");
  }

  /// Returns a SplitManager for split enumeration for TableLayouts accessed
  /// through 'this'.
  virtual ConnectorSplitManager* splitManager() = 0;

  /// Creates a table. The table properties are in 'options'. All options must
  /// be understood by the connector. To create a table, first make a
  /// ConnectorSession in a connector dependent manner, then call createTable
  /// to retrieve a Table object. Any transaction semantics are
  /// connector-dependent, and the ConnectorSession may be null for connectors
  /// which do not require it. finishWrite should be called to commit the new
  /// table and any writes even if no data is added. To create an empty table,
  /// call createTable, then beginWrite/finishWrite with the generated table
  /// object. To create the table with data, call createTable to generate a
  /// Table, call beginWrite with the Table object, perform writes against the
  /// table using the returned insert handle, then finishWrite to commit the
  /// changes. The table is not available via the findTable interface until
  /// after finishWrite completes.
  ///
  /// When 'ifNotExists' is true and the table already exists, returns nullptr.
  /// When 'ifNotExists' is false and the table already exists, raises an
  /// error. When 'explain' is true, the connector must interpret properties
  /// and return a valid Table with correct layout metadata, but must not
  /// create directories, write files, or register the table. No cleanup is
  /// needed after an explain call.
  virtual TablePtr createTable(
      const ConnectorSessionPtr& /*session*/,
      const SchemaTableName& /*tableName*/,
      const velox::RowTypePtr& /*rowType*/,
      const folly::F14FastMap<std::string, velox::Variant>& /*options*/,
      bool /*ifNotExists*/,
      bool /*explain*/) {
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
  ///
  /// When 'explain' is true, the connector must build and return a valid
  /// ConnectorWriteHandle for plan display, but must not allocate staging
  /// directories or acquire resources that need cleanup.
  virtual ConnectorWriteHandlePtr beginWrite(
      const ConnectorSessionPtr& /*session*/,
      const TablePtr& /*table*/,
      WriteKind /*kind*/,
      bool /*explain*/) {
    VELOX_UNSUPPORTED();
  }

  /// Finalizes the table write operation represented by the provided handle.
  /// Returns a future containing the number of rows written.
  ///
  /// @param session Connector session.
  /// @param handle Write handle returned by beginWrite.
  /// @param writeResults Result sets from the table writer fragments. Contains
  /// only data rows (row counts and file fragments). Stats rows are stripped
  /// by the runner before calling finishWrite.
  /// @param groupingKeys A RowVector with one row per stats group. Contains
  /// one column per grouping key from ConnectorWriteHandle::statsGroupingKeys,
  /// followed by a "$row_count" BIGINT column with the exact row count per
  /// group. nullptr when there are no grouping keys.
  /// @param groupStats Per-group column statistics. groupStats[i] contains
  /// per-column statistics for group i, aligned with rows in groupingKeys.
  /// Empty if no stats were collected. Each ColumnStatistics has a 'name'
  /// field identifying the table column it describes. Use
  /// Table::findColumn(stats.name) to match stats to columns.
  virtual RowsFuture finishWrite(
      [[maybe_unused]] const ConnectorSessionPtr& session,
      [[maybe_unused]] const ConnectorWriteHandlePtr& handle,
      [[maybe_unused]] const std::vector<velox::RowVectorPtr>& writeResults,
      [[maybe_unused]] velox::RowVectorPtr groupingKeys,
      [[maybe_unused]] std::vector<std::vector<ColumnStatistics>> groupStats) {
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
      const ConnectorSessionPtr& /*session*/,
      const ConnectorWriteHandlePtr& /*handle*/) noexcept {
    return {};
  }

  /// Drops a table with the specified name. If table doesn't exist and
  /// 'ifExists' is false, raises an error. Otherwise, returns true if table
  /// was dropped and false if table didn't exist.
  ///
  /// When 'explain' is true, the connector must perform the same existence
  /// check (respecting 'ifExists') but must not drop the table.
  virtual bool dropTable(
      const ConnectorSessionPtr& /*session*/,
      const SchemaTableName& /*tableName*/,
      bool /*ifExists*/,
      bool /*explain*/) {
    VELOX_UNSUPPORTED();
  }

  /// Adds 'columnName' of 'columnType' to an existing table.
  ///
  /// Outcomes (combinations of inputs and table state):
  ///   - Table missing, ifTableExists=false: throws "table missing".
  ///   - Table missing, ifTableExists=true:  returns std::nullopt (no-op).
  ///   - Column missing, any flags:          adds column, returns true.
  ///   - Column exists, ifNotExists=false:   throws "column exists".
  ///   - Column exists, ifNotExists=true:    returns false (no-op).
  ///
  /// When 'explain' is true, the connector must perform the same validation
  /// checks and return the same value (or throw the same user error) as a
  /// real call, but must not write to disk or mutate in-memory state. No
  /// cleanup is needed after an explain call.
  ///
  /// Returns:
  ///   - std::nullopt: table did not exist and 'ifTableExists' is true.
  ///   - true:         column was added (or would be added when 'explain' is
  ///                   true).
  ///   - false:        column already existed and 'ifNotExists' is true.
  virtual std::optional<bool> addColumn(
      const ConnectorSessionPtr& /*session*/,
      const SchemaTableName& /*tableName*/,
      const std::string& /*columnName*/,
      const velox::TypePtr& /*columnType*/,
      bool /*ifTableExists*/,
      bool /*ifNotExists*/,
      bool /*explain*/) {
    VELOX_UNSUPPORTED();
  }

  /// Returns the list of schema names available in this connector. Some
  /// connectors may return only a representative subset of schemas (e.g.,
  /// TPC-H returns a fixed list of scale-factor schemas). Use schemaExists()
  /// to check whether a specific schema exists.
  virtual std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) = 0;

  /// Returns true if the specified schema exists. This may accept schemas
  /// not listed by listSchemaNames() (e.g., TPC-H accepts any valid
  /// scale-factor schema like "sf42").
  virtual bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) = 0;

  /// Returns whether this connector supports listing table names.
  virtual bool listTableNamesSupported() const {
    return true;
  }

  /// Returns table names in the given schema. Results are not guaranteed to be
  /// in any particular order.
  virtual std::vector<std::string> listTableNames(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) = 0;

  /// Creates a schema with the given name and properties. If 'ifNotExists' is
  /// true, succeeds silently when the schema already exists. Otherwise, raises
  /// a user error if the schema already exists.
  virtual void createSchema(
      [[maybe_unused]] const ConnectorSessionPtr& session,
      [[maybe_unused]] const std::string& schemaName,
      [[maybe_unused]] bool ifNotExists,
      [[maybe_unused]] const folly::F14FastMap<std::string, velox::Variant>&
          properties) {
    VELOX_UNSUPPORTED();
  }

  /// Drops a schema with the given name. If 'ifExists' is true, succeeds
  /// silently when the schema does not exist. Otherwise, raises a user error
  /// if the schema does not exist.
  virtual void dropSchema(
      [[maybe_unused]] const ConnectorSessionPtr& session,
      [[maybe_unused]] const std::string& schemaName,
      [[maybe_unused]] bool ifExists) {
    VELOX_UNSUPPORTED();
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }
};

} // namespace facebook::axiom::connector

AXIOM_ENUM_FORMATTER(facebook::axiom::connector::WriteKind);
