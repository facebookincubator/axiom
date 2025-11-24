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

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/dwio/common/Options.h"

namespace facebook::axiom::connector::hive {

/// Describes a single partition of a Hive table. If the table is
/// bucketed, this resolves to a single file. If the table is
/// partitioned and not bucketed, this resolves to a leaf
/// directory. If the table is not bucketed and not partitioned,
/// this resolves to the directory corresponding to the table.
struct HivePartitionHandle : public PartitionHandle {
  HivePartitionHandle(
      folly::F14FastMap<std::string, std::optional<std::string>> partitionKeys,
      std::optional<int32_t> tableBucketNumber)
      : partitionKeys(std::move(partitionKeys)),
        tableBucketNumber(tableBucketNumber) {}

  const folly::F14FastMap<std::string, std::optional<std::string>>
      partitionKeys;
  const std::optional<int32_t> tableBucketNumber;
};

/// For Hive, 'partition' means 'bucket'.
class HivePartitionType : public connector::PartitionType {
 public:
  HivePartitionType(
      int32_t numPartitions,
      std::vector<velox::TypePtr> partitionKeyTypes)
      : numPartitions_(numPartitions),
        partitionKeyTypes_(std::move(partitionKeyTypes)) {}

  /// Types are compatible if numPartitions of one is an interger multiple of
  /// the other. The partition to use for copartitioning is the one with the
  /// fewer partitions. If numPartitions is the same, returns 'this'.
  const PartitionType* copartition(const PartitionType& any) const override;

  velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<velox::column_index_t>& channels,
      const std::vector<velox::VectorPtr>& constants,
      bool isLocal) const override;

  std::string toString() const override;

 private:
  const int32_t numPartitions_;
  const std::vector<velox::TypePtr> partitionKeyTypes_;
};

/// Describes a Hive table layout. Adds a file format and a list of
/// Hive partitioning columns and an optional bucket count to the base
/// TableLayout. The partitioning in TableLayout referes to bucketing.
/// 'numBuckets' is the number of Hive buckets if
/// 'partitionColumns' is not empty. 'hivePartitionColumns' refers to Hive
/// partitioning, i.e. columns whose value gives a directory in the file storage
/// tree.
class HiveTableLayout : public TableLayout {
 public:
  /// @param numPartitions Hive's bucket count.
  /// @param partitionedByColumns Hive's bucketed-by keys.
  /// @param sortedByColumns Hive's sorted-by keys. Applies within a single
  /// bucket.
  /// @param sortOrder Sorting order for 'sortedByColumns'. 1:1 with
  /// 'sortedByColumns'.
  /// @param hivePartitionedByColumns Hive's partitioned-by keys.
  HiveTableLayout(
      const std::string& name,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::optional<int32_t> numPartitions,
      std::vector<const Column*> partitionedByColumns,
      std::vector<const Column*> sortedByColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      std::vector<const Column*> hivePartitionedByColumns,
      velox::dwio::common::FileFormat fileFormat);

  velox::dwio::common::FileFormat fileFormat() const {
    return fileFormat_;
  }

  const std::vector<const Column*>& hivePartitionColumns() const {
    return hivePartitionColumns_;
  }

  std::optional<int32_t> numBuckets() const {
    return numBuckets_;
  }

  const PartitionType* partitionType() const override {
    return partitionType_.has_value() ? &partitionType_.value() : nullptr;
  }

  /// Returns SerDe parameters for this layout. Default implementation returns
  /// empty map. Derived classes can override to provide actual parameters.
  virtual const std::unordered_map<std::string, std::string>& serdeParameters()
      const {
    static const std::unordered_map<std::string, std::string> kEmpty;
    return kEmpty;
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields = {},
      std::optional<velox::TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) const override;

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns,
      std::optional<LookupKeys> lookupKeys) const override;

 protected:
  const velox::dwio::common::FileFormat fileFormat_;
  const std::vector<const Column*> hivePartitionColumns_;
  const std::optional<int32_t> numBuckets_;
  const std::optional<HivePartitionType> partitionType_;
};

class HiveConnectorWriteHandle : public ConnectorWriteHandle {
 public:
  HiveConnectorWriteHandle(
      velox::connector::hive::HiveInsertTableHandlePtr veloxHandle,
      velox::RowTypePtr resultType,
      TablePtr table,
      WriteKind kind)
      : ConnectorWriteHandle{std::move(veloxHandle), std::move(resultType)},
        table_{std::move(table)},
        kind_{kind} {}

  const TablePtr& table() const {
    return table_;
  }

  WriteKind kind() const {
    return kind_;
  }

 private:
  const TablePtr table_;
  const WriteKind kind_;
};

/// The full list of options accepted for createTable.
/// Any specified options not listed below will trigger
/// a validation error during table create.
class HiveWriteOptions {
 public:
  /// Comma-delimited list of columns to bucket the table by.
  /// "bucket_count" must also be specified if this option is set.
  /// The default is no bucketing columns.
  static constexpr auto kBucketedBy = "bucketed_by";

  /// The number of buckets to create the table with. The number
  /// of buckets must be an integer power of 2. "bucketed_by" must
  /// also be specified if this option is set.
  static constexpr auto kBucketCount = "bucket_count";

  /// Comma-delimited list of columns to partition the table by.
  /// The default is no partition columns.
  static constexpr auto kPartitionedBy = "partitioned_by";

  /// Comma-delimited list of sorting columns. Sorting is only
  /// supported for bucketed tables and sorting is only applied
  /// to individual buckets. The default is no sorting columns.
  static constexpr auto kSortedBy = "sorted_by";

  /// The table storage format. See velox::dwio::common::FileFormat.
  /// The default is DWRF format.
  static constexpr auto kFileFormat = "file_format";

  /// The table compression kind. See velox::common::CompressionKind.
  /// The default is ZSTD compression.
  static constexpr auto kCompressionKind = "compression_kind";

  /// Field delimiter for TEXT format files.
  static constexpr auto kFieldDelim = "field.delim";

  /// Null string format for TEXT format files.
  static constexpr auto kSerializationNullFormat = "serialization.null.format";
};

class HiveConnectorMetadata : public ConnectorMetadata {
 public:
  explicit HiveConnectorMetadata(
      velox::connector::hive::HiveConnector* hiveConnector)
      : hiveConnector_(hiveConnector),
        hiveConfig_(
            std::make_shared<velox::connector::hive::HiveConfig>(
                hiveConnector->connectorConfig())) {}

  ConnectorWriteHandlePtr beginWrite(
      const ConnectorSessionPtr& session,
      const TablePtr& table,
      WriteKind kind) override;

 protected:
  virtual void ensureInitialized() const {}

  virtual void validateOptions(
      const folly::F14FastMap<std::string, velox::Variant>& options) const;

  /// Return the filesystem path for the storage of the specified table.
  virtual std::string tablePath(std::string_view table) const = 0;

  /// Optionally, create a staging directory for the specified table.
  /// This directory, if provided, will be used for insert/delete/update into
  /// this table.
  /// @return The filesystem path of the staging directory.
  virtual std::optional<std::string> makeStagingDirectory(
      std::string_view table) const = 0;

  velox::connector::hive::HiveConnector* const hiveConnector_;
  const std::shared_ptr<velox::connector::hive::HiveConfig> hiveConfig_;
};

} // namespace facebook::axiom::connector::hive
