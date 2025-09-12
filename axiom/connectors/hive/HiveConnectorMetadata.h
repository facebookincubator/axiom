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

namespace facebook::velox::connector::hive {

/// Describes a single partition of a Hive table. If the table is
/// bucketed, this resolves to a single file. If the table is
/// partitioned and not bucketed, this resolves to a leaf
/// directory. If the table is not bucketed and not partitioned,
/// this resolves to the directory corresponding to the table.
struct HivePartitionHandle : public PartitionHandle {
  HivePartitionHandle(
      const std::unordered_map<std::string, std::optional<std::string>>
          partitionKeys,
      std::optional<int32_t> tableBucketNumber)
      : partitionKeys(partitionKeys), tableBucketNumber(tableBucketNumber) {}

  const std::unordered_map<std::string, std::optional<std::string>>
      partitionKeys;
  const std::optional<int32_t> tableBucketNumber;
};

class HiveConnectorSession : public connector::ConnectorSession {
 public:
  ~HiveConnectorSession() override = default;
};

/// Describes a Hive table layout. Adds a file format and a list of
/// Hive partitioning columns and an optional bucket count to the base
/// TableLayout. The partitioning in TableLayout referes to bucketing.
/// 'numBuckets' is the number of Hive buckets if
/// 'partitionColumns' is not empty. 'hivePartitionColumns' refers to Hive
/// partitioning, i.e. columns whose value gives a directory in the ile storage
/// tree.
class HiveTableLayout : public TableLayout {
 public:
  HiveTableLayout(
      const std::string& name,
      const Table* table,
      connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitioning,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      std::vector<const Column*> hivePartitionColumns,
      dwio::common::FileFormat fileFormat,
      std::optional<int32_t> numBuckets = std::nullopt)
      : TableLayout(
            name,
            table,
            connector,
            columns,
            partitioning,
            orderColumns,
            sortOrder,
            lookupKeys,
            true),
        fileFormat_(fileFormat),
        hivePartitionColumns_(hivePartitionColumns),
        numBuckets_(numBuckets) {}

  dwio::common::FileFormat fileFormat() const {
    return fileFormat_;
  }

  const std::vector<const Column*>& hivePartitionColumns() const {
    return hivePartitionColumns_;
  }

  std::optional<int32_t> numBuckets() const {
    return numBuckets_;
  }

 protected:
  const dwio::common::FileFormat fileFormat_;
  const std::vector<const Column*> hivePartitionColumns_;
  const std::optional<int32_t> numBuckets_;
};

class HiveConnectorMetadata : public ConnectorMetadata {
 public:
  explicit HiveConnectorMetadata(HiveConnector* hiveConnector)
      : hiveConnector_(hiveConnector),
        hiveConfig_(
            std::make_shared<HiveConfig>(hiveConnector->connectorConfig())) {}

  ColumnHandlePtr createColumnHandle(
      const TableLayout& layout,
      const std::string& columnName,
      std::vector<common::Subfield> subfields = {},
      std::optional<TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) override;

  ConnectorTableHandlePtr createTableHandle(
      const TableLayout& layout,
      std::vector<ColumnHandlePtr> columnHandles,
      core::ExpressionEvaluator& evaluator,
      std::vector<core::TypedExprPtr> filters,
      std::vector<core::TypedExprPtr>& rejectedFilters,
      RowTypePtr dataColumns,
      std::optional<LookupKeys> lookupKeys) override;

  ConnectorInsertTableHandlePtr createInsertTableHandle(
      const TableLayout& layout,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      WriteKind kind,
      const ConnectorSessionPtr& session) override;

  void createTable(
      const std::string& tableName,
      const velox::RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      const velox::connector::ConnectorSessionPtr& session,
      bool errorIfExists = true,
      velox::connector::TableKind tableKind =
          velox::connector::TableKind::kTable) override {
    VELOX_UNSUPPORTED();
  }

  void finishWrite(
      const velox::connector::TableLayout& layout,
      const velox::connector::ConnectorInsertTableHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writerResult,
      velox::connector::WriteKind kind,
      const velox::connector::ConnectorSessionPtr& session) override {
    VELOX_UNSUPPORTED();
  }

  WritePartitionInfo writePartitionInfo(
      const ConnectorInsertTableHandlePtr& handle) override {
    VELOX_UNSUPPORTED();
  }

  std::vector<ColumnHandlePtr> rowIdHandles(
      const TableLayout& layout,
      WriteKind kind) override {
    VELOX_UNSUPPORTED();
  }

  virtual dwio::common::FileFormat fileFormat() const = 0;

 protected:
  virtual void ensureInitialized() const {}

  virtual void validateOptions(
      const std::unordered_map<std::string, std::string>& options) const;

  virtual std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      std::optional<std::string> writeDirectory,
      connector::hive::LocationHandle::TableType tableType =
          connector::hive::LocationHandle::TableType::kNew) = 0;

  /// Returns the path to the filesystem root for the data managed by
  /// 'this'. Directories inside this correspond to schemas and
  /// tables.
  virtual std::string dataPath() const = 0;

  HiveConnector* const hiveConnector_;
  const std::shared_ptr<HiveConfig> hiveConfig_;
};

} // namespace facebook::velox::connector::hive
