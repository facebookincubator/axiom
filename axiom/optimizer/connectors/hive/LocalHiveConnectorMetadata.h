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

#include "axiom/optimizer/connectors/hive/HiveConnectorMetadata.h"
#include "axiom/optimizer/connectors/hive/StatisticsBuilder.h"
#include "velox/common/base/Fs.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::connector::hive {

/// Describes a file in a table. Input to split enumeration.
struct FileInfo {
  std::string path;
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  std::optional<int32_t> bucketNumber;
};

class LocalHiveSplitSource : public SplitSource {
 public:
  LocalHiveSplitSource(
      std::vector<const FileInfo*> files,
      dwio::common::FileFormat format,
      const std::string& connectorId,
      SplitOptions options)
      : options_(options),
        format_(format),
        connectorId_(connectorId),
        files_(files) {}

  std::vector<SplitSource::SplitAndGroup> getSplits(
      uint64_t targetBytes) override;

 private:
  const SplitOptions options_;
  const dwio::common::FileFormat format_;
  const std::string connectorId_;
  std::vector<const FileInfo*> files_;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> fileSplits_;
  int32_t currentFile_{-1};
  int32_t currentSplit_{0};
};

class LocalHiveConnectorMetadata;

class LocalHiveSplitManager : public ConnectorSplitManager {
 public:
  LocalHiveSplitManager(LocalHiveConnectorMetadata* /* metadata */) {}
  std::vector<PartitionHandlePtr> listPartitions(
      const ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

/// A HiveTableLayout backed by local files. Implements sampling by reading
/// local files and stores the file list inside 'this'.
class LocalHiveTableLayout : public HiveTableLayout {
 public:
  LocalHiveTableLayout(
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
      : HiveTableLayout(
            name,
            table,
            connector,
            columns,
            partitioning,
            orderColumns,
            sortOrder,
            lookupKeys,
            hivePartitionColumns,
            fileFormat,
            numBuckets) {}

  std::pair<int64_t, int64_t> sample(
      const connector::ConnectorTableHandlePtr& handle,
      float pct,
      const std::vector<core::TypedExprPtr>& extraFilters,
      RowTypePtr outputType,
      const std::vector<common::Subfield>& fields,
      HashStringAllocator* allocator,
      std::vector<ColumnStatistics>* statistics) const override;

  const std::vector<std::unique_ptr<const FileInfo>>& files() const {
    return files_;
  }

  void setFiles(std::vector<std::unique_ptr<const FileInfo>> files) {
    files_ = std::move(files);
  }

  /// Like sample() above, but fills 'builders' with the data.
  std::pair<int64_t, int64_t> sample(
      const connector::ConnectorTableHandlePtr& handle,
      float pct,
      RowTypePtr scanType,
      const std::vector<common::Subfield>& fields,
      HashStringAllocator* allocator,
      std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const;

 private:
  std::vector<std::unique_ptr<const FileInfo>> files_;
  std::vector<std::unique_ptr<const FileInfo>> ownedFiles_;
};

class LocalTable : public Table {
 public:
  LocalTable(
      std::string name,
      RowTypePtr type,
      std::unordered_map<std::string, std::string> options = {})
      : Table(
            std::move(name),
            std::move(type),
            TableKind::kTable,
            std::move(options)) {}

  std::unordered_map<std::string, std::unique_ptr<Column>>& columns() {
    return columns_;
  }
  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  const std::unordered_map<std::string, const Column*>& columnMap()
      const override;

  void makeDefaultLayout(
      std::vector<std::unique_ptr<const FileInfo>> files,
      LocalHiveConnectorMetadata& metadata);

  uint64_t numRows() const override {
    return numRows_;
  }

  /// Samples  'samplePct' % rows of the table and sets the num distincts
  /// estimate for the columns. uses 'pool' for temporary data.
  void sampleNumDistincts(float samplePct, memory::MemoryPool* pool);

 private:
  // Serializes initialization, e.g. exportedColumns_.
  mutable std::mutex mutex_;

  // All columns. Filled by loadTable().
  std::unordered_map<std::string, std::unique_ptr<Column>> columns_;

  // Non-owning columns map used for exporting the column set as abstract
  // columns.
  mutable std::unordered_map<std::string, const Column*> exportedColumns_;

  ///  Table layouts. For a Hive table this is normally one layout with all
  ///  columns included.
  std::vector<std::unique_ptr<TableLayout>> layouts_;

  // Copy of 'llayouts_' for use in layouts().
  std::vector<const TableLayout*> exportedLayouts_;

  int64_t numRows_{0};
  int64_t numSampledRows_{0};

  friend class LocalHiveConnectorMetadata;
};

class LocalHiveConnectorMetadata : public HiveConnectorMetadata {
 public:
  explicit LocalHiveConnectorMetadata(HiveConnector* hiveConnector);

  void initialize() override;

  TablePtr findTable(const std::string& name) override;

  ConnectorSplitManager* splitManager() override {
    ensureInitialized();
    return &splitManager_;
  }

  dwio::common::FileFormat fileFormat() const override {
    return format_;
  }

  const std::shared_ptr<ConnectorQueryCtx>& connectorQueryCtx() const {
    return connectorQueryCtx_;
  }

  HiveConnector* hiveConnector() const {
    return hiveConnector_;
  }

  /// Rereads the contents of the data path and re-creates the tables
  /// and stats. This is used in tests after adding tables.
  void reinitialize();

  /// returns the set of known tables. This is not part of the
  /// ConnectorMetadata API. This This is only needed for running the
  /// DuckDB parser on testing queries since the latter needs a set of
  /// tables for name resolution.
  const std::unordered_map<std::string, std::shared_ptr<LocalTable>>& tables()
      const {
    ensureInitialized();
    return tables_;
  }

  std::shared_ptr<core::QueryCtx> makeQueryCtx(const std::string& queryId);

  void createTable(
      const std::string& tableName,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      const ConnectorSessionPtr& session,
      bool errorIfExists = true,
      TableKind kind = TableKind::kTable) override;

  void finishWrite(
      const TableLayout& layout,
      const ConnectorInsertTableHandlePtr& /*handle*/,
      const std::vector<RowVectorPtr>& /*writerResult*/,
      WriteKind /*kind*/,
      const ConnectorSessionPtr& /*session*/) override;

 protected:
  std::string dataPath() const override {
    return hiveConfig_->hiveLocalDataPath();
  }

  std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
      std::string targetDirectory,
      std::optional<std::string> writeDirectory = std::nullopt,
      connector::hive::LocationHandle::TableType tableType =
          connector::hive::LocationHandle::TableType::kNew) override {
    return std::make_shared<connector::hive::LocationHandle>(
        targetDirectory, writeDirectory.value_or(targetDirectory), tableType);
  }

 private:
  void ensureInitialized() const override;
  void makeQueryCtx();
  void makeConnectorQueryCtx();
  std::shared_ptr<LocalTable> createTableFromSchema(
      const std::string& name,
      const std::string& path);
  void readTables(const std::string& path);

  void loadTable(const std::string& tableName, const fs::path& tablePath);

  std::shared_ptr<LocalTable> findTableLocked(const std::string& name) const;

  mutable std::mutex mutex_;
  mutable bool initialized_{false};
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> schemaPool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx_;
  dwio::common::FileFormat format_;
  std::unordered_map<std::string, std::shared_ptr<LocalTable>> tables_;
  LocalHiveSplitManager splitManager_;
};

} // namespace facebook::velox::connector::hive
