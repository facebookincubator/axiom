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

#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "axiom/connectors/hive/StatisticsBuilder.h"
#include "velox/common/base/Fs.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"

namespace facebook::axiom::connector::hive {

/// Describes a file in a table. Input to split enumeration.
struct FileInfo {
  std::string path;
  folly::F14FastMap<std::string, std::optional<std::string>> partitionKeys;
  std::optional<int32_t> bucketNumber;
};

class LocalHiveSplitSource : public SplitSource {
 public:
  LocalHiveSplitSource(
      std::vector<const FileInfo*> files,
      velox::dwio::common::FileFormat format,
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
  const velox::dwio::common::FileFormat format_;
  const std::string connectorId_;
  std::vector<const FileInfo*> files_;
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> fileSplits_;
  int32_t currentFile_{-1};
  int32_t currentSplit_{0};
};

class LocalHiveConnectorMetadata;

class LocalHiveSplitManager : public ConnectorSplitManager {
 public:
  LocalHiveSplitManager(LocalHiveConnectorMetadata* /* metadata */) {}
  std::vector<PartitionHandlePtr> listPartitions(
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
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
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitioning,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      std::vector<const Column*> hivePartitionColumns,
      velox::dwio::common::FileFormat fileFormat,
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
      const velox::connector::ConnectorTableHandlePtr& handle,
      float pct,
      const std::vector<velox::core::TypedExprPtr>& extraFilters,
      velox::RowTypePtr outputType,
      const std::vector<velox::common::Subfield>& fields,
      velox::HashStringAllocator* allocator,
      std::vector<ColumnStatistics>* statistics) const override;

  const std::vector<std::unique_ptr<const FileInfo>>& files() const {
    return files_;
  }

  void setFiles(std::vector<std::unique_ptr<const FileInfo>> files) {
    files_ = std::move(files);
  }

  /// Like sample() above, but fills 'builders' with the data.
  std::pair<int64_t, int64_t> sample(
      const velox::connector::ConnectorTableHandlePtr& handle,
      float pct,
      velox::RowTypePtr scanType,
      const std::vector<velox::common::Subfield>& fields,
      velox::HashStringAllocator* allocator,
      std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const;

 private:
  std::vector<std::unique_ptr<const FileInfo>> files_;
  std::vector<std::unique_ptr<const FileInfo>> ownedFiles_;
};

class LocalTable : public Table {
 public:
  LocalTable(
      std::string name,
      velox::RowTypePtr type,
      folly::F14FastMap<std::string, std::string> options = {})
      : Table(
            std::move(name),
            std::move(type),
            TableKind::kTable,
            std::move(options)) {}

  folly::F14FastMap<std::string, std::unique_ptr<Column>>& columns() {
    return columns_;
  }
  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  const folly::F14FastMap<std::string, const Column*>& columnMap()
      const override;

  void makeDefaultLayout(
      std::vector<std::unique_ptr<const FileInfo>> files,
      LocalHiveConnectorMetadata& metadata);

  uint64_t numRows() const override {
    return numRows_;
  }

  /// Samples  'samplePct' % rows of the table and sets the num distincts
  /// estimate for the columns. uses 'pool' for temporary data.
  void sampleNumDistincts(float samplePct, velox::memory::MemoryPool* pool);

 private:
  // Serializes initialization, e.g. exportedColumns_.
  mutable std::mutex mutex_;

  // All columns. Filled by loadTable().
  folly::F14FastMap<std::string, std::unique_ptr<Column>> columns_;

  // Non-owning columns map used for exporting the column set as abstract
  // columns.
  mutable folly::F14FastMap<std::string, const Column*> exportedColumns_;

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
  explicit LocalHiveConnectorMetadata(
      velox::connector::hive::HiveConnector* hiveConnector);

  void initialize() override;

  TablePtr findTable(std::string_view name) override;

  ConnectorSplitManager* splitManager() override {
    ensureInitialized();
    return &splitManager_;
  }

  velox::dwio::common::FileFormat fileFormat() const {
    return format_;
  }

  const std::shared_ptr<velox::connector::ConnectorQueryCtx>&
  connectorQueryCtx() const {
    return connectorQueryCtx_;
  }

  velox::connector::hive::HiveConnector* hiveConnector() const {
    return hiveConnector_;
  }

  /// Rereads the contents of the data path and re-creates the tables
  /// and stats. This is used in tests after adding tables.
  void reinitialize();

  /// returns the set of known tables. This is not part of the
  /// ConnectorMetadata API. This This is only needed for running the
  /// DuckDB parser on testing queries since the latter needs a set of
  /// tables for name resolution.
  const folly::F14FastMap<std::string, std::shared_ptr<LocalTable>>& tables()
      const {
    ensureInitialized();
    return tables_;
  }

  std::shared_ptr<velox::core::QueryCtx> makeQueryCtx(
      const std::string& queryId);

  void createTable(
      const std::string& tableName,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, std::string>& options,
      const ConnectorSessionPtr& session,
      bool errorIfExists = true,
      TableKind kind = TableKind::kTable) override;

  void finishWrite(
      const TableLayout& layout,
      const velox::connector::ConnectorInsertTableHandlePtr& /*handle*/,
      const std::vector<velox::RowVectorPtr>& /*writerResult*/,
      WriteKind /*kind*/,
      const ConnectorSessionPtr& /*session*/) override;

 protected:
  std::string tablePath(std::string_view table) const override {
    return fmt::format("{}/{}", hiveConfig_->hiveLocalDataPath(), table);
  }

 private:
  void ensureInitialized() const override;
  void makeQueryCtx();
  void makeConnectorQueryCtx();
  std::shared_ptr<LocalTable> createTableFromSchema(
      std::string_view name,
      std::string_view path);
  void readTables(std::string_view path);

  void loadTable(std::string_view tableName, const fs::path& tablePath);

  std::shared_ptr<LocalTable> findTableLocked(std::string_view name) const;

  mutable std::mutex mutex_;
  mutable bool initialized_{false};
  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<velox::memory::MemoryPool> schemaPool_;
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  std::shared_ptr<velox::connector::ConnectorQueryCtx> connectorQueryCtx_;
  velox::dwio::common::FileFormat format_;
  folly::F14FastMap<std::string, std::shared_ptr<LocalTable>> tables_;
  LocalHiveSplitManager splitManager_;
};

} // namespace facebook::axiom::connector::hive
