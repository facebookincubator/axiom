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

#include <utility>

#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "axiom/connectors/hive/StatisticsBuilder.h"
#include "folly/coro/Task.h"
#include "velox/common/base/Fs.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"

namespace facebook::axiom::connector::hive {

class LocalHiveSplitSource : public SplitSource {
 public:
  LocalHiveSplitSource(
      std::vector<const FileInfo*> files,
      velox::dwio::common::FileFormat format,
      const std::string& connectorId,
      SplitOptions options,
      std::unordered_map<std::string, std::string> serdeParameters = {})
      : options_(options),
        format_(format),
        connectorId_(connectorId),
        files_(std::move(files)),
        serdeParameters_(std::move(serdeParameters)) {}

  folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override;

 private:
  const SplitOptions options_;
  const velox::dwio::common::FileFormat format_;
  const std::string connectorId_;
  std::vector<const FileInfo*> files_;
  const std::unordered_map<std::string, std::string> serdeParameters_;
  size_t fileIdx_{0};
  int64_t splitWithinFile_{0};
};

class LocalHiveConnectorMetadata;

class LocalHiveSplitManager : public ConnectorSplitManager {
 public:
  explicit LocalHiveSplitManager(LocalHiveConnectorMetadata* /* metadata */) {}

  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

// Write-time stats for a single partition (or the whole table if
// unpartitioned). Loaded from persisted .stats files during loadTable.
struct PartitionStats {
  // Partition key=value pairs. Empty for unpartitioned tables.
  folly::F14FastMap<std::string, std::string> partitionKeys;

  // Total number of rows in this partition.
  uint64_t numRows{0};

  // Per-column stats (min, max, numValues, numDistinct).
  // TODO: Store HLL sketches instead of NDV counts to allow for more
  // accurate aggregation across partitions.
  std::vector<ColumnStatistics> columnStats;
};

/// A HiveTableLayout backed by local files. Implements sampling by reading
/// local files and stores the file list inside 'this'.
class LocalHiveTableLayout : public HiveTableLayout {
 public:
  LocalHiveTableLayout(
      const std::string& label,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::optional<int32_t> numBuckets,
      const std::vector<const Column*>& partitioning,
      const std::vector<const Column*>& orderColumns,
      const std::vector<SortOrder>& sortOrder,
      std::vector<const Column*> lookupKeys,
      std::vector<const Column*> hivePartitionColumns,
      velox::dwio::common::FileFormat fileFormat,
      std::shared_ptr<HiveMetadataConfig> hiveMetadataConfig,
      std::unordered_map<std::string, std::string> serdeParameters = {})
      : HiveTableLayout(
            label,
            table,
            connector,
            std::move(columns),
            numBuckets,
            partitioning,
            orderColumns,
            sortOrder,
            std::move(lookupKeys),
            std::move(hivePartitionColumns),
            fileFormat),
        hiveMetadataConfig_(std::move(hiveMetadataConfig)),
        serdeParameters_(std::move(serdeParameters)) {}

  bool supportsSampling() const override {
    return true;
  }

  SampleResult sample(
      const velox::connector::ConnectorTableHandlePtr& handle) const override;

  const std::vector<std::unique_ptr<const FileInfo>>& files() const {
    return files_;
  }

  void setFiles(std::vector<std::unique_ptr<const FileInfo>> files) {
    files_ = std::move(files);
  }

  /// Sets per-partition write-time stats loaded from persisted .stats files.
  void setPartitionStats(std::vector<PartitionStats> partitionStats) {
    partitionStats_ = std::move(partitionStats);
  }

  const std::unordered_map<std::string, std::string>& serdeParameters()
      const override {
    return serdeParameters_;
  }

  /// Like sample() above, but fills 'builders' with the data.
  std::pair<int64_t, int64_t> sample(
      const velox::connector::ConnectorTableHandlePtr& handle,
      float pct,
      const std::vector<velox::common::Subfield>& fields,
      velox::HashStringAllocator* allocator,
      std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const;

  /// Returns estimated statistics by pruning files using partition key and
  /// hidden column filters from 'filterConjuncts', then aggregating per-file
  /// stats.
  folly::coro::Task<std::optional<FilteredTableStats>> co_estimateStats(
      ConnectorSessionPtr session,
      velox::connector::ConnectorTableHandlePtr tableHandle,
      std::vector<std::string> columns,
      std::vector<velox::core::TypedExprPtr> filterConjuncts) const override;

 private:
  // Configuration for local Hive metadata.
  std::shared_ptr<HiveMetadataConfig> hiveMetadataConfig_;
  std::vector<std::unique_ptr<const FileInfo>> files_;
  std::vector<std::unique_ptr<const FileInfo>> ownedFiles_;
  // Per-partition (or per-table for unpartitioned) write-time stats loaded
  // from persisted .stats files. Populated during loadTable.
  std::vector<PartitionStats> partitionStats_;
  std::unordered_map<std::string, std::string> serdeParameters_;
};

class LocalTable : public HiveTable {
 public:
  LocalTable(
      SchemaTableName name,
      velox::RowTypePtr type,
      bool bucketed,
      folly::F14FastMap<std::string, velox::Variant> options,
      std::vector<std::string> partitionColumnNames = {});

  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  void addLayout(std::unique_ptr<LocalHiveTableLayout> layout) {
    exportedLayouts_.push_back(layout.get());
    layouts_.push_back(std::move(layout));
  }

  // Creates or updates the default layout with the given files. Returns the
  // layout pointer.
  LocalHiveTableLayout* makeDefaultLayout(
      std::vector<std::unique_ptr<const FileInfo>> files,
      LocalHiveConnectorMetadata& metadata);

  uint64_t numRows() const override {
    return numRows_;
  }

  void incrementNumRows(uint64_t n) {
    numRows_ += n;
  }

 private:
  // Serializes initialization, e.g. exportedColumns_.
  mutable std::mutex mutex_;

  // Table layouts. For a Hive table this is normally one layout with all
  // columns included.
  std::vector<std::unique_ptr<TableLayout>> layouts_;

  // Copy of 'layouts_' for use in layouts().
  std::vector<const TableLayout*> exportedLayouts_;

  int64_t numRows_{0};
};

class LocalHiveConnectorMetadata : public HiveConnectorMetadata {
 public:
  static constexpr std::string_view kDefaultSchema = "default";

  explicit LocalHiveConnectorMetadata(
      velox::connector::hive::HiveConnector* hiveConnector);

  TablePtr findTable(const SchemaTableName& tableName) override;

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override {
    return {std::string(kDefaultSchema)};
  }

  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override {
    return schemaName == kDefaultSchema;
  }

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

  /// Returns the metadata configuration (data path, file format, etc.).
  const std::shared_ptr<HiveMetadataConfig>& hiveMetadataConfig() const {
    return hiveMetadataConfig_;
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

  TablePtr createTable(
      const ConnectorSessionPtr& session,
      const SchemaTableName& tableName,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, velox::Variant>& options,
      bool explain) override;

  RowsFuture finishWrite(
      const ConnectorSessionPtr& session,
      const ConnectorWriteHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writeResults,
      velox::RowVectorPtr groupingKeys,
      std::vector<std::vector<ColumnStatistics>> groupStats) override;

  velox::ContinueFuture abortWrite(
      const ConnectorSessionPtr& session,
      const ConnectorWriteHandlePtr& handle) noexcept override;

  std::string tablePath(const SchemaTableName& tableName) const override {
    return fmt::format(
        "{}/{}", hiveMetadataConfig_->localDataPath(), tableName.table);
  }

  std::optional<std::string> makeStagingDirectory(
      const SchemaTableName& tableName) const override;

  bool dropTable(
      const ConnectorSessionPtr& session,
      const SchemaTableName& tableName,
      bool ifExists) override;

  std::optional<bool> addColumn(
      const ConnectorSessionPtr& session,
      const SchemaTableName& tableName,
      const std::string& columnName,
      const velox::TypePtr& columnType,
      bool ifTableExists,
      bool ifNotExists) override;

  /// Shortcut for dropTable(session, tableName, true).
  bool dropTableIfExists(const SchemaTableName& tableName) {
    return dropTable(nullptr, tableName, true);
  }

  bool dropTableIfExists(std::string_view tableName) {
    return dropTableIfExists(
        {std::string(kDefaultSchema), std::string(tableName)});
  }

  /// Loads or reloads a table from disk, discovering any new files.
  /// This is useful when files are manually added to a table directory.
  void reloadTableFromPath(const SchemaTableName& tableName);

 private:
  // Used to lazy initialize this in ensureInitialized() and to implement
  // reinitialize().
  void initialize();
  void ensureInitialized() const override;
  void makeQueryCtx();
  void makeConnectorQueryCtx();
  void readTables(std::string_view path);
  void loadTable(std::string_view tableName, const fs::path& tablePath);

  // Loads persisted write-time stats from .stats files and stores them in the
  // layout. Sets table-level row count and column stats.
  void loadTableWithWriteTimeStats(
      std::shared_ptr<LocalTable> table,
      LocalHiveTableLayout* layout,
      const fs::path& tablePath);

  std::shared_ptr<LocalTable> findTableLocked(std::string_view name) const;

  mutable std::mutex mutex_;
  mutable bool initialized_{false};
  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<velox::memory::MemoryPool> schemaPool_;
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  std::shared_ptr<velox::connector::ConnectorQueryCtx> connectorQueryCtx_;
  velox::dwio::common::FileFormat format_{
      velox::dwio::common::FileFormat::UNKNOWN};
  folly::F14FastMap<std::string, std::shared_ptr<LocalTable>> tables_;
  LocalHiveSplitManager splitManager_;
  std::shared_ptr<HiveMetadataConfig> hiveMetadataConfig_;
};

} // namespace facebook::axiom::connector::hive
