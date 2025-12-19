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

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include <dirent.h>
#include <folly/Conv.h>
#include <folly/json.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include "axiom/optimizer/JsonUtil.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/Expr.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"

namespace facebook::axiom::connector::hive {

namespace {

/// Context for sampling operations that keeps memory pool alive
struct SampleContext {
  std::shared_ptr<velox::memory::MemoryPool> samplePool;
  std::shared_ptr<velox::core::QueryCtx> queryCtx;
  std::shared_ptr<velox::connector::ConnectorQueryCtx> connectorQueryCtx;
};

// Helper function to create a ConstantVector from a partition value string.
velox::VectorPtr makePartitionVector(
    const std::string& value,
    const velox::TypePtr& type,
    velox::vector_size_t size,
    velox::memory::MemoryPool* pool) {
  using namespace velox;

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return std::make_shared<ConstantVector<bool>>(
          pool, size, false, type, folly::to<bool>(value));
    case TypeKind::TINYINT:
      return std::make_shared<ConstantVector<int8_t>>(
          pool, size, false, type, folly::to<int8_t>(value));
    case TypeKind::SMALLINT:
      return std::make_shared<ConstantVector<int16_t>>(
          pool, size, false, type, folly::to<int16_t>(value));
    case TypeKind::INTEGER:
      return std::make_shared<ConstantVector<int32_t>>(
          pool, size, false, type, folly::to<int32_t>(value));
    case TypeKind::BIGINT:
      return std::make_shared<ConstantVector<int64_t>>(
          pool, size, false, type, folly::to<int64_t>(value));
    case TypeKind::REAL:
      return std::make_shared<ConstantVector<float>>(
          pool, size, false, type, folly::to<float>(value));
    case TypeKind::DOUBLE:
      return std::make_shared<ConstantVector<double>>(
          pool, size, false, type, folly::to<double>(value));
    case TypeKind::VARCHAR:
      return std::make_shared<ConstantVector<StringView>>(
          pool, size, false, type, StringView(value));
    case TypeKind::VARBINARY:
      return std::make_shared<ConstantVector<StringView>>(
          pool, size, false, type, StringView(value));
    default:
      VELOX_UNSUPPORTED(
          "Unsupported partition column type: {}", type->toString());
  }
}

// Helper function to test if a partition value (as string) passes a filter.
// Converts the string value to the appropriate type and calls the
// corresponding test method on the filter.
bool testPartitionValue(
    const velox::common::Filter* filter,
    const velox::TypePtr& type,
    const std::string& value) {
  using namespace velox;

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return filter->testBool(folly::to<bool>(value));
    case TypeKind::TINYINT:
      return filter->testInt64(folly::to<int8_t>(value));
    case TypeKind::SMALLINT:
      return filter->testInt64(folly::to<int16_t>(value));
    case TypeKind::INTEGER:
      return filter->testInt64(folly::to<int32_t>(value));
    case TypeKind::BIGINT:
      return filter->testInt64(folly::to<int64_t>(value));
    case TypeKind::REAL:
      return filter->testFloat(folly::to<float>(value));
    case TypeKind::DOUBLE:
      return filter->testDouble(folly::to<double>(value));
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return filter->testBytes(value.data(), value.length());
    case TypeKind::HUGEINT:
      return filter->testInt128(folly::to<int128_t>(value));
    default:
      VELOX_UNSUPPORTED(
          "Unsupported partition column type: {}", type->toString());
  }
}

// Structure to hold separated data and partition column information.
struct SeparatedColumns {
  // Data columns
  std::vector<std::string> dataNames;
  std::vector<velox::TypePtr> dataTypes;
  std::vector<size_t> dataFieldIndices;

  // Partition columns
  std::vector<std::string> partitionNames;
  std::vector<velox::TypePtr> partitionTypes;
  std::vector<size_t> partitionFieldIndices;
};

// Helper function to separate fields into data and partition columns.
SeparatedColumns separateDataAndPartitionColumns(
    const std::vector<velox::common::Subfield>& fields,
    const velox::RowTypePtr& rowType,
    const std::vector<const Column*>& hivePartitionColumns) {
  // Build a set of partition column names for quick lookup
  std::unordered_set<std::string> partitionColumnNames;
  for (const auto* partCol : hivePartitionColumns) {
    partitionColumnNames.insert(partCol->name());
  }

  SeparatedColumns result;

  for (size_t i = 0; i < fields.size(); ++i) {
    const auto& name = fields[i].baseName();
    const auto& type = rowType->findChild(name);

    if (partitionColumnNames.count(name)) {
      result.partitionNames.push_back(name);
      result.partitionTypes.push_back(type);
      result.partitionFieldIndices.push_back(i);
    } else {
      result.dataNames.push_back(name);
      result.dataTypes.push_back(type);
      result.dataFieldIndices.push_back(i);
    }
  }

  return result;
}

/// Creates a new table handle with only data column filters, removing any
/// filters that concern hive partition columns.
velox::connector::ConnectorTableHandlePtr filterPartitionColumnFilters(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<const Column*>& hivePartitionColumns) {
  auto* hiveHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  if (!hiveHandle) {
    return tableHandle;
  }

  // Build set of partition column names
  std::unordered_set<std::string> partitionColumnNames;
  for (const auto* col : hivePartitionColumns) {
    partitionColumnNames.insert(col->name());
  }

  // Filter out partition column filters
  velox::common::SubfieldFilters dataColumnFilters;
  for (const auto& [subfield, filter] : hiveHandle->subfieldFilters()) {
    const auto& columnName = subfield.toString();
    if (partitionColumnNames.find(columnName) == partitionColumnNames.end()) {
      // This is a data column filter, keep it
      dataColumnFilters.emplace(subfield.clone(), filter);
    }
  }

  // If filters are unchanged, return the original handle
  if (dataColumnFilters.size() == hiveHandle->subfieldFilters().size()) {
    return tableHandle;
  }

  // Create new table handle with only data column filters
  return std::make_shared<velox::connector::hive::HiveTableHandle>(
      hiveHandle->connectorId(),
      hiveHandle->tableName(),
      hiveHandle->isFilterPushdownEnabled(),
      std::move(dataColumnFilters),
      hiveHandle->remainingFilter(),
      hiveHandle->dataColumns(),
      hiveHandle->tableParameters(),
      hiveHandle->filterColumnHandles(),
      hiveHandle->sampleRate());
}

/// Fills statistics builders for hive partition columns by extracting values
/// from the partition list.
void fillHivePartitionColumnStats(
    const std::vector<connector::PartitionHandlePtr>& partitions,
    const std::vector<std::string>& partitionColumnNames,
    const std::vector<velox::TypePtr>& partitionColumnTypes,
    velox::HashStringAllocator* allocator,
    std::vector<std::unique_ptr<StatisticsBuilder>>& builders) {
  using namespace velox;

  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  // Create builders for each partition column
  builders.reserve(partitionColumnNames.size());
  for (const auto& type : partitionColumnTypes) {
    builders.push_back(StatisticsBuilder::create(type, options));
  }

  // Extract partition values from each partition and feed to builders
  for (const auto& partitionHandle : partitions) {
    auto* partition =
        dynamic_cast<const connector::hive::LocalHivePartitionHandle*>(
            partitionHandle.get());
    if (!partition) {
      continue;
    }

    // For each partition column, extract its value and add to the builder
    for (size_t i = 0; i < partitionColumnNames.size(); ++i) {
      const auto& columnName = partitionColumnNames[i];
      auto it = partition->partitionKeys.find(columnName);
      if (it != partition->partitionKeys.end()) {
        const auto& optValue = it->second;
        const auto& type = partitionColumnTypes[i];

        // Create a constant vector with the partition value
        // If the value is nullopt, we need to handle it appropriately
        if (optValue.has_value()) {
          auto partitionVector =
              makePartitionVector(optValue.value(), type, 1, allocator->pool());
          // Add to builder
          builders[i]->add(partitionVector);
        } else {
          // Handle null partition value - create a null vector
          auto nullVector =
              velox::BaseVector::createNullConstant(type, 1, allocator->pool());
          builders[i]->add(nullVector);
        }
      }
    }
  }
}

// Helper function to create statistics builders for a list of fields.
std::vector<std::unique_ptr<StatisticsBuilder>> makeStatisticsBuilders(
    const std::vector<velox::common::Subfield>& fields,
    const velox::RowTypePtr& rowType,
    velox::HashStringAllocator* allocator) {
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  builders.reserve(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto& type = rowType->findChild(fields[i].baseName());
    builders.push_back(StatisticsBuilder::create(type, options));
  }
  return builders;
}

// Overload for creating builders from a RowType directly (by index).
std::vector<std::unique_ptr<StatisticsBuilder>> makeStatisticsBuilders(
    const velox::RowTypePtr& rowType,
    velox::HashStringAllocator* allocator) {
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  builders.reserve(rowType->size());
  for (size_t i = 0; i < rowType->size(); ++i) {
    builders.push_back(StatisticsBuilder::create(rowType->childAt(i), options));
  }
  return builders;
}

} // namespace

std::vector<PartitionHandlePtr> LocalHiveSplitManager::listPartitions(
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& tableHandle) {
  using namespace velox;

  // Cast to HiveTableHandle to access subfield filters
  auto* hiveHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  if (!hiveHandle) {
    // Fallback for non-Hive handles
    folly::F14FastMap<std::string, std::optional<std::string>> empty;
    return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
  }

  // Get the table and layout
  auto* metadata = ConnectorMetadata::metadata(tableHandle->connectorId());
  auto table = metadata->findTable(tableHandle->name());
  if (!table || table->layouts().empty()) {
    folly::F14FastMap<std::string, std::optional<std::string>> empty;
    return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
  }

  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
  if (!layout) {
    folly::F14FastMap<std::string, std::optional<std::string>> empty;
    return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
  }

  // Get partition columns
  const auto& partitionColumns = layout->hivePartitionColumns();

  // If there are no partition columns or no partitions, return a single
  // unpartitioned handle
  if (partitionColumns.empty() || layout->partitions().empty()) {
    folly::F14FastMap<std::string, std::optional<std::string>> empty;
    return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
  }

  // Extract filters that apply to partition columns
  const auto& subfieldFilters = hiveHandle->subfieldFilters();
  std::unordered_map<std::string, common::FilterPtr> partitionFilters;

  for (const auto& partitionCol : partitionColumns) {
    common::Subfield subfield(partitionCol->name());
    auto it = subfieldFilters.find(subfield);
    if (it != subfieldFilters.end()) {
      partitionFilters[partitionCol->name()] = it->second;
    }
  }

  // Filter partitions based on partition column filters
  std::vector<PartitionHandlePtr> result;

  for (const auto& partition : layout->partitions()) {
    bool matches = true;

    // Test each partition filter
    for (const auto& [columnName, filter] : partitionFilters) {
      // Find the corresponding partition column
      const Column* partitionCol = nullptr;
      for (const auto* col : partitionColumns) {
        if (col->name() == columnName) {
          partitionCol = col;
          break;
        }
      }

      if (!partitionCol) {
        continue;
      }

      // Get the value for this column from the partition
      auto valueIt = partition->partitionKeys.find(columnName);
      if (valueIt == partition->partitionKeys.end()) {
        // Partition doesn't have a value for this column
        // Test as null if filter allows nulls
        if (!filter->testNull()) {
          matches = false;
          break;
        }
      } else {
        const auto& optValue = valueIt->second;
        if (!optValue.has_value()) {
          // Value is null (represented as std::nullopt)
          if (!filter->testNull()) {
            matches = false;
            break;
          }
        } else {
          // Test the value against the filter
          try {
            if (!testPartitionValue(
                    filter.get(), partitionCol->type(), optValue.value())) {
              matches = false;
              break;
            }
          } catch (const std::exception&) {
            // If conversion fails, partition doesn't match
            matches = false;
            break;
          }
        }
      }
    }

    if (matches) {
      // Add this partition to the result
      result.push_back(partition);
    }
  }

  // If no partitions matched, return an empty result
  // (not a single unpartitioned handle, as that would scan all files)
  return result;
}

std::vector<PartitionStatisticsPtr>
LocalHiveSplitManager::getPartitionStatistics(
    std::span<const PartitionHandlePtr> partitions,
    const std::vector<std::string>& columns) {
  std::vector<PartitionStatisticsPtr> result;
  result.reserve(partitions.size());

  for (const auto& partition : partitions) {
    // Cast to LocalHivePartitionHandle
    auto* localPartition =
        dynamic_cast<const LocalHivePartitionHandle*>(partition.get());
    if (!localPartition) {
      // If it's not a LocalHivePartitionHandle, return empty statistics
      auto stats = std::make_shared<PartitionStatistics>();
      result.push_back(stats);
      continue;
    }

    // Create a new PartitionStatistics object for this partition
    auto stats = std::make_shared<PartitionStatistics>();

    // Set row and file counts from the partition
    stats->numRows = localPartition->stats.numRows;
    stats->numFiles = localPartition->stats.numFiles;

    // Filter column statistics for the requested columns
    if (!columns.empty()) {
      // Build a map from column name to index in partition's statistics
      std::unordered_map<std::string, size_t> columnIndexMap;
      for (size_t i = 0; i < localPartition->stats.columns.size(); ++i) {
        columnIndexMap[localPartition->stats.columns[i]] = i;
      }

      // Extract statistics for the requested columns
      for (const auto& columnName : columns) {
        auto it = columnIndexMap.find(columnName);
        if (it != columnIndexMap.end()) {
          // Column found in partition statistics
          stats->columns.push_back(columnName);
          stats->columnStatistics.push_back(
              localPartition->stats.columnStatistics[it->second]);
        } else {
          // Column not found, add empty statistics
          stats->columns.push_back(columnName);
          ColumnStatistics emptyStats;
          emptyStats.name = columnName;
          stats->columnStatistics.push_back(emptyStats);
        }
      }
    } else {
      // If no specific columns requested, return all column statistics
      stats->columns = localPartition->stats.columns;
      stats->columnStatistics = localPartition->stats.columnStatistics;
    }

    result.push_back(stats);
  }

  return result;
}

std::shared_ptr<SplitSource> LocalHiveSplitManager::getSplitSource(
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& partitions,
    SplitOptions options) {
  auto tableName = tableHandle->name();
  auto* metadata = ConnectorMetadata::metadata(tableHandle->connectorId());
  auto table = metadata->findTable(tableName);
  VELOX_CHECK_NOT_NULL(
      table, "Could not find {} in its ConnectorMetadata", tableName);
  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);

  std::vector<const FileInfo*> selectedFiles;

  if (!layout->hivePartitionColumns().empty()) {
    // Use files from the provided partitions
    for (const auto& partitionHandle : partitions) {
      auto* localPartition =
          dynamic_cast<const LocalHivePartitionHandle*>(partitionHandle.get());
      if (localPartition) {
        for (const auto* file : localPartition->files) {
          selectedFiles.push_back(file);
        }
      }
    }
  } else {
    // No partitions in the layout, use all files from the layout
    auto& files = layout->files();
    for (auto& file : files) {
      selectedFiles.push_back(file.get());
    }
  }

  return std::make_shared<LocalHiveSplitSource>(
      std::move(selectedFiles),
      layout->fileFormat(),
      layout->connector()->connectorId(),
      options,
      layout->serdeParameters());
}

namespace {
// Integer division that rounds up if remainder is non-zero.
template <typename T>
T ceil2(T x, T y) {
  return (x + y - 1) / y;
}
} // namespace

std::vector<SplitSource::SplitAndGroup> LocalHiveSplitSource::getSplits(
    uint64_t targetBytes) {
  std::vector<SplitAndGroup> result;
  uint64_t bytes = 0;
  for (;;) {
    if (currentFile_ >= static_cast<int32_t>(files_.size())) {
      result.push_back(SplitSource::SplitAndGroup{nullptr, 0});
      return result;
    }

    if (currentSplit_ >= fileSplits_.size()) {
      fileSplits_.clear();
      ++currentFile_;
      if (currentFile_ >= files_.size()) {
        result.push_back(SplitSource::SplitAndGroup{nullptr, 0});
        return result;
      }

      currentSplit_ = 0;
      const auto& filePath = files_[currentFile_]->path;
      const auto fileSize = fs::file_size(filePath);
      int64_t splitsPerFile =
          ceil2<uint64_t>(fileSize, options_.fileBytesPerSplit);
      if (options_.targetSplitCount) {
        auto numFiles = files_.size();
        if (splitsPerFile * numFiles < options_.targetSplitCount) {
          // Divide the file into more splits but still not smaller than 64MB.
          auto perFile = ceil2<uint64_t>(options_.targetSplitCount, numFiles);
          int64_t bytesInSplit = ceil2<uint64_t>(fileSize, perFile);
          splitsPerFile = ceil2<uint64_t>(
              fileSize, std::max<uint64_t>(bytesInSplit, 32 << 20));
        }
      }
      // Take the upper bound.
      const int64_t splitSize = ceil2<uint64_t>(fileSize, splitsPerFile);
      for (int i = 0; i < splitsPerFile; ++i) {
        auto builder =
            velox::connector::hive::HiveConnectorSplitBuilder(filePath)
                .connectorId(connectorId_)
                .fileFormat(format_)
                .start(i * splitSize)
                .length(splitSize);

        auto* info = files_[currentFile_];
        if (info->bucketNumber.has_value()) {
          builder.tableBucketNumber(info->bucketNumber.value());
        }
        for (auto& pair : info->partitionKeys) {
          builder.partitionKey(pair.first, pair.second);
        }
        if (!serdeParameters_.empty()) {
          builder.serdeParameters(serdeParameters_);
        }
        fileSplits_.push_back(builder.build());
      }
    }
    result.push_back(SplitAndGroup{std::move(fileSplits_[currentSplit_++]), 0});
    bytes +=
        reinterpret_cast<const velox::connector::hive::HiveConnectorSplit*>(
            result.back().split.get())
            ->length;
    if (bytes > targetBytes) {
      return result;
    }
  }
}

LocalHiveConnectorMetadata::LocalHiveConnectorMetadata(
    velox::connector::hive::HiveConnector* hiveConnector)
    : HiveConnectorMetadata(hiveConnector), splitManager_(this) {}

void LocalHiveConnectorMetadata::reinitialize() {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  tables_.clear();
  initialize();
  initialized_ = true;
}

void LocalHiveConnectorMetadata::initialize() {
  auto formatName = hiveConfig_->hiveLocalFileFormat();
  auto path = hiveConfig_->hiveLocalDataPath();
  format_ = formatName == "dwrf" ? velox::dwio::common::FileFormat::DWRF
      : formatName == "parquet"  ? velox::dwio::common::FileFormat::PARQUET
      : formatName == "text"     ? velox::dwio::common::FileFormat::TEXT
                                 : velox::dwio::common::FileFormat::UNKNOWN;
  makeQueryCtx();
  makeConnectorQueryCtx();
  readTables(path);
}

void LocalHiveConnectorMetadata::ensureInitialized() const {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  if (initialized_) {
    return;
  }
  initialized_ = true;
  const_cast<LocalHiveConnectorMetadata*>(this)->initialize();
}

std::shared_ptr<velox::core::QueryCtx> LocalHiveConnectorMetadata::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::string> config;
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[hiveConnector_->connectorId()] =
      std::const_pointer_cast<velox::config::ConfigBase>(hiveConfig_->config());

  return velox::core::QueryCtx::create(
      hiveConnector_->executor(),
      velox::core::QueryConfig(config),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      nullptr,
      queryId);
}

void LocalHiveConnectorMetadata::makeQueryCtx() {
  if (queryCtx_) {
    return;
  }
  queryCtx_ = makeQueryCtx("local_hive_metadata");
}

void LocalHiveConnectorMetadata::makeConnectorQueryCtx() {
  if (connectorQueryCtx_) {
    return;
  }
  velox::common::SpillConfig spillConfig;
  velox::common::PrefixSortConfig prefixSortConfig;
  schemaPool_ = queryCtx_->pool()->addLeafChild("schemaReader");
  connectorQueryCtx_ = std::make_shared<velox::connector::ConnectorQueryCtx>(
      schemaPool_.get(),
      queryCtx_->pool(),
      queryCtx_->connectorSessionProperties(hiveConnector_->connectorId()),
      &spillConfig,
      prefixSortConfig,
      std::make_unique<velox::exec::SimpleExpressionEvaluator>(
          queryCtx_.get(), schemaPool_.get()),
      queryCtx_->cache(),
      "scan_for_schema",
      "schema",
      "N/a",
      0,
      queryCtx_->queryConfig().sessionTimezone());
}

SampleContext createSampleContext(const LocalHiveConnectorMetadata* metadata) {
  static std::atomic<int64_t> sampleCounter{0};

  // Create a new root pool for this sample operation
  auto queryId = fmt::format("sample:{}", ++sampleCounter);
  auto pool = velox::memory::memoryManager()->addRootPool(queryId);

  // Create QueryCtx
  std::unordered_map<std::string, std::string> config;
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[metadata->hiveConnector()->connectorId()] =
      std::const_pointer_cast<velox::config::ConfigBase>(
          metadata->hiveConfig()->config());

  auto queryCtx = velox::core::QueryCtx::create(
      metadata->hiveConnector()->executor(),
      velox::core::QueryConfig(config),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      pool,
      nullptr,
      queryId);

  // Create ConnectorQueryCtx
  velox::common::SpillConfig spillConfig;
  velox::common::PrefixSortConfig prefixSortConfig;
  auto samplePool = queryCtx->pool()->addLeafChild("sampleReader");

  auto connectorQueryCtx =
      std::make_shared<velox::connector::ConnectorQueryCtx>(
          samplePool.get(),
          queryCtx->pool(),
          queryCtx->connectorSessionProperties(
              metadata->hiveConnector()->connectorId()),
          &spillConfig,
          prefixSortConfig,
          std::make_unique<velox::exec::SimpleExpressionEvaluator>(
              queryCtx.get(), samplePool.get()),
          queryCtx->cache(),
          queryId,
          "sample_task",
          "sample_node",
          0,
          queryCtx->queryConfig().sessionTimezone());

  return SampleContext{
      .samplePool = std::move(samplePool),
      .queryCtx = std::move(queryCtx),
      .connectorQueryCtx = std::move(connectorQueryCtx)};
}

void LocalHiveConnectorMetadata::readTables(std::string_view path) {
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_directory() ||
        dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    loadTable(dirEntry.path().filename().native(), dirEntry.path());
  }
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sample(
    const velox::connector::ConnectorTableHandlePtr& handle,
    float pct,
    const std::vector<velox::core::TypedExprPtr>& extraFilters,
    velox::RowTypePtr scanType,
    const std::vector<velox::common::Subfield>& fields,
    velox::HashStringAllocator* allocator,
    std::vector<ColumnStatistics>* statistics) const {
  VELOX_CHECK(extraFilters.empty());

  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  auto result = sample(handle, pct, scanType, fields, allocator, &builders);
  if (!statistics) {
    return result;
  }

  statistics->resize(builders.size());
  for (auto i = 0; i < builders.size(); ++i) {
    ColumnStatistics runnerStats;
    if (builders[i]) {
      builders[i]->build(runnerStats);
    }
    // Set the column name from the field
    if (i < fields.size()) {
      runnerStats.name = fields[i].baseName();
    }
    (*statistics)[i] = std::move(runnerStats);
  }
  return result;
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sample(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    float pct,
    velox::RowTypePtr scanType,
    const std::vector<velox::common::Subfield>& fields,
    velox::HashStringAllocator* allocator,
    std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const {
  using namespace velox;

  // Get metadata and split manager
  auto* metadata = ConnectorMetadata::metadata(tableHandle->connectorId());
  auto* splitManager =
      dynamic_cast<LocalHiveSplitManager*>(metadata->splitManager());
  VELOX_CHECK_NOT_NULL(splitManager);

  // Separate fields into data columns and partition columns using helper
  auto separated = separateDataAndPartitionColumns(
      fields, rowType(), hivePartitionColumns());

  // Create builders for DATA columns only (no partition columns)
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  builders.reserve(separated.dataFieldIndices.size());
  for (size_t idx : separated.dataFieldIndices) {
    const auto& type = rowType()->findChild(fields[idx].baseName());
    builders.push_back(StatisticsBuilder::create(type, options));
  }

  // Create output type for file reader (data columns only)
  const auto outputType =
      ROW(std::move(separated.dataNames), std::move(separated.dataTypes));

  // Create column handles for data columns only
  velox::connector::ColumnHandleMap columnHandles;
  for (size_t i = 0; i < separated.dataFieldIndices.size(); ++i) {
    const auto& name = fields[separated.dataFieldIndices[i]].baseName();
    const auto& type = rowType()->findChild(name);
    columnHandles[name] =
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            name,
            velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
            type,
            type);
  }

  // Create new contexts for this sample operation (thread-safe)
  auto* localMetadata = reinterpret_cast<LocalHiveConnectorMetadata*>(
      ConnectorMetadata::metadata(connector()));
  auto sampleContext = createSampleContext(localMetadata);

  const auto maxRowsToScan = table().numRows() * (pct / 100);

  int64_t passingRows = 0;
  int64_t scannedRows = 0;

  // Use split manager to list partitions
  std::vector<PartitionHandlePtr> partitions =
      splitManager->listPartitions(nullptr, tableHandle);

  // Get split source from split manager
  SplitOptions splitOptions;
  auto splitSource = splitManager->getSplitSource(
      nullptr, tableHandle, partitions, splitOptions);

  // Create a table handle with only data column filters for sampleFile
  auto dataColumnTableHandle =
      filterPartitionColumnFilters(tableHandle, hivePartitionColumns());

  // Enumerate splits using the split source
  for (;;) {
    // Get next batch of splits
    constexpr uint64_t kTargetBytes = 256 << 20; // 256MB
    auto splitsAndGroups = splitSource->getSplits(kTargetBytes);

    bool done = false;
    for (const auto& splitAndGroup : splitsAndGroups) {
      if (!splitAndGroup.split) {
        done = true;
        break;
      }

      // Sample this split using sampleFile
      auto [splitScanned, splitPassed] = sampleFile(
          splitAndGroup.split,
          outputType,
          dataColumnTableHandle,
          columnHandles,
          sampleContext.connectorQueryCtx.get(),
          builders,
          maxRowsToScan,
          scannedRows);

      scannedRows += splitScanned;
      passingRows += splitPassed;

      if (scannedRows >= maxRowsToScan) {
        break;
      }
    }

    if (done || scannedRows >= maxRowsToScan) {
      break;
    }
  }

  if (statsBuilders) {
    // The caller expects builders for all fields, so create a sparse array
    statsBuilders->resize(fields.size());

    // Fill data column builders
    for (size_t i = 0; i < separated.dataFieldIndices.size(); ++i) {
      size_t fieldIdx = separated.dataFieldIndices[i];
      (*statsBuilders)[fieldIdx] = std::move(builders[i]);
    }

    // Fill partition column builders if requested
    if (!separated.partitionFieldIndices.empty()) {
      std::vector<std::unique_ptr<StatisticsBuilder>> partitionBuilders;
      fillHivePartitionColumnStats(
          partitions,
          separated.partitionNames,
          separated.partitionTypes,
          allocator,
          partitionBuilders);

      // Move partition builders to the output array
      for (size_t i = 0; i < separated.partitionFieldIndices.size(); ++i) {
        size_t fieldIdx = separated.partitionFieldIndices[i];
        (*statsBuilders)[fieldIdx] = std::move(partitionBuilders[i]);
      }
    }
  }
  return std::pair(scannedRows, passingRows);
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sampleFile(
    const std::shared_ptr<velox::connector::ConnectorSplit>& split,
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    std::vector<std::unique_ptr<StatisticsBuilder>>& builders,
    int64_t maxRowsToScan,
    int64_t& currentScannedRows) const {
  auto dataSource = connector()->createDataSource(
      outputType, tableHandle, columnHandles, connectorQueryCtx);

  dataSource->addSplit(split);

  constexpr int32_t kBatchSize = 10'000;
  int64_t passingRows = 0;
  int64_t scannedRows = 0;

  for (;;) {
    velox::ContinueFuture ignore{velox::ContinueFuture::makeEmpty()};
    auto data = dataSource->next(kBatchSize, ignore).value();
    if (data == nullptr) {
      scannedRows += dataSource->getCompletedRows();
      break;
    }

    const auto rowCount = data->size();
    passingRows += rowCount;

    // Update builders
    for (size_t i = 0; i < builders.size(); ++i) {
      if (builders[i]) {
        builders[i]->add(data->childAt(i));
      }
    }

    if (currentScannedRows + scannedRows + dataSource->getCompletedRows() >
        maxRowsToScan) {
      scannedRows += dataSource->getCompletedRows();
      break;
    }
  }

  return std::pair(scannedRows, passingRows);
}

void LocalTable::makeDefaultLayout(
    std::vector<std::unique_ptr<const FileInfo>> files,
    LocalHiveConnectorMetadata& metadata) {
  if (!layouts_.empty()) {
    // The table already has a layout made from a schema file.
    reinterpret_cast<LocalHiveTableLayout*>(layouts_[0].get())
        ->setFiles(std::move(files));
    return;
  }
  std::vector<const Column*> columns;
  columns.reserve(type_->size());
  for (const auto& name : type_->names()) {
    columns.push_back(columns_[name].get());
  }

  std::vector<const Column*> empty;
  auto layout = std::make_unique<LocalHiveTableLayout>(
      name_,
      this,
      metadata.hiveConnector(),
      std::move(columns),
      std::nullopt,
      empty,
      empty,
      std::vector<SortOrder>{},
      empty,
      empty,
      metadata.fileFormat());
  layout->setFiles(std::move(files));
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

namespace {

// Extracts the digits after the last / in the file path and returns them as an
// integer.
int32_t extractDigitsAfterLastSlash(std::string_view path) {
  size_t lastSlashPos = path.find_last_of('/');
  VELOX_CHECK(lastSlashPos != std::string::npos, "No slash found in {}", path);
  std::string digits;
  for (size_t i = lastSlashPos + 1; i < path.size(); ++i) {
    char c = path[i];
    if (std::isdigit(c)) {
      digits += c;
    } else {
      break;
    }
  }
  VELOX_CHECK(
      !digits.empty(),
      "Bad bucketed file name: No digits at start of name {}",
      path);
  return std::stoi(digits);
}

void listFiles(
    std::string_view path,
    std::function<int32_t(std::string_view)> parseBucketNumber,
    int32_t prefixSize,
    std::vector<std::unique_ptr<const FileInfo>>& result,
    std::vector<std::shared_ptr<LocalHivePartitionHandle>>* partitions =
        nullptr) {
  // Track files in current directory (raw pointers to files added to result)
  std::vector<FileInfo*> currentDirFiles;
  bool hasSubdirs = false;

  for (auto const& dirEntry : fs::directory_iterator{path}) {
    // Ignore hidden files.
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }

    if (dirEntry.is_directory()) {
      hasSubdirs = true;
      listFiles(
          fmt::format("{}/{}", path, dirEntry.path().filename().c_str()),
          parseBucketNumber,
          prefixSize,
          result,
          partitions);
    }
    if (!dirEntry.is_regular_file()) {
      continue;
    }
    auto file = std::make_unique<FileInfo>();
    file->path = fmt::format("{}/{}", path, dirEntry.path().filename().c_str());
    if (parseBucketNumber) {
      file->bucketNumber = parseBucketNumber(file->path);
    }
    std::vector<std::string> dirs;
    folly::split('/', path.substr(prefixSize, path.size()), dirs);
    for (auto& dir : dirs) {
      std::vector<std::string> parts;
      folly::split('=', dir, parts);
      if (parts.size() == 2) {
        // Check if the value represents a Hive null
        if (parts[1] == "__HIVE_DEFAULT_PARTITION__") {
          file->partitionKeys[parts[0]] = std::nullopt;
        } else {
          file->partitionKeys[parts[0]] = parts[1];
        }
      }
    }

    // Store raw pointer if this might be a leaf directory
    if (partitions) {
      currentDirFiles.push_back(file.get());
    }

    result.push_back(std::move(file));
  }

  // If this is a leaf directory (has files but no subdirs) and we're tracking
  // partitions, create a LocalHivePartitionHandle
  if (partitions && !currentDirFiles.empty() && !hasSubdirs) {
    // Extract partition keys from the first file
    folly::F14FastMap<std::string, std::optional<std::string>> partitionKeys;
    if (!currentDirFiles.empty() &&
        !currentDirFiles[0]->partitionKeys.empty()) {
      partitionKeys = currentDirFiles[0]->partitionKeys;
    }

    auto partition = std::make_shared<LocalHivePartitionHandle>(
        std::string(path),
        std::move(partitionKeys),
        std::move(currentDirFiles));
    partitions->push_back(std::move(partition));
  }
}

struct CreateTableOptions {
  std::optional<velox::common::CompressionKind> compressionKind;
  std::optional<velox::dwio::common::FileFormat> fileFormat;

  std::vector<std::string> partitionedByColumns;

  std::optional<int32_t> numBuckets;
  std::vector<std::string> bucketedByColumns;
  std::vector<std::string> sortedByColumns;

  // SerDe options. Primarily used for TEXT format files, but may evolve
  // to support other formats in the future.
  std::optional<velox::dwio::common::SerDeOptions> serdeOptions;
};

namespace {
int32_t parseBucketNumber(const velox::Variant& value) {
  switch (value.kind()) {
    case velox::TypeKind::TINYINT:
      return value.value<int8_t>();
    case velox::TypeKind::SMALLINT:
      return value.value<int16_t>();
    case velox::TypeKind::INTEGER:
      return value.value<int32_t>();
    case velox::TypeKind::BIGINT: {
      const auto numBuckets = value.value<int64_t>();
      VELOX_USER_CHECK_LE(
          numBuckets,
          std::numeric_limits<int32_t>::max(),
          "{} must not exceed 32-bit integer range",
          HiveWriteOptions::kBucketCount);
      VELOX_USER_CHECK_GT(
          numBuckets, 0, "{} must be > 0", HiveWriteOptions::kBucketCount);
      return numBuckets;
    }
    default:
      VELOX_USER_FAIL(
          "Unsupported {} type: {}",
          HiveWriteOptions::kBucketCount,
          velox::TypeKindName::toName(value.kind()));
  }
}
} // namespace

CreateTableOptions parseCreateTableOptions(
    const folly::F14FastMap<std::string, velox::Variant>& options,
    velox::dwio::common::FileFormat defaultFileFormat) {
  CreateTableOptions result;

  auto it = options.find(HiveWriteOptions::kCompressionKind);
  if (it != options.end()) {
    result.compressionKind =
        velox::common::stringToCompressionKind(it->second.value<std::string>());
  }

  it = options.find(HiveWriteOptions::kFileFormat);
  if (it != options.end()) {
    result.fileFormat =
        velox::dwio::common::toFileFormat(it->second.value<std::string>());
    VELOX_USER_CHECK(
        result.fileFormat != velox::dwio::common::FileFormat::UNKNOWN,
        "Bad file format: {}",
        it->second.value<std::string>());
  } else {
    result.fileFormat = defaultFileFormat;
  }

  it = options.find(HiveWriteOptions::kPartitionedBy);
  if (it != options.end()) {
    result.partitionedByColumns = it->second.array<std::string>();
  }

  it = options.find(HiveWriteOptions::kBucketedBy);
  if (it != options.end()) {
    result.bucketedByColumns = it->second.array<std::string>();

    it = options.find(HiveWriteOptions::kBucketCount);
    VELOX_USER_CHECK(
        it != options.end(),
        "{} is required if {} is specified",
        HiveWriteOptions::kBucketCount,
        HiveWriteOptions::kBucketedBy);

    const auto numBuckets = parseBucketNumber(it->second);

    VELOX_USER_CHECK_GT(
        numBuckets, 0, "{} must be > 0", HiveWriteOptions::kBucketCount);
    VELOX_USER_CHECK_EQ(
        numBuckets & (numBuckets - 1),
        0,
        "{} must be power of 2",
        HiveWriteOptions::kBucketCount);

    result.numBuckets = numBuckets;

    it = options.find("sorted_by");
    if (it != options.end()) {
      result.sortedByColumns = it->second.array<std::string>();
    }
  }

  // Parse SerDe options
  it = options.find(HiveWriteOptions::kFieldDelim);
  if (it != options.end()) {
    velox::dwio::common::SerDeOptions serdeOpts;
    std::string delimiter = it->second.value<std::string>();
    VELOX_USER_CHECK_EQ(
        delimiter.size(),
        1,
        "{} must be a single character",
        HiveWriteOptions::kFieldDelim);
    serdeOpts.separators[0] = delimiter[0];
    result.serdeOptions = serdeOpts;
  }

  it = options.find(HiveWriteOptions::kSerializationNullFormat);
  if (it != options.end()) {
    if (!result.serdeOptions.has_value()) {
      result.serdeOptions = velox::dwio::common::SerDeOptions();
    }
    result.serdeOptions->nullString = it->second.value<std::string>();
  }

  return result;
}

velox::RowTypePtr parseSchema(const folly::dynamic& obj) {
  velox::type::fbhive::HiveTypeParser parser;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  for (const auto& column : obj["dataColumns"]) {
    names.push_back(column["name"].asString());
    types.push_back(parser.parse(column["type"].asString()));
  }

  for (const auto& column : obj["partitionColumns"]) {
    names.push_back(column["name"].asString());
    types.push_back(parser.parse(column["type"].asString()));
  }

  return velox::ROW(std::move(names), std::move(types));
}

CreateTableOptions parseCreateTableOptions(
    const folly::dynamic& obj,
    velox::dwio::common::FileFormat defaultFileFormat) {
  CreateTableOptions options;

  if (obj.count("compressionKind")) {
    options.compressionKind = velox::common::stringToCompressionKind(
        obj["compressionKind"].asString());
  }

  if (obj.count("fileFormat")) {
    options.fileFormat =
        velox::dwio::common::toFileFormat(obj["fileFormat"].asString());
  } else {
    options.fileFormat = defaultFileFormat;
  }

  // Parse SerDe options
  if (obj.count("serdeOptions")) {
    const auto& serdeObj = obj["serdeOptions"];
    velox::dwio::common::SerDeOptions serdeOpts;
    if (serdeObj.count("fieldDelim")) {
      std::string delimiter = serdeObj["fieldDelim"].asString();
      VELOX_USER_CHECK_EQ(
          delimiter.size(), 1, "fieldDelim must be a single character");
      serdeOpts.separators[0] = delimiter[0];
    }
    if (serdeObj.count("nullString")) {
      serdeOpts.nullString = serdeObj["nullString"].asString();
    }
    options.serdeOptions = serdeOpts;
  }

  for (auto column : obj["partitionColumns"]) {
    options.partitionedByColumns.push_back(column["name"].asString());
  }

  if (obj.count("bucketProperty")) {
    const auto& bucketObj = obj["bucketProperty"];
    options.numBuckets = atoi(bucketObj["bucketCount"].asString().c_str());

    for (const auto& column : bucketObj["bucketedBy"]) {
      options.bucketedByColumns.push_back(column.asString());
    }

    for (const auto& column : bucketObj["sortedBy"]) {
      options.sortedByColumns.push_back(column.asString());
    }
  }

  return options;
}

folly::dynamic toJsonArray(const std::vector<std::string>& values) {
  auto json = folly::dynamic::array();
  for (const auto& value : values) {
    json.push_back(value);
  }
  return json;
}

folly::dynamic toSchemaJson(
    const velox::RowTypePtr& rowType,
    const CreateTableOptions& options) {
  folly::dynamic schema = folly::dynamic::object;

  if (options.compressionKind.has_value()) {
    schema["compressionKind"] =
        velox::common::compressionKindToString(options.compressionKind.value());
  }

  if (options.fileFormat.has_value()) {
    schema["fileFormat"] =
        velox::dwio::common::toString(options.fileFormat.value());
  }
  // Save SerDe options
  if (options.serdeOptions.has_value()) {
    folly::dynamic serdeOpts = folly::dynamic::object;
    const auto& opts = options.serdeOptions.value();
    serdeOpts["fieldDelim"] =
        std::string(1, static_cast<char>(opts.separators[0]));
    serdeOpts["nullString"] = opts.nullString;
    schema["serdeOptions"] = serdeOpts;
  }

  if (options.numBuckets.has_value()) {
    folly::dynamic buckets = folly::dynamic::object;
    buckets["bucketCount"] = fmt::format("{}", options.numBuckets.value());

    buckets["bucketedBy"] = toJsonArray(options.bucketedByColumns);
    buckets["sortedBy"] = toJsonArray(options.sortedByColumns);
    schema["bucketProperty"] = buckets;
  }

  const std::unordered_set<std::string> partitionedByColumns(
      options.partitionedByColumns.begin(), options.partitionedByColumns.end());

  auto dataColumns = folly::dynamic::array();
  auto partitionColumns = folly::dynamic::array();

  bool isPartition = false;
  for (auto i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);

    folly::dynamic column = folly::dynamic::object();
    column["name"] = name;
    column["type"] =
        velox::type::fbhive::HiveTypeSerializer::serialize(rowType->childAt(i));

    if (partitionedByColumns.contains(name)) {
      partitionColumns.push_back(column);
      isPartition = true;
    } else {
      VELOX_USER_CHECK(!isPartition, "Partitioning columns must be last");
      dataColumns.push_back(column);
    }
  }

  schema["dataColumns"] = dataColumns;
  schema["partitionColumns"] = partitionColumns;

  return schema;
}

std::shared_ptr<LocalTable> createLocalTable(
    std::string_view name,
    const velox::RowTypePtr& schema,
    const CreateTableOptions& createTableOptions,
    velox::connector::Connector* connector) {
  folly::F14FastMap<std::string, velox::Variant> options;
  if (createTableOptions.compressionKind.has_value()) {
    options[HiveWriteOptions::kCompressionKind] =
        velox::common::compressionKindToString(
            createTableOptions.compressionKind.value());
  }

  if (createTableOptions.fileFormat.has_value()) {
    options[HiveWriteOptions::kFileFormat] = std::string(
        velox::dwio::common::toString(createTableOptions.fileFormat.value()));
  }

  if (createTableOptions.serdeOptions.has_value()) {
    const auto& serdeOpts = createTableOptions.serdeOptions.value();
    options[HiveWriteOptions::kFieldDelim] =
        std::string(1, static_cast<char>(serdeOpts.separators[0]));
    options[HiveWriteOptions::kSerializationNullFormat] = serdeOpts.nullString;
  }

  auto table = std::make_shared<LocalTable>(
      std::string{name}, schema, std::move(options));

  std::vector<const Column*> partitionedBy;
  for (const auto& name : createTableOptions.partitionedByColumns) {
    auto column = table->findColumn(name);
    VELOX_CHECK_NOT_NULL(column, "Partitioned-by column not found: {}", name);
    partitionedBy.push_back(column);
  }

  std::optional<int32_t> numBuckets = createTableOptions.numBuckets;
  std::vector<const Column*> bucketedBy;
  std::vector<const Column*> sortedBy;
  std::vector<SortOrder> sortOrders;
  if (numBuckets.has_value()) {
    for (const auto& name : createTableOptions.bucketedByColumns) {
      auto column = table->findColumn(name);
      VELOX_CHECK_NOT_NULL(column, "Bucketed-by column not found: {}", name);
      bucketedBy.push_back(column);
    }

    for (const auto& name : createTableOptions.sortedByColumns) {
      auto column = table->findColumn(name);
      VELOX_CHECK_NOT_NULL(column, "Sorted-by column not found: {}", name);
      sortedBy.push_back(column);
      sortOrders.push_back(SortOrder{true, true}); // ASC NULLS FIRST.
    }
  }

  std::vector<const Column*> columns;
  columns.reserve(table->columns().size());
  for (const auto& name : table->type()->names()) {
    columns.emplace_back(table->findColumn(name));
  }

  // Convert SerDeOptions to serdeParameters map
  std::unordered_map<std::string, std::string> serdeParameters;
  if (createTableOptions.serdeOptions.has_value()) {
    const auto& serdeOpts = createTableOptions.serdeOptions.value();
    serdeParameters[velox::dwio::common::SerDeOptions::kFieldDelim] =
        std::string(1, static_cast<char>(serdeOpts.separators[0]));
    serdeParameters
        [velox::dwio::common::TableParameter::kSerializationNullFormat] =
            serdeOpts.nullString;
  }

  auto layout = std::make_unique<LocalHiveTableLayout>(
      table->name(),
      table.get(),
      connector,
      columns,
      numBuckets,
      bucketedBy,
      sortedBy,
      sortOrders,
      /*lookupKeys=*/std::vector<const Column*>{},
      partitionedBy,
      createTableOptions.fileFormat.value(),
      std::move(serdeParameters));
  table->addLayout(std::move(layout));
  return table;
}

std::string schemaPath(std::string_view path) {
  return fmt::format("{}/.schema", path);
}

std::shared_ptr<LocalTable> createTableFromSchema(
    std::string_view name,
    std::string_view path,
    velox::dwio::common::FileFormat defaultFileFormat,
    velox::connector::Connector* connector) {
  auto jsons = readConcatenatedDynamicsFromFile(schemaPath(path));
  if (jsons.empty()) {
    return nullptr;
  }

  VELOX_CHECK_EQ(jsons.size(), 1);
  auto json = jsons[0];

  const auto options = parseCreateTableOptions(json, defaultFileFormat);
  const auto schema = parseSchema(json);

  return createLocalTable(name, schema, options, connector);
}
} // namespace

void LocalHiveConnectorMetadata::loadTable(
    std::string_view tableName,
    const fs::path& tablePath) {
  // open each file in the directory and check their type and add up the row
  // counts.
  auto table = createTableFromSchema(
      tableName, tablePath.native(), format_, hiveConnector());

  velox::RowTypePtr tableType;
  if (table) {
    tables_[tableName] = table;
    tableType = table->type();
  }

  std::function<int32_t(std::string_view)> parseBucketNumber = nullptr;
  bool hasPartitionColumns =
      table && !table->layouts()[0]->partitionColumns().empty();
  bool hasHivePartitionColumns =
      table && !table->layouts()[0]->discretePredicateColumns().empty();
  if (hasPartitionColumns) {
    parseBucketNumber = extractDigitsAfterLastSlash;
  }

  std::vector<std::unique_ptr<const FileInfo>> files;
  std::vector<std::shared_ptr<LocalHivePartitionHandle>> partitions;
  std::string pathString = tablePath;

  // listFiles will create partitions and populate their files if
  // hasHivePartitionColumns
  listFiles(
      pathString,
      parseBucketNumber,
      pathString.size(),
      files,
      hasHivePartitionColumns ? &partitions : nullptr);

  // Track partition-level statistics: rows, files, and column stats
  struct PartitionStats {
    int64_t rows{0};
    int32_t fileCount{0};
    std::vector<ColumnStatistics> columnStats;
  };
  std::unordered_map<std::string, PartitionStats> partitionStatsMap;

  // Map from file path to partition for tracking stats
  std::unordered_map<std::string, LocalHivePartitionHandle*> fileToPartition;

  if (hasHivePartitionColumns) {
    for (const auto& partition : partitions) {
      for (const auto* file : partition->files) {
        fileToPartition[file->path] = partition.get();
      }
    }
  }

  for (auto& info : files) {
    // If the table has a schema it has a layout that gives the file format.
    // Otherwise we default it from 'this'.
    velox::dwio::common::ReaderOptions readerOptions{schemaPool_.get()};
    auto fileFormat = table == nullptr || table->layouts().empty()
        ? format_
        : reinterpret_cast<const HiveTableLayout*>(table->layouts()[0])
              ->fileFormat();
    readerOptions.setFileFormat(fileFormat);

    // TEXT format requires the schema to be set in reader options.
    if (fileFormat == velox::dwio::common::FileFormat::TEXT && tableType) {
      readerOptions.setFileSchema(tableType);
    }

    auto input = std::make_unique<velox::dwio::common::BufferedInput>(
        std::make_shared<velox::LocalReadFile>(info->path),
        readerOptions.memoryPool());
    std::unique_ptr<velox::dwio::common::Reader> reader =
        velox::dwio::common::getReaderFactory(readerOptions.fileFormat())
            ->createReader(std::move(input), readerOptions);

    const auto& fileType = reader->rowType();
    if (!tableType) {
      tableType = fileType;
    } else if (fileType->size() > tableType->size()) {
      // The larger type is the later since there is only addition of columns.
      // TODO: Check the column types are compatible where they overlap.
      tableType = fileType;
    }

    auto it = tables_.find(tableName);
    if (it != tables_.end()) {
      table = it->second;
    } else {
      tables_[tableName] =
          std::make_shared<LocalTable>(std::string{tableName}, tableType);
      table = tables_[tableName];
    }

    const auto rows = reader->numberOfRows();
    if (rows.has_value()) {
      table->incrementNumRows(rows.value());

      // Track rows per partition
      if (hasHivePartitionColumns) {
        auto partitionIt = fileToPartition.find(info->path);
        if (partitionIt != fileToPartition.end()) {
          auto* partition = partitionIt->second;
          partition->mutableStats()->numRows += rows.value();
          partition->mutableStats()->numFiles++;

          // Initialize column stats array if first file in partition
          if (partition->mutableStats()->columnStatistics.empty()) {
            partition->mutableStats()->columnStatistics.resize(
                fileType->size());
            partition->mutableStats()->columns.resize(fileType->size());
            for (size_t i = 0; i < fileType->size(); ++i) {
              partition->mutableStats()->columns[i] = fileType->nameOf(i);
              partition->mutableStats()->columnStatistics[i].name =
                  fileType->nameOf(i);
            }
          }
        }
      }
    }

    for (auto i = 0; i < fileType->size(); ++i) {
      const auto& name = fileType->nameOf(i);

      Column* column;
      auto columnIt = table->columns().find(name);
      if (columnIt != table->columns().end()) {
        column = columnIt->second.get();
      } else {
        auto newColumn = std::make_unique<Column>(name, fileType->childAt(i));
        column = newColumn.get();
        table->columns()[name] = std::move(newColumn);
      }

      if (auto readerStats = reader->columnStatistics(i)) {
        column->mutableStats()->numValues +=
            readerStats->getNumberOfValues().value_or(0);

        const auto numValues = readerStats->getNumberOfValues();
        if (rows.has_value() && rows.value() > 0 && numValues.has_value()) {
          column->mutableStats()->nullPct =
              100 * (rows.value() - numValues.value()) / rows.value();
        }

        // Accumulate partition-level column statistics
        if (hasHivePartitionColumns) {
          auto partitionIt = fileToPartition.find(info->path);
          if (partitionIt != fileToPartition.end()) {
            auto* partition = partitionIt->second;
            if (i < partition->mutableStats()->columnStatistics.size()) {
              auto& colStats = partition->mutableStats()->columnStatistics[i];
              colStats.numValues += numValues.value_or(0);
            }
          }
        }
      }
    }
  }
  VELOX_CHECK_NOT_NULL(table, "Table directory {} is empty", tablePath);

  table->makeDefaultLayout(std::move(files), *this);

  // Set partitions on the layout (they were created by listFiles with stats)
  if (hasHivePartitionColumns && !partitions.empty()) {
    // Convert to const shared_ptrs for the layout
    std::vector<std::shared_ptr<const LocalHivePartitionHandle>>
        constPartitions;
    constPartitions.reserve(partitions.size());
    for (auto& partition : partitions) {
      constPartitions.push_back(std::move(partition));
    }

    auto* layout = const_cast<LocalHiveTableLayout*>(
        reinterpret_cast<const LocalHiveTableLayout*>(table->layouts()[0]));
    layout->setPartitions(std::move(constPartitions));
  }

  float pct = 10;
  if (table->numRows() > 1'000'000) {
    // Set pct to sample ~100K rows.
    pct = 100 * 100'000 / table->numRows();
  }
  table->sampleNumDistincts(pct, schemaPool_.get());
}

namespace {

bool isMixedOrder(const StatisticsBuilder& stats) {
  return stats.numAscending() && stats.numDescending();
}

bool isInteger(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return true;
    default:
      return false;
  }
}

template <typename T>
T numericValue(const velox::Variant& v) {
  switch (v.kind()) {
    case velox::TypeKind::TINYINT:
      return static_cast<T>(v.value<velox::TypeKind::TINYINT>());
    case velox::TypeKind::SMALLINT:
      return static_cast<T>(v.value<velox::TypeKind::SMALLINT>());
    case velox::TypeKind::INTEGER:
      return static_cast<T>(v.value<velox::TypeKind::INTEGER>());
    case velox::TypeKind::BIGINT:
      return static_cast<T>(v.value<velox::TypeKind::BIGINT>());
    case velox::TypeKind::REAL:
      return static_cast<T>(v.value<velox::TypeKind::REAL>());
    case velox::TypeKind::DOUBLE:
      return static_cast<T>(v.value<velox::TypeKind::DOUBLE>());
    default:
      VELOX_UNREACHABLE();
  }
}

/// Adjusts the numDistinct estimate based on sampling ratio and type
/// constraints.
///
/// For sampled data, this function scales the distinct count estimate to
/// account for rows that weren't sampled:
/// - If the sampled distinct count is low (< sampledRows / 50), we assume the
///   column has few distinct values (enumeration), so unsampled rows likely
///   contain the same values. The distinct count is left as-is.
/// - If the sampled distinct count is high, we assume unsampled rows will have
///   new values, so we scale up by the inverse of the sampling ratio:
///   estimatedDistinct = totalRows / (sampledRows / sampledDistinct)
///
/// For integer types, the number of distinct values cannot exceed 1 + max -
/// min. This provides an upper bound when the range is known and smaller than
/// the scaled estimate.
void adjustNumDistincts(
    const StatisticsBuilder& builder,
    ColumnStatistics& stats,
    int64_t numSampledRows,
    int64_t numTotalRows) {
  auto estimate = stats.numDistinct;
  int64_t approxNumDistinct =
      estimate.has_value() ? estimate.value() : numTotalRows;

  // Only adjust if we sampled less than 100% of the data
  if (numSampledRows < numTotalRows) {
    // If distinct count is high relative to sample size, scale up
    if (approxNumDistinct > numSampledRows / 50) {
      // Calculate average duplicates per distinct value
      float numDups = numSampledRows / static_cast<float>(approxNumDistinct);
      // Scale to total rows assuming same duplication rate
      approxNumDistinct = std::min<float>(numTotalRows, numTotalRows / numDups);

      // For integer types, distinct count cannot exceed 1 + max - min
      if (isInteger(builder.type()->kind())) {
        auto min = stats.min;
        auto max = stats.max;
        if (min.has_value() && max.has_value() && isMixedOrder(builder)) {
          auto range = numericValue<float>(max.value()) -
              numericValue<float>(min.value());
          approxNumDistinct = std::min<float>(approxNumDistinct, 1 + range);
        }
      }
    }

    stats.numDistinct = approxNumDistinct;
  }
}
} // namespace

std::pair<int64_t, int64_t> LocalHiveTableLayout::samplePartitions(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    float pct,
    velox::RowTypePtr scanType,
    const std::vector<velox::common::Subfield>& fields,
    velox::HashStringAllocator* allocator,
    std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) {
  using namespace velox;

  // Separate fields into data columns and partition columns using helper
  auto separated = separateDataAndPartitionColumns(
      fields, rowType(), hivePartitionColumns());

  // Create statistics builders for DATA columns only (no partition columns)
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  std::vector<std::unique_ptr<StatisticsBuilder>> tableBuilders;
  tableBuilders.reserve(separated.dataFieldIndices.size());
  for (size_t idx : separated.dataFieldIndices) {
    const auto& type = rowType()->findChild(fields[idx].baseName());
    tableBuilders.push_back(StatisticsBuilder::create(type, options));
  }

  // Create output type for file reader (data columns only, no partition
  // columns)
  const auto outputType =
      ROW(std::move(separated.dataNames), std::move(separated.dataTypes));

  // Create column handles for data columns only
  velox::connector::ColumnHandleMap columnHandles;
  for (size_t i = 0; i < separated.dataFieldIndices.size(); ++i) {
    const auto& name = fields[separated.dataFieldIndices[i]].baseName();
    const auto& type = rowType()->findChild(name);
    columnHandles[name] =
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            name,
            velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
            type,
            type);
  }

  // Create new contexts for this sample operation (thread-safe)
  auto* localMetadata = reinterpret_cast<LocalHiveConnectorMetadata*>(
      ConnectorMetadata::metadata(connector()));
  auto sampleContext = createSampleContext(localMetadata);

  // Assert that there are no filters in the table handle
  auto* hiveHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  if (hiveHandle) {
    VELOX_CHECK(
        hiveHandle->subfieldFilters().empty(),
        "samplePartitions does not support filters");
  }

  // Sample all partitions
  std::vector<PartitionHandlePtr> partitionsToSample;
  for (const auto& partition : partitions_) {
    partitionsToSample.push_back(partition);
  }

  int64_t totalScannedRows = 0;
  int64_t totalPassingRows = 0;

  // Process each partition
  for (auto& partitionHandle : partitionsToSample) {
    auto* partition = dynamic_cast<LocalHivePartitionHandle*>(
        const_cast<PartitionHandle*>(partitionHandle.get()));
    if (!partition) {
      continue;
    }

    // Create partition-level statistics builders for DATA columns only
    std::vector<std::unique_ptr<StatisticsBuilder>> partitionBuilders;
    partitionBuilders.reserve(separated.dataFieldIndices.size());
    for (size_t idx : separated.dataFieldIndices) {
      const auto& type = rowType()->findChild(fields[idx].baseName());
      partitionBuilders.push_back(StatisticsBuilder::create(type, options));
    }

    // Determine how many rows to sample from this partition
    const int64_t partitionRows = partition->stats.numRows;
    const int64_t maxRowsToSample =
        static_cast<int64_t>(partitionRows * (pct / 100));

    int64_t partitionScannedRows = 0;
    int64_t partitionPassingRows = 0;

    // Sample files in this partition using partition->files
    for (const auto* file : partition->files) {
      // Create a split for this file
      auto split = std::make_shared<velox::connector::hive::HiveConnectorSplit>(
          connector()->connectorId(), file->path, fileFormat_);

      // Use sampleFile to read and update stats
      auto [fileScanned, filePassed] = sampleFile(
          split,
          outputType,
          tableHandle,
          columnHandles,
          sampleContext.connectorQueryCtx.get(),
          partitionBuilders,
          maxRowsToSample,
          partitionScannedRows);

      partitionScannedRows += fileScanned;
      partitionPassingRows += filePassed;

      if (partitionScannedRows >= maxRowsToSample) {
        break;
      }
    }

    totalScannedRows += partitionScannedRows;
    totalPassingRows += partitionPassingRows;

    // Merge partition-level builders into table-level builders
    for (size_t i = 0; i < partitionBuilders.size(); ++i) {
      if (partitionBuilders[i] && tableBuilders[i]) {
        tableBuilders[i]->merge(*partitionBuilders[i]);
      }
    }

    // Build column stats for this partition from partition builders (data
    // columns only)
    auto* mutablePartition = partition;
    mutablePartition->mutableStats()->columns.clear();
    mutablePartition->mutableStats()->columnStatistics.clear();

    // numFiles and numRows were already set during loadTable
    // Just update column statistics from the sampled data (data columns only)
    for (size_t i = 0; i < partitionBuilders.size(); ++i) {
      if (partitionBuilders[i]) {
        size_t fieldIdx = separated.dataFieldIndices[i];
        const auto& colName = fields[fieldIdx].baseName();
        mutablePartition->mutableStats()->columns.push_back(colName);

        ColumnStatistics colStats;
        colStats.name = colName;
        // Build partition-level column statistics from the partition builder
        // This sets numDistinct, min, max, nullPct, etc. from the sampled data
        partitionBuilders[i]->build(colStats, 1.0f);
        // Adjust numDistinct based on sampling ratio and type constraints
        adjustNumDistincts(
            *partitionBuilders[i],
            colStats,
            partitionScannedRows,
            partitionRows);
        mutablePartition->mutableStats()->columnStatistics.push_back(
            std::move(colStats));
      }
    }
  }

  if (statsBuilders) {
    // Return builders for data columns only
    // The caller expects builders for all fields, so create a sparse array
    statsBuilders->resize(fields.size());
    for (size_t i = 0; i < separated.dataFieldIndices.size(); ++i) {
      size_t fieldIdx = separated.dataFieldIndices[i];
      (*statsBuilders)[fieldIdx] = std::move(tableBuilders[i]);
    }
  }

  return std::pair(totalScannedRows, totalPassingRows);
}

void LocalTable::sampleNumDistincts(
    float samplePct,
    velox::memory::MemoryPool* pool) {
  using namespace velox;

  std::vector<velox::common::Subfield> fields;
  fields.reserve(type_->size());
  for (auto i = 0; i < type_->size(); ++i) {
    fields.push_back(velox::common::Subfield(type_->nameOf(i)));
  }

  // Sample the table. Adjust distinct values according to the samples.
  auto allocator = std::make_unique<velox::HashStringAllocator>(pool);
  auto* layout = layouts_[0].get();

  auto* localLayout = dynamic_cast<LocalHiveTableLayout*>(layout);
  VELOX_CHECK_NOT_NULL(localLayout, "Expecting a local hive layout");

  // Get metadata and create table handle (used by both paths)
  auto* metadata = ConnectorMetadata::metadata(layout->connector());
  auto* localHiveMetadata = dynamic_cast<LocalHiveConnectorMetadata*>(metadata);
  VELOX_CHECK_NOT_NULL(localHiveMetadata);

  std::vector<velox::connector::ColumnHandlePtr> columns;
  columns.reserve(type_->size());
  for (auto i = 0; i < type_->size(); ++i) {
    columns.push_back(layout->createColumnHandle(
        /*session=*/nullptr, type_->nameOf(i)));
  }

  auto& evaluator =
      *localHiveMetadata->connectorQueryCtx()->expressionEvaluator();

  std::vector<velox::core::TypedExprPtr> ignore;
  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr, columns, evaluator, {}, ignore);

  // If table has partition columns, delegate to samplePartitions
  if (!localLayout->hivePartitionColumns().empty()) {
    std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
    localLayout->samplePartitions(
        tableHandle, samplePct, type_, fields, allocator.get(), &statsBuilders);

    // Update numDistincts for all columns from the builders
    numSampledRows_ = numRows_ * (samplePct / 100); // Approximate
    for (auto i = 0; i < statsBuilders.size(); ++i) {
      if (statsBuilders[i]) {
        auto* column = columns_[type_->nameOf(i)].get();
        ColumnStatistics& stats = *column->mutableStats();
        statsBuilders[i]->build(stats);
        adjustNumDistincts(*statsBuilders[i], stats, numSampledRows_, numRows_);
      }
    }
    return;
  }

  // No partition columns - proceed with regular sampling
  // Get the split manager
  auto* splitManager =
      dynamic_cast<LocalHiveSplitManager*>(metadata->splitManager());
  VELOX_CHECK_NOT_NULL(splitManager);

  // Build statistics builders for all fields (no partition columns)
  auto statsBuilders = makeStatisticsBuilders(type_, allocator.get());

  // Create column handles map
  velox::connector::ColumnHandleMap columnHandles;
  for (auto i = 0; i < fields.size(); ++i) {
    const auto& name = type_->nameOf(i);
    const auto& colType = type_->childAt(i);
    columnHandles[name] =
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            name,
            velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
            colType,
            colType);
  }

  // Use split manager to list partitions
  std::vector<PartitionHandlePtr> partitions =
      splitManager->listPartitions(nullptr, tableHandle);

  // Get split source from split manager
  SplitOptions splitOptions;
  auto splitSource = splitManager->getSplitSource(
      nullptr, tableHandle, partitions, splitOptions);

  const auto maxRowsToScan = numRows_ * (samplePct / 100);
  int64_t scannedRows = 0;
  int64_t passingRows = 0;

  auto connectorQueryCtx = localHiveMetadata->connectorQueryCtx();

  // Enumerate splits using the split source
  for (;;) {
    // Get next batch of splits
    constexpr uint64_t kTargetBytes = 256 << 20; // 256MB
    auto splitsAndGroups = splitSource->getSplits(kTargetBytes);

    bool done = false;
    for (const auto& splitAndGroup : splitsAndGroups) {
      if (!splitAndGroup.split) {
        done = true;
        break;
      }

      // Sample this split using sampleFile
      auto [splitScanned, splitPassed] = localLayout->sampleFile(
          splitAndGroup.split,
          type_,
          tableHandle,
          columnHandles,
          connectorQueryCtx.get(),
          statsBuilders,
          maxRowsToScan,
          scannedRows);

      scannedRows += splitScanned;
      passingRows += splitPassed;

      if (scannedRows >= maxRowsToScan) {
        break;
      }
    }

    if (done || scannedRows >= maxRowsToScan) {
      break;
    }
  }

  (void)passingRows; // Intentionally unused, part of return signature
  numSampledRows_ = scannedRows;
  for (auto i = 0; i < statsBuilders.size(); ++i) {
    if (statsBuilders[i]) {
      auto* column = columns_[type_->nameOf(i)].get();
      ColumnStatistics& stats = *column->mutableStats();
      statsBuilders[i]->build(stats);
      adjustNumDistincts(*statsBuilders[i], stats, numSampledRows_, numRows_);
    }
  }
}

const folly::F14FastMap<std::string, const Column*>& LocalTable::columnMap()
    const {
  std::lock_guard<std::mutex> l(mutex_);
  if (columns_.empty()) {
    return exportedColumns_;
  }
  for (const auto& [name, column] : columns_) {
    exportedColumns_[name] = column.get();
  }
  return exportedColumns_;
}

TablePtr LocalHiveConnectorMetadata::findTable(std::string_view name) {
  ensureInitialized();
  std::lock_guard<std::recursive_mutex> l(mutex_);
  return findTableLocked(name);
}

std::shared_ptr<LocalTable> LocalHiveConnectorMetadata::findTableLocked(
    std::string_view name) const {
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second;
}

namespace {

// Recursively delete directory contents.
void deleteDirectoryContents(const std::string& path);

// Recursively delete directory.
void deleteDirectoryRecursive(const std::string& path) {
  deleteDirectoryContents(path);
  rmdir(path.c_str());
}

void deleteDirectoryContents(const std::string& path) {
  DIR* dir = opendir(path.c_str());
  if (!dir) {
    return;
  }

  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    std::string name = entry->d_name;
    if (name == "." || name == "..") {
      continue;
    }
    std::string fullPath = path + "/" + name;
    struct stat st;
    if (stat(fullPath.c_str(), &st) == 0) {
      if (S_ISDIR(st.st_mode)) {
        deleteDirectoryRecursive(fullPath);
      } else {
        unlink(fullPath.c_str());
      }
    }
  }
  closedir(dir);
}

// Create a temporary directory.
// Its path contains two parts 'path' as prefix, 'name' as middle part and
// unique id as suffix.
std::string createTemporaryDirectory(
    std::string_view path,
    std::string_view name) {
  auto templatePath = fmt::format("{}_{}_XXXXXX", path, name);
  const char* resultPath = ::mkdtemp(templatePath.data());
  VELOX_CHECK_NOT_NULL(
      resultPath,
      "Cannot create temp directory, template was {}",
      templatePath);
  return resultPath;
}

// Move all files and directories from sourceDir to targetDir.
void move(const fs::path& sourceDir, const fs::path& targetDir) {
  VELOX_CHECK(
      fs::is_directory(sourceDir),
      "Source directory does not exist or is not a directory: {}",
      sourceDir.string());
  // Create the target directory if it doesn't exist
  fs::create_directories(targetDir);
  // Iterate through the source directory
  for (const auto& entry : fs::directory_iterator(sourceDir)) {
    // Compute the relative path from the source directory
    fs::path relPath = fs::relative(entry.path(), sourceDir);
    fs::path destPath = targetDir / relPath;
    // Create enclosing directories in the target if they don't exist
    fs::create_directories(destPath.parent_path());
    // Move the file/directory to the target directory
    fs::rename(entry.path(), destPath);
  }
}

// Check if directory exists.
bool dirExists(const std::string& path) {
  struct stat info;
  return stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
}

// Create directory (recursively).
void createDir(const std::string& path) {
  if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    throw std::runtime_error("Failed to create directory: " + path);
  }
}

} // namespace

TablePtr LocalHiveConnectorMetadata::createTable(
    const ConnectorSessionPtr& session,
    const std::string& tableName,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  validateOptions(options);
  ensureInitialized();
  auto path = tablePath(tableName);
  if (dirExists(path)) {
    VELOX_USER_FAIL("Table {} already exists", tableName);
  } else {
    createDir(path);
  }

  auto createTableOptions = parseCreateTableOptions(options, format_);

  const std::string jsonStr =
      folly::toPrettyJson(toSchemaJson(rowType, createTableOptions));
  const std::string filePath = schemaPath(path);

  std::lock_guard<std::recursive_mutex> l(mutex_);
  VELOX_USER_CHECK_NULL(
      findTableLocked(tableName), "table {} already exists", tableName);
  {
    std::ofstream outputFile(filePath);
    VELOX_CHECK(outputFile.is_open());

    outputFile << jsonStr;
    outputFile.close();
  }
  return createLocalTable(
      tableName, rowType, createTableOptions, hiveConnector());
}

RowsFuture LocalHiveConnectorMetadata::finishWrite(
    const ConnectorSessionPtr& /*session*/,
    const ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& writeResults) {
  uint64_t rows = 0;
  velox::DecodedVector decoded;
  for (const auto& result : writeResults) {
    decoded.decode(*result->childAt(0));
    for (velox::vector_size_t i = 0; i < decoded.size(); ++i) {
      if (decoded.isNullAt(i)) {
        continue;
      }
      rows += decoded.valueAt<int64_t>(i);
    }
  }
  std::lock_guard<std::recursive_mutex> l(mutex_);
  auto hiveHandle =
      std::dynamic_pointer_cast<const HiveConnectorWriteHandle>(handle);
  VELOX_CHECK_NOT_NULL(hiveHandle, "expecting a Hive write handle");
  auto veloxHandle = std::dynamic_pointer_cast<
      const velox::connector::hive::HiveInsertTableHandle>(
      handle->veloxHandle());
  VELOX_CHECK_NOT_NULL(veloxHandle, "expecting a Hive insert handle");
  const auto& targetPath = veloxHandle->locationHandle()->targetPath();
  const auto& writePath = veloxHandle->locationHandle()->writePath();

  move(writePath, targetPath);
  deleteDirectoryRecursive(writePath);
  loadTable(hiveHandle->table()->name(), targetPath);
  return rows;
}

void LocalHiveConnectorMetadata::reloadTableFromPath(
    std::string_view tableName) {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  loadTable(tableName, tablePath(tableName));
}

velox::ContinueFuture LocalHiveConnectorMetadata::abortWrite(
    const ConnectorSessionPtr& session,
    const ConnectorWriteHandlePtr& handle) noexcept try {
  std::lock_guard<std::recursive_mutex> l(mutex_);
  auto hiveHandle =
      std::dynamic_pointer_cast<const HiveConnectorWriteHandle>(handle);
  VELOX_CHECK_NOT_NULL(hiveHandle, "expecting a Hive write handle");
  auto veloxHandle = std::dynamic_pointer_cast<
      const velox::connector::hive::HiveInsertTableHandle>(
      handle->veloxHandle());
  VELOX_CHECK_NOT_NULL(veloxHandle, "expecting a Hive insert handle");
  const auto& writePath = veloxHandle->locationHandle()->writePath();
  deleteDirectoryRecursive(writePath);

  if (hiveHandle->kind() == WriteKind::kCreate) {
    const auto& targetPath = veloxHandle->locationHandle()->targetPath();
    deleteDirectoryRecursive(targetPath);

    tables_.erase(hiveHandle->table()->name());
  }
  return {};
} catch (const std::exception& e) {
  LOG(ERROR) << e.what() << " while aborting write to Local Hive table";
  return folly::exception_wrapper{folly::current_exception()};
}

std::optional<std::string> LocalHiveConnectorMetadata::makeStagingDirectory(
    std::string_view tableName) const {
  return createTemporaryDirectory(hiveConfig_->hiveLocalDataPath(), tableName);
}

bool LocalHiveConnectorMetadata::dropTable(
    const ConnectorSessionPtr& /* session */,
    std::string_view tableName,
    bool ifExists) {
  ensureInitialized();

  std::lock_guard<std::recursive_mutex> l(mutex_);
  if (!tables_.contains(tableName)) {
    if (ifExists) {
      return false;
    }
    VELOX_USER_FAIL("Table does not exist: {}", tableName);
  }

  deleteDirectoryRecursive(tablePath(tableName));
  return tables_.erase(tableName) == 1;
}

} // namespace facebook::axiom::connector::hive
