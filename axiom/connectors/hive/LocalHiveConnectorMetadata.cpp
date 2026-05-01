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
#include <folly/CppAttributes.h>
#include <folly/json.h>
#include <sys/stat.h>
#include <unistd.h>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "axiom/optimizer/JsonUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/HivePartitionName.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::connector::hive {

folly::coro::Task<std::vector<PartitionHandlePtr>>
LocalHiveSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  // All tables are unpartitioned.
  folly::F14FastMap<std::string, std::optional<std::string>> empty;
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
}

namespace {

// Merges 'source' into 'target': sums numValues, takes min of mins, max of
// maxes, and max of numDistinct.
void mergeColumnStatsValues(
    ColumnStatistics& target,
    const ColumnStatistics& source) {
  target.numValues += source.numValues;

  if (source.min.has_value()) {
    if (!target.min.has_value() || source.min.value() < target.min.value()) {
      target.min = source.min;
    }
  }
  if (source.max.has_value()) {
    if (!target.max.has_value() || target.max.value() < source.max.value()) {
      target.max = source.max;
    }
  }

  if (source.numDistinct.has_value()) {
    target.numDistinct =
        std::max(target.numDistinct.value_or(0), source.numDistinct.value());
  }
}

// Merges a vector of column stats into an existing vector, matching by name.
void mergeColumnStats(
    std::vector<ColumnStatistics>& existing,
    const std::vector<ColumnStatistics>& incoming) {
  folly::F14FastMap<std::string, size_t> nameToIndex;
  for (size_t i = 0; i < existing.size(); ++i) {
    nameToIndex[existing[i].name] = i;
  }

  for (const auto& stats : incoming) {
    auto it = nameToIndex.find(stats.name);
    if (it == nameToIndex.end()) {
      nameToIndex[stats.name] = existing.size();
      existing.push_back(stats);
      continue;
    }

    mergeColumnStatsValues(existing[it->second], stats);
  }
}

// Extracts the leading digits after the last '/' in the file path.
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

velox::common::Filter* FOLLY_NULLABLE findFilter(
    const velox::connector::hive::HiveTableHandle& tableHandle,
    const std::string& columnName) {
  auto it =
      tableHandle.subfieldFilters().find(velox::common::Subfield(columnName));
  if (it != tableHandle.subfieldFilters().end()) {
    return it->second.get();
  }

  return nullptr;
}

// Filters files based on $path and $bucket filters from the table handle.
// Returns a list of file pointers that match the filters.
std::vector<const FileInfo*> filterFilesByTableHandle(
    const std::vector<std::unique_ptr<const FileInfo>>& files,
    const velox::connector::hive::HiveTableHandle& tableHandle) {
  auto* pathFilter = findFilter(tableHandle, HiveTable::kPath);
  auto* bucketFilter = findFilter(tableHandle, HiveTable::kBucket);

  std::vector<const FileInfo*> selectedFiles;
  for (const auto& file : files) {
    if (pathFilter &&
        !pathFilter->testBytes(
            file->path.c_str(), static_cast<int32_t>(file->path.size()))) {
      continue;
    }
    if (bucketFilter && file->bucketNumber.has_value() &&
        !bucketFilter->testInt64(file->bucketNumber.value())) {
      continue;
    }
    selectedFiles.push_back(file.get());
  }
  return selectedFiles;
}

// Tests a partition key value against a filter. 'value' is the string from the
// directory name (e.g. "2" from "k=2"). 'type' determines how to convert the
// string before testing.
bool testPartitionValue(
    const velox::common::Filter& filter,
    const std::optional<std::string>& value,
    const velox::Type& type) {
  if (!value.has_value()) {
    return filter.testNull();
  }

  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return filter.testBool(folly::to<bool>(value.value()));
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return filter.testInt64(folly::to<int64_t>(value.value()));
    case velox::TypeKind::VARCHAR:
      return filter.testBytes(
          value.value().c_str(), static_cast<int32_t>(value.value().size()));
    default:
      VELOX_UNREACHABLE(
          "Unsupported partition column type: {}", type.toString());
  }
}

// Represents a filter extracted from a filter conjunct that can be evaluated
// from file metadata (partition keys, $path, $bucket).
struct MetadataFilter {
  std::string columnName;
  std::shared_ptr<velox::common::Filter> filter;

  // Partition column for partition key filters. Null for $path and $bucket.
  const Column* column{nullptr};
};

// Classifies filter conjuncts by converting each to subfieldFilters. Conjuncts
// fully converted to subfieldFilters on accepted columns are collected in
// 'metadataFilters'. All others are reported in 'rejectedFilterIndices'. When
// 'allowPathAndBucket' is true, $path and $bucket filters are accepted in
// addition to partition column filters.
void classifyFilterConjuncts(
    const std::vector<velox::core::TypedExprPtr>& filterConjuncts,
    velox::core::ExpressionEvaluator& evaluator,
    const folly::F14FastMap<std::string, const Column*>& partitionColumnsByName,
    bool allowPathAndBucket,
    std::vector<MetadataFilter>& metadataFilters,
    std::vector<int32_t>& rejectedFilterIndices) {
  for (int32_t i = 0; i < filterConjuncts.size(); ++i) {
    velox::common::SubfieldFilters subfieldFilters;
    double sampleRate = 1.0;
    auto remaining = velox::connector::hive::extractFiltersFromRemainingFilter(
        filterConjuncts[i], &evaluator, subfieldFilters, sampleRate);

    if (remaining != nullptr) {
      rejectedFilterIndices.push_back(i);
      continue;
    }

    bool allAccepted = true;
    for (const auto& [subfield, filter] : subfieldFilters) {
      const auto& name = subfield.baseName();
      if (!partitionColumnsByName.count(name) &&
          !(allowPathAndBucket &&
            (name == HiveTable::kPath || name == HiveTable::kBucket))) {
        allAccepted = false;
        break;
      }
    }

    if (!allAccepted) {
      rejectedFilterIndices.push_back(i);
      continue;
    }

    for (auto& [subfield, filter] : subfieldFilters) {
      const auto& name = subfield.baseName();
      auto it = partitionColumnsByName.find(name);
      metadataFilters.emplace_back(
          MetadataFilter{
              name,
              std::move(filter),
              it != partitionColumnsByName.end() ? it->second : nullptr});
    }
  }
}

// Estimates table stats from persisted partition-level stats. Applies partition
// filters, merges matching partitions' column stats, and filters to requested
// columns.
FilteredTableStats estimateStatsFromPartitionStats(
    const std::vector<PartitionStats>& partitionStats,
    const std::vector<velox::core::TypedExprPtr>& filterConjuncts,
    velox::core::ExpressionEvaluator& evaluator,
    const folly::F14FastMap<std::string, const Column*>& partitionColumnsByName,
    const std::vector<const Column*>& requestedColumns) {
  std::vector<MetadataFilter> metadataFilters;
  std::vector<int32_t> rejectedFilterIndices;
  classifyFilterConjuncts(
      filterConjuncts,
      evaluator,
      partitionColumnsByName,
      /*allowPathAndBucket=*/false,
      metadataFilters,
      rejectedFilterIndices);

  uint64_t totalRows{0};
  std::vector<ColumnStatistics> mergedColumnStats;
  for (const auto& partition : partitionStats) {
    bool matched = true;
    for (const auto& metadataFilter : metadataFilters) {
      VELOX_CHECK_NOT_NULL(metadataFilter.column);
      auto it = partition.partitionKeys.find(metadataFilter.columnName);
      if (it == partition.partitionKeys.end()) {
        matched = false;
        break;
      }
      if (!testPartitionValue(
              *metadataFilter.filter,
              it->second,
              *metadataFilter.column->type())) {
        matched = false;
        break;
      }
    }
    if (!matched) {
      continue;
    }

    totalRows += partition.numRows;
    mergeColumnStats(mergedColumnStats, partition.columnStats);
  }

  // Filter to only the requested columns and fill in missing ones.
  folly::F14FastMap<std::string, size_t> statsNameToIndex;
  for (size_t i = 0; i < mergedColumnStats.size(); ++i) {
    statsNameToIndex[mergedColumnStats[i].name] = i;
  }

  std::vector<ColumnStatistics> columnStats;
  columnStats.reserve(requestedColumns.size());
  for (const auto* column : requestedColumns) {
    auto it = statsNameToIndex.find(column->name());
    if (it != statsNameToIndex.end()) {
      auto& stats = mergedColumnStats[it->second];
      if (totalRows > 0) {
        stats.nullPct = 100.0f *
            static_cast<float>(totalRows - stats.numValues) /
            static_cast<float>(totalRows);
      }
      columnStats.push_back(std::move(stats));
    } else {
      // Column not in any partition stats (schema evolution: column added
      // after existing partitions were written). All values are null.
      ColumnStatistics stats;
      stats.name = column->name();
      stats.nullPct = totalRows > 0 ? 100.0f : 0.0f;
      columnStats.push_back(std::move(stats));
    }
  }

  return FilteredTableStats{
      totalRows, std::move(columnStats), std::move(rejectedFilterIndices)};
}

} // namespace

std::shared_ptr<SplitSource> LocalHiveSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    SplitOptions options) {
  // Since there are only unpartitioned tables now, always makes a SplitSource
  // that goes over all the files in the handle's layout.
  auto metadata = ConnectorMetadataRegistry::get(tableHandle->connectorId());
  auto table = metadata->findTable(
      {std::string(LocalHiveConnectorMetadata::kDefaultSchema),
       tableHandle->name()});
  VELOX_CHECK_NOT_NULL(
      table, "Could not find {} in its ConnectorMetadata", tableHandle->name());
  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);

  auto* hiveTableHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(hiveTableHandle);

  auto selectedFiles =
      filterFilesByTableHandle(layout->files(), *hiveTableHandle);

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

folly::coro::Task<SplitBatch> LocalHiveSplitSource::co_getSplits(
    uint32_t maxSplitCount,
    int32_t /*bucket*/) {
  SplitBatch batch;
  const size_t limit = maxSplitCount;
  while (batch.splits.size() < limit && fileIdx_ < files_.size()) {
    const auto& filePath = files_[fileIdx_]->path;
    const auto fileSize = fs::file_size(filePath);
    int64_t splitsPerFile =
        ceil2<uint64_t>(fileSize, options_.fileBytesPerSplit);
    if (options_.targetSplitCount) {
      auto numFiles = files_.size();
      if (splitsPerFile * numFiles < options_.targetSplitCount) {
        auto perFile = ceil2<uint64_t>(options_.targetSplitCount, numFiles);
        int64_t bytesInSplit = ceil2<uint64_t>(fileSize, perFile);
        splitsPerFile = ceil2<uint64_t>(
            fileSize, std::max<uint64_t>(bytesInSplit, 32 << 20));
      }
    }
    const int64_t splitSize = ceil2<uint64_t>(fileSize, splitsPerFile);
    while (splitWithinFile_ < splitsPerFile && batch.splits.size() < limit) {
      auto builder = velox::connector::hive::HiveConnectorSplitBuilder(filePath)
                         .connectorId(connectorId_)
                         .fileFormat(format_)
                         .start(splitWithinFile_ * splitSize)
                         .length(splitSize);

      auto* info = files_[fileIdx_];
      if (info->bucketNumber.has_value()) {
        builder.tableBucketNumber(info->bucketNumber.value());
      }
      for (auto& pair : info->partitionKeys) {
        builder.partitionKey(pair.first, pair.second);
      }
      if (!serdeParameters_.empty()) {
        builder.serdeParameters(serdeParameters_);
      }
      batch.splits.push_back(builder.build());
      ++splitWithinFile_;
    }
    if (splitWithinFile_ >= splitsPerFile) {
      ++fileIdx_;
      splitWithinFile_ = 0;
    }
  }
  batch.noMoreSplits = (fileIdx_ >= files_.size());
  co_return batch;
}

LocalHiveConnectorMetadata::LocalHiveConnectorMetadata(
    velox::connector::hive::HiveConnector* hiveConnector)
    : HiveConnectorMetadata(hiveConnector),
      splitManager_(this),
      hiveMetadataConfig_(
          std::make_shared<HiveMetadataConfig>(
              hiveConnector->connectorConfig())) {}

void LocalHiveConnectorMetadata::reinitialize() {
  std::lock_guard<std::mutex> l(mutex_);
  tables_.clear();
  initialize();
  initialized_ = true;
}

void LocalHiveConnectorMetadata::initialize() {
  static folly::once_flag kTypeSerDeRegistered;
  folly::call_once(
      kTypeSerDeRegistered, []() { velox::Type::registerSerDe(); });

  const auto formatName = hiveMetadataConfig_->localFileFormat();
  const auto path = hiveMetadataConfig_->localDataPath();

  format_ = velox::dwio::common::toFileFormat(formatName);

  makeQueryCtx();
  makeConnectorQueryCtx();
  readTables(path);
}

void LocalHiveConnectorMetadata::ensureInitialized() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (initialized_) {
    return;
  }
  const_cast<LocalHiveConnectorMetadata*>(this)->initialize();
  initialized_ = true;
}

std::shared_ptr<velox::core::QueryCtx> LocalHiveConnectorMetadata::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::string> config;
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[hiveConnector_->connectorId()] =
      std::const_pointer_cast<velox::config::ConfigBase>(
          hiveConnector_->connectorConfig());

  return velox::core::QueryCtx::create(
      hiveConnector_->executor(),
      velox::core::QueryConfig(std::move(config)),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      nullptr,
      queryId);
}

void LocalHiveConnectorMetadata::makeQueryCtx() {
  queryCtx_ = makeQueryCtx("local_hive_metadata");
}

void LocalHiveConnectorMetadata::makeConnectorQueryCtx() {
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

void LocalHiveConnectorMetadata::readTables(std::string_view path) {
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_directory() ||
        dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    loadTable(dirEntry.path().filename().native(), dirEntry.path());
  }
}

SampleResult LocalHiveTableLayout::sample(
    const velox::connector::ConnectorTableHandlePtr& handle) const {
  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  auto result = sample(handle, 1, {}, nullptr, &builders);
  return SampleResult{result.first, result.second};
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sample(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    float pct,
    const std::vector<velox::common::Subfield>& fields,
    velox::HashStringAllocator* allocator,
    std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const {
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};

  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  velox::connector::ColumnHandleMap columnHandles;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(fields.size());
  types.reserve(fields.size());

  for (const auto& field : fields) {
    const auto& name = field.baseName();
    const auto* column = table().findColumn(name);
    VELOX_CHECK_NOT_NULL(column, "Column not found: {}", name);
    const auto& type = column->type();

    names.push_back(name);
    types.push_back(type);

    columnHandles[name] = createColumnHandle(nullptr, name);
    builders.push_back(StatisticsBuilder::create(type, options));
  }

  const auto outputType = ROW(std::move(names), std::move(types));

  auto metadataPtr = ConnectorMetadataRegistry::get(connector()->connectorId());
  auto connectorQueryCtx =
      dynamic_cast<const LocalHiveConnectorMetadata*>(metadataPtr.get())
          ->connectorQueryCtx();

  const auto maxRowsToScan = table().numRows() * (pct / 100);

  // Filter files based on $path and $bucket filters from tableHandle.
  auto* hiveTableHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(hiveTableHandle);
  auto selectedFiles = filterFilesByTableHandle(files_, *hiveTableHandle);

  int64_t passingRows = 0;
  int64_t scannedRows = 0;

  for (const auto* file : selectedFiles) {
    auto dataSource = connector()->createDataSource(
        outputType, tableHandle, columnHandles, connectorQueryCtx.get());

    auto split = velox::connector::hive::HiveConnectorSplitBuilder(file->path)
                     .fileFormat(fileFormat_)
                     .connectorId(connector()->connectorId())
                     .build();
    dataSource->addSplit(split);
    constexpr int32_t kBatchSize = 1'000;
    for (;;) {
      velox::ContinueFuture ignore{velox::ContinueFuture::makeEmpty()};
      auto data = dataSource->next(kBatchSize, ignore).value();
      if (data == nullptr) {
        scannedRows += dataSource->getCompletedRows();
        break;
      }

      passingRows += data->size();
      if (!builders.empty()) {
        StatisticsBuilder::updateBuilders(data, builders);
      }

      if (scannedRows + dataSource->getCompletedRows() > maxRowsToScan) {
        scannedRows += dataSource->getCompletedRows();
        break;
      }
    }
  }

  if (statsBuilders) {
    *statsBuilders = std::move(builders);
  }
  return std::pair(scannedRows, passingRows);
}

folly::coro::Task<std::optional<FilteredTableStats>>
LocalHiveTableLayout::co_estimateStats(
    ConnectorSessionPtr /*session*/,
    velox::connector::ConnectorTableHandlePtr /*tableHandle*/,
    std::vector<std::string> columns,
    std::vector<velox::core::TypedExprPtr> filterConjuncts) const {
  folly::F14FastMap<std::string, const Column*> partitionColumnsByName;
  for (const auto* column : hivePartitionColumns()) {
    partitionColumnsByName[column->name()] = column;
  }

  auto connectorMetadata =
      ConnectorMetadataRegistry::get(connector()->connectorId());
  auto* localHiveMetadata =
      dynamic_cast<const LocalHiveConnectorMetadata*>(connectorMetadata.get());
  auto& evaluator =
      *localHiveMetadata->connectorQueryCtx()->expressionEvaluator();

  std::vector<const Column*> requestedColumns;
  requestedColumns.reserve(columns.size());
  for (const auto& columnName : columns) {
    const auto* column = table().findColumn(columnName);
    VELOX_CHECK_NOT_NULL(column, "Column not found: {}", columnName);
    requestedColumns.push_back(column);
  }

  co_return estimateStatsFromPartitionStats(
      partitionStats_,
      filterConjuncts,
      evaluator,
      partitionColumnsByName,
      requestedColumns);
}

LocalHiveTableLayout* LocalTable::makeDefaultLayout(
    std::vector<std::unique_ptr<const FileInfo>> files,
    LocalHiveConnectorMetadata& metadata) {
  if (!layouts_.empty()) {
    auto* layout = reinterpret_cast<LocalHiveTableLayout*>(layouts_[0].get());
    layout->setFiles(std::move(files));
    return layout;
  }

  std::vector<const Column*> empty;
  auto layout = std::make_unique<LocalHiveTableLayout>(
      name().table,
      /*table=*/this,
      metadata.hiveConnector(),
      allColumns(),
      /*numBuckets=*/std::nullopt,
      /*partitioning=*/empty,
      /*orderColumns=*/empty,
      /*sortOrder=*/std::vector<SortOrder>{},
      /*lookupKeys=*/empty,
      /*hivePartitionColumns=*/empty,
      metadata.fileFormat(),
      metadata.hiveMetadataConfig());
  layout->setFiles(std::move(files));
  auto* result = layout.get();
  exportedLayouts_.push_back(result);
  layouts_.push_back(std::move(layout));
  return result;
}

namespace {

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
      return static_cast<int32_t>(numBuckets);
    }
    default:
      VELOX_USER_FAIL(
          "Unsupported {} type: {}",
          HiveWriteOptions::kBucketCount,
          velox::TypeKindName::toName(value.kind()));
  }
}
std::string toLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), ::tolower);
  return value;
}

} // namespace

CreateTableOptions parseCreateTableOptions(
    const folly::F14FastMap<std::string, velox::Variant>& options,
    velox::dwio::common::FileFormat defaultFileFormat) {
  CreateTableOptions result;

  auto it = options.find(HiveWriteOptions::kCompressionKind);
  if (it != options.end()) {
    result.compressionKind = velox::common::stringToCompressionKind(
        toLower(it->second.value<std::string>()));
  }

  it = options.find(HiveWriteOptions::kFileFormat);
  if (it != options.end()) {
    result.fileFormat = velox::dwio::common::toFileFormat(
        toLower(it->second.value<std::string>()));
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
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;

  auto parseColumn = [&](const auto& column) {
    names.push_back(column["name"].asString());
    types.push_back(
        velox::ISerializable::deserialize<velox::Type>(column["type"]));
  };

  for (const auto& column : obj["dataColumns"]) {
    parseColumn(column);
  }

  for (const auto& column : obj["partitionColumns"]) {
    parseColumn(column);
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

  for (const auto& partCol : obj["partitionColumns"]) {
    options.partitionedByColumns.push_back(partCol["name"].asString());
  }

  if (obj.count("bucketProperty")) {
    const auto& bucketObj = obj["bucketProperty"];
    options.numBuckets = atoi(bucketObj["bucketCount"].asString().c_str());

    for (const auto& bucketCol : bucketObj["bucketedBy"]) {
      options.bucketedByColumns.push_back(bucketCol.asString());
    }

    for (const auto& sortCol : bucketObj["sortedBy"]) {
      options.sortedByColumns.push_back(sortCol.asString());
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
    column["type"] = rowType->childAt(i)->serialize();

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
    velox::connector::Connector* connector,
    std::shared_ptr<HiveMetadataConfig> hiveMetadataConfig) {
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

  // Convert a vector of strings to a Variant array of VARCHAR.
  auto toVariantArray = [](const std::vector<std::string>& values) {
    std::vector<velox::Variant> variants;
    variants.reserve(values.size());
    for (const auto& value : values) {
      variants.emplace_back(value);
    }
    return velox::Variant::array(std::move(variants));
  };

  if (!createTableOptions.partitionedByColumns.empty()) {
    options[HiveWriteOptions::kPartitionedBy] =
        toVariantArray(createTableOptions.partitionedByColumns);
  }

  if (!createTableOptions.bucketedByColumns.empty()) {
    options[HiveWriteOptions::kBucketedBy] =
        toVariantArray(createTableOptions.bucketedByColumns);
  }

  if (createTableOptions.numBuckets.has_value()) {
    options[HiveWriteOptions::kBucketCount] =
        velox::Variant(createTableOptions.numBuckets.value());
  }

  if (!createTableOptions.sortedByColumns.empty()) {
    options[HiveWriteOptions::kSortedBy] =
        toVariantArray(createTableOptions.sortedByColumns);
  }

  const std::optional<int32_t> numBuckets = createTableOptions.numBuckets;

  auto table = std::make_shared<LocalTable>(
      SchemaTableName{
          std::string(LocalHiveConnectorMetadata::kDefaultSchema),
          std::string{name}},
      schema,
      numBuckets.has_value(),
      std::move(options),
      createTableOptions.partitionedByColumns);

  std::vector<const Column*> partitionedBy;
  for (const auto& columnName : createTableOptions.partitionedByColumns) {
    auto column = table->findColumn(columnName);
    VELOX_CHECK_NOT_NULL(
        column, "Partitioned-by column not found: {}", columnName);
    partitionedBy.push_back(column);
  }

  std::vector<const Column*> bucketedBy;
  std::vector<const Column*> sortedBy;
  std::vector<SortOrder> sortOrders;
  if (numBuckets.has_value()) {
    for (const auto& columnName : createTableOptions.bucketedByColumns) {
      auto column = table->findColumn(columnName);
      VELOX_CHECK_NOT_NULL(
          column, "Bucketed-by column not found: {}", columnName);
      bucketedBy.push_back(column);
    }

    for (const auto& columnName : createTableOptions.sortedByColumns) {
      auto column = table->findColumn(columnName);
      VELOX_CHECK_NOT_NULL(
          column, "Sorted-by column not found: {}", columnName);
      sortedBy.push_back(column);
      sortOrders.push_back(SortOrder{true, true}); // ASC NULLS FIRST.
    }
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
      table->name().table,
      table.get(),
      connector,
      table->allColumns(),
      numBuckets,
      bucketedBy,
      sortedBy,
      sortOrders,
      /*lookupKeys=*/std::vector<const Column*>{},
      partitionedBy,
      createTableOptions.fileFormat.value(),
      std::move(hiveMetadataConfig),
      std::move(serdeParameters));
  table->addLayout(std::move(layout));
  return table;
}

std::shared_ptr<LocalTable> createTableFromSchema(
    std::string_view name,
    std::string_view path,
    velox::dwio::common::FileFormat defaultFileFormat,
    velox::connector::Connector* connector,
    std::shared_ptr<HiveMetadataConfig> hiveMetadataConfig) {
  auto jsons = readConcatenatedDynamicsFromFile(schemaPath(path));
  if (jsons.empty()) {
    return nullptr;
  }

  VELOX_CHECK_EQ(jsons.size(), 1);
  const auto& json = jsons[0];

  const auto options = parseCreateTableOptions(json, defaultFileFormat);
  const auto schema = parseSchema(json);

  return createLocalTable(
      name, schema, options, connector, std::move(hiveMetadataConfig));
}

} // namespace

// Loads persisted write-time stats from .stats files and stores them in the
// layout for co_estimateStats. Sets table-level row count and column stats
// from persisted data.
void LocalHiveConnectorMetadata::loadTableWithWriteTimeStats(
    std::shared_ptr<LocalTable> table,
    LocalHiveTableLayout* layout,
    const fs::path& tablePath) {
  std::vector<PartitionStats> allPartitionStats;
  auto persistedStats = PersistedStats::read(tablePath.string());
  if (persistedStats.has_value()) {
    // Unpartitioned table: single entry with empty partition keys.
    PartitionStats partitionEntry;
    partitionEntry.numRows = persistedStats->numRows;
    partitionEntry.columnStats = std::move(persistedStats->columns);

    table->incrementNumRows(partitionEntry.numRows);

    // Set table-level Column stats (copy, not move).
    for (const auto& colStats : partitionEntry.columnStats) {
      auto* column = table->findColumn(colStats.name);
      if (column != nullptr) {
        const_cast<Column*>(column)->setStats(
            std::make_unique<ColumnStatistics>(colStats));
      }
    }

    allPartitionStats.push_back(std::move(partitionEntry));
  } else {
    // Partitioned table: read per-partition .stats files.
    for (const auto& entry : std::filesystem::directory_iterator(tablePath)) {
      if (!entry.is_directory()) {
        continue;
      }
      auto persisted = PersistedStats::read(entry.path().string());
      if (!persisted.has_value()) {
        continue;
      }

      PartitionStats partitionEntry;
      partitionEntry.numRows = persisted->numRows;
      partitionEntry.columnStats = std::move(persisted->columns);

      table->incrementNumRows(partitionEntry.numRows);

      // Parse partition key=value pairs from directory name.
      const auto dirName = entry.path().filename().string();
      auto equalsPos = dirName.find('=');
      if (equalsPos != std::string::npos) {
        partitionEntry.partitionKeys[dirName.substr(0, equalsPos)] =
            dirName.substr(equalsPos + 1);
      }
      allPartitionStats.push_back(std::move(partitionEntry));
    }
  }

  layout->setPartitionStats(std::move(allPartitionStats));
}

void LocalHiveConnectorMetadata::loadTable(
    std::string_view tableName,
    const fs::path& tablePath) {
  auto table = createTableFromSchema(
      tableName,
      tablePath.native(),
      format_,
      hiveConnector(),
      hiveMetadataConfig_);
  VELOX_CHECK_NOT_NULL(
      table,
      "Schema file (.schema) not found for table: {}. "
      "Run LocalTableBuilder::build() first.",
      tablePath);

  tables_[tableName] = table;

  std::function<int32_t(std::string_view)> parseBucketNumber = nullptr;
  if (!table->layouts()[0]->partitionColumns().empty()) {
    parseBucketNumber = extractDigitsAfterLastSlash;
  }

  std::vector<std::unique_ptr<const FileInfo>> files;
  std::string pathString = tablePath;
  FileInfo::listFiles(pathString, parseBucketNumber, pathString.size(), files);

  auto* layout = table->makeDefaultLayout(std::move(files), *this);
  loadTableWithWriteTimeStats(table, layout, tablePath);
}

LocalTable::LocalTable(
    SchemaTableName name,
    velox::RowTypePtr type,
    bool bucketed,
    folly::F14FastMap<std::string, velox::Variant> options,
    std::vector<std::string> partitionColumnNames)
    : HiveTable(
          std::move(name),
          std::move(type),
          bucketed,
          /*includeHiddenColumns=*/true,
          std::move(options),
          std::move(partitionColumnNames)) {}

TablePtr LocalHiveConnectorMetadata::findTable(
    const SchemaTableName& tableName) {
  VELOX_USER_CHECK_EQ(
      tableName.schema,
      kDefaultSchema,
      "Unsupported schema: {}",
      tableName.schema);
  ensureInitialized();
  std::lock_guard<std::mutex> l(mutex_);
  return findTableLocked(tableName.table);
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

// Returns a RowVector with the last column removed.
velox::RowVectorPtr dropLastColumn(const velox::RowVectorPtr& rowVector) {
  const auto& rowType = rowVector->type()->asRow();
  return std::make_shared<velox::RowVector>(
      rowVector->pool(),
      velox::ROW(
          {rowType.names().begin(), rowType.names().end() - 1},
          {rowType.children().begin(), rowType.children().end() - 1}),
      /*nulls=*/nullptr,
      rowVector->size(),
      std::vector<velox::VectorPtr>(
          rowVector->children().begin(), rowVector->children().end() - 1));
}

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
    std::string fullPath = fmt::format("{}/{}", path, name);
    struct stat st{};
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
    VELOX_USER_CHECK(
        !fs::exists(destPath),
        "Partition already exists: {}",
        destPath.string());
    // Create enclosing directories in the target if they don't exist
    fs::create_directories(destPath.parent_path());
    // Move the file/directory to the target directory
    fs::rename(entry.path(), destPath);
  }
}

// Check if directory exists.
bool dirExists(const std::string& path) {
  struct stat info{};
  return stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
}

// Create directory (recursively).
void createDir(const std::string& path) {
  if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    VELOX_USER_FAIL("Failed to create directory: {}", path);
  }
}

} // namespace

TablePtr LocalHiveConnectorMetadata::createTable(
    const ConnectorSessionPtr& /*session*/,
    const SchemaTableName& tableName,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, velox::Variant>& options,
    bool ifNotExists,
    bool explain) {
  VELOX_USER_CHECK(
      schemaExists(nullptr, tableName.schema),
      "Schema does not exist: {}",
      tableName.schema);
  validateOptions(options);
  ensureInitialized();
  auto createTableOptions = parseCreateTableOptions(options, format_);

  if (explain) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (findTableLocked(tableName.table)) {
        if (ifNotExists) {
          return nullptr;
        }
        VELOX_USER_FAIL("Table already exists: {}", tableName.toString());
      }
    }
    return createLocalTable(
        tableName.table,
        rowType,
        createTableOptions,
        hiveConnector(),
        hiveMetadataConfig_);
  }

  auto path = tablePath(tableName);
  if (dirExists(path)) {
    if (ifNotExists) {
      return nullptr;
    }
    VELOX_USER_FAIL("Table already exists: {}", tableName.toString());
  } else {
    createDir(path);
  }

  const std::string jsonStr =
      folly::toPrettyJson(toSchemaJson(rowType, createTableOptions));
  const std::string filePath = schemaPath(path);

  std::lock_guard<std::mutex> l(mutex_);
  if (findTableLocked(tableName.table)) {
    deleteDirectoryRecursive(path);
    if (ifNotExists) {
      return nullptr;
    }
    VELOX_USER_FAIL("Table already exists: {}", tableName.toString());
  }
  {
    std::ofstream outputFile(filePath);
    VELOX_CHECK(outputFile.is_open());

    outputFile << jsonStr;
    outputFile.close();
  }
  auto table = createLocalTable(
      tableName.table,
      rowType,
      createTableOptions,
      hiveConnector(),
      hiveMetadataConfig_);
  tables_[tableName.table] = table;
  return table;
}

RowsFuture LocalHiveConnectorMetadata::finishWrite(
    const ConnectorSessionPtr& /*session*/,
    const ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& writeResults,
    velox::RowVectorPtr groupingKeys,
    std::vector<std::vector<ColumnStatistics>> groupStats) {
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
  std::lock_guard<std::mutex> l(mutex_);
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

  // Persist write-time stats. For partitioned tables, write per-partition
  // .stats files. For unpartitioned tables, write a single .stats file in
  // the table directory. Merges with existing stats from prior writes.
  if (groupingKeys != nullptr) {
    VELOX_CHECK_EQ(
        groupStats.size(),
        groupingKeys->size(),
        "Stats group count does not match grouping keys count");

    // The last column in groupingKeys is $row_count (per-group row count).
    // Preceding columns are partition keys.
    const auto numColumns = groupingKeys->childrenSize();
    VELOX_CHECK_GT(numColumns, 0);
    auto* rowCounts = groupingKeys->childAt(numColumns - 1)
                          ->asUnchecked<velox::SimpleVector<int64_t>>();

    // Drop the $row_count column (last) to get only partition keys.
    auto partitionKeys = dropLastColumn(groupingKeys);

    for (auto i = 0; i < groupingKeys->size(); ++i) {
      const auto partitionRows = rowCounts->valueAt(i);
      auto partitionDirName =
          velox::connector::hive::HivePartitionName::partitionName(
              i, partitionKeys, /*partitionKeyAsLowerCase=*/true);
      PersistedStats::write(
          fmt::format("{}/{}", targetPath, partitionDirName),
          {static_cast<uint64_t>(partitionRows), std::move(groupStats[i])});
    }
  } else {
    PersistedStats::write(
        targetPath,
        {rows,
         groupStats.empty() ? std::vector<ColumnStatistics>{}
                            : std::move(groupStats[0])});
  }

  // loadTable reads the .schema and .stats files, populating in-memory
  // Column stats.
  loadTable(hiveHandle->table()->name().table, targetPath);

  return rows;
}

void LocalHiveConnectorMetadata::reloadTableFromPath(
    const SchemaTableName& tableName) {
  std::lock_guard<std::mutex> l(mutex_);
  loadTable(tableName.table, tablePath(tableName));
}

velox::ContinueFuture LocalHiveConnectorMetadata::abortWrite(
    const ConnectorSessionPtr& /*session*/,
    const ConnectorWriteHandlePtr& handle) noexcept try {
  std::lock_guard<std::mutex> l(mutex_);
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

    tables_.erase(hiveHandle->table()->name().table);
  }
  return {};
} catch (const std::exception& e) {
  LOG(ERROR) << e.what() << " while aborting write to Local Hive table";
  return folly::exception_wrapper{folly::current_exception()};
}

std::optional<std::string> LocalHiveConnectorMetadata::makeStagingDirectory(
    const SchemaTableName& tableName) const {
  return createTemporaryDirectory(
      hiveMetadataConfig_->localDataPath(), tableName.table);
}

bool LocalHiveConnectorMetadata::dropTable(
    const ConnectorSessionPtr& /* session */,
    const SchemaTableName& tableName,
    bool ifExists) {
  ensureInitialized();

  std::lock_guard<std::mutex> l(mutex_);
  if (!tables_.contains(tableName.table)) {
    if (ifExists) {
      return false;
    }
    VELOX_USER_FAIL("Table does not exist: {}", tableName);
  }

  deleteDirectoryRecursive(tablePath(tableName));
  return tables_.erase(tableName.table) == 1;
}

} // namespace facebook::axiom::connector::hive
