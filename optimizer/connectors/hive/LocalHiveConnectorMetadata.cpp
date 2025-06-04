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

#include "optimizer/connectors/hive/LocalHiveConnectorMetadata.h" //@manual
#include "velox/common/base/Fs.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::connector::hive {

std::vector<std::shared_ptr<const PartitionHandle>>
LocalHiveSplitManager::listPartitions(
    const ConnectorTableHandlePtr& tableHandle) {
  // All tables are unpartitioned.
  std::unordered_map<std::string, std::optional<std::string>> empty;
  return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
}

std::shared_ptr<SplitSource> LocalHiveSplitManager::getSplitSource(
    const ConnectorTableHandlePtr& tableHandle,
    std::vector<std::shared_ptr<const PartitionHandle>> partitions) {
  // Since there are only unpartitioned tables now, always makes a SplitSource
  // that goes over all the files in the handle's layout.
  auto tableName = tableHandle->name();
  auto* metadata = getConnector(tableHandle->connectorId())->metadata();
  auto* table = metadata->findTable(tableName);
  VELOX_CHECK_NOT_NULL(
      table, "Could not find {} in its ConnectorMetadata", tableName);
  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);
  auto files = layout->files();
  return std::make_shared<LocalHiveSplitSource>(
      files, 2, layout->fileFormat(), layout->connector()->connectorId());
}

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
      auto filePath = files_[currentFile_];
      const auto fileSize = fs::file_size(filePath);
      // Take the upper bound.
      const int splitSize = std::ceil((fileSize) / splitsPerFile_);
      for (int i = 0; i < splitsPerFile_; ++i) {
        fileSplits_.push_back(
            connector::hive::HiveConnectorSplitBuilder(filePath)
                .connectorId(connectorId_)
                .fileFormat(format_)
                .start(i * splitSize)
                .length(splitSize)
                .build());
      }
    }
    result.push_back(SplitAndGroup{std::move(fileSplits_[currentSplit_++]), 0});
    bytes +=
        reinterpret_cast<const HiveConnectorSplit*>(result.back().split.get())
            ->length;
    if (bytes > targetBytes) {
      return result;
    }
  }
}

LocalHiveConnectorMetadata::LocalHiveConnectorMetadata(
    HiveConnector* hiveConnector)
    : HiveConnectorMetadata(hiveConnector),
      hiveConfig_(
          std::make_shared<HiveConfig>(hiveConnector_->connectorConfig())),
      splitManager_(this) {}

void LocalHiveConnectorMetadata::reinitialize() {
  std::lock_guard<std::mutex> l(mutex_);
  tables_.clear();
  initialize();
  initialized_ = true;
}

void LocalHiveConnectorMetadata::initialize() {
  auto formatName = hiveConfig_->hiveLocalFileFormat();
  auto path = hiveConfig_->hiveLocalDataPath();
  format_ = formatName == "dwrf" ? dwio::common::FileFormat::DWRF
      : formatName == "parquet"  ? dwio::common::FileFormat::PARQUET
                                 : dwio::common::FileFormat::UNKNOWN;
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

std::shared_ptr<core::QueryCtx> LocalHiveConnectorMetadata::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::string> config;
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[hiveConnector_->connectorId()] =
      std::const_pointer_cast<config::ConfigBase>(hiveConfig_->config());

  return core::QueryCtx::create(
      hiveConnector_->executor(),
      core::QueryConfig(config),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      nullptr,
      queryId);
}

void LocalHiveConnectorMetadata::makeQueryCtx() {
  queryCtx_ = makeQueryCtx("local_hive_metadata");
}

void LocalHiveConnectorMetadata::makeConnectorQueryCtx() {
  common::SpillConfig spillConfig;
  common::PrefixSortConfig prefixSortConfig;
  schemaPool_ = queryCtx_->pool()->addLeafChild("schemaReader");
  connectorQueryCtx_ = std::make_shared<connector::ConnectorQueryCtx>(
      schemaPool_.get(),
      queryCtx_->pool(),
      queryCtx_->connectorSessionProperties(hiveConnector_->connectorId()),
      &spillConfig,
      prefixSortConfig,
      std::make_unique<exec::SimpleExpressionEvaluator>(
          queryCtx_.get(), schemaPool_.get()),
      queryCtx_->cache(),
      "scan_for_schema",
      "schema",
      "N/a",
      0,
      queryCtx_->queryConfig().sessionTimezone());
}

void LocalHiveConnectorMetadata::readTables(const std::string& path) {
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_directory() ||
        dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    loadTable(dirEntry.path().filename(), dirEntry.path());
  }
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sample(
    const connector::ConnectorTableHandlePtr& handle,
    float pct,
    std::vector<core::TypedExprPtr> extraFilters,
    RowTypePtr scanType,
    const std::vector<common::Subfield>& fields,
    HashStringAllocator* allocator,
    std::vector<ColumnStatistics>* statistics) const {
  std::vector<std::unique_ptr<StatisticsBuilder>> builders;
  VELOX_CHECK(extraFilters.empty());
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
    (*statistics)[i] = std::move(runnerStats);
  }
  return result;
}

std::pair<int64_t, int64_t> LocalHiveTableLayout::sample(
    const connector::ConnectorTableHandlePtr& tableHandle,
    float pct,
    RowTypePtr scanType,
    const std::vector<common::Subfield>& fields,
    HashStringAllocator* allocator,
    std::vector<std::unique_ptr<StatisticsBuilder>>* statsBuilders) const {
  StatisticsBuilderOptions options = {
      .maxStringLength = 100, .countDistincts = true, .allocator = allocator};
  std::vector<std::unique_ptr<StatisticsBuilder>> builders;

  std::unordered_map<
      std::string,
      std::shared_ptr<velox::connector::ColumnHandle>>
      columnHandles;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto& field : fields) {
    auto& path = field.path();
    auto column =
        dynamic_cast<const common::Subfield::NestedField*>(path[0].get())
            ->name();
    const auto idx = rowType()->getChildIdx(column);
    names.push_back(rowType()->nameOf(idx));
    types.push_back(rowType()->childAt(idx));
    columnHandles[names.back()] =
        std::make_shared<connector::hive::HiveColumnHandle>(
            names.back(),
            connector::hive::HiveColumnHandle::ColumnType::kRegular,
            types.back(),
            types.back());
    builders.push_back(StatisticsBuilder::create(types.back(), options));
  }

  const auto outputType = ROW(std::move(names), std::move(types));
  int64_t passingRows = 0;
  int64_t scannedRows = 0;
  for (auto& file : files_) {
    // TODO: make createDataSource take a ConnectorTableHandlePtr instead of a
    // shared_ptr to mutable handle.
    auto handleCopy =
        std::const_pointer_cast<connector::ConnectorTableHandle>(tableHandle);
    auto connectorQueryCtx =
        reinterpret_cast<LocalHiveConnectorMetadata*>(connector()->metadata())
            ->connectorQueryCtx();
    auto dataSource = connector()->createDataSource(
        outputType, handleCopy, columnHandles, connectorQueryCtx.get());

    auto split = connector::hive::HiveConnectorSplitBuilder(file)
                     .fileFormat(fileFormat_)
                     .connectorId(connector()->connectorId())
                     .build();
    dataSource->addSplit(split);
    constexpr int32_t kBatchSize = 1000;
    for (;;) {
      ContinueFuture ignore{ContinueFuture::makeEmpty()};

      auto data = dataSource->next(kBatchSize, ignore).value();
      if (data == nullptr) {
        scannedRows += dataSource->getCompletedRows();
        break;
      }
      passingRows += data->size();
      if (!builders.empty()) {
        StatisticsBuilder::updateStatsBuilders(data, builders);
      }
      if (scannedRows + dataSource->getCompletedRows() >
          table()->numRows() * (pct / 100)) {
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

void LocalTable::makeDefaultLayout(
    std::vector<std::string> files,
    LocalHiveConnectorMetadata& metadata) {
  std::vector<const Column*> columns;
  for (auto i = 0; i < type_->size(); ++i) {
    auto name = type_->nameOf(i);
    columns.push_back(columns_[name].get());
  }
  auto* connector = metadata.hiveConnector();
  auto format = metadata.fileFormat();
  std::vector<const Column*> empty;
  auto layout = std::make_unique<LocalHiveTableLayout>(
      name_,
      this,
      connector,
      std::move(columns),
      empty,
      empty,
      std::vector<SortOrder>{},
      empty,
      empty,
      format,
      std::nullopt);
  layout->setFiles(std::move(files));
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

void mergeReaderStats(
    Column* column,
    velox::dwio::common::ColumnStatistics* input) {
  auto* stats = column->mutableStats();
  auto c = input->getNumberOfValues();
  stats->numValues += c.has_value() ? c.value() : 0;
}

void LocalHiveConnectorMetadata::loadTable(
    const std::string& tableName,
    const fs::path& tablePath) {
  // open each file in the directory and check their type and add up the row
  // counts.
  RowTypePtr tableType;
  LocalTable* table = nullptr;
  std::vector<std::string> files;

  for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
    if (!dirEntry.is_regular_file()) {
      continue;
    }
    // Ignore hidden files.
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    auto it = tables_.find(tableName);
    if (it != tables_.end()) {
      table = reinterpret_cast<LocalTable*>(it->second.get());
    } else {
      tables_[tableName] = std::make_unique<LocalTable>(tableName, format_);
      table = tables_[tableName].get();
    }
    dwio::common::ReaderOptions readerOptions{schemaPool_.get()};
    readerOptions.setFileFormat(format_);
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(dirEntry.path().string()),
        readerOptions.memoryPool());
    std::unique_ptr<dwio::common::Reader> reader =
        dwio::common::getReaderFactory(readerOptions.fileFormat())
            ->createReader(std::move(input), readerOptions);
    const auto fileType = reader->rowType();
    if (!tableType) {
      tableType = fileType;
    } else if (fileType->size() > tableType->size()) {
      // The larger type is the later since there is only addition of columns.
      // TODO: Check the column types are compatible where they overlap.
      tableType = fileType;
    }
    const auto rows = reader->numberOfRows();

    if (rows.has_value()) {
      table->numRows_ += rows.value();
    }
    for (auto i = 0; i < fileType->size(); ++i) {
      auto name = fileType->nameOf(i);
      Column* column;
      auto columnIt = table->columns().find(name);
      if (columnIt != table->columns().end()) {
        column = columnIt->second.get();
      } else {
        table->columns()[name] =
            std::make_unique<Column>(name, fileType->childAt(i));
        column = table->columns()[name].get();
      }

      auto readerStats = reader->columnStatistics(i);
      if (readerStats) {
        auto numValues = readerStats->getNumberOfValues();
        mergeReaderStats(column, readerStats.get());
        if (rows.has_value() && rows.value() > 0 && numValues.has_value()) {
          column->mutableStats()->nullPct =
              100 * (rows.value() - numValues.value()) / rows.value();
        }
      }
    }

    files.push_back(dirEntry.path());
  }
  VELOX_CHECK_NOT_NULL(table, "Table directory {} is empty", tablePath);

  table->setType(tableType);
  table->makeDefaultLayout(std::move(files), *this);
  float pct = 10;
  if (table->numRows() > 1000000) {
    // Set pct to sample ~100K rows.
    pct = 100 * 100000 / table->numRows();
  }
  table->sampleNumDistincts(pct, schemaPool_.get());
}

bool isMixedOrder(const StatisticsBuilder& stats) {
  return stats.numAsc() && stats.numDesc();
}

bool isInteger(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return true;
    default:
      return false;
  }
}

template <typename T>
T numericValue(const variant& v) {
  switch (v.kind()) {
    case TypeKind::TINYINT:
      return static_cast<T>(v.value<TypeKind::TINYINT>());
    case TypeKind::SMALLINT:
      return static_cast<T>(v.value<TypeKind::SMALLINT>());
    case TypeKind::INTEGER:
      return static_cast<T>(v.value<TypeKind::INTEGER>());
    case TypeKind::BIGINT:
      return static_cast<T>(v.value<TypeKind::BIGINT>());
    case TypeKind::REAL:
      return static_cast<T>(v.value<TypeKind::REAL>());
    case TypeKind::DOUBLE:
      return static_cast<T>(v.value<TypeKind::DOUBLE>());
    default:
      VELOX_UNREACHABLE();
  }
}

void LocalTable::sampleNumDistincts(float samplePct, memory::MemoryPool* pool) {
  std::vector<common::Subfield> fields;
  for (auto i = 0; i < type_->size(); ++i) {
    fields.push_back(common::Subfield(type_->nameOf(i)));
  }

  // Sample the table. Adjust distinct values according to the samples.
  auto allocator = std::make_unique<HashStringAllocator>(pool);
  auto* layout = layouts_[0].get();
  std::vector<connector::ColumnHandlePtr> columns;
  for (auto i = 0; i < type_->size(); ++i) {
    columns.push_back(layout->connector()->metadata()->createColumnHandle(
        *layout, type_->nameOf(i)));
  }
  auto* metadata = dynamic_cast<const LocalHiveConnectorMetadata*>(
      layout->connector()->metadata());
  auto& evaluator = *metadata->connectorQueryCtx()->expressionEvaluator();
  std::vector<core::TypedExprPtr> ignore;
  auto handle = layout->connector()->metadata()->createTableHandle(
      *layout, columns, evaluator, {}, ignore);
  std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
  auto* localLayout = dynamic_cast<LocalHiveTableLayout*>(layout);
  VELOX_CHECK_NOT_NULL(localLayout, "Expecting a local hive layout");
  auto [sampled, passed] = localLayout->sample(
      handle, samplePct, type_, fields, allocator.get(), &statsBuilders);
  numSampledRows_ = sampled;
  for (auto i = 0; i < statsBuilders.size(); ++i) {
    if (statsBuilders[i]) {
      auto* column = columns_[type_->nameOf(i)].get();
      ColumnStatistics& stats = *column->mutableStats();
      statsBuilders[i]->build(stats);
      auto estimate = stats.numDistinct;
      int64_t approxNumDistinct =
          estimate.has_value() ? estimate.value() : numRows_;
      // For tiny tables the sample is 100% and the approxNumDistinct is
      // accurate. For partial samples, the distinct estimate is left to be the
      // distinct estimate of the sample if there are few distincts. This is an
      // enumeration where values in unsampled rows are likely the same. If
      // there are many distincts, we multiply by 1/sample rate assuming that
      // unsampled rows will mostly have new values.

      if (numSampledRows_ < numRows_) {
        if (approxNumDistinct > sampled / 50) {
          float numDups =
              numSampledRows_ / static_cast<float>(approxNumDistinct);
          approxNumDistinct = std::min<float>(numRows_, numRows_ / numDups);

          // If the type is an integer type, num distincts cannot be larger than
          // max - min.

          if (isInteger(statsBuilders[i]->type()->kind())) {
            auto min = stats.min;
            auto max = stats.max;
            if (min.has_value() && max.has_value() &&
                isMixedOrder(*statsBuilders[i])) {
              auto range = numericValue<float>(max.value()) -
                  numericValue<float>(min.value());
              approxNumDistinct = std::min<float>(approxNumDistinct, range);
            }
          }
        }

        const_cast<Column*>(findColumn(type_->nameOf(i)))
            ->mutableStats()
            ->numDistinct = approxNumDistinct;
      }
    }
  }
}

const std::unordered_map<std::string, const Column*>& LocalTable::columnMap()
    const {
  std::lock_guard<std::mutex> l(mutex_);
  if (columns_.empty()) {
    return exportedColumns_;
  }
  for (auto& pair : columns_) {
    exportedColumns_[pair.first] = pair.second.get();
  }
  return exportedColumns_;
}

const Table* LocalHiveConnectorMetadata::findTable(const std::string& name) {
  ensureInitialized();
  auto it = tables_.find(name);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

namespace {
class LocalHiveConnectorMetadataFactory : public HiveConnectorMetadataFactory {
 public:
  std::shared_ptr<ConnectorMetadata> create(HiveConnector* connector) override {
    auto hiveConfig =
        std::make_shared<HiveConfig>(connector->connectorConfig());
    auto path = hiveConfig->hiveLocalDataPath();
    if (path.empty()) {
      return nullptr;
    }
    return std::make_shared<LocalHiveConnectorMetadata>(connector);
  }
};

bool dummy = registerHiveConnectorMetadataFactory(
    std::make_unique<LocalHiveConnectorMetadataFactory>());
} // namespace

} // namespace facebook::velox::connector::hive
