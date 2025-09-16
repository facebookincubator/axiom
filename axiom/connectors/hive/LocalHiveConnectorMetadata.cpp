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
#include <folly/FileUtil.h>
#include <folly/json.h>
#include <sys/stat.h>
#include <unistd.h>
#include "axiom/optimizer/JsonUtil.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/Expr.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"

namespace facebook::axiom::connector::hive {

std::vector<PartitionHandlePtr> LocalHiveSplitManager::listPartitions(
    const velox::connector::ConnectorTableHandlePtr& tableHandle) {
  // All tables are unpartitioned.
  folly::F14FastMap<std::string, std::optional<std::string>> empty;
  return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
}

std::shared_ptr<SplitSource> LocalHiveSplitManager::getSplitSource(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    SplitOptions options) {
  // Since there are only unpartitioned tables now, always makes a SplitSource
  // that goes over all the files in the handle's layout.
  auto tableName = tableHandle->name();
  auto* metadata = ConnectorMetadata::metadata(tableHandle->connectorId());
  auto table = metadata->findTable(tableName);
  VELOX_CHECK_NOT_NULL(
      table, "Could not find {} in its ConnectorMetadata", tableName);
  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);
  auto& files = layout->files();
  std::vector<const FileInfo*> selectedFiles;
  for (auto& file : files) {
    selectedFiles.push_back(file.get());
  }
  return std::make_shared<LocalHiveSplitSource>(
      std::move(selectedFiles),
      layout->fileFormat(),
      layout->connector()->connectorId(),
      options);
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
  std::lock_guard<std::mutex> l(mutex_);
  tables_.clear();
  initialize();
  initialized_ = true;
}

void LocalHiveConnectorMetadata::initialize() {
  auto formatName = hiveConfig_->hiveLocalFileFormat();
  auto path = hiveConfig_->hiveLocalDataPath();
  format_ = formatName == "dwrf" ? velox::dwio::common::FileFormat::DWRF
      : formatName == "parquet"  ? velox::dwio::common::FileFormat::PARQUET
                                 : velox::dwio::common::FileFormat::UNKNOWN;
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
    const auto& type = rowType()->findChild(name);

    names.push_back(name);
    types.push_back(type);

    columnHandles[name] =
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            name,
            velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
            type,
            type);
    builders.push_back(StatisticsBuilder::create(type, options));
  }

  const auto outputType = ROW(std::move(names), std::move(types));

  auto connectorQueryCtx = reinterpret_cast<LocalHiveConnectorMetadata*>(
                               ConnectorMetadata::metadata(connector()))
                               ->connectorQueryCtx();

  const auto maxRowsToScan = table().numRows() * (pct / 100);

  int64_t passingRows = 0;
  int64_t scannedRows = 0;
  for (const auto& file : files_) {
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
      empty,
      empty,
      std::vector<SortOrder>{},
      empty,
      empty,
      metadata.fileFormat(),
      std::nullopt);
  layout->setFiles(std::move(files));
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

std::shared_ptr<LocalTable> LocalHiveConnectorMetadata::createTableFromSchema(
    std::string_view name,
    std::string_view path) {
  auto jsons =
      axiom::readConcatenatedDynamicsFromFile(fmt::format("{}/.schema", path));
  if (jsons.empty()) {
    return nullptr;
  }
  VELOX_CHECK_EQ(jsons.size(), 1);
  auto json = jsons[0];

  velox::type::fbhive::HiveTypeParser parser;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  std::vector<std::unique_ptr<Column>> columns;
  for (auto column : json["dataColumns"]) {
    names.push_back(column["name"].asString());
    types.push_back(parser.parse(column["type"].asString()));
    columns.push_back(std::make_unique<Column>(names.back(), types.back()));
  }

  std::vector<const Column*> partition;
  for (auto column : json["partitionColumns"]) {
    names.push_back(column["name"].asString());
    types.push_back(parser.parse(column["type"].asString()));
    columns.push_back(std::make_unique<Column>(names.back(), types.back()));
    partition.push_back(columns.back().get());
  }

  folly::F14FastMap<std::string, std::string> options;
  if (json.count("compressionKind")) {
    options["compression_kind"] = json["compressionKind"].asString();
  }

  auto table = std::make_shared<LocalTable>(
      std::string{name},
      ROW(std::move(names), std::move(types)),
      std::move(options));
  tables_[name] = table;

  std::vector<const Column*> columnOrder;
  for (auto& column : columns) {
    columnOrder.push_back(column.get());
    auto& name = column->name();
    table->exportedColumns_[name] = column.get();
    table->columns_[name] = std::move(column);
  }

  std::vector<const Column*> bucket;
  std::vector<const Column*> order;
  std::vector<SortOrder> sortOrder;
  std::optional<int32_t> numBuckets = std::nullopt;
  if (json.count("bucketProperty")) {
    auto buckets = json["bucketProperty"];
    if (buckets.count("bucketedBy")) {
      for (const auto& name : buckets["bucketedBy"]) {
        auto column = table->findColumn(name.asString());
        VELOX_CHECK_NOT_NULL(
            column, "Bucketed-by column not found: {}", name.asString());
        bucket.push_back(column);
      }
      for (const auto& name : buckets["sortedBy"]) {
        auto column = table->findColumn(name.asString());
        VELOX_CHECK_NOT_NULL(
            column, "Sorted-by column not found: {}", name.asString());
        order.emplace_back(column);
        sortOrder.emplace_back(SortOrder{true, true}); // ASC NULLS FIRST.
      }
      numBuckets = atoi(buckets["bucketCount"].asString().c_str());
    }
  }

  auto format = format_;
  if (json.count("fileFormat")) {
    format = velox::dwio::common::toFileFormat(json["fileFormat"].asString());
  }

  std::vector<const Column*> empty;
  auto layout = std::make_unique<LocalHiveTableLayout>(
      table->name(),
      table.get(),
      hiveConnector(),
      columnOrder,
      bucket,
      order,
      sortOrder,
      empty,
      partition,
      format,
      numBuckets);
  table->exportedLayouts_.push_back(layout.get());
  table->layouts_.push_back(std::move(layout));
  return table;
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
    std::vector<std::unique_ptr<const FileInfo>>& result) {
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    // Ignore hidden files.
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }

    if (dirEntry.is_directory()) {
      listFiles(
          fmt::format("{}/{}", path, dirEntry.path().filename().c_str()),
          parseBucketNumber,
          prefixSize,
          result);
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
        file->partitionKeys[parts[0]] = parts[1];
      }
    }
    result.push_back(std::move(file));
  }
}
} // namespace

void LocalHiveConnectorMetadata::loadTable(
    std::string_view tableName,
    const fs::path& tablePath) {
  // open each file in the directory and check their type and add up the row
  // counts.
  auto table = createTableFromSchema(tableName, tablePath.native());

  velox::RowTypePtr tableType;
  if (table) {
    tableType = table->type();
  }

  std::function<int32_t(std::string_view)> parseBucketNumber = nullptr;
  if (table && !table->layouts()[0]->partitionColumns().empty()) {
    parseBucketNumber = extractDigitsAfterLastSlash;
  }

  std::vector<std::unique_ptr<const FileInfo>> files;
  std::string pathString = tablePath;
  listFiles(pathString, parseBucketNumber, pathString.size(), files);

  for (auto& info : files) {
    // If the table has a schema it has a layout that gives the file format.
    // Otherwise we default it from 'this'.
    velox::dwio::common::ReaderOptions readerOptions{schemaPool_.get()};
    readerOptions.setFileFormat(
        table == nullptr || table->layouts().empty()
            ? format_
            : reinterpret_cast<const HiveTableLayout*>(table->layouts()[0])
                  ->fileFormat());
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
      table->numRows_ += rows.value();
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
      }
    }
  }
  VELOX_CHECK_NOT_NULL(table, "Table directory {} is empty", tablePath);

  table->makeDefaultLayout(std::move(files), *this);
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
} // namespace

void LocalTable::sampleNumDistincts(
    float samplePct,
    velox::memory::MemoryPool* pool) {
  std::vector<velox::common::Subfield> fields;
  fields.reserve(type_->size());
  for (auto i = 0; i < type_->size(); ++i) {
    fields.push_back(velox::common::Subfield(type_->nameOf(i)));
  }

  // Sample the table. Adjust distinct values according to the samples.
  auto allocator = std::make_unique<velox::HashStringAllocator>(pool);
  auto* layout = layouts_[0].get();

  auto* metadata = ConnectorMetadata::metadata(layout->connector());

  std::vector<velox::connector::ColumnHandlePtr> columns;
  columns.reserve(type_->size());
  for (auto i = 0; i < type_->size(); ++i) {
    columns.push_back(metadata->createColumnHandle(*layout, type_->nameOf(i)));
  }

  auto* localHiveMetadata =
      dynamic_cast<const LocalHiveConnectorMetadata*>(metadata);
  auto& evaluator =
      *localHiveMetadata->connectorQueryCtx()->expressionEvaluator();

  std::vector<velox::core::TypedExprPtr> ignore;
  auto handle =
      metadata->createTableHandle(*layout, columns, evaluator, {}, ignore);

  auto* localLayout = dynamic_cast<LocalHiveTableLayout*>(layout);
  VELOX_CHECK_NOT_NULL(localLayout, "Expecting a local hive layout");

  std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
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
  std::lock_guard<std::mutex> l(mutex_);
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

// Helper: Recursively delete directory contents
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
        deleteDirectoryContents(fullPath);
        rmdir(fullPath.c_str());
      } else {
        unlink(fullPath.c_str());
      }
    }
  }
  closedir(dir);
}

// Helper: Check if directory exists.
bool dirExists(const std::string& path) {
  struct stat info;
  return stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
}

// Helper: Create directory (recursively)
void createDir(const std::string& path) {
  if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    throw std::runtime_error("Failed to create directory: " + path);
  }
}
} // namespace

void LocalHiveConnectorMetadata::createTable(
    const std::string& tableName,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::string>& options,
    const ConnectorSessionPtr& session,
    bool errorIfExists,
    TableKind kind) {
  VELOX_CHECK_EQ(kind, TableKind::kTable);
  validateOptions(options);
  ensureInitialized();
  auto path = fmt::format("{}/{}", dataPath(), tableName);
  if (dirExists(path)) {
    if (errorIfExists) {
      VELOX_USER_FAIL("Table {} already exists", tableName);
    } else {
      return;
    }
  } else {
    createDir(path);
  }

  folly::dynamic schema = folly::dynamic::object;

  auto it = options.find("compression_kind");
  if (it != options.end()) {
    //  Check the kind is recognized.
    velox::common::stringToCompressionKind(it->second);
    schema["compressionKind"] = it->second;
  }
  it = options.find("file_format");
  std::string fileFormat;
  if (it != options.end()) {
    VELOX_USER_CHECK(
        velox::dwio::common::toFileFormat(it->second) !=
            velox::dwio::common::FileFormat::UNKNOWN,
        "Bad file format {}",
        it->second);
    fileFormat = it->second;
  } else {
    fileFormat = toString(format_);
  }
  schema["fileFormat"] = fileFormat;
  folly::dynamic buckets = folly::dynamic::object;
  it = options.find("bucketed_by");
  if (it != options.end()) {
    folly::dynamic columns = folly::dynamic::array;
    std::vector<std::string> tokens;
    folly::split(",", it->second, tokens);
    for (auto& token : tokens) {
      token = folly::trimWhitespace(token);
      columns.push_back(token);
    }
    it = options.find("bucket_count");
    VELOX_USER_CHECK(
        it != options.end(),
        "bucket_count is required if bucketed_by is specified");
    auto numBuckets = atoi(it->second.c_str());
    VELOX_USER_CHECK_GT(numBuckets, 1);
    buckets["bucketCount"] = fmt::format("{}", numBuckets);
    buckets["bucketedBy"] = columns;
    folly::dynamic sorted = folly::dynamic::array;
    it = options.find("sorted_by");
    if (it != options.end()) {
      tokens.clear();
      folly::split(",", it->second, tokens);
      for (auto& token : tokens) {
        token = folly::trimWhitespace(token);
        sorted.push_back(token);
      }
    }
    buckets["sortedBy"] = sorted;
  }
  schema["bucketProperty"] = buckets;
  folly::dynamic dataColumns = folly::dynamic::array;
  folly::dynamic hivePartitionColumns = folly::dynamic::array;
  it = options.find("partitioned_by");
  std::vector<std::string> tokens;
  folly::split(",", it->second, tokens);
  for (auto& token : tokens) {
    token = folly::trimWhitespace(token);
  }

  bool isPartition = false;
  for (auto i = 0; i < rowType->size(); ++i) {
    auto& name = rowType->nameOf(i);
    folly::dynamic c = folly::dynamic::object();
    c["name"] = name;
    c["type"] =
        velox::type::fbhive::HiveTypeSerializer::serialize(rowType->childAt(i));

    if (std::find(tokens.begin(), tokens.end(), name) == tokens.end()) {
      if (isPartition) {
        VELOX_USER_FAIL("Partitioning columns must be last");
      }
      dataColumns.push_back(c);
    } else {
      hivePartitionColumns.push_back(c);
      isPartition = true;
    }
  }
  schema["dataColumns"] = dataColumns;
  schema["partitionColumns"] = hivePartitionColumns;
  std::string jsonStr = folly::toPrettyJson(schema);
  std::string filePath = path + "/.schema";

  std::lock_guard<std::mutex> l(mutex_);
  folly::writeFileAtomic(filePath, jsonStr.data(), jsonStr.size());
  tables_.erase(tableName);
  loadTable(tableName, path);
}

void LocalHiveConnectorMetadata::finishWrite(
    const TableLayout& layout,
    const velox::connector::ConnectorInsertTableHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& /*writerResult*/,
    WriteKind /*kind*/,
    const ConnectorSessionPtr& /*session*/) {
  std::lock_guard<std::mutex> l(mutex_);
  auto localHandle =
      dynamic_cast<const velox::connector::hive::HiveInsertTableHandle*>(
          handle.get());
  loadTable(layout.table().name(), localHandle->locationHandle()->targetPath());
}

} // namespace facebook::axiom::connector::hive
