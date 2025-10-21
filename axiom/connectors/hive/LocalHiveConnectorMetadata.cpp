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
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& tableHandle) {
  // All tables are unpartitioned.
  folly::F14FastMap<std::string, std::optional<std::string>> empty;
  return {std::make_shared<HivePartitionHandle>(empty, std::nullopt)};
}

std::shared_ptr<SplitSource> LocalHiveSplitManager::getSplitSource(
    const ConnectorSessionPtr& session,
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

struct CreateTableOptions {
  std::optional<velox::common::CompressionKind> compressionKind;
  std::optional<velox::dwio::common::FileFormat> fileFormat;

  std::vector<std::string> partitionedByColumns;

  std::optional<int32_t> numBuckets;
  std::vector<std::string> bucketedByColumns;
  std::vector<std::string> sortedByColumns;
};

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

    const auto numBuckets = it->second.value<int64_t>();
    VELOX_USER_CHECK_GT(numBuckets, 0, "bucket_count must be > 0");
    VELOX_USER_CHECK_EQ(
        numBuckets & (numBuckets - 1), 0, "bucket_count must be power of 2");

    result.numBuckets = numBuckets;

    it = options.find("sorted_by");
    if (it != options.end()) {
      result.sortedByColumns = it->second.array<std::string>();
    }
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
      createTableOptions.fileFormat.value());
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
      table->incrementNumRows(rows.value());
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
    columns.push_back(metadata->createColumnHandle(
        /*session=*/nullptr, *layout, type_->nameOf(i)));
  }

  auto* localHiveMetadata =
      dynamic_cast<const LocalHiveConnectorMetadata*>(metadata);
  auto& evaluator =
      *localHiveMetadata->connectorQueryCtx()->expressionEvaluator();

  std::vector<velox::core::TypedExprPtr> ignore;
  auto handle = metadata->createTableHandle(
      /*session=*/nullptr, *layout, columns, evaluator, {}, ignore);

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

  std::lock_guard<std::mutex> l(mutex_);
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
  loadTable(hiveHandle->table()->name(), targetPath);
  return rows;
}

velox::ContinueFuture LocalHiveConnectorMetadata::abortWrite(
    const ConnectorSessionPtr& session,
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
  std::lock_guard<std::mutex> l(mutex_);
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
