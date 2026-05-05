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

#include "axiom/connectors/hive/LocalTableBuilder.h"

#include <fstream>

#include <folly/dynamic.h>
#include <folly/json.h>

#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "axiom/connectors/hive/StatisticsBuilder.h"
#include "axiom/optimizer/JsonUtil.h"
#include "velox/common/base/Fs.h"
#include "velox/common/config/Config.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/Statistics.h"

namespace facebook::axiom::connector::hive {

namespace {

std::string statsPath(const std::string& path) {
  return fmt::format("{}/.stats", path);
}

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

folly::dynamic columnStatsToJson(const ColumnStatistics& stats) {
  folly::dynamic json = folly::dynamic::object;
  json["name"] = stats.name;
  json["numValues"] = stats.numValues;
  json["nullPct"] = stats.nullPct;
  if (stats.min.has_value()) {
    json["min"] = stats.min->serialize();
  }
  if (stats.max.has_value()) {
    json["max"] = stats.max->serialize();
  }
  if (stats.numDistinct.has_value()) {
    json["numDistinct"] = stats.numDistinct.value();
  }
  if (stats.maxLength.has_value()) {
    json["maxLength"] = stats.maxLength.value();
  }
  if (stats.avgLength.has_value()) {
    json["avgLength"] = stats.avgLength.value();
  }
  return json;
}

folly::dynamic persistedStatsToJson(const PersistedStats& stats) {
  folly::dynamic columns = folly::dynamic::array;
  for (const auto& colStats : stats.columns) {
    columns.push_back(columnStatsToJson(colStats));
  }
  folly::dynamic json = folly::dynamic::object;
  json["numRows"] = stats.numRows;
  json["columns"] = std::move(columns);
  return json;
}

// Creates an integer Variant with the TypeKind matching the column type.
// IntegerColumnStatistics::getMinimum/getMaximum return int64_t regardless of
// the actual integer width. Constructing Variant(int64_t) always produces
// BIGINT, which causes a type mismatch when the column is TINYINT, SMALLINT, or
// INTEGER. This helper narrows the value to the correct type.
velox::Variant makeIntegerVariant(velox::TypeKind kind, int64_t value) {
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return velox::Variant(static_cast<int8_t>(value));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(static_cast<int16_t>(value));
    case velox::TypeKind::INTEGER:
      return velox::Variant(static_cast<int32_t>(value));
    default:
      return velox::Variant(value);
  }
}

// Reads per-column min/max/count from DWIO reader and populates
// fileInfo->columnStats.
void populateFileColumnStats(
    const velox::dwio::common::Reader& reader,
    const velox::RowType& fileType,
    FileInfo* fileInfo,
    std::optional<uint64_t> numRows) {
  for (auto i = 0; i < fileType.size(); ++i) {
    const auto& name = fileType.nameOf(i);
    const auto& columnType = fileType.childAt(i);

    const auto& typeWithId = reader.typeWithId()->childByName(name);
    auto readerStats = reader.columnStatistics(typeWithId->id());
    if (!readerStats) {
      continue;
    }

    const auto numValues = readerStats->getNumberOfValues();

    ColumnStatistics fileColStats;
    fileColStats.name = name;
    fileColStats.numValues = numValues.value_or(0);

    if (numRows.has_value() && numRows.value() > 0 && numValues.has_value()) {
      fileColStats.nullPct =
          100 * (numRows.value() - numValues.value()) / numRows.value();
    }

    auto columnKind = columnType->kind();

    if (auto* intStats =
            dynamic_cast<const velox::dwio::common::IntegerColumnStatistics*>(
                readerStats.get())) {
      if (intStats->getMinimum().has_value()) {
        fileColStats.min =
            makeIntegerVariant(columnKind, intStats->getMinimum().value());
      }
      if (intStats->getMaximum().has_value()) {
        fileColStats.max =
            makeIntegerVariant(columnKind, intStats->getMaximum().value());
      }
    } else if (
        auto* dblStats =
            dynamic_cast<const velox::dwio::common::DoubleColumnStatistics*>(
                readerStats.get())) {
      if (dblStats->getMinimum().has_value()) {
        fileColStats.min = velox::Variant(dblStats->getMinimum().value());
      }
      if (dblStats->getMaximum().has_value()) {
        fileColStats.max = velox::Variant(dblStats->getMaximum().value());
      }
    } else if (
        auto* strStats =
            dynamic_cast<const velox::dwio::common::StringColumnStatistics*>(
                readerStats.get())) {
      if (strStats->getMinimum().has_value()) {
        fileColStats.min = velox::Variant::create<velox::TypeKind::VARCHAR>(
            strStats->getMinimum().value());
      }
      if (strStats->getMaximum().has_value()) {
        fileColStats.max = velox::Variant::create<velox::TypeKind::VARCHAR>(
            strStats->getMaximum().value());
      }
    }

    fileInfo->columnStats[name] = std::move(fileColStats);
  }
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

bool isMixedOrder(const ColumnStatistics& stats) {
  return stats.ascendingPct.has_value() && stats.descendingPct.has_value() &&
      stats.ascendingPct.value() > 0 && stats.descendingPct.value() > 0;
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

LocalTableBuilder::LocalTableBuilder(
    velox::memory::MemoryPool* pool,
    velox::dwio::common::FileFormat fileFormat,
    velox::connector::hive::HiveConnector* connector,
    float samplePct)
    : pool_(pool),
      fileFormat_(fileFormat),
      connector_(connector),
      samplePct_(samplePct) {}

void LocalTableBuilder::build(const std::string& tablePath) {
  velox::RowTypePtr tableType;

  // Check if .schema already exists and read it.
  const auto schemaFile = schemaPath(tablePath);
  const bool hasSchema = std::filesystem::exists(schemaFile);
  if (hasSchema) {
    auto dynamics = readConcatenatedDynamicsFromFile(schemaFile);
    VELOX_CHECK(!dynamics.empty(), "Empty schema file: {}", schemaFile);
    tableType = parseSchema(dynamics[0]);
  }

  // List all data files.
  std::vector<std::unique_ptr<const FileInfo>> files;
  FileInfo::listFiles(
      tablePath, /*parseBucketNumber=*/nullptr, tablePath.size(), files);
  VELOX_CHECK(!files.empty(), "Table directory is empty: {}", tablePath);

  uint64_t totalRows{0};

  // Aggregate per-column stats across all files, keyed by column name.
  folly::F14FastMap<std::string, ColumnStatistics> aggregatedStats;

  for (auto& info : files) {
    velox::io::IoStatistics dataIoStats;
    velox::io::IoStatistics metadataIoStats;
    velox::dwio::common::ReaderOptions readerOptions{
        pool_, &dataIoStats, &metadataIoStats};
    readerOptions.setFileFormat(fileFormat_);

    if (fileFormat_ == velox::dwio::common::FileFormat::TEXT && tableType) {
      readerOptions.setFileSchema(tableType);
    }

    auto input = std::make_unique<velox::dwio::common::BufferedInput>(
        std::make_shared<velox::LocalReadFile>(info->path),
        readerOptions.memoryPool());
    std::unique_ptr<velox::dwio::common::Reader> reader =
        velox::dwio::common::getReaderFactory(readerOptions.fileFormat())
            ->createReader(std::move(input), readerOptions);

    const auto& fileType = reader->rowType();

    // Merge schema: take the widest (most columns).
    if (!tableType) {
      tableType = fileType;
    } else if (fileType->size() > tableType->size()) {
      tableType = fileType;
    }

    const auto rows = reader->numberOfRows();
    if (rows.has_value()) {
      totalRows += rows.value();
    }

    auto* mutableInfo = const_cast<FileInfo*>(info.get());
    mutableInfo->numRows = rows;

    populateFileColumnStats(*reader, *fileType, mutableInfo, rows);

    // Merge file-level column stats into aggregated stats.
    for (const auto& [name, colStats] : mutableInfo->columnStats) {
      auto it = aggregatedStats.find(name);
      if (it == aggregatedStats.end()) {
        aggregatedStats[name] = colStats;
      } else {
        mergeColumnStatsValues(it->second, colStats);
      }
    }
  }

  // Write .schema if it doesn't exist.
  if (!hasSchema) {
    writeSchemaFile(tablePath, tableType, fileFormat_);
  }

  // Build ordered column stats vector matching the schema column order.
  std::vector<ColumnStatistics> orderedStats;
  orderedStats.reserve(tableType->size());
  for (auto i = 0; i < tableType->size(); ++i) {
    const auto& name = tableType->nameOf(i);
    auto it = aggregatedStats.find(name);
    if (it != aggregatedStats.end()) {
      orderedStats.push_back(it->second);
    } else {
      ColumnStatistics empty;
      empty.name = name;
      orderedStats.push_back(empty);
    }
  }

  // Remove any existing .stats file and write fresh stats.
  const auto statsFile = statsPath(tablePath);
  if (std::filesystem::exists(statsFile)) {
    std::filesystem::remove(statsFile);
  }
  PersistedStats::write(tablePath, {totalRows, std::move(orderedStats)});

  // Sample for NDV estimates if configured.
  if (connector_ != nullptr) {
    sampleNumDistincts(tablePath, tableType, totalRows);
  }
}

void LocalTableBuilder::sampleNumDistincts(
    const std::string& tablePath,
    const velox::RowTypePtr& schema,
    uint64_t totalRows) {
  if (totalRows == 0) {
    return;
  }

  float pct = samplePct_;
  if (totalRows > 1'000'000) {
    pct = std::min(pct, 100.0f * 100'000 / totalRows);
  }

  // Build column handles for all columns.
  velox::connector::ColumnHandleMap columnHandles;
  for (auto i = 0; i < schema->size(); ++i) {
    const auto& name = schema->nameOf(i);
    const auto& type = schema->childAt(i);
    columnHandles[name] =
        std::make_shared<velox::connector::hive::HiveColumnHandle>(
            name,
            velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
            type,
            type);
  }

  // Create a table handle with no filters.
  auto tableHandle = std::make_shared<velox::connector::hive::HiveTableHandle>(
      connector_->connectorId(),
      "local_table_builder",
      velox::common::SubfieldFilters{},
      /*remainingFilter=*/nullptr);

  // Create a ConnectorQueryCtx for sampling.
  velox::common::SpillConfig spillConfig;
  velox::common::PrefixSortConfig prefixSortConfig;
  // Use an empty config for session properties during sampling.
  auto sessionProperties = std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  auto connectorQueryCtx =
      std::make_shared<velox::connector::ConnectorQueryCtx>(
          pool_,
          pool_,
          sessionProperties.get(),
          &spillConfig,
          prefixSortConfig,
          /*expressionEvaluator=*/nullptr,
          /*cache=*/nullptr,
          "local_table_builder",
          "local_table_builder",
          "N/a",
          0,
          "");

  // Create statistics builders for each column.
  auto allocator = std::make_unique<velox::HashStringAllocator>(pool_);
  StatisticsBuilderOptions builderOptions = {
      .maxStringLength = 100,
      .countDistincts = true,
      .allocator = allocator.get()};

  std::vector<std::unique_ptr<connector::StatisticsBuilder>> builders;
  builders.reserve(schema->size());
  for (auto i = 0; i < schema->size(); ++i) {
    builders.push_back(
        connector::StatisticsBuilder::create(
            schema->childAt(i), builderOptions));
  }

  // List files and scan them.
  std::vector<std::unique_ptr<const FileInfo>> files;
  FileInfo::listFiles(
      tablePath, /*parseBucketNumber=*/nullptr, tablePath.size(), files);

  const auto maxRowsToScan = static_cast<int64_t>(totalRows * (pct / 100));
  int64_t scannedRows{0};
  int64_t numSampledRows{0};

  for (const auto& file : files) {
    auto dataSource = connector_->createDataSource(
        schema, tableHandle, columnHandles, connectorQueryCtx.get());

    auto split = velox::connector::hive::HiveConnectorSplitBuilder(file->path)
                     .fileFormat(fileFormat_)
                     .connectorId(connector_->connectorId())
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

      numSampledRows += data->size();
      connector::StatisticsBuilder::updateBuilders(data, builders);

      if (scannedRows + dataSource->getCompletedRows() > maxRowsToScan) {
        scannedRows += dataSource->getCompletedRows();
        break;
      }
    }
  }

  // Read existing stats, add NDV estimates, and write back.
  auto existingStats = PersistedStats::read(tablePath);
  VELOX_CHECK(existingStats.has_value(), "Stats file not found: {}", tablePath);

  for (size_t i = 0; i < builders.size(); ++i) {
    if (!builders[i]) {
      continue;
    }

    ColumnStatistics builderStats;
    builders[i]->build(builderStats);

    auto estimate = builderStats.numDistinct;
    int64_t approxNumDistinct =
        estimate.has_value() ? estimate.value() : totalRows;

    // For tiny tables the sample is 100% and the approxNumDistinct is
    // accurate. For partial samples, the distinct estimate is left to be the
    // distinct estimate of the sample if there are few distincts. This is an
    // enumeration where values in unsampled rows are likely the same. If
    // there are many distincts, we multiply by 1/sample rate assuming that
    // unsampled rows will mostly have new values.
    if (numSampledRows < static_cast<int64_t>(totalRows)) {
      if (approxNumDistinct > numSampledRows / 50) {
        float numDups = numSampledRows / static_cast<float>(approxNumDistinct);
        approxNumDistinct = std::min<float>(totalRows, totalRows / numDups);

        // If the type is an integer type, num distincts cannot be larger than
        // max - min.
        if (isInteger(builders[i]->type()->kind())) {
          auto min = builderStats.min;
          auto max = builderStats.max;
          if (min.has_value() && max.has_value() &&
              isMixedOrder(builderStats)) {
            auto range = numericValue<float>(max.value()) -
                numericValue<float>(min.value());
            approxNumDistinct = std::min<float>(approxNumDistinct, range);
          }
        }
      }
    }

    // Find matching column in persisted stats and update numDistinct.
    const auto& name = schema->nameOf(i);
    for (auto& colStats : existingStats->columns) {
      if (colStats.name == name) {
        colStats.numDistinct = approxNumDistinct;
        break;
      }
    }
  }

  // Write back the updated stats (overwrite, not merge).
  const auto statsFile = statsPath(tablePath);
  std::ofstream outputFile(statsFile);
  VELOX_CHECK(outputFile.is_open(), "Failed to open stats file: {}", statsFile);
  outputFile << folly::toPrettyJson(persistedStatsToJson(*existingStats));
  outputFile.close();
  VELOX_CHECK(!outputFile.fail(), "Failed to write stats file: {}", statsFile);
}

} // namespace facebook::axiom::connector::hive
