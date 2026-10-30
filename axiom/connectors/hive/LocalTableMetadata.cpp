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

#include "axiom/connectors/hive/LocalTableMetadata.h"

#include <algorithm>
#include <cmath>
#include <fstream>

#include <folly/Conv.h>
#include <folly/json.h>

#include "velox/common/base/Fs.h"

namespace facebook::axiom::connector::hive {

std::string schemaPath(std::string_view path) {
  return fmt::format("{}/.schema", path);
}

namespace {

std::string statsPath(const std::string& path) {
  return fmt::format("{}/.stats", path);
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
  if (stats.avgSizeInBytes.has_value()) {
    json["avgSizeInBytes"] = stats.avgSizeInBytes.value();
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

ColumnStatistics columnStatsFromJson(const folly::dynamic& json) {
  ColumnStatistics stats;
  stats.name = json["name"].asString();
  stats.numValues = json["numValues"].asInt();
  stats.nullPct = json["nullPct"].asDouble();
  if (json.count("min")) {
    stats.min = velox::Variant::create(json["min"]);
  }
  if (json.count("max")) {
    stats.max = velox::Variant::create(json["max"]);
  }
  if (json.count("numDistinct")) {
    stats.numDistinct = json["numDistinct"].asInt();
  }
  if (json.count("maxLength")) {
    stats.maxLength = json["maxLength"].asInt();
  }
  if (json.count("avgLength")) {
    stats.avgLength = json["avgLength"].asInt();
  }
  if (json.count("avgSizeInBytes")) {
    stats.avgSizeInBytes = json["avgSizeInBytes"].asInt();
  }
  return stats;
}

PersistedStats persistedStatsFromJson(const folly::dynamic& json) {
  PersistedStats result;
  result.numRows = json["numRows"].asInt();
  for (const auto& colJson : json["columns"]) {
    result.columns.push_back(columnStatsFromJson(colJson));
  }
  return result;
}

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

// Returns the total size in bytes of a column over 'numRows' rows, or
// std::nullopt when unknown. A side with no rows has size 0, and so does a
// side that lacks the column (null 'stats'), since its values are all null.
std::optional<double> totalSizeInBytes(
    const ColumnStatistics* stats,
    uint64_t numRows) {
  if (stats == nullptr || numRows == 0) {
    return 0;
  }
  if (!stats->avgSizeInBytes.has_value()) {
    return std::nullopt;
  }
  return static_cast<double>(*stats->avgSizeInBytes) * numRows;
}

// Averages the avgSizeInBytes of two sides weighted by row count. A null side
// does not have the column. The result is unknown unless at least one side
// measured the size.
std::optional<int32_t> mergeAvgSizeInBytes(
    const ColumnStatistics* lhs,
    uint64_t lhsNumRows,
    const ColumnStatistics* rhs,
    uint64_t rhsNumRows) {
  const auto measured = [](const ColumnStatistics* stats) {
    return stats != nullptr && stats->avgSizeInBytes.has_value();
  };
  if (!measured(lhs) && !measured(rhs)) {
    return std::nullopt;
  }
  const std::optional<double> lhsSize = totalSizeInBytes(lhs, lhsNumRows);
  const std::optional<double> rhsSize = totalSizeInBytes(rhs, rhsNumRows);
  const auto numRows = lhsNumRows + rhsNumRows;
  if (!lhsSize.has_value() || !rhsSize.has_value() || numRows == 0) {
    return std::nullopt;
  }
  return static_cast<int32_t>(
      std::llround((*lhsSize + *rhsSize) / static_cast<double>(numRows)));
}

} // namespace

void PersistedStats::merge(
    uint64_t otherNumRows,
    const std::vector<ColumnStatistics>& otherColumns) {
  folly::F14FastMap<std::string_view, const ColumnStatistics*> unmatched;
  for (const auto& column : otherColumns) {
    unmatched[column.name] = &column;
  }

  for (auto& column : columns) {
    const auto it = unmatched.find(column.name);
    const auto* otherColumn = it == unmatched.end() ? nullptr : it->second;
    const auto avgSizeInBytes =
        mergeAvgSizeInBytes(&column, numRows, otherColumn, otherNumRows);
    if (otherColumn != nullptr) {
      mergeColumnStatsValues(column, *otherColumn);
      unmatched.erase(it);
    }
    column.avgSizeInBytes = avgSizeInBytes;
  }

  for (const auto& otherColumn : otherColumns) {
    if (!unmatched.contains(otherColumn.name)) {
      continue;
    }
    auto& column = columns.emplace_back(otherColumn);
    column.avgSizeInBytes =
        mergeAvgSizeInBytes(nullptr, numRows, &otherColumn, otherNumRows);
  }

  numRows += otherNumRows;
}

std::optional<PersistedStats> PersistedStats::read(
    const std::string& directory) {
  const auto file = statsPath(directory);
  if (!std::filesystem::exists(file)) {
    return std::nullopt;
  }
  std::ifstream inputFile(file);
  if (!inputFile.is_open()) {
    return std::nullopt;
  }
  std::string content(
      (std::istreambuf_iterator<char>(inputFile)),
      std::istreambuf_iterator<char>());
  try {
    return persistedStatsFromJson(folly::parseJson(content));
  } catch (const folly::json::parse_error& e) {
    VELOX_FAIL("Failed to parse persisted stats JSON: {}", e.what());
  }
}

namespace {

// Writes 'stats' to the .stats file in 'directory', replacing its contents.
void writeStatsFile(const std::string& directory, const PersistedStats& stats) {
  const auto file = statsPath(directory);
  std::ofstream outputFile(file);
  VELOX_CHECK(outputFile.is_open(), "Failed to open stats file: {}", file);
  outputFile << folly::toPrettyJson(persistedStatsToJson(stats));
  outputFile.close();
  VELOX_CHECK(!outputFile.fail(), "Failed to write stats file: {}", file);
}

} // namespace

void PersistedStats::write(const std::string& directory, PersistedStats stats) {
  auto existing = PersistedStats::read(directory);
  if (existing.has_value()) {
    existing->merge(stats.numRows, stats.columns);
    stats = std::move(existing.value());
  }
  writeStatsFile(directory, stats);
}

void PersistedStats::replace(
    const std::string& directory,
    uint64_t numRows,
    std::vector<ColumnStatistics> columns) {
  auto stats = PersistedStats::read(directory).value_or(PersistedStats{});
  if (stats.numRows != numRows) {
    for (auto& column : stats.columns) {
      column.numValues = stats.numRows == 0
          ? 0
          : std::llround(
                static_cast<double>(column.numValues) * numRows /
                stats.numRows);
      if (column.numDistinct.has_value()) {
        column.numDistinct = std::min(*column.numDistinct, column.numValues);
      }
    }
  }
  stats.numRows = numRows;
  for (auto& column : columns) {
    auto it = std::find_if(
        stats.columns.begin(), stats.columns.end(), [&](const auto& existing) {
          return existing.name == column.name;
        });
    if (it == stats.columns.end()) {
      stats.columns.push_back(std::move(column));
    } else {
      *it = std::move(column);
    }
  }
  writeStatsFile(directory, stats);
}

void FileInfo::listFiles(
    std::string_view path,
    const std::function<int32_t(std::string_view)>& parseBucketNumber,
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
        file->partitionKeys[parts.at(0)] = parts.at(1);
      }
    }
    result.push_back(std::move(file));
  }
}

folly::dynamic columnToJson(std::string_view name, const velox::TypePtr& type) {
  folly::dynamic column = folly::dynamic::object;
  column["name"] = std::string(name);
  column["type"] = type->serialize();
  return column;
}

void writeSchemaFile(
    const std::string& tablePath,
    const velox::RowTypePtr& rowType,
    velox::dwio::common::FileFormat fileFormat) {
  folly::dynamic dataColumns = folly::dynamic::array();
  for (auto i = 0; i < rowType->size(); ++i) {
    dataColumns.push_back(
        columnToJson(rowType->nameOf(i), rowType->childAt(i)));
  }

  folly::dynamic schema = folly::dynamic::object;
  schema["dataColumns"] = dataColumns;
  schema["partitionColumns"] = folly::dynamic::array();
  schema["fileFormat"] =
      std::string(velox::dwio::common::FileFormatName::toName(fileFormat));

  const auto file = schemaPath(tablePath);
  std::ofstream outputFile(file);
  VELOX_CHECK(outputFile.is_open(), "Failed to open schema file: {}", file);
  outputFile << folly::toPrettyJson(schema);
  outputFile.close();
  VELOX_CHECK(!outputFile.fail(), "Failed to write schema file: {}", file);
}

} // namespace facebook::axiom::connector::hive
