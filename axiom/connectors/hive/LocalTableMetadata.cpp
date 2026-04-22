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

} // namespace

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

void PersistedStats::write(const std::string& directory, PersistedStats stats) {
  auto existing = PersistedStats::read(directory);
  if (existing.has_value()) {
    stats.numRows += existing->numRows;
    mergeColumnStats(existing->columns, stats.columns);
    stats.columns = std::move(existing->columns);
  }

  const auto file = statsPath(directory);
  std::ofstream outputFile(file);
  VELOX_CHECK(outputFile.is_open(), "Failed to open stats file: {}", file);
  outputFile << folly::toPrettyJson(persistedStatsToJson(stats));
  outputFile.close();
  VELOX_CHECK(!outputFile.fail(), "Failed to write stats file: {}", file);
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

void writeSchemaFile(
    const std::string& tablePath,
    const velox::RowTypePtr& rowType,
    velox::dwio::common::FileFormat fileFormat) {
  folly::dynamic dataColumns = folly::dynamic::array();
  for (auto i = 0; i < rowType->size(); ++i) {
    folly::dynamic col = folly::dynamic::object();
    col["name"] = rowType->nameOf(i);
    col["type"] = rowType->childAt(i)->serialize();
    dataColumns.push_back(col);
  }

  folly::dynamic schema = folly::dynamic::object;
  schema["dataColumns"] = dataColumns;
  schema["partitionColumns"] = folly::dynamic::array();
  schema["fileFormat"] = std::string(velox::dwio::common::toString(fileFormat));

  const auto file = schemaPath(tablePath);
  std::ofstream outputFile(file);
  VELOX_CHECK(outputFile.is_open(), "Failed to open schema file: {}", file);
  outputFile << folly::toPrettyJson(schema);
  outputFile.close();
  VELOX_CHECK(!outputFile.fail(), "Failed to write schema file: {}", file);
}

} // namespace facebook::axiom::connector::hive
