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

#include "axiom/connectors/tests/TestTableJson.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <folly/String.h>
#include <folly/json.h>
#include <re2/re2.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "velox/functions/prestosql/types/parser/TypeParser.h"

namespace facebook::axiom::connector {
namespace {

// Converts a JSON scalar to a Variant matching 'type'. Returns nullopt for
// types without a natural scalar min/max (e.g. complex types).
std::optional<velox::Variant> toVariant(
    const velox::TypePtr& type,
    const folly::dynamic& value) {
  switch (type->kind()) {
    case velox::TypeKind::TINYINT:
      return velox::Variant(static_cast<int8_t>(value.asInt()));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(static_cast<int16_t>(value.asInt()));
    case velox::TypeKind::INTEGER:
      return velox::Variant(static_cast<int32_t>(value.asInt()));
    case velox::TypeKind::BIGINT:
      return velox::Variant(static_cast<int64_t>(value.asInt()));
    case velox::TypeKind::REAL:
      return velox::Variant(static_cast<float>(value.asDouble()));
    case velox::TypeKind::DOUBLE:
      return velox::Variant(value.asDouble());
    case velox::TypeKind::VARCHAR:
      return velox::Variant(value.asString());
    case velox::TypeKind::BOOLEAN:
      return velox::Variant(value.asBool());
    default:
      return std::nullopt;
  }
}

ColumnStatistics parseColumnStatistics(
    const velox::TypePtr& type,
    const folly::dynamic& column,
    uint64_t numRows) {
  // ColumnStatistics::name stays empty for a top-level column; it names a
  // subfield only for struct/map children.
  ColumnStatistics stats;

  // A missing nullFraction means unknown: leave the null-related fields at
  // their defaults rather than asserting the column has no nulls.
  if (auto* nullFraction = column.get_ptr("nullFraction")) {
    const double fraction = nullFraction->asDouble();
    stats.nullPct = static_cast<float>(fraction * 100.0);
    stats.nonNull = fraction == 0.0;
    stats.numValues =
        static_cast<int64_t>(static_cast<double>(numRows) * (1.0 - fraction));
  }

  if (auto* numDistinct = column.get_ptr("numDistinct")) {
    // The metastore reports NDV per partition, which can exceed a partition's
    // row count; cap it so setStats' numDistinct <= numRows invariant holds.
    stats.numDistinct =
        std::min<int64_t>(numDistinct->asInt(), static_cast<int64_t>(numRows));
  }
  if (auto* min = column.get_ptr("min")) {
    stats.min = toVariant(type, *min);
  }
  if (auto* max = column.get_ptr("max")) {
    stats.max = toVariant(type, *max);
  }
  if (auto* maxLength = column.get_ptr("maxLength")) {
    stats.maxLength = static_cast<int32_t>(maxLength->asInt());
  }
  return stats;
}

// Translates a shell-style glob (supporting '*' and '?') over a single file
// name into an RE2 pattern. RE2::FullMatch anchors the match, so no surrounding
// ^...$ is needed.
std::string globToRe2Pattern(const std::string& glob) {
  std::string pattern;
  pattern.reserve(glob.size() * 2);
  for (const char c : glob) {
    switch (c) {
      case '*':
        pattern += ".*";
        break;
      case '?':
        pattern += '.';
        break;
      default:
        if (std::strchr(".^$+{}[]()|\\", c) != nullptr) {
          pattern += '\\';
        }
        pattern += c;
    }
  }
  return pattern;
}

// Resolves 'entry' against 'baseDir' and expands it. A plain path returns
// itself (existence is checked when the file is opened); a glob returns the
// sorted set of matching files in its directory.
std::vector<std::string> expandEntry(
    const std::string& entry,
    const std::string& baseDir) {
  std::filesystem::path path(entry);
  if (path.is_relative() && !baseDir.empty()) {
    path = std::filesystem::path(baseDir) / path;
  }

  const std::string name = path.filename().string();
  if (name.find('*') == std::string::npos &&
      name.find('?') == std::string::npos) {
    return {path.string()};
  }

  const auto directory = path.parent_path();
  if (!std::filesystem::is_directory(directory)) {
    return {};
  }
  const re2::RE2 regex(globToRe2Pattern(name));
  std::vector<std::string> matches;
  for (const auto& dirEntry : std::filesystem::directory_iterator(directory)) {
    if (dirEntry.is_regular_file() &&
        re2::RE2::FullMatch(dirEntry.path().filename().string(), regex)) {
      matches.push_back(dirEntry.path().string());
    }
  }
  std::sort(matches.begin(), matches.end());
  return matches;
}

} // namespace

void TestTableJson::load(TestConnector& connector, std::string_view json) {
  const folly::dynamic table = folly::parseJson(json);
  const auto name = table["name"].asString();
  const auto numRows = static_cast<uint64_t>(table["numRows"].asInt());

  const auto& columns = table["columns"];
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  std::unordered_map<std::string, ColumnStatistics> columnStats;
  for (const auto& column : columns) {
    auto columnName = column["name"].asString();
    auto type =
        velox::functions::prestosql::parseType(column["type"].asString());
    columnStats.emplace(
        columnName, parseColumnStatistics(type, column, numRows));
    names.push_back(std::move(columnName));
    types.push_back(std::move(type));
  }

  connector.addTable(name, velox::ROW(std::move(names), std::move(types)));
  connector.setStats(name, numRows, columnStats);
}

void TestTableJson::loadFile(
    TestConnector& connector,
    const std::string& path) {
  std::ifstream input(path);
  VELOX_USER_CHECK(input, "Failed to open table JSON file: {}", path);
  std::stringstream buffer;
  buffer << input.rdbuf();
  load(connector, buffer.str());
}

void TestTableJson::loadFromConfig(
    TestConnector& connector,
    const velox::config::ConfigBase& config) {
  const auto tables = config.get<std::string>(std::string(kTables));
  if (!tables.has_value() || tables->empty()) {
    return;
  }
  const auto baseDir =
      config.get<std::string>(std::string(kConfigDir)).value_or("");

  std::vector<std::string> entries;
  folly::split(',', tables.value(), entries);
  for (const auto& entry : entries) {
    const auto trimmed = folly::trimWhitespace(entry);
    if (trimmed.empty()) {
      continue;
    }
    for (const auto& path : expandEntry(std::string(trimmed), baseDir)) {
      loadFile(connector, path);
    }
  }
}

} // namespace facebook::axiom::connector
