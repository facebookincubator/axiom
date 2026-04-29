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

#include "axiom/connectors/ducklake/DuckLakeCatalogClient.h"

#include <duckdb.hpp>

#include <cctype>
#include <filesystem>
#include <memory>
#include <mutex>
#include <regex>

#include "folly/String.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::ducklake {

namespace {

using duckdb::idx_t;

std::string quoteSqlString(std::string_view value) {
  std::string result{"'"};
  result.reserve(value.size() + 2);
  for (const auto c : value) {
    if (c == '\'') {
      result += "''";
    } else {
      result.push_back(c);
    }
  }
  result.push_back('\'');
  return result;
}

std::string livePredicate(std::string_view alias, int64_t snapshotId) {
  return fmt::format(
      "{}.begin_snapshot <= {} AND ({}.end_snapshot IS NULL OR {}.end_snapshot > {})",
      alias,
      snapshotId,
      alias,
      alias,
      snapshotId);
}

std::string lower(std::string input) {
  folly::toLowerAscii(input);
  return input;
}

bool hasUriScheme(std::string_view path) {
  const auto schemeSeparator = path.find("://");
  if (schemeSeparator == std::string_view::npos || schemeSeparator == 0) {
    return false;
  }

  for (auto i = size_t{0}; i < schemeSeparator; ++i) {
    const auto c = path[i];
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '+' &&
        c != '-' && c != '.') {
      return false;
    }
  }
  return true;
}

std::string joinUriPath(std::string_view parent, std::string_view child) {
  if (parent.empty()) {
    return std::string{child};
  }
  if (child.empty()) {
    return std::string{parent};
  }
  if (parent.back() == '/') {
    return fmt::format("{}{}", parent, child);
  }
  return fmt::format("{}/{}", parent, child);
}

std::string resolvePath(
    std::string_view parent,
    std::string_view path,
    bool pathIsRelative) {
  if (path.empty()) {
    return std::string{parent};
  }

  if (!pathIsRelative || hasUriScheme(path) ||
      std::filesystem::path{path}.is_absolute()) {
    return std::string{path};
  }

  if (hasUriScheme(parent)) {
    return joinUriPath(parent, path);
  }

  if (parent.empty()) {
    return std::filesystem::path{path}.lexically_normal().string();
  }

  return (std::filesystem::path{parent} / std::filesystem::path{path})
      .lexically_normal()
      .string();
}

std::string catalogDirectory(const std::string& metadataPath) {
  if (hasUriScheme(metadataPath) || metadataPath == ":memory:") {
    return {};
  }
  auto parent = std::filesystem::absolute(metadataPath).parent_path();
  return parent.empty() ? std::string{"."} : parent.string();
}

uint64_t toUnsigned(int64_t value, std::string_view columnName) {
  VELOX_USER_CHECK_GE(
      value, 0, "DuckLake metadata value must be non-negative: {}", columnName);
  return static_cast<uint64_t>(value);
}

std::optional<int64_t> optionalInt64(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  auto value = const_cast<duckdb::MaterializedQueryResult&>(result).GetValue(
      column, row);
  if (value.IsNull()) {
    return std::nullopt;
  }
  return value.GetValue<int64_t>();
}

bool isNull(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  return const_cast<duckdb::MaterializedQueryResult&>(result)
      .GetValue(column, row)
      .IsNull();
}

std::optional<std::string> optionalString(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  auto value = const_cast<duckdb::MaterializedQueryResult&>(result).GetValue(
      column, row);
  if (value.IsNull()) {
    return std::nullopt;
  }
  return value.GetValue<std::string>();
}

bool boolValue(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  return const_cast<duckdb::MaterializedQueryResult&>(result)
      .GetValue(column, row)
      .GetValue<bool>();
}

int64_t int64Value(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  return const_cast<duckdb::MaterializedQueryResult&>(result)
      .GetValue(column, row)
      .GetValue<int64_t>();
}

std::string stringValue(
    const duckdb::MaterializedQueryResult& result,
    idx_t column,
    idx_t row) {
  return const_cast<duckdb::MaterializedQueryResult&>(result)
      .GetValue(column, row)
      .GetValue<std::string>();
}

velox::TypePtr parsePrimitiveType(std::string typeName) {
  typeName = folly::trimWhitespace(typeName).str();
  auto normalized = lower(typeName);

  if (normalized == "boolean") {
    return velox::BOOLEAN();
  }
  if (normalized == "int8") {
    return velox::TINYINT();
  }
  if (normalized == "int16") {
    return velox::SMALLINT();
  }
  if (normalized == "int32") {
    return velox::INTEGER();
  }
  if (normalized == "int64") {
    return velox::BIGINT();
  }
  if (normalized == "uint8") {
    return velox::SMALLINT();
  }
  if (normalized == "uint16") {
    return velox::INTEGER();
  }
  if (normalized == "uint32") {
    return velox::BIGINT();
  }
  if (normalized == "float32") {
    return velox::REAL();
  }
  if (normalized == "float64") {
    return velox::DOUBLE();
  }
  if (normalized == "varchar" || normalized == "json" ||
      normalized == "uuid") {
    return velox::VARCHAR();
  }
  if (normalized == "blob") {
    return velox::VARBINARY();
  }
  if (normalized == "date") {
    return velox::DATE();
  }
  if (normalized == "time") {
    return velox::TIME_MICRO_UTC();
  }
  if (normalized == "timestamp" || normalized == "timestamptz" ||
      normalized == "timestamp_s" || normalized == "timestamp_ms" ||
      normalized == "timestamp_ns") {
    return velox::TIMESTAMP();
  }

  static const std::regex kDecimalPattern{
      R"(^decimal\s*\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)$)"};
  std::smatch match;
  if (std::regex_match(normalized, match, kDecimalPattern)) {
    auto precision = folly::to<int32_t>(match[1].str());
    auto scale = folly::to<int32_t>(match[2].str());
    VELOX_USER_CHECK_LE(
        precision, 38, "DuckLake decimal precision is too large: {}", typeName);
    VELOX_USER_CHECK_GE(
        precision, 1, "DuckLake decimal precision is invalid: {}", typeName);
    VELOX_USER_CHECK_GE(
        scale, 0, "DuckLake decimal scale is invalid: {}", typeName);
    VELOX_USER_CHECK_LE(
        scale,
        precision,
        "DuckLake decimal scale exceeds precision: {}",
        typeName);
    return velox::DECIMAL(
        static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
  }

  VELOX_USER_FAIL("DuckLake column type is not supported yet: {}", typeName);
}

std::unique_ptr<duckdb::DBConfig> makeReadOnlyConfig() {
  auto config = std::make_unique<duckdb::DBConfig>();
  config->options.access_mode = duckdb::AccessMode::READ_ONLY;
  return config;
}

} // namespace

class DuckDbDuckLakeCatalogClient : public DuckLakeCatalogClient {
 public:
  explicit DuckDbDuckLakeCatalogClient(DuckLakeCatalogSpec spec)
      : spec_{std::move(spec)},
        catalogDirectory_{catalogDirectory(spec_.metadataPath)},
        config_{makeReadOnlyConfig()} {
    try {
      db_ = std::make_unique<duckdb::DuckDB>(
          spec_.metadataPath, config_.get());
      connection_ = std::make_unique<duckdb::Connection>(*db_);
    } catch (const std::exception& error) {
      VELOX_USER_FAIL(
          "Failed to open DuckLake DuckDB catalog: {}. Catalog path: {}",
          error.what(),
          spec_.metadataPath);
    }
  }

  std::vector<std::string> listSchemaNames() override {
    std::lock_guard<std::mutex> lock{mutex_};
    const auto snapshotId = currentSnapshotId();
    if (!snapshotId.has_value()) {
      return {};
    }

    auto result = query(fmt::format(
        "SELECT schema_name FROM ducklake_schema WHERE {} ORDER BY schema_name",
        livePredicate("ducklake_schema", snapshotId.value())));

    std::vector<std::string> schemas;
    schemas.reserve(result->RowCount());
    for (auto row = idx_t{0}; row < result->RowCount(); ++row) {
      schemas.push_back(stringValue(*result, 0, row));
    }
    return schemas;
  }

  std::optional<DuckLakeTableMetadata> loadTable(
      const SchemaTableName& tableName) override {
    std::lock_guard<std::mutex> lock{mutex_};
    const auto snapshotId = currentSnapshotId();
    if (!snapshotId.has_value()) {
      return std::nullopt;
    }

    auto tableResult = query(fmt::format(
        "SELECT "
        "s.schema_id, s.path, s.path_is_relative, "
        "t.table_id, t.path, t.path_is_relative, "
        "COALESCE(ts.record_count, 0) "
        "FROM ducklake_schema s "
        "JOIN ducklake_table t ON t.schema_id = s.schema_id "
        "LEFT JOIN ducklake_table_stats ts ON ts.table_id = t.table_id "
        "WHERE {} AND {} AND s.schema_name = {} AND t.table_name = {} "
        "ORDER BY t.table_id LIMIT 1",
        livePredicate("s", snapshotId.value()),
        livePredicate("t", snapshotId.value()),
        quoteSqlString(tableName.schema),
        quoteSqlString(tableName.table)));

    if (tableResult->RowCount() == 0) {
      return std::nullopt;
    }

    DuckLakeTableMetadata table;
    table.name = tableName;
    table.snapshotId = snapshotId.value();
    table.dataPath = loadCatalogDataPath();

    const auto schemaPath = stringValue(*tableResult, 1, 0);
    const auto schemaPathIsRelative = boolValue(*tableResult, 2, 0);
    table.schemaPath =
        resolvePath(table.dataPath, schemaPath, schemaPathIsRelative);

    table.tableId = int64Value(*tableResult, 3, 0);
    const auto tablePath = stringValue(*tableResult, 4, 0);
    const auto tablePathIsRelative = boolValue(*tableResult, 5, 0);
    table.tablePath =
        resolvePath(table.schemaPath, tablePath, tablePathIsRelative);

    table.numRows = toUnsigned(int64Value(*tableResult, 6, 0), "record_count");

    validateReadableTable(table.tableId, snapshotId.value());
    loadColumns(table);
    loadDataFiles(table);
    return table;
  }

 private:
  std::unique_ptr<duckdb::MaterializedQueryResult> query(
      std::string_view sql) {
    auto result = connection_->Query(std::string{sql});
    VELOX_USER_CHECK(
        !result->HasError(),
        "DuckLake catalog query failed: {}. SQL: {}",
        result->GetError(),
        sql);
    return result;
  }

  std::optional<int64_t> currentSnapshotId() {
    auto result = query("SELECT MAX(snapshot_id) FROM ducklake_snapshot");
    if (result->RowCount() == 0) {
      return std::nullopt;
    }
    return optionalInt64(*result, 0, 0);
  }

  std::optional<std::string> loadGlobalMetadataValue(std::string_view key) {
    auto result = query(fmt::format(
        "SELECT value FROM ducklake_metadata "
        "WHERE \"key\" = {} AND scope IS NULL LIMIT 1",
        quoteSqlString(key)));
    if (result->RowCount() == 0) {
      return std::nullopt;
    }
    return optionalString(*result, 0, 0);
  }

  std::string loadCatalogDataPath() {
    auto dataPath = loadGlobalMetadataValue("data_path");
    VELOX_USER_CHECK(
        dataPath.has_value(), "DuckLake catalog is missing global data_path");
    return resolvePath(catalogDirectory_, dataPath.value(), true);
  }

  int64_t countRows(std::string_view sql) {
    auto result = query(sql);
    VELOX_CHECK_EQ(result->RowCount(), 1);
    return int64Value(*result, 0, 0);
  }

  void validateReadableTable(int64_t tableId, int64_t snapshotId) {
    const auto encrypted = loadGlobalMetadataValue("encrypted");
    VELOX_USER_CHECK(
        !encrypted.has_value() || lower(encrypted.value()) != "true",
        "Encrypted DuckLake tables are not supported yet");

    const auto numDeleteFiles = countRows(fmt::format(
        "SELECT COUNT(*) FROM ducklake_delete_file "
        "WHERE table_id = {} AND {}",
        tableId,
        livePredicate("ducklake_delete_file", snapshotId)));
    VELOX_USER_CHECK_EQ(
        numDeleteFiles,
        0,
        "DuckLake delete files are not supported yet for table id: {}",
        tableId);

    const auto numInlinedTables = countRows(fmt::format(
        "SELECT COUNT(*) FROM ducklake_inlined_data_tables WHERE table_id = {}",
        tableId));
    VELOX_USER_CHECK_EQ(
        numInlinedTables,
        0,
        "DuckLake inlined data tables are not supported yet for table id: {}",
        tableId);
  }

  void loadColumns(DuckLakeTableMetadata& table) {
    auto result = query(fmt::format(
        "SELECT column_id, column_order, column_name, column_type, "
        "nulls_allowed "
        "FROM ducklake_column "
        "WHERE table_id = {} AND {} AND parent_column IS NULL "
        "ORDER BY column_order",
        table.tableId,
        livePredicate("ducklake_column", table.snapshotId)));

    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    names.reserve(result->RowCount());
    types.reserve(result->RowCount());
    table.columns.reserve(result->RowCount());

    for (auto row = idx_t{0}; row < result->RowCount(); ++row) {
      DuckLakeColumnMetadata column{
          .columnId = int64Value(*result, 0, row),
          .columnOrder = int64Value(*result, 1, row),
          .name = stringValue(*result, 2, row),
          .duckLakeType = stringValue(*result, 3, row),
          .type = nullptr,
          .nullsAllowed = boolValue(*result, 4, row),
      };
      column.type = parsePrimitiveType(column.duckLakeType);
      names.push_back(column.name);
      types.push_back(column.type);
      table.columns.push_back(std::move(column));
    }

    table.rowType = velox::ROW(std::move(names), std::move(types));
  }

  void loadDataFiles(DuckLakeTableMetadata& table) {
    auto result = query(fmt::format(
        "SELECT data_file_id, path, path_is_relative, file_format, "
        "record_count, file_size_bytes, footer_size, row_id_start, "
        "encryption_key, partial_max "
        "FROM ducklake_data_file "
        "WHERE table_id = {} AND {} "
        "ORDER BY file_order, data_file_id",
        table.tableId,
        livePredicate("ducklake_data_file", table.snapshotId)));

    table.dataFiles.reserve(result->RowCount());
    for (auto row = idx_t{0}; row < result->RowCount(); ++row) {
      const auto format = lower(stringValue(*result, 3, row));
      VELOX_USER_CHECK_EQ(
          format,
          "parquet",
          "DuckLake data file format is not supported yet: {}",
          format);
      VELOX_USER_CHECK(
          isNull(*result, 8, row),
          "Encrypted DuckLake data files are not supported yet");
      VELOX_USER_CHECK(
          isNull(*result, 9, row),
          "DuckLake partial data files are not supported yet");

      const auto path = stringValue(*result, 1, row);
      const auto pathIsRelative = boolValue(*result, 2, row);
      const auto fileSize = optionalInt64(*result, 5, row);
      const auto footerSize = optionalInt64(*result, 6, row);

      DuckLakeDataFile file{
          .dataFileId = int64Value(*result, 0, row),
          .path = resolvePath(table.tablePath, path, pathIsRelative),
          .recordCount =
              toUnsigned(int64Value(*result, 4, row), "record_count"),
          .fileSizeBytes = fileSize.has_value()
              ? std::make_optional(
                    toUnsigned(fileSize.value(), "file_size_bytes"))
              : std::nullopt,
          .footerSize = footerSize.has_value()
              ? std::make_optional(
                    toUnsigned(footerSize.value(), "footer_size"))
              : std::nullopt,
          .rowIdStart = optionalInt64(*result, 7, row),
      };
      table.dataFiles.push_back(std::move(file));
    }
  }

  DuckLakeCatalogSpec spec_;
  std::string catalogDirectory_;
  std::unique_ptr<duckdb::DBConfig> config_;
  std::unique_ptr<duckdb::DuckDB> db_;
  std::unique_ptr<duckdb::Connection> connection_;
  std::mutex mutex_;
};

std::unique_ptr<DuckLakeCatalogClient> DuckLakeCatalogClient::create(
    DuckLakeCatalogSpec spec) {
  switch (spec.backend) {
    case DuckLakeCatalogBackend::kDuckDb:
      return std::make_unique<DuckDbDuckLakeCatalogClient>(std::move(spec));
    case DuckLakeCatalogBackend::kSqlite:
      VELOX_USER_FAIL(
          "DuckLake SQLite catalog backend is not supported yet: {}",
          spec.metadataPath);
    case DuckLakeCatalogBackend::kPostgres:
      VELOX_USER_FAIL(
          "DuckLake PostgreSQL catalog backend is not supported yet: {}",
          spec.metadataPath);
  }
  VELOX_UNREACHABLE();
}

} // namespace facebook::axiom::connector::ducklake
