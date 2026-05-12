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

#include "axiom/connectors/ducklake/tests/DuckLakeTestUtils.h"

#include <vector>

#include <fmt/core.h>
#include "axiom/connectors/ducklake/SqlStringLiteral.h"

namespace facebook::axiom::connector::ducklake {

std::optional<std::string> runDuckDb(
    duckdb::Connection& connection,
    std::string_view sql) {
  auto result = connection.Query(std::string{sql});
  if (result->HasError()) {
    return result->GetError();
  }
  return std::nullopt;
}

bool isDuckLakeExtensionSetupFailure(
    std::string_view sql,
    std::string_view error) {
  if (sql == "INSTALL ducklake" || sql == "LOAD ducklake") {
    return true;
  }
  return sql.find("ATTACH 'ducklake:") != std::string_view::npos &&
      (error.find("Extension") != std::string_view::npos ||
       error.find("extension") != std::string_view::npos ||
       error.find("ducklake") != std::string_view::npos);
}

bool hasParquetFile(const std::filesystem::path& directory) {
  if (!std::filesystem::exists(directory)) {
    return false;
  }
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
      return true;
    }
  }
  return false;
}

std::optional<DuckLakeCreationError> createDuckLakeTable(
    const std::filesystem::path& directory) {
  const auto catalogPath = directory / "metadata.ducklake";
  const auto dataDirectory = directory / "metadata.ducklake.files";

  duckdb::DuckDB db(nullptr);
  duckdb::Connection connection{db};
  std::vector<std::string> statements{
      fmt::format(
          "ATTACH {} AS lake (DATA_INLINING_ROW_LIMIT 0)",
          quoteSqlStringLiteral(
              fmt::format("ducklake:{}", catalogPath.string()))),
      "USE lake",
      "CREATE TABLE numbers(id INTEGER, name VARCHAR)",
      "INSERT INTO numbers VALUES (1, 'one'), (2, 'two'), (3, 'three')",
  };

  if (auto error = runDuckDb(connection, "INSTALL ducklake")) {
    return DuckLakeCreationError{
        fmt::format("DuckDB failed to install DuckLake: {}", error.value()),
        true,
    };
  }
  if (auto error = runDuckDb(connection, "LOAD ducklake")) {
    return DuckLakeCreationError{
        fmt::format("DuckDB failed to load DuckLake: {}", error.value()),
        true,
    };
  }
  for (const auto& sql : statements) {
    if (auto error = runDuckDb(connection, sql)) {
      return DuckLakeCreationError{
          fmt::format("DuckDB failed to run '{}': {}", sql, error.value()),
          isDuckLakeExtensionSetupFailure(sql, error.value()),
      };
    }
  }

  if (!std::filesystem::exists(catalogPath)) {
    return DuckLakeCreationError{
        fmt::format(
            "DuckLake catalog was not created: {}", catalogPath.string()),
    };
  }
  if (!hasParquetFile(dataDirectory)) {
    return DuckLakeCreationError{
        fmt::format(
            "DuckLake data file was not created: {}", dataDirectory.string()),
    };
  }

  return std::nullopt;
}

} // namespace facebook::axiom::connector::ducklake
