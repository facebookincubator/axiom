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

#pragma once

#include <duckdb.hpp>

#include <filesystem>
#include <optional>
#include <string>

namespace facebook::axiom::connector::ducklake {

/// Describes a DuckLake fixture creation failure.
struct DuckLakeCreationError {
  /// Stores the diagnostic message returned by DuckDB or the fixture helper.
  std::string message;

  /// Records whether the failure is caused by missing DuckLake extension setup.
  bool isEnvironmentIssue{false};
};

/// Runs a SQL statement against a DuckDB connection.
///
/// @param connection DuckDB connection to execute against.
/// @param sql SQL statement to run.
/// @returns DuckDB error message when execution fails.
std::optional<std::string> runDuckDb(
    duckdb::Connection& connection,
    std::string_view sql);

/// Checks whether a DuckDB error is caused by missing DuckLake extension setup.
///
/// @param sql SQL statement that failed.
/// @param error DuckDB error message.
/// @returns True when the error should skip tests instead of failing them.
bool isDuckLakeExtensionSetupFailure(
    std::string_view sql,
    std::string_view error);

/// Checks whether a directory tree contains at least one Parquet file.
///
/// @param directory Directory tree to scan.
/// @returns True when a regular `.parquet` file exists below the directory.
bool hasParquetFile(const std::filesystem::path& directory);

/// Creates DuckLake test tables with data flushed to Parquet files.
///
/// @param directory Temporary directory where catalog and data files are
/// stored.
/// @returns Creation error when DuckDB cannot build the fixture.
std::optional<DuckLakeCreationError> createDuckLakeTable(
    const std::filesystem::path& directory);

} // namespace facebook::axiom::connector::ducklake
