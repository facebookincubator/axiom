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

struct DuckLakeCreationError {
  std::string message;
  bool isEnvironmentIssue{false};
};

std::optional<std::string> runDuckDb(
    duckdb::Connection& connection,
    std::string_view sql);

bool isDuckLakeExtensionSetupFailure(
    std::string_view sql,
    std::string_view error);

bool hasParquetFile(const std::filesystem::path& directory);

std::optional<DuckLakeCreationError> createDuckLakeTable(
    const std::filesystem::path& directory);

} // namespace facebook::axiom::connector::ducklake
