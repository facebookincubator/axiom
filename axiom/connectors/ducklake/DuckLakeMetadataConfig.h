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

#include <memory>
#include <string>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::axiom::connector::ducklake {

/// Identifies the relational database backend that stores DuckLake metadata.
enum class DuckLakeCatalogBackend {
  /// Uses a DuckDB database file or in-memory DuckDB database as the catalog.
  kDuckDb,

  /// Uses a SQLite database as the catalog. This is parsed but not read yet.
  kSqlite,

  /// Uses a PostgreSQL database as the catalog. This is parsed but not read
  /// yet.
  kPostgres,
};

/// Describes the backend and location encoded in a DuckLake catalog URL.
///
/// Axiom accepts URLs with the `ducklake:` prefix and an optional backend
/// prefix, for example `ducklake:metadata.ducklake` or
/// `ducklake:duckdb:/tmp/metadata.ducklake`. When the backend prefix is
/// omitted, DuckDB is assumed to match DuckLake's default local catalog form.
struct DuckLakeCatalogSpec {
  /// Selects which relational database implementation stores catalog metadata.
  DuckLakeCatalogBackend backend;

  /// Identifies the backend-specific metadata location or connection string.
  std::string metadataPath;

  /// Parses and validates a DuckLake catalog URL.
  ///
  /// Throws a user-facing Velox exception when the URL is empty, does not start
  /// with `ducklake:`, or omits the backend-specific metadata location.
  static DuckLakeCatalogSpec parse(std::string_view catalogUrl);
};

/// Reads DuckLake connector settings from Velox connector configuration.
class DuckLakeMetadataConfig {
 public:
  /// Names the connector config entry containing the DuckLake catalog URL.
  static constexpr const char* kCatalogUrl = "ducklake_catalog_url";

  /// Creates a config wrapper over the immutable Velox connector config.
  explicit DuckLakeMetadataConfig(
      std::shared_ptr<const velox::config::ConfigBase> config);

  /// Returns the configured DuckLake catalog URL or an empty string if absent.
  std::string catalogUrl() const;

  /// Parses and returns the configured DuckLake catalog backend and location.
  DuckLakeCatalogSpec catalogSpec() const;

 private:
  const std::shared_ptr<const velox::config::ConfigBase> config_;
};

} // namespace facebook::axiom::connector::ducklake
