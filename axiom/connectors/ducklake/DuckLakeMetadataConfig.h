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

/// Identifies the catalog database backend encoded in a DuckLake URL.
enum class DuckLakeCatalogBackend {
  kDuckDb,
  kSqlite,
  kPostgres,
};

/// Describes the metadata database location from a DuckLake catalog URL.
struct DuckLakeCatalogSpec {
  DuckLakeCatalogBackend backend;
  std::string metadataPath;

  /// Parses a DuckLake catalog URL such as `ducklake:metadata.ducklake`.
  static DuckLakeCatalogSpec parse(std::string_view catalogUrl);
};

/// Reads DuckLake metadata settings from a connector configuration.
class DuckLakeMetadataConfig {
 public:
  /// DuckLake catalog URL, e.g. `ducklake:metadata.ducklake`.
  static constexpr const char* kCatalogUrl = "ducklake_catalog_url";

  explicit DuckLakeMetadataConfig(
      std::shared_ptr<const velox::config::ConfigBase> config);

  /// Returns the configured DuckLake catalog URL.
  std::string catalogUrl() const;

  /// Returns the parsed DuckLake catalog location.
  DuckLakeCatalogSpec catalogSpec() const;

 private:
  const std::shared_ptr<const velox::config::ConfigBase> config_;
};

} // namespace facebook::axiom::connector::ducklake
