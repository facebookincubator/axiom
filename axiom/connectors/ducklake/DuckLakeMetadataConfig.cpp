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

#include "axiom/connectors/ducklake/DuckLakeMetadataConfig.h"

#include "folly/String.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

namespace facebook::axiom::connector::ducklake {

namespace {

constexpr std::string_view kDuckLakePrefix{"ducklake:"};
constexpr std::string_view kDuckDbPrefix{"duckdb:"};
constexpr std::string_view kSqlitePrefix{"sqlite:"};
constexpr std::string_view kPostgresPrefix{"postgres:"};

std::string trim(std::string_view input) {
  return folly::trimWhitespace(std::string{input}).str();
}

DuckLakeCatalogSpec makeSpec(
    DuckLakeCatalogBackend backend,
    std::string_view metadataPath) {
  auto path = trim(metadataPath);
  VELOX_USER_CHECK(
      !path.empty(), "DuckLake catalog URL is missing metadata location");
  return DuckLakeCatalogSpec{backend, std::move(path)};
}

bool startsWith(std::string_view input, std::string_view prefix) {
  return input.size() >= prefix.size() &&
      input.substr(0, prefix.size()) == prefix;
}

} // namespace

DuckLakeCatalogSpec DuckLakeCatalogSpec::parse(std::string_view catalogUrl) {
  const auto trimmedUrl = trim(catalogUrl);
  VELOX_USER_CHECK(!trimmedUrl.empty(), "DuckLake catalog URL is empty");
  VELOX_USER_CHECK(
      startsWith(trimmedUrl, kDuckLakePrefix),
      "DuckLake catalog URL must start with 'ducklake:': {}",
      trimmedUrl);

  std::string_view rest{trimmedUrl};
  rest.remove_prefix(kDuckLakePrefix.size());
  VELOX_USER_CHECK(
      !rest.empty(), "DuckLake catalog URL is missing metadata location");

  if (startsWith(rest, kDuckDbPrefix)) {
    rest.remove_prefix(kDuckDbPrefix.size());
    return makeSpec(DuckLakeCatalogBackend::kDuckDb, rest);
  }

  if (startsWith(rest, kSqlitePrefix)) {
    rest.remove_prefix(kSqlitePrefix.size());
    return makeSpec(DuckLakeCatalogBackend::kSqlite, rest);
  }

  if (startsWith(rest, kPostgresPrefix)) {
    rest.remove_prefix(kPostgresPrefix.size());
    return makeSpec(DuckLakeCatalogBackend::kPostgres, rest);
  }

  return makeSpec(DuckLakeCatalogBackend::kDuckDb, rest);
}

DuckLakeMetadataConfig::DuckLakeMetadataConfig(
    std::shared_ptr<const velox::config::ConfigBase> config)
    : config_{std::move(config)} {
  VELOX_CHECK_NOT_NULL(
      config_, "Config is null for DuckLakeMetadataConfig initialization");
}

std::string DuckLakeMetadataConfig::catalogUrl() const {
  return config_->get<std::string>(kCatalogUrl, "");
}

DuckLakeCatalogSpec DuckLakeMetadataConfig::catalogSpec() const {
  return DuckLakeCatalogSpec::parse(catalogUrl());
}

} // namespace facebook::axiom::connector::ducklake
