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

#include "axiom/cli/CatalogProperties.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include <folly/String.h>
#include "velox/common/base/Exceptions.h"

namespace axiom::sql {
namespace {

constexpr std::string_view kConnectorNameProperty = "connector.name";

std::unordered_map<std::string, std::string> parsePropertiesFile(
    const std::filesystem::path& filePath) {
  std::ifstream input(filePath);
  VELOX_USER_CHECK(
      input, "Failed to open catalog config file: {}", filePath.string());

  std::unordered_map<std::string, std::string> properties;
  std::string line;
  size_t lineNumber{0};
  while (std::getline(input, line)) {
    ++lineNumber;
    auto trimmedLine = folly::trimWhitespace(line).str();
    if (trimmedLine.empty() || trimmedLine.starts_with('#')) {
      continue;
    }

    auto separatorPosition = trimmedLine.find_first_of('=');
    VELOX_USER_CHECK(
        separatorPosition != std::string::npos,
        "Catalog config line must use key=value syntax: {}:{}",
        filePath.string(),
        lineNumber);

    auto key =
        folly::trimWhitespace(trimmedLine.substr(0, separatorPosition)).str();
    auto value =
        folly::trimWhitespace(trimmedLine.substr(separatorPosition + 1)).str();
    VELOX_USER_CHECK(
        !key.empty(),
        "Catalog config key is empty: {}:{}",
        filePath.string(),
        lineNumber);
    VELOX_USER_CHECK(
        properties.emplace(key, value).second,
        "Duplicate catalog config property {} in {}:{}",
        key,
        filePath.string(),
        lineNumber);
  }

  return properties;
}

} // namespace

std::vector<CatalogProperties> loadCatalogProperties(std::string_view etcDir) {
  if (etcDir.empty()) {
    return {};
  }

  const auto catalogDirectory = std::filesystem::path(etcDir);
  VELOX_USER_CHECK(
      std::filesystem::exists(catalogDirectory),
      "Catalog config directory does not exist: {}",
      catalogDirectory.string());
  VELOX_USER_CHECK(
      std::filesystem::is_directory(catalogDirectory),
      "Catalog config path is not a directory: {}",
      catalogDirectory.string());

  std::vector<std::filesystem::path> catalogFiles;
  for (const auto& entry :
       std::filesystem::directory_iterator(catalogDirectory)) {
    if (entry.is_regular_file() && entry.path().extension() == ".properties") {
      catalogFiles.push_back(entry.path());
    }
  }
  std::sort(catalogFiles.begin(), catalogFiles.end());

  std::vector<CatalogProperties> catalogs;
  catalogs.reserve(catalogFiles.size());
  for (const auto& filePath : catalogFiles) {
    auto properties = parsePropertiesFile(filePath);
    auto connectorNameIterator =
        properties.find(std::string(kConnectorNameProperty));
    VELOX_USER_CHECK(
        connectorNameIterator != properties.end(),
        "Catalog config is missing required property {}: {}",
        kConnectorNameProperty,
        filePath.string());

    auto catalogName = filePath.stem().string();
    VELOX_USER_CHECK(
        !catalogName.empty(),
        "Catalog config file name must not be empty: {}",
        filePath.string());

    auto connectorName = connectorNameIterator->second;
    properties.erase(connectorNameIterator);
    catalogs.push_back(
        CatalogProperties{
            .catalogName = std::move(catalogName),
            .connectorName = std::move(connectorName),
            .connectorConfig = std::move(properties),
        });
  }

  return catalogs;
}

} // namespace axiom::sql
