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

#include <string>
#include <unordered_map>
#include <vector>

namespace axiom::sql {

/// Describes one connector catalog properties file.
struct CatalogProperties {
  /// Catalog name derived from the file name without the `.properties` suffix.
  std::string catalogName;

  /// Connector implementation name from the `connector.name` property.
  std::string connectorName;

  /// Connector-specific properties excluding `connector.name`.
  std::unordered_map<std::string, std::string> connectorConfig;
};

/// Loads connector catalogs from `${etcDir}/*.properties`.
std::vector<CatalogProperties> loadCatalogProperties(std::string_view etcDir);

} // namespace axiom::sql
