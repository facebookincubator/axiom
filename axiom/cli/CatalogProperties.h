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
#include <vector>

#include <folly/container/F14Map.h>

namespace axiom::sql {

/// Describes one catalog: its name, its connector and the connector's
/// configuration.
struct CatalogProperties {
  /// Name queries use to reference the catalog.
  std::string catalogName;

  /// Connector implementation name, such as `tpch` or `hive`.
  std::string connectorName;

  /// Connector-specific properties.
  folly::F14FastMap<std::string, std::string> connectorConfig;
};

/// Loads connector catalogs from `${etcDir}/*.properties`. Each file describes
/// one catalog, named after the file without the `.properties` suffix. Its
/// `connector.name` property names the connector, and its other properties
/// configure it.
std::vector<CatalogProperties> loadCatalogProperties(std::string_view etcDir);

} // namespace axiom::sql
