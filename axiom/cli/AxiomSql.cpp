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

#include <fmt/ranges.h>
#include <folly/container/F14Map.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <iostream>
#include <set>
#include "axiom/cli/CatalogProperties.h"
#include "axiom/cli/Connectors.h"
#include "axiom/cli/Console.h"
#include "axiom/cli/SystemUser.h"
#include "velox/common/base/Exceptions.h"

DEFINE_string(
    catalog,
    "",
    "Default catalog (connector). If empty, uses tpch or hive if --data_path is set.");
DEFINE_string(schema, "", "Default schema.");

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  facebook::axiom::Connectors connectors;
  axiom::sql::SqlQueryRunner runner{axiom::sql::SystemUser::resolve()};
  runner.initialize([&]() {
    VELOX_USER_CHECK(
        FLAGS_data_path.empty() || FLAGS_etc_dir.empty(),
        "--data_path and --etc_dir are mutually exclusive. Use --data_path for "
        "the local Hive shorthand or --etc_dir for catalog .properties files.");

    auto defaultConnector = connectors.registerTpchConnector();
    folly::F14FastMap<std::string, std::string> defaultSchemas = {
        {
            defaultConnector->connectorId(),
            "tiny",
        },
    };
    std::set<std::string> catalogIds = {defaultConnector->connectorId()};

    connectors.registerTestConnector();
    catalogIds.insert(facebook::axiom::Connectors::kTestConnectorId);
    defaultSchemas.emplace(
        facebook::axiom::Connectors::kTestConnectorId, "default");

    auto registerConnector =
        [&](std::string id,
            std::string_view name,
            const folly::F14FastMap<std::string, std::string>& config) {
          VELOX_USER_CHECK(
              catalogIds.insert(id).second,
              "Catalog is already registered: {}",
              id);
          connectors.registerConnector(name, config, id);
          if (name == "tpch") {
            defaultSchemas.emplace(id, "tiny");
          } else if (name == "hive" || name == "test") {
            defaultSchemas.emplace(id, "default");
          }
        };

    if (!FLAGS_data_path.empty()) {
      connectors.registerLocalHiveConnector(FLAGS_data_path, FLAGS_data_format);
      catalogIds.insert(facebook::axiom::Connectors::kLocalHiveConnectorId);
      defaultSchemas.emplace(
          facebook::axiom::Connectors::kLocalHiveConnectorId, "default");
    }

    for (auto& catalogProperties :
         axiom::sql::loadCatalogProperties(FLAGS_etc_dir)) {
      registerConnector(
          std::move(catalogProperties.catalogName),
          catalogProperties.connectorName,
          catalogProperties.connectorConfig);
    }

    std::string connectorId;
    if (!FLAGS_catalog.empty()) {
      connectorId = FLAGS_catalog;
    } else if (!FLAGS_data_path.empty()) {
      connectorId = facebook::axiom::Connectors::kLocalHiveConnectorId;
    } else {
      connectorId = defaultConnector->connectorId();
    }

    std::string schema = FLAGS_schema;
    if (schema.empty()) {
      auto defaultSchemaIterator = defaultSchemas.find(connectorId);
      VELOX_USER_CHECK(
          defaultSchemaIterator != defaultSchemas.end() &&
              !defaultSchemaIterator->second.empty(),
          "Schema must be specified for connector {}",
          connectorId);
      schema = defaultSchemaIterator->second;
    }

    return std::make_pair(connectorId, schema);
  });

  // Register after initialize() so sessionConfig() is available.
  connectors.registerSystemConnector(runner.sessionConfig());
  connectors.registerFileConnector();

  axiom::sql::Console console{runner};
  console.initialize();
  // Invalid CLI flags throw VeloxUserError; surface them as a clean
  // 'Error: ...' line and a non-zero exit.
  try {
    console.run();
  } catch (const facebook::velox::VeloxUserError& e) {
    std::cerr << "Error: " << e.message() << std::endl;
    return 1;
  }

  return 0;
}
