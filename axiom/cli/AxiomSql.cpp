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
#include <folly/executors/FunctionScheduler.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <filesystem>
#include <iostream>
#include <set>
#include "axiom/cli/CatalogProperties.h"
#include "axiom/cli/Connectors.h"
#include "axiom/cli/Console.h"
#include "axiom/cli/SystemUser.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestTableJson.h"
#include "velox/common/base/Exceptions.h"

DEFINE_string(
    catalog,
    "",
    "Default catalog (connector). If empty, uses tpch or hive if --data_path is set.");
DEFINE_string(schema, "", "Default schema.");
DEFINE_bool(
    v2,
    false,
    "Use the v2 optimizer. EXPLAIN (type graph|optimized) is not supported "
    "under v2 yet.");

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  facebook::axiom::Connectors connectors;
  // Progress-polling scheduler for the console's live progress bar.
  folly::FunctionScheduler progressScheduler;
  axiom::sql::SqlQueryRunner runner{
      axiom::sql::SystemUser::resolve(), &progressScheduler, FLAGS_v2};
  auto initializeConnectors = [&]() {
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
      // Expose the catalog directory so connectors can resolve relative file
      // paths in their properties (e.g. the test connector's `tables`).
      catalogProperties.connectorConfig.insert_or_assign(
          std::string(facebook::axiom::connector::TestTableJson::kConfigDir),
          std::filesystem::absolute(FLAGS_etc_dir).string());
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

    // Not every catalog has a built-in default schema. Leave the schema empty
    // instead of refusing to start: only bare table names need it, and the
    // session can set one later with `use <catalog>.<schema>`.
    std::string schema = FLAGS_schema;
    if (schema.empty()) {
      if (const auto it = defaultSchemas.find(connectorId);
          it != defaultSchemas.end()) {
        schema = it->second;
      }
    }

    return std::make_pair(connectorId, schema);
  };

  // Both connector registration and the console throw VeloxUserError on bad
  // flags or catalog properties. Keep them inside the handler so either one
  // prints an 'Error: ' line and exits non-zero instead of escaping main.
  try {
    runner.initialize(initializeConnectors);

    // Register after initialize() so sessionConfig() is available.
    connectors.registerSystemConnector(runner.sessionConfig());
    connectors.registerFileConnector();

    // --catalog is only checked here because the system and file catalogs are
    // registered above, after the connectors the flag usually names.
    const auto& catalog = runner.defaultConnectorId();
    VELOX_USER_CHECK_NOT_NULL(
        facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(catalog),
        "Catalog does not exist: {}",
        catalog);

    if (runner.defaultSchema().empty()) {
      std::cerr << "Catalog '" << catalog
                << "' has no default schema. Qualify table names as "
                   "'<schema>.<table>', or run 'use "
                << catalog << ".<schema>' to set one." << std::endl;
    }

    axiom::sql::Console console{runner};
    console.initialize();
    console.run();
  } catch (const facebook::velox::VeloxUserError& e) {
    std::cerr << "Error: " << e.message() << std::endl;
    return 1;
  }

  return 0;
}
