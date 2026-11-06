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
#include <csignal>
#include <filesystem>
#include <iostream>
#include "axiom/cli/CatalogProperties.h"
#include "axiom/cli/Connectors.h"
#include "axiom/cli/Console.h"
#include "axiom/cli/SystemUser.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestTableJson.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/ConnectorRegistry.h"

DEFINE_string(
    catalog,
    "",
    "Default catalog (connector). If empty, uses tpch or hive if --data_path is set.");
DEFINE_string(schema, "", "Default schema.");
DEFINE_bool(
    v1,
    false,
    "Use the legacy v1 optimizer. EXPLAIN (type graph) is supported only "
    "under v1.");

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  auto connectorRegistry =
      facebook::velox::connector::ConnectorRegistry::create();
  auto metadataRegistry =
      facebook::axiom::connector::ConnectorMetadataRegistry::create();
  facebook::axiom::Connectors connectors{*connectorRegistry, *metadataRegistry};
  // Progress-polling scheduler for the console's live progress bar.
  folly::FunctionScheduler progressScheduler;
  axiom::sql::SqlQueryRunner runner{
      axiom::sql::SystemUser::resolve(),
      *connectorRegistry,
      *metadataRegistry,
      &progressScheduler,
      !FLAGS_v1};
  auto initializeConnectors = [&]() {
    VELOX_USER_CHECK(
        FLAGS_data_path.empty() || FLAGS_etc_dir.empty(),
        "--data_path and --etc_dir are mutually exclusive. Use --data_path for "
        "the local Hive shorthand or --etc_dir for catalog .properties files.");

    auto catalogs = axiom::sql::loadCatalogProperties(FLAGS_etc_dir);
    for (auto& catalog : catalogs) {
      // Expose the catalog directory so connectors can resolve relative file
      // paths in their properties (e.g. the test connector's `tables`).
      catalog.connectorConfig.insert_or_assign(
          std::string(facebook::axiom::connector::TestTableJson::kConfigDir),
          std::filesystem::absolute(FLAGS_etc_dir).string());
    }

    using facebook::axiom::Connectors;
    folly::F14FastMap<std::string, std::string> defaultSchemas;
    for (const auto& catalog : connectors.registerCatalogs(
             std::move(catalogs),
             {
                 {.catalogName = Connectors::kTpchConnectorId,
                  .connectorName = "tpch"},
                 {.catalogName = Connectors::kTestConnectorId,
                  .connectorName = "test"},
             })) {
      if (catalog.connectorName == "tpch") {
        defaultSchemas.emplace(catalog.catalogName, "tiny");
      } else if (
          catalog.connectorName == "hive" || catalog.connectorName == "test") {
        defaultSchemas.emplace(catalog.catalogName, "default");
      }
    }

    if (!FLAGS_data_path.empty()) {
      connectors.registerLocalHiveConnector(FLAGS_data_path, FLAGS_data_format);
      defaultSchemas.emplace(Connectors::kLocalHiveConnectorId, "default");
    }

    std::string connectorId;
    if (!FLAGS_catalog.empty()) {
      connectorId = FLAGS_catalog;
    } else if (!FLAGS_data_path.empty()) {
      connectorId = Connectors::kLocalHiveConnectorId;
    } else {
      connectorId = Connectors::kTpchConnectorId;
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

    // Register after initialize() so the session configuration is available.
    connectors.registerSystemConnector(runner.sessionConfig());
    connectors.registerFileConnector();

    // --catalog is only checked here because the system and file catalogs are
    // registered above, after the connectors the flag usually names.
    const auto& catalog = runner.defaultConnectorId();
    VELOX_USER_CHECK_NOT_NULL(
        metadataRegistry->find(catalog), "Catalog does not exist: {}", catalog);

    axiom::sql::Console console{runner};
    console.initialize();
    switch (console.run()) {
      case axiom::sql::Console::Outcome::kSucceeded:
        return 0;
      case axiom::sql::Console::Outcome::kFailed:
        return 1;
      case axiom::sql::Console::Outcome::kCancelled:
        // What a shell reports for a process that Ctrl+C ended.
        return 128 + SIGINT;
    }
    VELOX_UNREACHABLE();
  } catch (const facebook::velox::VeloxUserError& e) {
    std::cerr << "Error: " << e.message() << std::endl;
    return 1;
  }
}
