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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include "axiom/cli/Connectors.h"
#include "axiom/cli/Console.h"

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
  axiom::sql::SqlQueryRunner runner;
  runner.initialize([&]() {
    auto defaultConnector = connectors.registerTpchConnector();

    connectors.registerTestConnector();

    if (!FLAGS_data_path.empty()) {
      defaultConnector = connectors.registerLocalHiveConnector(
          FLAGS_data_path, FLAGS_data_format);
    }

    std::string connectorId =
        FLAGS_catalog.empty() ? defaultConnector->connectorId() : FLAGS_catalog;

    std::optional<std::string> schema =
        FLAGS_schema.empty() ? std::nullopt : std::make_optional(FLAGS_schema);

    return std::make_pair(connectorId, schema);
  });

  axiom::sql::Console console{runner};
  console.initialize();
  console.run();

  return 0;
}
