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

#include "axiom/optimizer/tests/TpchDataGenerator.h"
#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer::test {

// static
void TpchDataGenerator::createTables(
    const std::vector<velox::tpch::Table>& tables,
    std::string_view path,
    double scaleFactor,
    dwio::common::FileFormat format,
    const TableStartingCallback& onTableStarting,
    const TableCreatedCallback& onTableCreated) {
  velox::filesystems::registerLocalFileSystem();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  Connectors connectors;
  ::axiom::sql::SqlQueryRunner runner;
  runner.initialize([&]() {
    connectors.registerTpchConnector();
    connectors.registerLocalHiveConnector(
        std::string(path), std::string(velox::dwio::common::toString(format)));
    return std::make_pair(
        std::string(Connectors::kLocalHiveConnectorId), std::string("default"));
  });

  LOG(INFO) << "Creating TPC-H tables in " << path;

  for (const auto& table : tables) {
    const auto tableName = tpch::toTableName(table);
    LOG(INFO) << "Creating TPC-H table " << tableName
              << " scaleFactor=" << scaleFactor;

    if (onTableStarting) {
      onTableStarting(tableName);
    }

    auto sql = fmt::format(
        "CREATE TABLE {} WITH (file_format = '{}') AS SELECT * FROM tpch.\"{}\".{}",
        tableName,
        velox::dwio::common::toString(format),
        fmt::format("sf{}", scaleFactor),
        tableName);

    auto result = runner.run(sql, {.numWorkers = 1, .numDrivers = 1});
    int64_t numRows = 0;
    if (!result.results.empty() && result.results[0]->size() > 0) {
      numRows = result.results[0]
                    ->childAt(0)
                    ->asUnchecked<velox::SimpleVector<int64_t>>()
                    ->valueAt(0);
    }

    LOG(INFO) << "Created " << tableName << " with " << numRows << " rows";

    if (onTableCreated) {
      onTableCreated(tableName, numRows);
    }
  }
}

} // namespace facebook::axiom::optimizer::test
