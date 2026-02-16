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
#include <folly/system/HardwareConcurrency.h>
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::axiom::optimizer::test {

namespace {
void registerHiveConnector(const std::string& id) {
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  parquet::registerParquetWriterFactory();
  dwrf::registerDwrfWriterFactory();

  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::hive::HiveConnectorFactory hiveConnectorFactory;
  auto hiveConnector = hiveConnectorFactory.newConnector(id, emptyConfig);
  velox::connector::registerConnector(std::move(hiveConnector));
}

void unregisterHiveConnector(const std::string& id) {
  velox::connector::unregisterConnector(id);

  parquet::unregisterParquetWriterFactory();
  dwrf::unregisterDwrfWriterFactory();
}
} // namespace

// static
int64_t TpchDataGenerator::createTable(
    tpch::Table table,
    std::string_view path,
    double scaleFactor,
    dwio::common::FileFormat format,
    common::CompressionKind compression) {
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("leaf");

  const auto tableName = tpch::toTableName(table);
  const auto tableSchema = tpch::getTableSchema(table);

  int32_t numSplits = 1;
  if (table != tpch::Table::TBL_NATION && table != tpch::Table::TBL_REGION &&
      scaleFactor > 1) {
    numSplits = std::min<int32_t>(scaleFactor, 200);
  }

  const auto tableDirectory = fmt::format("{}/{}", path, tableName);
  auto plan = PlanBuilder()
                  .tpchTableScan(table, tableSchema->names(), scaleFactor)
                  .startTableWriter()
                  .outputDirectoryPath(tableDirectory)
                  .fileFormat(format)
                  .compressionKind(compression)
                  .endTableWriter()
                  .planNode();

  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
  splits.reserve(numSplits);
  for (auto i = 0; i < numSplits; ++i) {
    splits.push_back(
        std::make_shared<velox::connector::tpch::TpchConnectorSplit>(
            std::string(PlanBuilder::kTpchDefaultConnectorId), numSplits, i));
  }

  const int32_t numDrivers =
      std::min<int32_t>(numSplits, folly::hardware_concurrency());

  auto result = AssertQueryBuilder(plan)
                    .splits(std::move(splits))
                    .maxDrivers(numDrivers)
                    .copyResults(pool.get());

  return result->childAt(0)->as<SimpleVector<int64_t>>()->valueAt(0);
}

//  static
void TpchDataGenerator::createTables(
    std::string_view path,
    double scaleFactor,
    dwio::common::FileFormat format,
    common::CompressionKind compression) {
  SCOPE_EXIT {
    unregisterHiveConnector(std::string(PlanBuilder::kHiveDefaultConnectorId));
    unregisterTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));
  };

  registerHiveConnector(std::string(PlanBuilder::kHiveDefaultConnectorId));
  registerTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));

  LOG(INFO) << "Creating TPC-H tables in " << path;

  for (const auto& table : tpch::tables) {
    const auto tableName = tpch::toTableName(table);
    LOG(INFO) << "Creating TPC-H table " << tableName
              << " scaleFactor=" << scaleFactor;

    auto numRows = createTable(table, path, scaleFactor, format, compression);

    LOG(INFO) << "Created " << tableName << " with " << numRows << " rows";
  }
}

// static
void TpchDataGenerator::registerTpchConnector(const std::string& id) {
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpch::TpchConnectorFactory factory;
  auto connector = factory.newConnector(id, emptyConfig);
  velox::connector::registerConnector(connector);

  connector::ConnectorMetadata::registerMetadata(
      id,
      std::make_shared<connector::tpch::TpchConnectorMetadata>(
          dynamic_cast<velox::connector::tpch::TpchConnector*>(
              connector.get())));
}

// static
void TpchDataGenerator::unregisterTpchConnector(const std::string& id) {
  connector::ConnectorMetadata::unregisterMetadata(id);
  velox::connector::unregisterConnector(id);
}

} // namespace facebook::axiom::optimizer::test
