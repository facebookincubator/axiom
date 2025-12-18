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

#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include <folly/system/HardwareConcurrency.h>
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

DEFINE_string(
    data_path,
    "",
    "Path to TPC-H data directory. If empty, the test creates a temp directory and deletes it on exit");
DEFINE_bool(create_dataset, true, "Creates the TPC-H tables");
DEFINE_double(tpch_scale, 0.1, "Scale factor");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::axiom::optimizer::test {

namespace {
void doCreateTables(std::string_view path) {
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("leaf");

  LOG(INFO) << "Creating TPC-H tables in " << path;

  for (const auto& table : tpch::tables) {
    const auto tableName = tpch::toTableName(table);
    const auto tableSchema = tpch::getTableSchema(table);

    int32_t numSplits = 1;
    if (tableName != "nation" && tableName != "region" &&
        FLAGS_tpch_scale > 1) {
      numSplits = std::min<int32_t>(FLAGS_tpch_scale, 200);
    }

    const auto tableDirectory = fmt::format("{}/{}", path, tableName);
    auto plan =
        PlanBuilder()
            .tpchTableScan(table, tableSchema->names(), FLAGS_tpch_scale)
            .startTableWriter()
            .outputDirectoryPath(tableDirectory)
            .fileFormat(dwio::common::FileFormat::PARQUET)
            .compressionKind(common::CompressionKind::CompressionKind_SNAPPY)
            .endTableWriter()
            .planNode();

    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
    for (auto i = 0; i < numSplits; ++i) {
      splits.push_back(
          std::make_shared<velox::connector::tpch::TpchConnectorSplit>(
              std::string(PlanBuilder::kTpchDefaultConnectorId), numSplits, i));
    }

    const int32_t numDrivers =
        std::min<int32_t>(numSplits, folly::hardware_concurrency());

    LOG(INFO) << "Creating TPC-H table " << tableName
              << " scaleFactor=" << FLAGS_tpch_scale
              << " numSplits=" << numSplits << " numDrivers=" << numDrivers
              << " hw concurrency=" << folly::hardware_concurrency();
    auto rows = AssertQueryBuilder(plan)
                    .splits(std::move(splits))
                    .maxDrivers(numDrivers)
                    .copyResults(pool.get());
  }
}

void registerHiveConnector(const std::string& id) {
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::hive::HiveConnectorFactory factory;
  velox::connector::registerConnector(factory.newConnector(id, emptyConfig));
}

} // namespace

//  static
void ParquetTpchTest::createTables(std::string_view path) {
  SCOPE_EXIT {
    velox::connector::unregisterConnector(
        std::string(PlanBuilder::kHiveDefaultConnectorId));
    unregisterTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));

    parquet::unregisterParquetWriterFactory();
  };

  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  parquet::registerParquetWriterFactory();

  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::hive::HiveConnectorFactory hiveConnectorFactory;
  auto hiveConnector = hiveConnectorFactory.newConnector(
      std::string(PlanBuilder::kHiveDefaultConnectorId), emptyConfig);
  velox::connector::registerConnector(std::move(hiveConnector));

  registerTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));

  doCreateTables(path);
}

// static
void ParquetTpchTest::registerTpchConnector(const std::string& id) {
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
void ParquetTpchTest::unregisterTpchConnector(const std::string& id) {
  connector::ConnectorMetadata::unregisterMetadata(id);
  velox::connector::unregisterConnector(id);
}

} // namespace facebook::axiom::optimizer::test
