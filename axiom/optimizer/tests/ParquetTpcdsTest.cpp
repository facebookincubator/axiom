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

#include "axiom/optimizer/tests/ParquetTpcdsTest.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/system/HardwareConcurrency.h>
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpcds/TpcdsConnector.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

DEFINE_string(
    tpcds_data_path,
    "",
    "Path to TPC-DS data directory. If empty, the test creates a temp directory and deletes it on exit");
DEFINE_bool(create_tpcds_dataset, true, "Creates the TPC-DS tables");
DEFINE_double(tpcds_scale, 1, "TPC-DS scale factor");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::axiom::optimizer::test {

namespace {
void doCreateTables(std::string_view path) {
  LOG(INFO) << "Creating TPC-DS tables in " << path;

  const auto numThreads = folly::hardware_concurrency();
  auto executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(numThreads);

  std::vector<folly::SemiFuture<folly::Unit>> futures;
  for (const auto& table : tpcds::tables) {
    auto f =
        folly::via(executor.get(), [table, path = std::string(path)]() {
          auto rootPool = memory::memoryManager()->addRootPool();
          auto pool = rootPool->addLeafChild("leaf");
          const auto tableName = std::string(tpcds::toTableName(table));
          const auto tableSchema = tpcds::getTableSchema(table);

          int32_t numSplits = 1;
          if (FLAGS_tpcds_scale > 1) {
            numSplits = std::min<int32_t>(FLAGS_tpcds_scale, 200);
          }

          const auto tableDirectory =
              fmt::format("{}/{}", path, tableName);
          auto plan =
              PlanBuilder()
                  .tpcdsTableScan(
                      table, tableSchema->names(), FLAGS_tpcds_scale)
                  .startTableWriter()
                  .outputDirectoryPath(tableDirectory)
                  .fileFormat(dwio::common::FileFormat::PARQUET)
                  .compressionKind(
                      common::CompressionKind::CompressionKind_SNAPPY)
                  .endTableWriter()
                  .planNode();

          std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>
              splits;
          for (auto i = 0; i < numSplits; ++i) {
            splits.push_back(
                std::make_shared<
                    velox::connector::tpcds::TpcdsConnectorSplit>(
                    std::string(PlanBuilder::kTpcdsDefaultConnectorId),
                    numSplits,
                    i));
          }

          const int32_t numDrivers =
              std::min<int32_t>(numSplits, folly::hardware_concurrency());

          LOG(INFO) << "Creating TPC-DS table " << tableName
                    << " scaleFactor=" << FLAGS_tpcds_scale
                    << " numSplits=" << numSplits
                    << " numDrivers=" << numDrivers
                    << " hw concurrency=" << folly::hardware_concurrency();
          auto rows = AssertQueryBuilder(plan)
                          .splits(std::move(splits))
                          .maxDrivers(numDrivers)
                          .copyResults(pool.get());
        })
            .semi();
    futures.push_back(std::move(f));
  }

  folly::collectAll(std::move(futures)).get();
  executor->join();
  LOG(INFO) << "All TPC-DS tables created.";
}

} // namespace

//  static
void ParquetTpcdsTest::createTables(std::string_view path) {
  SCOPE_EXIT {
    velox::connector::unregisterConnector(
        std::string(PlanBuilder::kHiveDefaultConnectorId));
    unregisterTpcdsConnector(
        std::string(PlanBuilder::kTpcdsDefaultConnectorId));

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

  registerTpcdsConnector(std::string(PlanBuilder::kTpcdsDefaultConnectorId));

  doCreateTables(path);
}

// static
void ParquetTpcdsTest::registerTpcdsConnector(const std::string& id) {
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpcds::TpcdsConnectorFactory factory;
  auto connector = factory.newConnector(id, emptyConfig);
  velox::connector::registerConnector(connector);
}

// static
void ParquetTpcdsTest::unregisterTpcdsConnector(const std::string& id) {
  velox::connector::unregisterConnector(id);
}

} // namespace facebook::axiom::optimizer::test
