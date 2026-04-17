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

#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/serializers/RegisterAllVectorSerdes.h"

namespace facebook::axiom::runner::test {

void LocalRunnerTestBase::SetUpTestCase() {
  HiveConnectorTestBase::SetUpTestCase();
  velox::registerAllNamedVectorSerdes();
  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
}

void LocalRunnerTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();

  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();

  velox::exec::ExchangeSource::factories().clear();
  velox::exec::ExchangeSource::registerFactory(
      velox::exec::test::createLocalExchangeSource);

  setupConnector();
}

void LocalRunnerTestBase::TearDown() {
  connector::ConnectorMetadata::unregisterMetadata(
      velox::exec::test::kHiveConnectorId);
  velox::exec::ExchangeSource::factories().clear();
  velox::parquet::unregisterParquetWriterFactory();
  velox::parquet::unregisterParquetReaderFactory();
  HiveConnectorTestBase::TearDown();
}

std::shared_ptr<velox::core::QueryCtx> LocalRunnerTestBase::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  connectorConfigs[velox::exec::test::kHiveConnectorId] =
      std::make_shared<velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>{});

  return velox::core::QueryCtx::create(
      executor_.get(),
      velox::core::QueryConfig({}),
      std::move(connectorConfigs),
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      queryId);
}

void LocalRunnerTestBase::setupConnector() {
  std::unordered_map<std::string, std::string> configs;
  configs[connector::hive::HiveMetadataConfig::kLocalDataPath] =
      tempDirectory_->getPath();
  configs[connector::hive::HiveMetadataConfig::kLocalFileFormat] =
      velox::dwio::common::toString(velox::dwio::common::FileFormat::DWRF);
  resetHiveConnector(
      std::make_shared<velox::config::ConfigBase>(std::move(configs)));

  auto hiveConnector =
      velox::connector::getConnector(velox::exec::test::kHiveConnectorId);

  connector::ConnectorMetadata::registerMetadata(
      velox::exec::test::kHiveConnectorId,
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          dynamic_cast<velox::connector::hive::HiveConnector*>(
              hiveConnector.get())));
}

void LocalRunnerTestBase::makeTables(const std::vector<TableSpec>& specs) {
  if (tempDirectory_) {
    return;
  }
  tempDirectory_ = velox::common::testutil::TempDirectoryPath::create();

  const auto dataPath = tempDirectory_->getPath();
  auto fs = velox::filesystems::getFileSystem(dataPath, {});
  for (const auto& spec : specs) {
    const auto tablePath = fmt::format("{}/{}", dataPath, spec.name);
    fs->mkdir(tablePath);
    for (auto i = 0; i < spec.numFiles; ++i) {
      auto vectors = HiveConnectorTestBase::makeVectors(
          spec.columns, spec.numVectorsPerFile, spec.rowsPerVector);
      if (spec.customizeData) {
        for (const auto& vector : vectors) {
          spec.customizeData(vector);
        }
      }
      auto filePath = fmt::format("{}/f{}", tablePath, i);
      writeToFile(filePath, vectors);
    }
  }

  // Write .schema and .stats metadata for each table so that
  // LocalHiveConnectorMetadata can load them.
  for (const auto& spec : specs) {
    const auto tablePath = fmt::format("{}/{}", dataPath, spec.name);
    connector::hive::writeSchemaFile(
        tablePath, spec.columns, velox::dwio::common::FileFormat::DWRF);

    const uint64_t totalRows = static_cast<uint64_t>(spec.rowsPerVector) *
        spec.numVectorsPerFile * spec.numFiles;
    connector::hive::PersistedStats::write(tablePath, {totalRows, {}});
  }
}

} // namespace facebook::axiom::runner::test
