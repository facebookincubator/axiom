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

#include "axiom/cli/Connectors.h"

#include <folly/system/HardwareConcurrency.h>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"

namespace facebook::axiom {

namespace {

void initializeFileFormats() {
  velox::dwio::common::registerFileSinks();
  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
  velox::dwrf::registerDwrfReaderFactory();
  velox::dwrf::registerDwrfWriterFactory();
  velox::text::registerTextReaderFactory();
  velox::text::registerTextWriterFactory();
}

} // namespace

Connectors::Connectors() {
  initialize();
}

Connectors::~Connectors() {
  for (const auto& connectorId : connectorIds_) {
    // Unregister metadata first since it may reference the connector.
    connector::ConnectorMetadata::unregisterMetadata(connectorId);
    velox::connector::unregisterConnector(connectorId);
  }
}

void Connectors::initialize() {
  static folly::once_flag kInitialized;
  folly::call_once(kInitialized, [this]() {
    initializeFileFormats();
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        std::thread::hardware_concurrency(),
        std::make_shared<folly::NamedThreadFactory>("io"));
  });
}

std::shared_ptr<velox::connector::Connector> Connectors::registerTpchConnector(
    const std::string& connectorId) {
  auto emptyConfig = std::make_shared<velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpch::TpchConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, emptyConfig);
  connectorIds_.push_back(connector->connectorId());
  velox::connector::registerConnector(connector);

  auto tpchConnector =
      dynamic_cast<velox::connector::tpch::TpchConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(tpchConnector);
  connector::ConnectorMetadata::registerMetadata(
      connector->connectorId(),
      std::make_shared<connector::tpch::TpchConnectorMetadata>(tpchConnector));

  return connector;
}

std::shared_ptr<velox::connector::Connector>
Connectors::registerLocalHiveConnector(
    const std::string& dataPath,
    const std::string& dataFormat,
    const std::string& connectorId) {
  std::unordered_map<std::string, std::string> connectorConfig = {
      {connector::hive::HiveMetadataConfig::kLocalDataPath, dataPath},
      {connector::hive::HiveMetadataConfig::kLocalFileFormat, dataFormat},
  };

  auto config =
      std::make_shared<velox::config::ConfigBase>(std::move(connectorConfig));

  velox::connector::hive::HiveConnectorFactory factory;
  auto connector = factory.newConnector(connectorId, config, ioExecutor());
  connectorIds_.push_back(connector->connectorId());
  velox::connector::registerConnector(connector);

  auto hiveConnector =
      dynamic_cast<velox::connector::hive::HiveConnector*>(connector.get());
  VELOX_CHECK_NOT_NULL(hiveConnector);
  connector::ConnectorMetadata::registerMetadata(
      connector->connectorId(),
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          hiveConnector));

  return connector;
}

} // namespace facebook::axiom
