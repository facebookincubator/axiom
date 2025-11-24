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
#include "axiom/cli/Console.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"

namespace facebook::axiom {
namespace {

class Connectors {
 public:
  Connectors(const std::string& dataPath, const std::string& dataFormat)
      : dataPath_{dataPath}, dataFormat_{dataFormat} {}

  std::string initialize(optimizer::VeloxHistory& history) {
    velox::dwio::common::registerFileSinks();
    velox::parquet::registerParquetReaderFactory();
    velox::parquet::registerParquetWriterFactory();
    velox::dwrf::registerDwrfReaderFactory();
    velox::dwrf::registerDwrfWriterFactory();
    velox::text::registerTextReaderFactory();
    velox::text::registerTextWriterFactory();

    std::shared_ptr<velox::connector::Connector> connector;

    // Register the Hive connector.
    if (!dataPath_.empty()) {
      ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(8);
      connector = registerHiveConnector(
          dataPath_, dataFormat_, ioExecutor_.get(), history);
    }

    // Register the TPC-H connector.
    {
      auto tpchConnector = registerTpchConnector();
      if (connector == nullptr) {
        connector = tpchConnector;
      }
    }

    return connector->connectorId();
  }

 private:
  std::shared_ptr<velox::connector::Connector> registerTpchConnector() {
    auto emptyConfig = std::make_shared<velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});

    velox::connector::tpch::TpchConnectorFactory factory;
    auto connector = factory.newConnector("tpch", emptyConfig);
    velox::connector::registerConnector(connector);

    connector::ConnectorMetadata::registerMetadata(
        connector->connectorId(),
        std::make_shared<connector::tpch::TpchConnectorMetadata>(
            dynamic_cast<velox::connector::tpch::TpchConnector*>(
                connector.get())));

    return connector;
  }

  std::shared_ptr<velox::connector::Connector> registerHiveConnector(
      const std::string& dataPath,
      const std::string& dataFormat,
      folly::IOThreadPoolExecutor* ioExecutor,
      optimizer::VeloxHistory& history) {
    std::unordered_map<std::string, std::string> connectorConfig = {
        {velox::connector::hive::HiveConfig::kLocalDataPath, dataPath},
        {velox::connector::hive::HiveConfig::kLocalFileFormat, dataFormat},
    };

    auto config =
        std::make_shared<velox::config::ConfigBase>(std::move(connectorConfig));

    velox::connector::hive::HiveConnectorFactory factory;
    auto connector = factory.newConnector("hive", config, ioExecutor);
    velox::connector::registerConnector(connector);

    connector::ConnectorMetadata::registerMetadata(
        connector->connectorId(),
        std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
            dynamic_cast<velox::connector::hive::HiveConnector*>(
                connector.get())));

    if (!dataPath.empty()) {
      history.updateFromFile(dataPath + "/.history");
    }

    return connector;
  }

  const std::string dataPath_;
  const std::string dataFormat_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
};
} // namespace
} // namespace facebook::axiom

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);

  facebook::axiom::Connectors connectors{FLAGS_data_path, FLAGS_data_format};

  axiom::sql::SqlQueryRunner runner;
  runner.initialize([&](auto& history) {
    return std::make_pair(connectors.initialize(history), std::nullopt);
  });

  axiom::sql::Console console{runner};
  console.initialize();
  console.run();

  return 0;
}
