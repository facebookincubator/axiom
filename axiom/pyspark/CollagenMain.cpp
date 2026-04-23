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

#include "axiom/pyspark/CollagenMain.h"

#include <folly/io/async/EventBase.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/pyspark/runners/LocalRunner.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/serializers/PrestoSerializer.h"

DEFINE_string(
    local_hive_data_path,
    "",
    "Path for local-hive connector data. If empty, a temp directory is created.");

namespace axiom::collagen {
namespace {

std::string makeDataDirectory() {
  char dirname[] = "/tmp/collagen_hive_XXXXXX";
  char* dir = mkdtemp(dirname);
  if (dir == nullptr) {
    LOG(FATAL) << "Failed to create temp directory for LocalHiveConnector";
  }
  return std::string(dir);
}

} // namespace

CollagenMain::CollagenMain(
    std::string runnerId,
    std::string catalog,
    std::string schema,
    int port)
    : runnerId_(std::move(runnerId)),
      catalog_(std::move(catalog)),
      schema_(std::move(schema)),
      port_(port) {}

CollagenMain::~CollagenMain() {
  facebook::axiom::connector::ConnectorMetadata::unregisterMetadata(catalog_);
  facebook::velox::connector::unregisterConnector(catalog_);
}

void CollagenMain::init() {
  LOG(INFO) << "Starting Collagen service with catalog=" << catalog_
            << ", schema=" << schema_ << ", runner=" << runnerId_;

  initMemory();
  registerRunner();
  registerSerde();
  registerFileSystems();
  registerConnector();
  registerFunctions();

  std::string serverAddress(fmt::format("0.0.0.0:{}", port_));
  service_ = std::make_unique<CollagenService>(runnerId_, catalog_, schema_);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(service_.get());

  server_ = builder.BuildAndStart();
}

void CollagenMain::run() {
  CollagenSignalHandler handler(folly::getEventBase(), server_.get());
  server_->Wait();
}

void CollagenMain::initMemory() {
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
}

void CollagenMain::registerRunner() {
  if (runnerId_ == "local") {
    runner::registerLocalRunnerFactory(runnerId_);
  } else {
    LOG(FATAL) << "Unknown runner id: " << runnerId_
               << ". OSS supported runners: local";
  }
}

void CollagenMain::registerSerde() {
  facebook::velox::core::PlanNode::registerSerDe();
  facebook::velox::Type::registerSerDe();
  facebook::velox::common::Filter::registerSerDe();
  facebook::velox::connector::hive::HiveConnector::registerSerDe();
  facebook::velox::core::ITypedExpr::registerSerDe();
}

void CollagenMain::registerFileSystems() {
  facebook::velox::filesystems::registerLocalFileSystem();
  facebook::velox::dwio::common::registerFileSinks();
  facebook::velox::dwrf::registerDwrfReaderFactory();
  facebook::velox::dwrf::registerDwrfWriterFactory();
  facebook::velox::parquet::registerParquetReaderFactory();
  facebook::velox::parquet::registerParquetWriterFactory();
}

void CollagenMain::registerTestConnector() {
  auto connector =
      std::make_shared<facebook::axiom::connector::TestConnector>(catalog_);

  connector->addTable(
      "training_table",
      facebook::velox::ROW({"viewer_rid"}, {facebook::velox::BIGINT()}));
  connector->addTable(
      "feature_table",
      facebook::velox::ROW({"primary_rid"}, {facebook::velox::BIGINT()}));

  facebook::velox::connector::registerConnector(connector);
}

void CollagenMain::registerTpchConnector() {
  auto emptyConfig = std::make_shared<facebook::velox::config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  facebook::velox::connector::tpch::TpchConnectorFactory factory;
  auto tpchConnector = factory.newConnector(catalog_, emptyConfig);
  facebook::velox::connector::registerConnector(tpchConnector);

  facebook::axiom::connector::ConnectorMetadata::registerMetadata(
      catalog_,
      std::make_shared<facebook::axiom::connector::tpch::TpchConnectorMetadata>(
          dynamic_cast<facebook::velox::connector::tpch::TpchConnector*>(
              tpchConnector.get())));
}

void CollagenMain::registerLocalHiveConnector() {
  std::string dataPath = FLAGS_local_hive_data_path.empty()
      ? makeDataDirectory()
      : FLAGS_local_hive_data_path;

  std::unordered_map<std::string, std::string> connectorConfig = {
      {facebook::axiom::connector::hive::HiveMetadataConfig::kLocalDataPath,
       dataPath},
      {facebook::axiom::connector::hive::HiveMetadataConfig::kLocalFileFormat,
       "dwrf"},
  };
  auto config = std::make_shared<facebook::velox::config::ConfigBase>(
      std::move(connectorConfig));

  facebook::velox::connector::hive::HiveConnectorFactory factory;
  auto hiveConnector = factory.newConnector(catalog_, config);
  facebook::velox::connector::registerConnector(hiveConnector);

  facebook::axiom::connector::ConnectorMetadata::registerMetadata(
      catalog_,
      std::make_shared<
          facebook::axiom::connector::hive::LocalHiveConnectorMetadata>(
          dynamic_cast<facebook::velox::connector::hive::HiveConnector*>(
              hiveConnector.get())));
}

void CollagenMain::registerConnector() {
  if (catalog_ == "test") {
    registerTestConnector();
  } else if (catalog_ == "tpch") {
    registerTpchConnector();
  } else if (catalog_ == "local-hive") {
    registerLocalHiveConnector();
  } else {
    LOG(FATAL) << "Unknown catalog: " << catalog_
               << ". OSS supported catalogs: test, tpch, local-hive";
  }
}

void CollagenMain::registerFunctions() {
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();
  facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  facebook::axiom::optimizer::FunctionRegistry::registerPrestoFunctions();
}

} // namespace axiom::collagen
