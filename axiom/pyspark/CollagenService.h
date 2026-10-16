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

#pragma once

#include <folly/io/async/AsyncSignalHandler.h>
#include <grpcpp/grpcpp.h> // @manual=//grpc_fb/cpp:grpc
#include <signal.h>
#include <string>

#include "axiom/connectors/ConnectorContext.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/pyspark/third-party/protos/base.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/CustomMemoryResourceRegistry.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/OutputTransportRegistry.h"

namespace facebook::axiom::optimizer {
struct PlanAndStats;
}

namespace axiom::collagen {

/// Implements the Spark Connect API for one Collagen server.
///
/// Each request creates one connector context and carries it through Spark
/// conversion, Axiom optimization, and runner construction. Planning receives
/// the server's function registry. The service supplies LocalRunner with a
/// factory that creates a Velox query context from its execution registries.
///
/// Example:
///   CollagenService service{
///       "local",
///       "tpch",
///       "tiny",
///       connectorRegistry,
///       metadataRegistry,
///       functionRegistry,
///       outputTransportRegistry,
///       exchangeTransportRegistry,
///       customMemoryResourceRegistry};
///   builder.RegisterService(&service);
///
/// Invariants:
///   - All registry pointers are non-null.
class CollagenService final
    : public spark::connect::SparkConnectService::Service {
 public:
  CollagenService(
      std::string runnerId,
      std::string catalog,
      std::string schema,
      std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
          connectorRegistry,
      std::shared_ptr<
          facebook::axiom::connector::ConnectorMetadataRegistry::Registry>
          metadataRegistry,
      std::shared_ptr<const facebook::axiom::optimizer::FunctionRegistry>
          functionRegistry,
      std::shared_ptr<facebook::velox::exec::OutputTransportRegistry::Registry>
          outputTransportRegistry,
      std::shared_ptr<
          facebook::velox::exec::ExchangeTransportRegistry::Registry>
          exchangeTransportRegistry,
      std::shared_ptr<
          facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
          customMemoryResourceRegistry);

  ~CollagenService() = default;

  // External grpc API:
  grpc::Status ExecutePlan(
      grpc::ServerContext* context,
      const spark::connect::ExecutePlanRequest* request,
      grpc::ServerWriter<spark::connect::ExecutePlanResponse>* writer) override;

  grpc::Status AnalyzePlan(
      grpc::ServerContext* context,
      const spark::connect::AnalyzePlanRequest* request,
      spark::connect::AnalyzePlanResponse* response) override;

  grpc::Status Config(
      grpc::ServerContext* context,
      const spark::connect::ConfigRequest* request,
      spark::connect::ConfigResponse* response) override;

  grpc::Status AddArtifacts(
      grpc::ServerContext* context,
      grpc::ServerReader<spark::connect::AddArtifactsRequest>* request,
      spark::connect::AddArtifactsResponse* response) override;

 private:
  using PlanAndStats = facebook::axiom::optimizer::PlanAndStats;

  PlanAndStats plan(
      const spark::connect::Plan& plan,
      std::string& logicalPlanStr,
      const facebook::axiom::connector::ConnectorContextPtr& connectorContext);

  // Creates a metadata context backed by this service's catalog registry.
  facebook::axiom::connector::ConnectorContextPtr makeConnectorContext();

  // Runner id (e.g., "local").
  const std::string runnerId_;
  // Catalog name (e.g., "tpch", "local-hive", "test").
  const std::string catalog_;
  // Schema to prepend to table names (e.g., "tiny" for TPCH).
  const std::string schema_;
  // Supplies execution connectors to local runners.
  const std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_;
  // Backs every request's connector context.
  const std::shared_ptr<
      facebook::axiom::connector::ConnectorMetadataRegistry::Registry>
      metadataRegistry_;
  // Defines function semantics for planning and local execution.
  const std::shared_ptr<const facebook::axiom::optimizer::FunctionRegistry>
      functionRegistry_;
  // Routes output buffers created by local queries.
  const std::shared_ptr<
      facebook::velox::exec::OutputTransportRegistry::Registry>
      outputTransportRegistry_;
  // Routes exchange clients created by local queries.
  const std::shared_ptr<
      facebook::velox::exec::ExchangeTransportRegistry::Registry>
      exchangeTransportRegistry_;
  // Supplies custom memory resources to local queries.
  const std::shared_ptr<
      facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
      customMemoryResourceRegistry_;

  using MemoryPool = facebook::velox::memory::MemoryPool;

  std::shared_ptr<MemoryPool> rootPool_{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<MemoryPool> pool_{rootPool_->addLeafChild("collagen_server")};
};

class CollagenSignalHandler : public folly::AsyncSignalHandler {
 public:
  explicit CollagenSignalHandler(folly::EventBase* eb, grpc::Server* server)
      : folly::AsyncSignalHandler(eb), server_(server) {
    // Catch only SIGTERM.
    registerSignalHandler(SIGTERM);
  }

  void signalReceived(int signum) noexcept override {
    LOG(INFO) << "CollagenSignalHandler received signal: " << signum
              << ". Shutting down...";
    server_->Shutdown();
  }

 private:
  grpc::Server* server_;
};

} // namespace axiom::collagen
