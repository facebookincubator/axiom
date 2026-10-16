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

#include <grpcpp/grpcpp.h> // @manual=//grpc_fb/cpp:grpc
#include <memory>
#include <string>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/pyspark/CollagenService.h"
#include "velox/common/memory/CustomMemoryResourceRegistry.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/OutputTransportRegistry.h"

namespace axiom::collagen {

/// Owns the process resources and connector registries for one Collagen
/// server. Subclasses override registration hooks to install deployment-
/// specific runners, connectors, and functions before run() starts serving.
///
/// Connector and metadata registrations belong to this instance and complete
/// during init(). Every request uses one connector context and this server's
/// optimizer functions. The service configures LocalRunner query contexts from
/// the server's execution registries. A host subclass registers the
/// process-wide connector required by a delegated task backend. Velox execution
/// functions remain process-wide.
///
/// Example:
///   CollagenMain server{"local", "tpch", "tiny", 15'002};
///   server.init();
///   server.run();
///
/// Invariants:
///   - `init()` completes catalog registration before `run()` serves requests.
class CollagenMain {
 public:
  CollagenMain(
      std::string runnerId,
      std::string catalog,
      std::string schema,
      int port);

  virtual ~CollagenMain();

  virtual void init();

  void run();

 protected:
  virtual void initMemory();
  virtual void registerRunner();
  virtual void registerSerde();
  virtual void registerFileSystems();
  virtual void registerConnector();
  virtual void registerFunctions();

  void registerTpchConnector();
  void registerTestConnector();
  void registerLocalHiveConnector();
  // Registers an execution connector and its metadata as one catalog.
  void registerCatalog(
      std::shared_ptr<facebook::velox::connector::Connector> connector,
      std::shared_ptr<facebook::axiom::connector::ConnectorMetadata> metadata);

  const std::string runnerId_;
  const std::string catalog_;
  const std::string schema_;
  const int port_;

  // Supplies execution connectors to local queries.
  const std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_{
          facebook::velox::connector::ConnectorRegistry::create()};
  // Backs every request's connector context.
  const std::shared_ptr<
      facebook::axiom::connector::ConnectorMetadataRegistry::Registry>
      metadataRegistry_{
          facebook::axiom::connector::ConnectorMetadataRegistry::create()};
  // Defines function semantics for planning and local execution.
  const std::shared_ptr<facebook::axiom::optimizer::FunctionRegistry>
      functionRegistry_{
          facebook::axiom::optimizer::FunctionRegistry::createPresto()};
  // Routes output buffers created by local queries.
  const std::shared_ptr<
      facebook::velox::exec::OutputTransportRegistry::Registry>
      outputTransportRegistry_{
          facebook::velox::exec::OutputTransportRegistry::create()};
  // Routes exchange clients created by local queries.
  const std::shared_ptr<
      facebook::velox::exec::ExchangeTransportRegistry::Registry>
      exchangeTransportRegistry_{
          facebook::velox::exec::ExchangeTransportRegistry::create()};
  // Supplies custom memory resources to local queries.
  const std::shared_ptr<
      facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
      customMemoryResourceRegistry_{
          facebook::velox::memory::CustomMemoryResourceRegistry::createRegistry(
              nullptr)};

  std::unique_ptr<CollagenService> service_;
  std::unique_ptr<grpc::Server> server_;
};

} // namespace axiom::collagen
