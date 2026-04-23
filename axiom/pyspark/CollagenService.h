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

#include "axiom/pyspark/third-party/protos/base.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::axiom::optimizer {
struct PlanAndStats;
}

namespace axiom::collagen {

class CollagenService final
    : public spark::connect::SparkConnectService::Service {
 public:
  CollagenService(
      std::string runnerId,
      std::string catalog,
      std::string schema);

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
      std::string& logicalPlanStr);

  // Runner id (e.g., "local").
  const std::string runnerId_;
  // Catalog name (e.g., "tpch", "local-hive", "test").
  const std::string catalog_;
  // Schema to prepend to table names (e.g., "tiny" for TPCH).
  const std::string schema_;

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
