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

#include <grpcpp/grpcpp.h> // @manual=//grpc_fb/cpp:grpc
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/pyspark/CollagenService.h"
#include "axiom/pyspark/runners/LocalRunner.h"
#include "axiom/pyspark/third-party/protos/base.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/common/memory/CustomMemoryResourceRegistry.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace axiom::collagen {
namespace {

using facebook::axiom::connector::TestConnector;

const std::string kCatalog{"shared"};
const std::string kRunner{"collagen_registry_ownership_runner"};

// Runs one Collagen service against an engine-owned in-memory catalog.
class ServiceHarness {
 public:
  explicit ServiceHarness(facebook::velox::RowVectorPtr data)
      : tableRootPool_{facebook::velox::memory::memoryManager()->addRootPool(
            "collagen_registry")},
        connector_{std::make_shared<TestConnector>(
            std::string{kCatalog},
            /*config=*/nullptr,
            tableRootPool_)},
        service_{
            std::string{kRunner},
            std::string{kCatalog},
            std::string{TestConnector::kDefaultSchema},
            connectorRegistry_,
            metadataRegistry_,
            functionRegistry_,
            outputTransportRegistry_,
            exchangeTransportRegistry_,
            customMemoryResourceRegistry_} {
    outputTransportRegistry_->insert(
        std::string{facebook::velox::core::TransportKind::kInMemory},
        facebook::velox::exec::DefaultOutputBufferManager::
            makeDefaultTransportEntry());
    exchangeTransportRegistry_->insert(
        std::string{facebook::velox::core::TransportKind::kInMemory},
        facebook::velox::exec::InMemoryExchangeClient::
            makeDefaultTransportEntry());
    connector_->addTable(
        "engine_data",
        facebook::velox::ROW("value", facebook::velox::BIGINT()));
    connector_->appendData("engine_data", data);
    facebook::axiom::connector::ConnectorMetadataRegistry::registerCatalog(
        *connectorRegistry_,
        *metadataRegistry_,
        connector_,
        connector_->metadata());

    grpc::ServerBuilder builder;
    int port{0};
    builder.AddListeningPort(
        "127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    VELOX_CHECK_NOT_NULL(server_);
    stub_ = spark::connect::SparkConnectService::NewStub(
        grpc::CreateChannel(
            fmt::format("127.0.0.1:{}", port),
            grpc::InsecureChannelCredentials()));
  }

  ~ServiceHarness() {
    server_->Shutdown();
    server_->Wait();
  }

  ServiceHarness(const ServiceHarness&) = delete;
  ServiceHarness& operator=(const ServiceHarness&) = delete;

  std::vector<int64_t> execute() {
    spark::connect::ExecutePlanRequest request;
    request.set_session_id("registry-ownership");
    request.mutable_plan()
        ->mutable_root()
        ->mutable_read()
        ->mutable_named_table()
        ->set_unparsed_identifier("engine_data");

    grpc::ClientContext context;
    auto reader = stub_->ExecutePlan(&context, request);
    spark::connect::ExecutePlanResponse response;
    std::vector<int64_t> result;
    while (reader->Read(&response)) {
      if (response.has_arrow_batch()) {
        const auto& data = response.arrow_batch().data();
        auto buffer = arrow::Buffer::Wrap(data.data(), data.size());
        auto maybeReader = arrow::ipc::RecordBatchStreamReader::Open(
            std::make_shared<arrow::io::BufferReader>(buffer));
        VELOX_CHECK(maybeReader.ok(), maybeReader.status().ToString());
        const auto batchReader = std::move(maybeReader).ValueOrDie();
        while (true) {
          auto maybeBatch = batchReader->Next();
          VELOX_CHECK(maybeBatch.ok(), maybeBatch.status().ToString());
          const auto batch = std::move(maybeBatch).ValueOrDie();
          if (!batch) {
            break;
          }
          const auto values =
              std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
          for (auto row = 0; row < batch->num_rows(); ++row) {
            result.push_back(values->Value(row));
          }
        }
      }
    }
    const auto status = reader->Finish();
    VELOX_CHECK(status.ok(), status.error_message());
    VELOX_CHECK(!result.empty());
    return result;
  }

 private:
  const std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_{
          facebook::velox::connector::ConnectorRegistry::create()};
  const std::shared_ptr<
      facebook::axiom::connector::ConnectorMetadataRegistry::Registry>
      metadataRegistry_{
          facebook::axiom::connector::ConnectorMetadataRegistry::create()};
  const std::shared_ptr<facebook::axiom::optimizer::FunctionRegistry>
      functionRegistry_{
          facebook::axiom::optimizer::FunctionRegistry::createPresto()};
  const std::shared_ptr<
      facebook::velox::exec::OutputTransportRegistry::Registry>
      outputTransportRegistry_{
          facebook::velox::exec::OutputTransportRegistry::create()};
  const std::shared_ptr<
      facebook::velox::exec::ExchangeTransportRegistry::Registry>
      exchangeTransportRegistry_{
          facebook::velox::exec::ExchangeTransportRegistry::create()};
  const std::shared_ptr<
      facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
      customMemoryResourceRegistry_{
          facebook::velox::memory::CustomMemoryResourceRegistry::createRegistry(
              nullptr)};
  const std::shared_ptr<facebook::velox::memory::MemoryPool> tableRootPool_;
  const std::shared_ptr<TestConnector> connector_;
  CollagenService service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<spark::connect::SparkConnectService::Stub> stub_;
};

class CollagenRegistryOwnershipTest
    : public testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    facebook::velox::memory::MemoryManager::testingSetInstance({});
    facebook::velox::functions::prestosql::registerAllScalarFunctions();
    facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();
    runner::registerLocalRunnerFactory(kRunner);
  }

  static void TearDownTestSuite() {
    facebook::velox::exec::test::waitForAllTasksToBeDeleted();
  }
};

// A service reads an engine-only catalog and leaves process-wide registries
// unchanged.
TEST_F(CollagenRegistryOwnershipTest, catalogResolvesThroughEngine) {
  EXPECT_EQ(
      facebook::velox::connector::ConnectorRegistry::tryGet(kCatalog), nullptr);
  EXPECT_EQ(
      facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(kCatalog),
      nullptr);

  {
    ServiceHarness service{makeRowVector({makeFlatVector<int64_t>({22})})};
    EXPECT_EQ(service.execute(), std::vector<int64_t>{22});
  }

  EXPECT_EQ(
      facebook::velox::connector::ConnectorRegistry::tryGet(kCatalog), nullptr);
  EXPECT_EQ(
      facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(kCatalog),
      nullptr);
}

} // namespace
} // namespace axiom::collagen
