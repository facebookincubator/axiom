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

#include "axiom/pyspark/CollagenService.h"

#include <folly/json.h>
#include <glog/logging.h>

#include "axiom/optimizer/ToVelox.h"
#include "axiom/pyspark/ArrowIpcLib.h"
#include "axiom/pyspark/Exception.h"
#include "axiom/pyspark/Optimizer.h"
#include "axiom/pyspark/SparkPlanPrinter.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/runners/Runner.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/QueryCtx.h"

using namespace ::facebook;
using facebook::axiom::optimizer::PlanAndStats;

DEFINE_bool(skip_run, false, "Skip running the query and return.");

namespace {

std::vector<facebook::velox::RowVectorPtr> getDummyRowVector(
    facebook::velox::memory::MemoryPool* pool) {
  static auto responseType =
      facebook::velox::ROW({"show_string"}, {facebook::velox::VARCHAR()});
  static auto response = {
      facebook::velox::RowVector::createEmpty(responseType, pool)};
  return response;
}

::axiom::collagen::runner::QueryCtxFactory makeQueryCtxFactory(
    std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
        connectorRegistry,
    std::shared_ptr<facebook::velox::exec::OutputTransportRegistry::Registry>
        outputTransportRegistry,
    std::shared_ptr<facebook::velox::exec::ExchangeTransportRegistry::Registry>
        exchangeTransportRegistry,
    std::shared_ptr<
        facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
        customMemoryResourceRegistry) {
  return
      [connectorRegistry = std::move(connectorRegistry),
       outputTransportRegistry = std::move(outputTransportRegistry),
       exchangeTransportRegistry = std::move(exchangeTransportRegistry),
       customMemoryResourceRegistry = std::move(customMemoryResourceRegistry)](
          folly::Executor* executor,
          const std::unordered_map<std::string, std::string>& queryConfigs,
          const std::shared_ptr<facebook::velox::memory::MemoryPool>& pool) {
        auto queryCtx = facebook::velox::core::QueryCtx::create(
            executor,
            facebook::velox::core::QueryConfig{queryConfigs},
            /*connectorConfigs=*/{},
            facebook::velox::cache::AsyncDataCache::getInstance(),
            pool,
            /*spillExecutor=*/nullptr);
        queryCtx->setRegistry(
            facebook::velox::connector::ConnectorRegistry::kRegistryKey,
            connectorRegistry);
        queryCtx->setRegistry(
            facebook::velox::exec::OutputTransportRegistry::kRegistryKey,
            outputTransportRegistry);
        queryCtx->setRegistry(
            facebook::velox::exec::ExchangeTransportRegistry::kRegistryKey,
            exchangeTransportRegistry);
        queryCtx->setRegistry(
            facebook::velox::memory::kCustomMemoryResourceRegistryKey,
            customMemoryResourceRegistry);
        return queryCtx;
      };
}

} // namespace

namespace axiom::collagen {

CollagenService::CollagenService(
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
    std::shared_ptr<facebook::velox::exec::ExchangeTransportRegistry::Registry>
        exchangeTransportRegistry,
    std::shared_ptr<
        facebook::velox::memory::CustomMemoryResourceRegistry::Registry>
        customMemoryResourceRegistry)
    : runnerId_(std::move(runnerId)),
      catalog_(std::move(catalog)),
      schema_(std::move(schema)),
      connectorRegistry_(std::move(connectorRegistry)),
      metadataRegistry_(std::move(metadataRegistry)),
      functionRegistry_(std::move(functionRegistry)),
      outputTransportRegistry_(std::move(outputTransportRegistry)),
      exchangeTransportRegistry_(std::move(exchangeTransportRegistry)),
      customMemoryResourceRegistry_(std::move(customMemoryResourceRegistry)) {
  VELOX_CHECK_NOT_NULL(connectorRegistry_);
  VELOX_CHECK_NOT_NULL(metadataRegistry_);
  VELOX_CHECK_NOT_NULL(functionRegistry_);
  VELOX_CHECK_NOT_NULL(outputTransportRegistry_);
  VELOX_CHECK_NOT_NULL(exchangeTransportRegistry_);
  VELOX_CHECK_NOT_NULL(customMemoryResourceRegistry_);
  if (rootPool_->reclaimer() == nullptr) {
    rootPool_->setReclaimer(facebook::velox::memory::MemoryReclaimer::create());
  }
}

facebook::axiom::connector::ConnectorContextPtr
CollagenService::makeConnectorContext() {
  return std::make_shared<facebook::axiom::connector::ConnectorContext>(
      /*queryId=*/"",
      /*user=*/"collagen",
      facebook::axiom::connector::ConnectorProperties{},
      facebook::axiom::connector::ConnectorContext::noopStatWriterProvider(),
      metadataRegistry_);
}

// In Spark Connect, Relation represents any execution plan node.
PlanAndStats CollagenService::plan(
    const spark::connect::Plan& plan,
    std::string& logicalPlanStr,
    const facebook::axiom::connector::ConnectorContextPtr& connectorContext) {
  auto startTime = std::chrono::high_resolution_clock::now();

  // TODO: just for debugging for now.
  LOG(INFO) << "Spark Connect logical plan:\n" << printSparkPlan(plan);

  LOG(INFO) << "Converting to Axiom plan:";
  SparkToAxiom converter(catalog_, schema_, pool_.get(), connectorContext);
  SparkPlanVisitorContext axiomContext;
  converter.visit(plan, axiomContext);

  logicalPlanStr = converter.toString();
  LOG(INFO) << "Axiom logical input:\n" << logicalPlanStr;

  // Optimize it into a Velox physical plan.
  auto veloxPlanAndStats = optimize(
      converter.planNode(),
      catalog_,
      pool_.get(),
      connectorContext,
      functionRegistry_);

  LOG(INFO) << "Generated Velox physical plan with "
            << veloxPlanAndStats.plan->fragments().size() << " fragments:\n"
            << veloxPlanAndStats.plan->toString();

  LOG(INFO) << "Plan generation took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - startTime)
                   .count()
            << "ms";
  return veloxPlanAndStats;
}

grpc::Status CollagenService::ExecutePlan(
    grpc::ServerContext* context,
    const spark::connect::ExecutePlanRequest* request,
    grpc::ServerWriter<spark::connect::ExecutePlanResponse>* writer) {
  LOG(INFO) << "Got execute plan request (session_id=" << request->session_id()
            << ").";
  // Set execute plan response structure.
  spark::connect::ExecutePlanResponse planResponse;
  planResponse.set_session_id(request->session_id());

  std::string logicalPlanStr;
  PlanAndStats veloxPlanStats;
  facebook::axiom::connector::ConnectorContextPtr connectorContext;

  try {
    connectorContext = makeConnectorContext();
    veloxPlanStats = plan(request->plan(), logicalPlanStr, connectorContext);

    if (veloxPlanStats.plan == nullptr) {
      return grpc::Status(
          grpc::StatusCode::INTERNAL,
          "Failed to generate Velox plan from Spark Connect plan.");
    }
  } catch (const CollagenException& e) {
    LOG(WARNING) << "Request failed:\n" << e.what();
    return e.status();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Unexpected exception: " << e.what();
    return grpc::Status(grpc::StatusCode::UNKNOWN, e.what());
  }

  auto runner = runner::buildRunner(
      runnerId_,
      "Test",
      veloxPlanStats.plan,
      std::move(veloxPlanStats.finishWrite),
      connectorContext,
      functionRegistry_,
      makeQueryCtxFactory(
          connectorRegistry_,
          outputTransportRegistry_,
          exchangeTransportRegistry_,
          customMemoryResourceRegistry_),
      rootPool_);
  std::vector<velox::RowVectorPtr> results;

  // TODO: these configs should be somewhere else. We should also allow clients
  // to set/overwrite them.
  std::unordered_map<std::string, std::string> queryConfigs = {
      {"selective_nimble_reader_enabled", "true"},
  };

  try {
    if (FLAGS_skip_run) {
      results = getDummyRowVector(pool_.get());
    } else {
      results = runner->execute(queryConfigs);
    }

    auto* arrowBatch = planResponse.mutable_arrow_batch();
    uint64_t rowCount;
    auto outputBuffer = toArrowIpc(results, rowCount);
    arrowBatch->set_data(outputBuffer->data(), outputBuffer->size());
    arrowBatch->set_row_count(rowCount);
    writer->Write(planResponse);
    LOG(INFO) << "Executed successfully got " << rowCount << " rows.";
    return grpc::Status::OK;
  } catch (const CollagenException& e) {
    LOG(WARNING) << "Request failed:\n" << e.what();
    return e.status();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Unexpected exception: " << e.what();
    return grpc::Status(grpc::StatusCode::UNKNOWN, e.what());
  }
}

grpc::Status CollagenService::AnalyzePlan(
    grpc::ServerContext* context,
    const spark::connect::AnalyzePlanRequest* request,
    spark::connect::AnalyzePlanResponse* response) {
  LOG(INFO) << "Got analyze plan request (session_id=" << request->session_id()
            << ").";
  response->set_session_id(request->session_id());

  PlanAndStats veloxPlanStats;
  try {
    std::string logicalPlanStr;
    switch (request->analyze_case()) {
      case spark::connect::AnalyzePlanRequest::kExplain:
        veloxPlanStats = plan(
            request->explain().plan(), logicalPlanStr, makeConnectorContext());
        break;

      default:
        break;
    }
    auto* explain = response->mutable_explain();

    // Build the complete explain string with both logical and physical plans
    std::string fullExplainStr = logicalPlanStr;

    // Append the Velox physical plan (MultiFragmentPlan) if available
    if (veloxPlanStats.plan != nullptr) {
      std::string veloxPlanStr = veloxPlanStats.plan->toString();
      fullExplainStr += "\n\n== Velox Physical Plan (MultiFragmentPlan) ==\n";
      fullExplainStr += veloxPlanStr;

      for (const auto& execFragment : veloxPlanStats.plan->fragments()) {
        fullExplainStr += "\n\n== Velox Plan JSON ==\n";
        fullExplainStr +=
            folly::toJson(execFragment.fragment.planNode->serialize());
      }

      // Also set the separate field for programmatic access
      explain->set_velox_plan_string(veloxPlanStr);
    }

    explain->set_explain_string(fullExplainStr);
    return grpc::Status::OK;
  } catch (const CollagenException& e) {
    veloxPlanStats.plan = nullptr;
    LOG(WARNING) << "Request failed:\n" << e.what();
    return e.status();
  } catch (const std::exception& e) {
    veloxPlanStats.plan = nullptr;
    LOG(WARNING) << "Unexpected exception: " << e.what();
    return grpc::Status(grpc::StatusCode::UNKNOWN, e.what());
  }
}

grpc::Status CollagenService::Config(
    grpc::ServerContext* context,
    const spark::connect::ConfigRequest* request,
    spark::connect::ConfigResponse* response) {
  LOG(INFO) << "Got config request (session_id=" << request->session_id()
            << ").";
  return grpc::Status(
      grpc::StatusCode::UNIMPLEMENTED, "Config() method not implemented yet.");
}

grpc::Status CollagenService::AddArtifacts(
    grpc::ServerContext* context,
    grpc::ServerReader<spark::connect::AddArtifactsRequest>* request,
    spark::connect::AddArtifactsResponse* response) {
  LOG(INFO) << "Got add artifacts request.";
  return grpc::Status(
      grpc::StatusCode::UNIMPLEMENTED,
      "AddArtifacts() method not implemented yet.");
}

} // namespace axiom::collagen
