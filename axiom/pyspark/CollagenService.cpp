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

#include "axiom/optimizer/PlanAndStats.h"
#include "axiom/pyspark/ArrowIpcLib.h"
#include "axiom/pyspark/Optimizer.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/runners/Runner.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

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

// Returns the gRPC status for a failed request. A Velox error carries whether
// the request was wrong, asked for something unsupported, or hit a bug.
grpc::Status toStatus(const std::exception& e) {
  const auto* veloxError =
      dynamic_cast<const facebook::velox::VeloxException*>(&e);
  if (veloxError == nullptr) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
  const auto& code = veloxError->errorCode();
  if (code == facebook::velox::error_code::kNotImplemented ||
      code == facebook::velox::error_code::kUnsupported) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, veloxError->message());
  }
  if (veloxError->exceptionType() ==
      facebook::velox::VeloxException::Type::kUser) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT, veloxError->message());
  }
  return grpc::Status(grpc::StatusCode::INTERNAL, veloxError->message());
}

// Returns the Spark type for a Velox type. Velox types carry no nullability,
// so every field, element and map value is nullable.
spark::connect::DataType toSparkType(const facebook::velox::TypePtr& type) {
  spark::connect::DataType result;
  if (type->isDate()) {
    result.mutable_date();
    return result;
  }
  if (type->isDecimal()) {
    const auto [precision, scale] =
        facebook::velox::getDecimalPrecisionScale(*type);
    result.mutable_decimal()->set_precision(precision);
    result.mutable_decimal()->set_scale(scale);
    return result;
  }
  switch (type->kind()) {
    case facebook::velox::TypeKind::UNKNOWN:
      result.mutable_null();
      break;
    case facebook::velox::TypeKind::BOOLEAN:
      result.mutable_boolean();
      break;
    case facebook::velox::TypeKind::TINYINT:
      result.mutable_byte();
      break;
    case facebook::velox::TypeKind::SMALLINT:
      result.mutable_short_();
      break;
    case facebook::velox::TypeKind::INTEGER:
      result.mutable_integer();
      break;
    case facebook::velox::TypeKind::BIGINT:
      result.mutable_long_();
      break;
    case facebook::velox::TypeKind::REAL:
      result.mutable_float_();
      break;
    case facebook::velox::TypeKind::DOUBLE:
      result.mutable_double_();
      break;
    case facebook::velox::TypeKind::VARCHAR:
      result.mutable_string();
      break;
    case facebook::velox::TypeKind::VARBINARY:
      result.mutable_binary();
      break;
    case facebook::velox::TypeKind::TIMESTAMP:
      result.mutable_timestamp();
      break;
    case facebook::velox::TypeKind::ARRAY: {
      auto* array = result.mutable_array();
      *array->mutable_element_type() = toSparkType(type->childAt(0));
      array->set_contains_null(true);
      break;
    }
    case facebook::velox::TypeKind::MAP: {
      auto* map = result.mutable_map();
      *map->mutable_key_type() = toSparkType(type->childAt(0));
      *map->mutable_value_type() = toSparkType(type->childAt(1));
      map->set_value_contains_null(true);
      break;
    }
    case facebook::velox::TypeKind::ROW: {
      const auto& rowType = type->asRow();
      auto* fields = result.mutable_struct_();
      for (uint32_t i = 0; i < rowType.size(); ++i) {
        auto* field = fields->add_fields();
        field->set_name(rowType.nameOf(i));
        *field->mutable_data_type() = toSparkType(rowType.childAt(i));
        field->set_nullable(true);
      }
      break;
    }
    default:
      VELOX_NYI("Type is not supported in a schema: {}", type->toString());
  }
  return result;
}

} // namespace

namespace axiom::collagen {

CollagenService::CollagenService(
    std::string runnerId,
    std::string catalog,
    std::string schema,
    std::shared_ptr<const facebook::axiom::optimizer::FunctionRegistry>
        functionRegistry)
    : runnerId_(std::move(runnerId)),
      catalog_(std::move(catalog)),
      schema_(std::move(schema)),
      functionRegistry_(std::move(functionRegistry)) {
  if (rootPool_->reclaimer() == nullptr) {
    rootPool_->setReclaimer(facebook::velox::memory::MemoryReclaimer::create());
  }
}

// In Spark Connect, Relation represents any execution plan node.
PlanAndStats CollagenService::plan(
    const spark::connect::Plan& plan,
    std::string& logicalPlanStr) {
  SparkToAxiom converter(catalog_, schema_, pool_.get());
  SparkPlanVisitorContext axiomContext;
  converter.visit(plan, axiomContext);

  logicalPlanStr = converter.toString();

  return optimize(
      converter.planNode(), catalog_, pool_.get(), functionRegistry_);
}

grpc::Status CollagenService::ExecutePlan(
    grpc::ServerContext* context,
    const spark::connect::ExecutePlanRequest* request,
    grpc::ServerWriter<spark::connect::ExecutePlanResponse>* writer) {
  // Set execute plan response structure.
  spark::connect::ExecutePlanResponse planResponse;
  planResponse.set_session_id(request->session_id());

  std::string logicalPlanStr;
  PlanAndStats veloxPlanStats;

  try {
    veloxPlanStats = plan(request->plan(), logicalPlanStr);

    if (veloxPlanStats.plan == nullptr) {
      return grpc::Status(
          grpc::StatusCode::INTERNAL,
          "Failed to generate Velox plan from Spark Connect plan.");
    }
  } catch (const std::exception& e) {
    return toStatus(e);
  }

  auto runner = runner::buildRunner(
      runnerId_,
      "Test",
      veloxPlanStats.plan,
      std::move(veloxPlanStats.finishWrite),
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
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return toStatus(e);
  }
}

grpc::Status CollagenService::AnalyzePlan(
    grpc::ServerContext* context,
    const spark::connect::AnalyzePlanRequest* request,
    spark::connect::AnalyzePlanResponse* response) {
  response->set_session_id(request->session_id());

  PlanAndStats veloxPlanStats;
  try {
    if (request->analyze_case() ==
        spark::connect::AnalyzePlanRequest::kSchema) {
      SparkToAxiom converter(catalog_, schema_, pool_.get());
      SparkPlanVisitorContext context;
      converter.visit(request->schema().plan(), context);
      *response->mutable_schema()->mutable_schema() =
          toSparkType(converter.planNode()->outputType());
      return grpc::Status::OK;
    }

    std::string logicalPlanStr;
    switch (request->analyze_case()) {
      case spark::connect::AnalyzePlanRequest::kExplain:
        veloxPlanStats = plan(request->explain().plan(), logicalPlanStr);
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

      for (const auto& stage : veloxPlanStats.plan->stages()) {
        fullExplainStr += "\n\n== Velox Plan JSON ==\n";
        fullExplainStr += folly::toJson(stage.plan->serialize());
      }

      // Also set the separate field for programmatic access
      explain->set_velox_plan_string(veloxPlanStr);
    }

    explain->set_explain_string(fullExplainStr);
    return grpc::Status::OK;
  } catch (const std::exception& e) {
    return toStatus(e);
  }
}

grpc::Status CollagenService::Config(
    grpc::ServerContext* context,
    const spark::connect::ConfigRequest* request,
    spark::connect::ConfigResponse* response) {
  return grpc::Status(
      grpc::StatusCode::UNIMPLEMENTED, "Config() method not implemented yet.");
}

grpc::Status CollagenService::AddArtifacts(
    grpc::ServerContext* context,
    grpc::ServerReader<spark::connect::AddArtifactsRequest>* request,
    spark::connect::AddArtifactsResponse* response) {
  return grpc::Status(
      grpc::StatusCode::UNIMPLEMENTED,
      "AddArtifacts() method not implemented yet.");
}

} // namespace axiom::collagen
