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

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::velox::optimizer {
namespace lp = facebook::velox::logical_plan;

FunctionMetadataCP FunctionRegistry::metadata(std::string_view name) const {
  auto it = metadata_.find(name);
  if (it == metadata_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void FunctionRegistry::registerFunction(
    std::string_view name,
    std::unique_ptr<FunctionMetadata> metadata) {
  metadata_[name] = std::move(metadata);
}

// static
FunctionRegistry* FunctionRegistry::instance() {
  static auto registry = std::make_unique<FunctionRegistry>();
  return registry.get();
}

FunctionMetadataCP functionMetadata(std::string_view name) {
  return FunctionRegistry::instance()->metadata(name);
}

namespace {
std::pair<std::vector<Step>, int32_t> rowConstructorSubfield(
    const std::vector<Step>& steps,
    const lp::CallExpr& call) {
  VELOX_CHECK(steps.back().kind == StepKind::kField);
  auto field = steps.back().field;
  auto idx = call.type()->as<TypeKind::ROW>().getChildIdx(field);
  auto newFields = steps;
  newFields.pop_back();
  return std::make_pair(newFields, idx);
}

std::unordered_map<PathCP, lp::ExprPtr> rowConstructorExplode(
    const lp::CallExpr* call,
    std::vector<PathCP>& paths) {
  std::unordered_map<PathCP, lp::ExprPtr> result;
  for (auto& path : paths) {
    const auto& steps = path->steps();
    if (steps.empty()) {
      return {};
    }
    const auto* prefixPath = toPath({steps.data(), 1});
    auto [it, emplaced] = result.try_emplace(prefixPath);
    if (!emplaced) {
      // There already is an expression for this path.
      continue;
    }
    VELOX_CHECK(steps.front().kind == StepKind::kField);
    auto nth = steps.front().id;
    it->second = call->inputAt(nth);
  }
  return result;
}
} // namespace

bool declareBuiltIn() {
  {
    LambdaInfo info{
        .ordinal = 1,
        .lambdaArg = {LambdaArg::kKey, LambdaArg::kValue},
        .argOrdinal = {0, 0}};

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 40;
    FunctionRegistry::instance()->registerFunction(
        "transform_values", std::move(metadata));
  }
  {
    LambdaInfo info{
        .ordinal = 1, .lambdaArg = {LambdaArg::kElement}, .argOrdinal = {0}};

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 20;
    FunctionRegistry::instance()->registerFunction(
        "transform", std::move(metadata));
  }
  {
    LambdaInfo info{
        .ordinal = 2,
        .lambdaArg = {LambdaArg::kElement, LambdaArg::kElement},
        .argOrdinal = {0, 1}};

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->cost = 20;
    FunctionRegistry::instance()->registerFunction("zip", std::move(metadata));
  }
  {
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->valuePathToArgPath = rowConstructorSubfield;
    metadata->explode = rowConstructorExplode;
    FunctionRegistry::instance()->registerFunction(
        "row_constructor", std::move(metadata));
  }
  return true;
}

namespace {
bool temp = declareBuiltIn();
}

} // namespace facebook::velox::optimizer
