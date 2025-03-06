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

#include "optimizer/FunctionRegistry.h" //@manual

namespace facebook::velox::optimizer {

FunctionMetadata* FunctionRegistry::metadata(const std::string& name) {
  auto it = metadata_.find(name);
  if (it == metadata_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void FunctionRegistry::registerFunction(
    const std::string& name,
    std::unique_ptr<FunctionMetadata> metadata) {
  metadata_[name] = std::move(metadata);
}

// static
FunctionRegistry* FunctionRegistry::instance() {
  static auto registry = std::make_unique<FunctionRegistry>();
  return registry.get();
}

bool declareBuiltin() {
  auto metadata = std::make_unique<FunctionMetadata>();
  LambdaInfo info;
  info.ordinal = 1;
  info.lambdaArg = {LambdaArg::kKey, LambdaArg::kValue};
  info.argOrdinal = {0, 0};
  metadata->lambdas.push_back(std::move(info));
  metadata->subfieldArg = 0;
  FunctionRegistry::instance()->registerFunction(
      "transform_values", std::move(metadata));
  return true;
}

} // namespace facebook::velox::optimizer
