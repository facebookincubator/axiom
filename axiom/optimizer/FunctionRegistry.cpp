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
#include "velox/expression/ExprConstants.h"

namespace facebook::velox::optimizer {
namespace lp = facebook::velox::logical_plan;

FunctionMetadataCP FunctionRegistry::metadata(std::string_view name) const {
  auto it = metadata_.find(name);
  if (it == metadata_.end()) {
    return nullptr;
  }
  return it->second.get();
}

bool FunctionRegistry::registerFunction(
    std::string_view name,
    std::unique_ptr<FunctionMetadata> metadata) {
  VELOX_USER_CHECK(!name.empty());
  return metadata_.emplace(name, std::move(metadata)).second;
}

void FunctionRegistry::registerEquality(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  equality_ = name;
}

bool FunctionRegistry::registerElementAt(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  if (elementAt_.has_value() && elementAt_.value() != name) {
    return false;
  }
  elementAt_ = name;
  return true;
}

bool FunctionRegistry::registerSubscript(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  if (subscript_.has_value() && subscript_.value() != name) {
    return false;
  }
  subscript_ = name;
  return true;
}

bool FunctionRegistry::registerCardinality(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  if (cardinality_.has_value() && cardinality_.value() != name) {
    return false;
  }
  cardinality_ = name;
  return true;
}

bool FunctionRegistry::registerSpecialForm(
    lp::SpecialForm specialForm,
    std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  return specialForms_.emplace(specialForm, name).second;
}

bool FunctionRegistry::registerReversibleFunction(
    std::string_view name,
    std::string_view reverseName) {
  VELOX_USER_CHECK(!name.empty());
  VELOX_USER_CHECK(!reverseName.empty());
  return reversibleFunctions_.emplace(name, reverseName).second;
}

bool FunctionRegistry::registerReversibleFunction(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  return reversibleFunctions_.emplace(name, name).second;
}

// static
FunctionRegistry* FunctionRegistry::instance() {
  static std::unique_ptr<FunctionRegistry> registry{new FunctionRegistry{}};
  return registry.get();
}

FunctionMetadataCP functionMetadata(std::string_view name) {
  return FunctionRegistry::instance()->metadata(name);
}

const std::string& specialForm(lp::SpecialForm specialForm) {
  return FunctionRegistry::instance()->specialForm(specialForm);
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

// static
void FunctionRegistry::registerPrestoFunctions(std::string_view prefix) {
  auto fullName = [&](std::string_view name) {
    return prefix.empty() ? std::string(name)
                          : fmt::format("{}{}", prefix, name);
  };

  auto registerFunction = [&](std::string_view name,
                              std::unique_ptr<FunctionMetadata> metadata) {
    FunctionRegistry::instance()->registerFunction(name, std::move(metadata));
  };

  {
    LambdaInfo info{
        .ordinal = 1,
        .lambdaArg = {LambdaArg::kKey, LambdaArg::kValue},
        .argOrdinal = {0, 0},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 40;
    registerFunction(fullName("transform_values"), std::move(metadata));
  }

  {
    LambdaInfo info{
        .ordinal = 1,
        .lambdaArg = {LambdaArg::kElement},
        .argOrdinal = {0},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->subfieldArg = 0;
    metadata->cost = 20;
    registerFunction(fullName("transform"), std::move(metadata));
  }

  {
    LambdaInfo info{
        .ordinal = 2,
        .lambdaArg = {LambdaArg::kElement, LambdaArg::kElement},
        .argOrdinal = {0, 1},
    };

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->lambdas.push_back(std::move(info));
    metadata->cost = 20;
    registerFunction(fullName("zip"), std::move(metadata));
  }

  {
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->valuePathToArgPath = rowConstructorSubfield;
    metadata->explode = rowConstructorExplode;
    // Presto row_constructor created without prefix, so we register it without
    // prefix too.
    registerFunction("row_constructor", std::move(metadata));
  }

  auto* registry = FunctionRegistry::instance();

  registry->registerEquality(fullName("eq"));
  registry->registerElementAt(fullName("element_at"));
  registry->registerSubscript(fullName("subscript"));
  registry->registerCardinality(fullName("cardinality"));

  registry->registerReversibleFunction(fullName("eq"));
  registry->registerReversibleFunction(fullName("lt"), fullName("gt"));
  registry->registerReversibleFunction(fullName("lte"), fullName("gte"));
  registry->registerReversibleFunction(fullName("plus"));
  registry->registerReversibleFunction(fullName("multiply"));

  // Presto special form functions created without prefix, so we register them
  // without prefix too.
  registry->registerSpecialForm(lp::SpecialForm::kAnd, expression::kAnd);
  registry->registerSpecialForm(lp::SpecialForm::kOr, expression::kOr);
  registry->registerSpecialForm(lp::SpecialForm::kCast, expression::kCast);
  registry->registerSpecialForm(
      lp::SpecialForm::kTryCast, expression::kTryCast);
  registry->registerSpecialForm(lp::SpecialForm::kTry, expression::kTry);
  registry->registerSpecialForm(lp::SpecialForm::kIf, expression::kIf);
  registry->registerSpecialForm(
      lp::SpecialForm::kCoalesce, expression::kCoalesce);
  registry->registerSpecialForm(lp::SpecialForm::kSwitch, expression::kSwitch);
  registry->registerSpecialForm(lp::SpecialForm::kIn, "in");
}

} // namespace facebook::velox::optimizer
