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
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "velox/expression/ExprConstants.h"

namespace facebook::axiom::optimizer {

namespace lp = facebook::axiom::logical_plan;

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

void FunctionRegistry::registerNegation(std::string_view name) {
  VELOX_USER_CHECK(!name.empty());
  negation_ = name;
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
  auto idx = call.type()->as<velox::TypeKind::ROW>().getChildIdx(field);
  auto newFields = steps;
  newFields.pop_back();
  return std::make_pair(newFields, idx);
}

folly::F14FastMap<PathCP, lp::ExprPtr> rowConstructorExplode(
    const lp::CallExpr* call,
    std::vector<PathCP>& paths) {
  folly::F14FastMap<PathCP, lp::ExprPtr> result;
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

std::optional<Value> subscriptConstraint(ExprCP expr, PlanState& state) {
  // Check if expr is a call before casting
  if (expr->type() != PlanType::kCallExpr) {
    return std::nullopt;
  }
  auto* call = expr->as<Call>();
  if (call->args().size() != 2) {
    return std::nullopt;
  }

  const auto& firstArg = call->args()[0];
  const auto& secondArg = call->args()[1];
  const auto& firstValue = value(state, firstArg);

  // Check if first argument has children set
  if (!firstValue.children) {
    return std::nullopt;
  }

  auto typeKind = firstValue.type->kind();

  // Case 1: Array type - return the single child value
  if (typeKind == velox::TypeKind::ARRAY) {
    if (!firstValue.children->values.empty()) {
      return firstValue.children->values[0];
    }
    return std::nullopt;
  }

  // Case 2 & 3: Map or Row type with second argument as literal
  if (typeKind == velox::TypeKind::MAP || typeKind == velox::TypeKind::ROW) {
    // Check if second argument is a literal before casting
    if (secondArg->type() != PlanType::kLiteralExpr) {
      return std::nullopt;
    }
    auto* literal = secondArg->as<Literal>();

    // Case 2: Map or Row with named children
    if (!firstValue.children->names.empty()) {
      // Find the index where the name matches the literal value
      const auto& literalValue = literal->literal();

      for (size_t i = 0; i < firstValue.children->names.size(); ++i) {
        Name childName = firstValue.children->names[i];

        // Match the name with the literal
        if (literalValue.kind() == velox::TypeKind::VARCHAR ||
            literalValue.kind() == velox::TypeKind::VARBINARY) {
          if (std::string_view(childName) ==
              literalValue.value<velox::StringView>()) {
            if (i < firstValue.children->values.size()) {
              return firstValue.children->values[i];
            }
          }
        } else if (
            literalValue.kind() == velox::TypeKind::INTEGER ||
            literalValue.kind() == velox::TypeKind::BIGINT) {
          // For numeric literals on ROW types, match by field index
          if (typeKind == velox::TypeKind::ROW) {
            int64_t index = literalValue.kind() == velox::TypeKind::INTEGER
                ? literalValue.value<int32_t>()
                : literalValue.value<int64_t>();
            if (index >= 0 && static_cast<size_t>(index) == i &&
                i < firstValue.children->values.size()) {
              return firstValue.children->values[i];
            }
          }
        }
      }
      return std::nullopt;
    }

    // Case 3: Map with unnamed children (empty names) - return second child
    // (values)
    if (typeKind == velox::TypeKind::MAP &&
        firstValue.children->names.empty()) {
      if (firstValue.children->values.size() >= 2) {
        return firstValue.children->values[1];
      }
    }
  }

  return std::nullopt;
}

std::optional<Value> coalesceConstraint(ExprCP expr, PlanState& state) {
  // Check if expr is a call before casting
  if (expr->type() != PlanType::kCallExpr) {
    return std::nullopt;
  }
  auto* call = expr->as<Call>();
  if (call->args().empty()) {
    return std::nullopt;
  }

  // Return the Value of the first argument
  return value(state, call->args()[0]);
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

    // Add constraint function to propagate statistics through row construction
    metadata->functionConstraint =
        [](ExprCP expr, PlanState& state) -> std::optional<Value> {
      // Check if expr is a call before casting
      if (expr->type() != PlanType::kCallExpr) {
        return std::nullopt;
      }
      auto* call = expr->as<Call>();

      const auto& resultValue = value(state, expr);

      // Result must be a ROW type
      if (resultValue.type->kind() != velox::TypeKind::ROW) {
        return std::nullopt;
      }

      auto& rowType = resultValue.type->as<velox::TypeKind::ROW>();
      const auto& args = call->args();

      // Create a Value for the row with children
      Value result = resultValue;

      auto* childValues = make<ChildValues>();
      childValues->names.reserve(rowType.size());
      childValues->values.reserve(args.size());

      // Set the names from the row type and values from the arguments
      for (size_t i = 0; i < rowType.size() && i < args.size(); ++i) {
        childValues->names.push_back(toName(rowType.nameOf(i)));
        childValues->values.push_back(value(state, args[i]));
      }

      const_cast<const ChildValues*&>(result.children) = childValues;

      return result;
    };

    // Presto row_constructor created without prefix, so we register it without
    // prefix too.
    registerFunction("row_constructor", std::move(metadata));
  }

  {
    // Register subscript function with constraint function
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->functionConstraint = subscriptConstraint;
    metadata->cost = 0.2;

    // Presto subscript created without prefix, so we register it without prefix
    // too.
    registerFunction("subscript", std::move(metadata));
  }

  {
    // Register coalesce special form with constraint function
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->functionConstraint = coalesceConstraint;

    registerFunction(SpecialFormCallNames::kCoalesce, std::move(metadata));
  }

  {
    // Register map_keys with constraint function
    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->functionConstraint =
        [](ExprCP expr, PlanState& state) -> std::optional<Value> {
      // Check if expr is a call before casting
      if (expr->type() != PlanType::kCallExpr) {
        return std::nullopt;
      }
      auto* call = expr->as<Call>();
      if (call->args().empty()) {
        return std::nullopt;
      }

      // Get the Value from the first argument (the map)
      const auto& firstArgValue = value(state, call->args()[0]);

      // The first argument must be a map
      if (firstArgValue.type->kind() != velox::TypeKind::MAP) {
        return std::nullopt;
      }

      auto& mapType = firstArgValue.type->as<velox::TypeKind::MAP>();

      // Create a Value for the result array
      Value result = value(state, expr);

      // Set the size to the map's size
      const_cast<float&>(result.size) = firstArgValue.size;

      // Create children with one value representing the array element (the key
      // type)
      auto* childValues = make<ChildValues>();
      childValues->values.reserve(1);

      // Create a value for the array element (the map's key type)
      Value elementValue(mapType.keyType().get(), firstArgValue.size);
      childValues->values.push_back(elementValue);

      const_cast<const ChildValues*&>(result.children) = childValues;

      return result;
    };
    metadata->cost = 0.1;

    registerFunction(fullName("map_keys"), std::move(metadata));
  }

  auto* registry = FunctionRegistry::instance();

  registry->registerEquality(fullName("eq"));
  registry->registerNegation(fullName("not"));
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
  registry->registerSpecialForm(lp::SpecialForm::kAnd, velox::expression::kAnd);
  registry->registerSpecialForm(lp::SpecialForm::kOr, velox::expression::kOr);
  registry->registerSpecialForm(
      lp::SpecialForm::kCast, velox::expression::kCast);
  registry->registerSpecialForm(
      lp::SpecialForm::kTryCast, velox::expression::kTryCast);
  registry->registerSpecialForm(lp::SpecialForm::kTry, velox::expression::kTry);
  registry->registerSpecialForm(lp::SpecialForm::kIf, velox::expression::kIf);
  registry->registerSpecialForm(
      lp::SpecialForm::kCoalesce, velox::expression::kCoalesce);
  registry->registerSpecialForm(
      lp::SpecialForm::kSwitch, velox::expression::kSwitch);
  registry->registerSpecialForm(lp::SpecialForm::kIn, "in");
}

} // namespace facebook::axiom::optimizer
