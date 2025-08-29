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

#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer::test {

namespace lp = facebook::velox::logical_plan;

namespace {
std::unordered_map<std::string, lp::ExprResolver::FunctionRewriteHook>
    functionHooks;

void registerFeatureFuncHook(
    const std::string& name,
    lp::ExprResolver::FunctionRewriteHook hook) {
  functionHooks[name] = hook;
}

std::pair<std::vector<Step>, int32_t> makeRowFromMapSubfield(
    const std::vector<Step>& steps,
    const lp::CallExpr& call) {
  VELOX_CHECK(steps.back().kind == StepKind::kField);
  auto& list = call.inputAt(2)
                   ->asUnchecked<lp::ConstantExpr>()
                   ->value()
                   ->value<TypeKind::ARRAY>();
  auto field = steps.back().field;
  int32_t len = strlen(field);
  int32_t found = -1;
  for (auto i = 0; i < list.size(); ++i) {
    auto& name = list[i].value<TypeKind::VARCHAR>();
    if (name.size() != len) {
      continue;
    }
    if (memcmp(name.data(), field, len) == 0) {
      found = i;
      break;
    }
  }
  VELOX_CHECK(
      found != -1, "Subfield not found in make_row_from_map: {}", field);

  auto newFields = steps;
  newFields.pop_back();
  newFields.push_back(optimizer::Step{
      .kind = optimizer::StepKind::kSubscript,
      .id = call.inputAt(1)
                ->asUnchecked<lp::ConstantExpr>()
                ->value()
                ->value<TypeKind::ARRAY>()[found]
                .value<int32_t>()});
  return std::make_pair(newFields, 0);
}

std::unordered_map<PathCP, lp::ExprPtr> makeRowFromMapExplodeGeneric(
    const lp::CallExpr* call,
    std::vector<PathCP>& paths,
    bool addCoalesce) {
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
    auto type = call->type()->childAt(0);
    auto subscriptType = call->inputAt(1)->type()->childAt(0);
    auto keys = call->inputAt(1)
                    ->asUnchecked<lp::ConstantExpr>()
                    ->value()
                    ->value<TypeKind::ARRAY>();
    lp::ExprPtr getter = std::make_shared<lp::CallExpr>(
        type,
        "subscript",
        std::vector<lp::ExprPtr>{
            call->inputAt(0),
            std::make_shared<lp::ConstantExpr>(
                subscriptType, std::make_shared<Variant>(keys[nth]))});
    if (addCoalesce) {
      lp::ConstantExprPtr deflt;
      switch (type->kind()) {
        case TypeKind::REAL:
          deflt = std::make_shared<lp::ConstantExpr>(
              REAL(),
              std::make_shared<Variant>(Variant(static_cast<float>(0))));
          break;
        case TypeKind::ARRAY: {
          auto emptyArray = Variant::array({});
          deflt = std::make_shared<lp::ConstantExpr>(
              type, std::make_shared<Variant>(Variant(emptyArray)));
          break;
        }
        case TypeKind::MAP: {
          auto emptyMap = Variant::map({});
          deflt = std::make_shared<lp::ConstantExpr>(
              type, std::make_shared<Variant>(Variant(emptyMap)));
          break;
        }
        default:
          VELOX_NYI("padded_make_row_from_map type {}", type->toString());
      }
      getter = std::make_shared<lp::SpecialFormExpr>(
          type,
          lp::SpecialForm::kCoalesce,
          std::vector<lp::ExprPtr>{getter, deflt});
    }
    it->second = getter;
  }
  return result;
}

std::unordered_map<PathCP, lp::ExprPtr> makeRowFromMapExplode(
    const lp::CallExpr* call,
    std::vector<PathCP>& paths) {
  return makeRowFromMapExplodeGeneric(call, paths, false);
}

std::unordered_map<PathCP, lp::ExprPtr> paddedMakeRowFromMapExplode(
    const lp::CallExpr* call,
    std::vector<PathCP>& paths) {
  return makeRowFromMapExplodeGeneric(call, paths, true);
}

lp::ExprPtr makeRowFromMapHook(
    const std::string& name,
    const std::vector<lp::ExprPtr>& args) {
  VELOX_CHECK_EQ(3, args.size());
  std::vector<std::string> nameStrings;
  std::vector<TypePtr> types;
  VELOX_CHECK_EQ(TypeKind::MAP, args[0]->type()->kind());
  auto type = args[0]->type()->childAt(1);
  auto* namesVariant = args[2]->asUnchecked<lp::ConstantExpr>()->value().get();
  auto namesArray = namesVariant->value<TypeKind::ARRAY>();
  for (auto i = 0; i < namesArray.size(); ++i) {
    nameStrings.push_back(namesArray[i].value<TypeKind::VARCHAR>());
    types.push_back(type);
  }
  auto rowType = ROW(std::move(nameStrings), std::move(types));
  return std::make_shared<lp::CallExpr>(rowType, name, args);
}

lp::ExprPtr makeNamedRowHook(
    const std::string& name,
    const std::vector<lp::ExprPtr>& args) {
  std::vector<std::string> newNames;
  std::vector<lp::ExprPtr> values;
  std::vector<TypePtr> types;
  for (auto i = 0; i < args.size(); i += 2) {
    VELOX_CHECK(args[i]->isConstant());
    newNames.push_back(args[i]
                           ->asUnchecked<lp::ConstantExpr>()
                           ->value()
                           ->value<TypeKind::VARCHAR>());
    types.push_back(args[i + 1]->type());
    values.push_back(args[i + 1]);
  }
  auto rowType = ROW(std::move(newNames), std::move(types));
  return std::make_shared<lp::CallExpr>(
      std::move(rowType), "row_constructor", std::move(values));
}

std::pair<std::vector<Step>, int32_t> makeNamedRowSubfield(
    const std::vector<Step>& steps,
    const lp::CallExpr& call) {
  VELOX_CHECK(steps.back().kind == StepKind::kField);
  auto field = steps.back().field;
  auto idx = call.type()->as<TypeKind::ROW>().getChildIdx(field);
  auto newFields = steps;
  newFields.pop_back();
  return std::make_pair(newFields, 2 * idx + 1);
}

std::unordered_map<PathCP, lp::ExprPtr> makeNamedRowExplode(
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
    it->second = call->inputAt(nth * 2 + 1);
  }
  return result;
}
} // namespace

void registerDfFunctions() {
  static const auto kMakeRowFromMap = "make_row_from_map";
  static const auto kPaddedMakeRowFromMap = "padded_make_row_from_map";
  static const auto kMakeNamedRow = "make_named_row";

  auto registry = FunctionRegistry::instance();

  {
    registerFeatureFuncHook(kMakeRowFromMap, makeRowFromMapHook);

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->logicalExplode = makeRowFromMapExplode;
    metadata->valuePathToArgPath = makeRowFromMapSubfield;
    registry->registerFunction(kMakeRowFromMap, std::move(metadata));
  }

  {
    registerFeatureFuncHook(kPaddedMakeRowFromMap, makeRowFromMapHook);

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->logicalExplode = paddedMakeRowFromMapExplode;
    metadata->valuePathToArgPath = makeRowFromMapSubfield;
    registry->registerFunction(kPaddedMakeRowFromMap, std::move(metadata));
  }

  {
    registerFeatureFuncHook(kMakeNamedRow, makeNamedRowHook);

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->valuePathToArgPath = makeNamedRowSubfield;
    metadata->logicalExplode = makeNamedRowExplode;
    registry->registerFunction(kMakeNamedRow, std::move(metadata));
  }
}

lp::ExprPtr resolveDfFunction(
    const std::string& name,
    const std::vector<lp::ExprPtr>& args) {
  auto it = functionHooks.find(name);
  if (it == functionHooks.end()) {
    return nullptr;
  }
  return it->second(name, args);
}

} // namespace facebook::velox::optimizer::test
