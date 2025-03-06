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
#include "optimizer/Plan.h" //@manual
#include "optimizer/PlanUtils.h" //@manual
#include "velox/exec/Aggregate.h"
#include "velox/expression/ConstantExpr.h"

namespace facebook::velox::optimizer {

using namespace facebook::velox;

using NodeSubfieldFunc = std::function<void(
    Optimization*,
    core::PlanNode* node,
    const std::vector<const RowType*>& context,
    const std::vector<ContextSource>& sources,
    bool isControl)>;

namespace {
template <typename T>
int64_t integerValueInner(const BaseVector* vector) {
  return vector->as<ConstantVector<T>>()->valueAt(0);
}

int64_t integerValue(const BaseVector* vector) {
  switch (vector->typeKind()) {
    case TypeKind::TINYINT:
      return integerValueInner<int8_t>(vector);
    case TypeKind::SMALLINT:
      return integerValueInner<int16_t>(vector);
    case TypeKind::INTEGER:
      return integerValueInner<int32_t>(vector);
    case TypeKind::BIGINT:
      return integerValueInner<int64_t>(vector);
    default:
      VELOX_FAIL();
  }
}

PathCP stepsToPath(const std::vector<Step>& steps) {
  std::vector<Step> reverse;
  for (int32_t i = steps.size() - 1; i >= 0; --i) {
    reverse.push_back(steps[i]);
  }
  return queryCtx()->toPath(make<Path>(std::move(reverse)));
}

RowTypePtr lambdaArgType(const core::ITypedExpr* expr) {
  auto* l = dynamic_cast<const core::LambdaTypedExpr*>(expr);
  VELOX_CHECK_NOT_NULL(l);
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < l->type()->size() - 1; ++i) {
    names.push_back(l->type()->as<TypeKind::ROW>().nameOf(i));
    types.push_back(l->type()->childAt(i));
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace

void Optimization::markFieldAccessed(
    const ContextSource& source,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl,
    const std::vector<const RowType*>& context,
    const std::vector<ContextSource>& sources) {
  auto fields = isControl ? &controlSubfields_ : &payloadSubfields_;
  if (source.planNode) {
    auto name = source.planNode->name();
    auto path = stepsToPath(steps);
    fields->nodeFields[source.planNode].resultPaths[ordinal].add(path->id());
    if (name == "Project") {
      auto* project =
          reinterpret_cast<const core::ProjectNode*>(source.planNode);
      markSubfields(
          project->projections()[ordinal].get(),
          steps,
          isControl,
          std::vector<const RowType*>{
              project->sources()[0]->outputType().get()},
          std::vector<ContextSource>{
              ContextSource{.planNode = project->sources()[0].get()}});
      return;
    }
    if (name == "Aggregation") {
      auto* agg =
          reinterpret_cast<const core::AggregationNode*>(source.planNode);
      std::vector<const RowType*> inputContext = {
          agg->sources()[0]->outputType().get()};
      std::vector<ContextSource> inputSources = {
          ContextSource{.planNode = agg->sources()[0].get()}};
      auto& keys = agg->groupingKeys();
      std::vector<Step> empty;
      if (ordinal < keys.size()) {
        markSubfields(
            keys[ordinal].get(), empty, isControl, inputContext, inputSources);
        return;
      }
      auto& aggregate = agg->aggregates()[ordinal - keys.size()];
      markSubfields(
          aggregate.call.get(), empty, isControl, inputContext, inputSources);
      if (aggregate.mask) {
        markSubfields(
            aggregate.mask.get(), empty, isControl, inputContext, inputSources);
      }
      markColumnSubfields(agg, aggregate.sortingKeys, 0);
      return;
    }
    auto& sourceInputs = source.planNode->sources();
    if (sourceInputs.empty()) {
      return;
    }
    auto fieldName = source.planNode->outputType()->nameOf(ordinal);
    for (auto i = 0; i < sourceInputs.size(); ++i) {
      auto& type = sourceInputs[i]->outputType();
      auto maybeIdx = type->getChildIdxIfExists(fieldName);
      if (maybeIdx.has_value()) {
        ContextSource s{.planNode = sourceInputs[i].get()};
        markFieldAccessed(
            s, maybeIdx.value(), steps, isControl, context, sources);
        return;
      }
    }
    VELOX_FAIL("Should have found source for expr");
  }
  // The source is a lambda arg. We apply the path to the corresponding
  // container arg of the 2nd order function call that has the lambda.
  auto* md =
      FunctionRegistry::instance()->metadata(toName(source.call->name()));
  auto* lInfo = md->lambdaInfo(source.lambdaOrdinal);
  auto nth = lInfo->argOrdinal[ordinal];
  auto callContext = context;
  callContext.erase(callContext.begin());
  auto callSources = sources;
  callSources.erase(callSources.begin());
  markSubfields(
      source.call->inputs()[nth].get(),
      steps,
      isControl,
      callContext,
      callSources);
}

std::optional<int32_t> Optimization::stepToArg(
    const Step& step,
    const FunctionMetadata* metadata) {
  auto it = std::find(
      metadata->fieldIndexForArg.begin(),
      metadata->fieldIndexForArg.end(),
      step.id);
  if (it != metadata->fieldIndexForArg.end()) {
    // The arg corresponding to the step is accessed.
    return metadata->argOrdinal[it - metadata->fieldIndexForArg.begin()];
  }
  return std::nullopt;
}

void Optimization::markSubfields(
    const core::ITypedExpr* expr,
    std::vector<Step>& steps,
    bool isControl,
    const std::vector<const RowType*> context,
    const std::vector<ContextSource>& sources) {
  if (auto* field = dynamic_cast<const core::DereferenceTypedExpr*>(expr)) {
    auto* input = field->inputs()[0].get();
    auto& name = input->type()->as<TypeKind::ROW>().nameOf(field->index());
    steps.push_back(Step{
        .kind = StepKind::kField,
        .field = (name.empty() ? nullptr : toName(name)),
        .id = field->index()});
    markSubfields(input, steps, isControl, context, sources);
    steps.pop_back();
    return;
  }
  if (auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    auto* input = field->inputs().empty() ? nullptr : field->inputs()[0].get();
    bool isLeaf =
        !input || dynamic_cast<const core::InputTypedExpr*>(input) != nullptr;
    if (isLeaf) {
      for (auto i = 0; i < sources.size(); ++i) {
        auto maybeIdx = context[i]->getChildIdxIfExists(field->name());
        if (maybeIdx.has_value()) {
          auto source = sources[i];
          markFieldAccessed(
              source, maybeIdx.value(), steps, isControl, context, sources);
          return;
        }
      }
      VELOX_FAIL("Field not found {}", field->name());
    }
    steps.push_back(
        Step{.kind = StepKind::kField, .field = toName(field->name())});
    markSubfields(input, steps, isControl, context, sources);
    steps.pop_back();
    return;
  }
  if (auto* call = dynamic_cast<const core::CallTypedExpr*>(expr)) {
    auto& name = call->name();
    if (name == "cardinality") {
      steps.push_back(Step{.kind = StepKind::kCardinality});
      markSubfields(
          call->inputs()[0].get(), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }
    if (name == "subscript" || name == "element_at") {
      auto constant = foldConstant(call->inputs()[1]);
      if (!constant) {
        std::vector<Step> subSteps;
        markSubfields(
            call->inputs()[1].get(), subSteps, isControl, context, sources);
        steps.push_back(Step{.kind = StepKind::kSubscript, .allFields = true});
        markSubfields(
            call->inputs()[0].get(), steps, isControl, context, sources);
        steps.pop_back();
        return;
      }
      auto value = constant->value();
      if (value->type()->kind() == TypeKind::VARCHAR) {
        std::string str = value->as<ConstantVector<StringView>>()->valueAt(0);
        steps.push_back(
            Step{.kind = StepKind::kSubscript, .field = toName(str)});
        markSubfields(
            call->inputs()[0].get(), steps, isControl, context, sources);
        steps.pop_back();
        return;
      }
      auto id = integerValue(constant->value().get());
      steps.push_back(Step{.kind = StepKind::kSubscript, .id = id});
      markSubfields(
          call->inputs()[0].get(), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }
    auto* metadata = FunctionRegistry::instance()->metadata(toName(name));
    if (!metadata) {
      for (auto i = 0; i < call->inputs().size(); ++i) {
        std::vector<Step> steps;
        markSubfields(
            call->inputs()[i].get(), steps, isControl, context, sources);
      }
      return;
    }
    // The function has non-default metadata. Record subfields.
    auto* fields = isControl ? &controlSubfields_ : &payloadSubfields_;
    auto path = stepsToPath(steps);
    fields->argFields[call].resultPaths[ResultAccess::kSelf].add(path->id());
    for (auto i = 0; i < call->inputs().size(); ++i) {
      if (metadata->subfieldArg.has_value() &&
          i == metadata->subfieldArg.value()) {
        // A subfield of func is a subfield of one arg.
        markSubfields(
            call->inputs()[metadata->subfieldArg.value()].get(),
            steps,
            isControl,
            context,
            sources);
        continue;
      }
      if (!steps.empty() && steps.back().kind == StepKind::kField) {
        auto maybeNth = stepToArg(steps.back(), metadata);
        if (maybeNth.has_value() && maybeNth.value() == i) {
          auto newSteps = steps;
          auto argPath = stepsToPath(newSteps);
          fields->argFields[call].resultPaths[maybeNth.value()].add(
              argPath->id());
          newSteps.pop_back();
          markSubfields(
              call->inputs()[maybeNth.value()].get(),
              newSteps,
              isControl,
              context,
              sources);
          continue;
        } else if (
            std::find(
                metadata->fieldIndexForArg.begin(),
                metadata->fieldIndexForArg.end(),
                i) != metadata->fieldIndexForArg.end()) {
          // The ith argument corresponds to some subfield field index
          // other than the one in path, so this argument is not
          // referenced.
          continue;
        }
      }
      if (auto* lambda = metadata->lambdaInfo(i)) {
        auto argType = lambdaArgType(call->inputs()[lambda->ordinal].get());
        std::vector<const RowType*> newContext = {argType.get()};
        newContext.insert(newContext.end(), context.begin(), context.end());
        std::vector<ContextSource> newSources = {
            ContextSource{.call = call, .lambdaOrdinal = i}};
        newSources.insert(newSources.end(), sources.begin(), sources.end());
        auto* l = reinterpret_cast<const core::LambdaTypedExpr*>(
            call->inputs()[i].get());
        std::vector<Step> empty;
        markSubfields(
            l->body().get(), empty, isControl, newContext, newSources);
        continue;
        markSubfields(
            call->inputs()[i].get(), empty, isControl, context, sources);
        continue;
      }
      // The argument is not special, just mark through without path.
      std::vector<Step> empty;
      markSubfields(
          call->inputs()[i].get(), empty, isControl, context, sources);
    }
    return;
  }
  if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
    return;
  }
  if (auto* castExpr = dynamic_cast<const core::CastTypedExpr*>(expr)) {
    std::vector<Step> steps;
    markSubfields(
        castExpr->inputs()[0].get(), steps, isControl, context, sources);
    return;
  }
  VELOX_UNREACHABLE("Unhandled expr: {}", expr->toString());
}

void Optimization::markColumnSubfields(
    const core::PlanNode* node,
    const std::vector<core::FieldAccessTypedExprPtr>& columns,
    int32_t source) {
  std::vector<const RowType*> context = {
      node->sources()[source]->outputType().get()};
  std::vector<ContextSource> sources = {
      {.planNode = node->sources()[source].get()}};
  for (auto i = 0; i < columns.size(); ++i) {
    std::vector<Step> steps;
    markSubfields(columns[i].get(), steps, true, context, sources);
  }
}

void Optimization::markControl(const core::PlanNode* node) {
  auto name = node->name();
  if (auto* join = dynamic_cast<const core::AbstractJoinNode*>(node)) {
    markColumnSubfields(node, join->leftKeys(), 0);
    markColumnSubfields(node, join->rightKeys(), 1);
    if (auto* filter = join->filter().get()) {
      std::vector<const RowType*> context = {
          join->sources()[0]->outputType().get(),
          join->sources()[1]->outputType().get()};
      std::vector<ContextSource> sources = {
          {.planNode = join->sources()[0].get()},
          {.planNode = join->sources()[1].get()}};
      std::vector<Step> steps;
      markSubfields(filter, steps, true, context, sources);
    }
  } else if (name == "Filter") {
    std::vector<const RowType*> context = {
        node->sources()[0]->outputType().get()};
    std::vector<ContextSource> sources = {
        {.planNode = node->sources()[0].get()}};
    std::vector<Step> steps;
    markSubfields(
        reinterpret_cast<const core::FilterNode*>(node)->filter().get(),
        steps,
        true,
        context,
        sources);
  } else if (name == "Aggregation") {
    auto* agg = dynamic_cast<const core::AggregationNode*>(node);
    markColumnSubfields(node, agg->groupingKeys(), 0);
  }
  for (auto& source : node->sources()) {
    markControl(source.get());
  }
}

void Optimization::markAllSubfields(
    const RowType* type,
    const core::PlanNode* node) {
  markControl(node);
  ContextSource source = {.planNode = node};
  std::vector<const RowType*> context;
  std::vector<ContextSource> sources;
  for (auto i = 0; i < type->size(); ++i) {
    std::vector<Step> steps;
    markFieldAccessed(source, i, steps, false, context, sources);
  }
}

std::vector<int32_t> Optimization::usedChannels(const core::PlanNode* node) {
  auto& control = controlSubfields_.nodeFields[node];
  auto& payload = payloadSubfields_.nodeFields[node];
  BitSet unique;
  std::vector<int32_t> result;
  for (auto& pair : control.resultPaths) {
    result.push_back(pair.first);
    unique.add(pair.first);
  }
  for (auto& pair : payload.resultPaths) {
    if (!unique.contains(pair.first)) {
      result.push_back(pair.first);
    }
  }
  return result;
}

namespace {

template <typename T>
core::TypedExprPtr makeKey(const TypePtr& type, T v) {
  return std::make_shared<core::ConstantTypedExpr>(type, variant(v));
}
} // namespace

core::TypedExprPtr stepToGetter(Step step, core::TypedExprPtr arg) {
  switch (step.kind) {
    case StepKind::kField: {
      if (step.field) {
        auto& type = arg->type()->childAt(
            arg->type()->as<TypeKind::ROW>().getChildIdx(step.field));
        return std::make_shared<core::FieldAccessTypedExpr>(
            type, arg, step.field);
      } else {
        auto& type = arg->type()->childAt(step.id);
        return std::make_shared<core::DereferenceTypedExpr>(type, arg, step.id);
      }
    }
    case StepKind::kSubscript: {
      auto& type = arg->type();
      if (type->kind() == TypeKind::MAP) {
        core::TypedExprPtr key;
        switch (type->as<TypeKind::MAP>().childAt(0)->kind()) {
          case TypeKind::VARCHAR:
            key = makeKey(VARCHAR(), step.field);
            break;
          case TypeKind::BIGINT:
            key = makeKey<int64_t>(BIGINT(), step.id);
            break;
          case TypeKind::INTEGER:
            key = makeKey<int32_t>(INTEGER(), step.id);
            break;
          case TypeKind::SMALLINT:
            key = makeKey<int16_t>(SMALLINT(), step.id);
            break;
          case TypeKind::TINYINT:
            key = makeKey<int8_t>(TINYINT(), step.id);
            break;
          default:
            VELOX_FAIL("Unsupported key type");
        }

        return std::make_shared<core::CallTypedExpr>(
            type->as<TypeKind::MAP>().childAt(0),
            std::vector<core::TypedExprPtr>{arg, key},
            "subscript");
      }
      return std::make_shared<core::CallTypedExpr>(
          type->childAt(0),
          std::vector<core::TypedExprPtr>{
              arg, makeKey<int32_t>(INTEGER(), step.id)},
          "subscript");
    }

    default:
      VELOX_NYI();
  }
}

} // namespace facebook::velox::optimizer
