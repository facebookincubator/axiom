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

#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::velox::optimizer {

using namespace facebook::velox;

namespace lp = facebook::velox::logical_plan;

namespace {

RowTypePtr lambdaArgType(const lp::Expr* expr) {
  auto* l = dynamic_cast<const lp::LambdaExpr*>(expr);
  VELOX_CHECK_NOT_NULL(l);
  return l->signature();
}
} // namespace

PathCP stepsToPath(const std::vector<Step>& steps) {
  std::vector<Step> reverse;
  for (int32_t i = steps.size() - 1; i >= 0; --i) {
    reverse.push_back(steps[i]);
  }
  return queryCtx()->toPath(make<Path>(std::move(reverse)));
}

void Optimization::markFieldAccessed(
    const LogicalContextSource& source,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl,
    const std::vector<const RowType*>& context,
    const std::vector<LogicalContextSource>& sources) {
  auto fields =
      isControl ? &logicalControlSubfields_ : &logicalPayloadSubfields_;
  if (source.planNode) {
    auto kind = source.planNode->kind();
    auto path = stepsToPath(steps);
    fields->nodeFields[source.planNode].resultPaths[ordinal].add(path->id());
    if (kind == lp::NodeKind::kProject) {
      auto* project = reinterpret_cast<const lp::ProjectNode*>(source.planNode);
      markSubfields(
          project->expressions()[ordinal].get(),
          steps,
          isControl,
          std::vector<const RowType*>{project->inputs()[0]->outputType().get()},
          std::vector<LogicalContextSource>{
              LogicalContextSource{.planNode = project->inputs()[0].get()}});
      return;
    }
    if (kind == lp::NodeKind::kAggregate) {
      auto* agg = reinterpret_cast<const lp::AggregateNode*>(source.planNode);
      std::vector<const RowType*> inputContext = {
          agg->inputs()[0]->outputType().get()};
      std::vector<LogicalContextSource> inputSources = {
          LogicalContextSource{.planNode = agg->inputs()[0].get()}};
      auto& keys = agg->groupingKeys();
      std::vector<Step> empty;
      if (ordinal < keys.size()) {
        markSubfields(
            keys[ordinal].get(), empty, isControl, inputContext, inputSources);
        return;
      }
      auto& aggregate = agg->aggregates()[ordinal - keys.size()];
      for (auto& in : aggregate->inputs()) {
        markSubfields(in.get(), empty, isControl, inputContext, inputSources);
      }
      if (aggregate->filter()) {
        markSubfields(
            aggregate->filter().get(),
            empty,
            isControl,
            inputContext,
            inputSources);
      }
      for (auto& field : aggregate->ordering()) {
        markSubfields(
            field.expression.get(),
            empty,
            isControl,
            inputContext,
            inputSources);
      }
      return;
    }
    if (kind == lp::NodeKind::kSet) {
      auto* set = reinterpret_cast<const lp::SetNode*>(source.planNode);
      for (auto in : set->inputs()) {
        std::vector<const RowType*> inputContext = {in->outputType().get()};
        std::vector<LogicalContextSource> inputSources = {
            LogicalContextSource{.planNode = in.get()}};
        markFieldAccessed(
            inputSources[0],
            ordinal,
            steps,
            isControl,
            inputContext,
            inputSources);
      }
    }
    auto& sourceInputs = source.planNode->inputs();
    if (sourceInputs.empty()) {
      return;
    }
    auto fieldName = source.planNode->outputType()->nameOf(ordinal);
    for (auto i = 0; i < sourceInputs.size(); ++i) {
      auto& type = sourceInputs[i]->outputType();
      auto maybeIdx = type->getChildIdxIfExists(fieldName);
      if (maybeIdx.has_value()) {
        LogicalContextSource s{.planNode = sourceInputs[i].get()};
        markFieldAccessed(
            s, maybeIdx.value(), steps, isControl, context, sources);
        return;
      }
    }
    VELOX_FAIL("Should have found source for expr {}", fieldName);
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

bool looksConstant(const lp::ExprPtr& expr) {
  if (expr->isConstant()) {
    return true;
  }
  if (expr->isInputReference()) {
    return false;
  }
  for (auto& in : expr->inputs()) {
    if (!looksConstant(in)) {
      return false;
    }
  }
  return true;
}

const lp::ConstantExprPtr Optimization::maybeFoldLogicalConstant(
    const lp::ExprPtr expr) {
  if (expr->isConstant()) {
    return std::static_pointer_cast<const lp::ConstantExpr>(expr);
  }
  if (looksConstant(expr)) {
    auto literal = translateExpr(expr);
    if (literal->type() == PlanType::kLiteral) {
      return std::make_shared<lp::ConstantExpr>(
          toTypePtr(literal->value().type),
          std::make_shared<Variant>(literal->as<Literal>()->literal()));
    }
  }
  return nullptr;
}

void Optimization::markSubfields(
    const lp::Expr* expr,
    std::vector<Step>& steps,
    bool isControl,
    const std::vector<const RowType*>& context,
    const std::vector<LogicalContextSource>& sources) {
  if (expr->isInputReference()) {
    auto& name = expr->asUnchecked<lp::InputReferenceExpr>()->name();
    for (auto i = 0; i < sources.size(); ++i) {
      auto maybeIdx = context[i]->getChildIdxIfExists(name);
      if (maybeIdx.has_value()) {
        auto source = sources[i];
        markFieldAccessed(
            source, maybeIdx.value(), steps, isControl, context, sources);
        return;
      }
    }
    VELOX_FAIL("Field not found {}", name);
  }

  if (isSpecialForm(expr, lp::SpecialForm::kDereference)) {
    VELOX_CHECK(expr->inputAt(1)->isConstant());
    auto* field = expr->inputAt(1)->asUnchecked<lp::ConstantExpr>();
    auto* input = expr->inputAt(0).get();
    Name name = nullptr;
    auto fieldIndex = maybeIntegerLiteral(field);
    // Always fill both index and name for a struct getter.
    if (fieldIndex.has_value()) {
      name =
          toName(input->type()->as<TypeKind::ROW>().nameOf(fieldIndex.value()));
    } else {
      name = toName(field->value()->value<TypeKind::VARCHAR>());
      fieldIndex = input->type()->as<TypeKind::ROW>().getChildIdx(
          field->value()->value<TypeKind::VARCHAR>());
    }
    steps.push_back(Step{
        .kind = StepKind::kField,
        .field = (name == nullptr || strlen(name) == 0) ? nullptr : name,
        .id = fieldIndex.has_value() ? fieldIndex.value() : 0});
    markSubfields(input, steps, isControl, context, sources);
    steps.pop_back();
    return;
  }
  if (expr->isCall()) {
    auto& name = expr->asUnchecked<lp::CallExpr>()->name();
    if (name == "cardinality") {
      steps.push_back(Step{.kind = StepKind::kCardinality});
      markSubfields(expr->inputAt(0).get(), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }
    if (name == "subscript" || name == "element_at") {
      auto constant = maybeFoldLogicalConstant(expr->inputAt(1));
      if (!constant) {
        std::vector<Step> subSteps;
        markSubfields(
            expr->inputAt(1).get(), subSteps, isControl, context, sources);
        steps.push_back(Step{.kind = StepKind::kSubscript, .allFields = true});
        markSubfields(
            expr->inputs()[0].get(), steps, isControl, context, sources);
        steps.pop_back();
        return;
      }
      auto& value = constant->value();
      if (value->kind() == TypeKind::VARCHAR) {
        std::string str = value->value<TypeKind::VARCHAR>();
        steps.push_back(
            Step{.kind = StepKind::kSubscript, .field = toName(str)});
        markSubfields(
            expr->inputs()[0].get(), steps, isControl, context, sources);
        steps.pop_back();
        return;
      }
      auto id = integerValue(value.get());
      steps.push_back(Step{.kind = StepKind::kSubscript, .id = id});
      markSubfields(
          expr->inputs()[0].get(), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }
    auto* metadata = FunctionRegistry::instance()->metadata(toName(name));
    if (!metadata || !metadata->processSubfields()) {
      for (auto i = 0; i < expr->inputs().size(); ++i) {
        std::vector<Step> steps;
        markSubfields(
            expr->inputs()[i].get(), steps, isControl, context, sources);
      }
      return;
    }
    // The function has non-default metadata. Record subfields.
    auto* call = reinterpret_cast<const lp::CallExpr*>(expr);
    auto* fields =
        isControl ? &logicalControlSubfields_ : &logicalPayloadSubfields_;
    auto path = stepsToPath(steps);
    fields->argFields[call].resultPaths[ResultAccess::kSelf].add(path->id());
    for (auto i = 0; i < expr->inputs().size(); ++i) {
      if (metadata->subfieldArg.has_value() &&
          i == metadata->subfieldArg.value()) {
        // A subfield of func is a subfield of one arg.
        markSubfields(
            expr->inputs()[metadata->subfieldArg.value()].get(),
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
          fields->argFields[expr].resultPaths[maybeNth.value()].add(
              argPath->id());
          newSteps.pop_back();
          markSubfields(
              expr->inputs()[maybeNth.value()].get(),
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
        auto argType = lambdaArgType(expr->inputs()[i].get());
        std::vector<const RowType*> newContext = {argType.get()};
        newContext.insert(newContext.end(), context.begin(), context.end());
        std::vector<LogicalContextSource> newSources = {LogicalContextSource{
            .call = expr->asUnchecked<lp::CallExpr>(), .lambdaOrdinal = i}};
        newSources.insert(newSources.end(), sources.begin(), sources.end());

        auto* l = expr->inputAt(i)->asUnchecked<lp::LambdaExpr>();
        std::vector<Step> empty;
        markSubfields(
            l->body().get(), empty, isControl, newContext, newSources);
        continue;
        markSubfields(
            expr->inputs()[i].get(), empty, isControl, context, sources);
        continue;
      }
      // The argument is not special, just mark through without path.
      std::vector<Step> empty;
      markSubfields(
          expr->inputs()[i].get(), empty, isControl, context, sources);
    }
    return;
  }
  if (expr->isConstant()) {
    return;
  }
  if (expr->isSpecialForm()) {
    for (auto i = 0; i < expr->inputs().size(); ++i) {
      std::vector<Step> steps;
      markSubfields(
          expr->inputs()[i].get(), steps, isControl, context, sources);
    }
    return;
  }
  VELOX_UNREACHABLE("Unhandled expr: {}", lp::ExprPrinter::toText(*expr));
}

void Optimization::markColumnSubfields(
    const lp::LogicalPlanNode* node,
    const std::vector<logical_plan::ExprPtr>& columns,
    int32_t source) {
  std::vector<const RowType*> context = {
      node->inputs()[source]->outputType().get()};
  std::vector<LogicalContextSource> sources = {
      {.planNode = node->inputs()[source].get()}};
  for (auto i = 0; i < columns.size(); ++i) {
    std::vector<Step> steps;
    markSubfields(columns[i].get(), steps, true, context, sources);
  }
}

void Optimization::markControl(const lp::LogicalPlanNode* node) {
  auto kind = node->kind();
  if (kind == lp::NodeKind::kJoin) {
    auto* join = reinterpret_cast<const lp::JoinNode*>(node);
    if (auto* filter = join->condition().get()) {
      std::vector<const RowType*> context = {
          join->left()->outputType().get(), join->right()->outputType().get()};
      std::vector<LogicalContextSource> sources = {
          {.planNode = join->left().get()}, {.planNode = join->right().get()}};
      std::vector<Step> steps;
      markSubfields(filter, steps, true, context, sources);
    }
  } else if (kind == lp::NodeKind::kFilter) {
    std::vector<const RowType*> context = {
        node->inputAt(0)->outputType().get()};
    std::vector<LogicalContextSource> sources = {
        {.planNode = node->inputAt(0).get()}};
    std::vector<Step> steps;
    markSubfields(
        reinterpret_cast<const lp::FilterNode*>(node)->predicate().get(),
        steps,
        true,
        context,
        sources);
  } else if (kind == lp::NodeKind::kAggregate) {
    auto* agg = dynamic_cast<const lp::AggregateNode*>(node);
    markColumnSubfields(node, agg->groupingKeys(), 0);
  } else if (kind == lp::NodeKind::kSort) {
    auto* order = dynamic_cast<const lp::SortNode*>(node);
    std::vector<lp::ExprPtr> keys;
    for (auto& k : order->ordering()) {
      keys.push_back(k.expression);
    }
    markColumnSubfields(node, keys, 0);
  } else if (kind == lp::NodeKind::kSet) {
    auto* set = reinterpret_cast<const lp::SetNode*>(node);
    if (set->operation() != lp::SetOperation::kUnionAll) {
      // If this is with a distinct every column is a control column.
      for (auto i = 0; i < set->outputType()->size(); ++i) {
        for (auto& in : set->inputs()) {
          std::vector<Step> empty;
          std::vector<const RowType*> inputContext = {in->outputType().get()};
          std::vector<LogicalContextSource> inputSources = {
              LogicalContextSource{.planNode = in.get()}};
          markFieldAccessed(
              inputSources[0], i, empty, true, inputContext, inputSources);
        }
      }
    }
  }
  for (auto& source : node->inputs()) {
    markControl(source.get());
  }
}

void Optimization::markAllSubfields(
    const RowType* type,
    const lp::LogicalPlanNode* node) {
  markControl(node);
  LogicalContextSource source = {.planNode = node};
  std::vector<const RowType*> context;
  std::vector<LogicalContextSource> sources;
  for (auto i = 0; i < type->size(); ++i) {
    std::vector<Step> steps;
    markFieldAccessed(source, i, steps, false, context, sources);
  }
}

std::vector<int32_t> Optimization::usedChannels(
    const lp::LogicalPlanNode* node) {
  auto& control = logicalControlSubfields_.nodeFields[node];
  auto& payload = logicalPayloadSubfields_.nodeFields[node];
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
lp::ExprPtr makeKey(const TypePtr& type, T v) {
  return std::make_shared<lp::ConstantExpr>(type, std::make_shared<Variant>(v));
}
} // namespace

lp::ExprPtr stepToLogicalPlanGetter(Step step, lp::ExprPtr arg) {
  switch (step.kind) {
    case StepKind::kField: {
      if (step.field) {
        auto& type = arg->type()->childAt(
            arg->type()->as<TypeKind::ROW>().getChildIdx(step.field));
        return std::make_shared<lp::SpecialFormExpr>(
            type,
            lp::SpecialForm::kDereference,
            std::vector<lp::ExprPtr>{
                arg,
                std::make_shared<lp::ConstantExpr>(
                    VARCHAR(), std::make_shared<Variant>(step.field))});
      } else {
        auto& type = arg->type()->childAt(step.id);
        return std::make_shared<lp::SpecialFormExpr>(
            type,
            lp::SpecialForm::kDereference,
            std::vector<lp::ExprPtr>{
                arg,
                std::make_shared<lp::ConstantExpr>(
                    BIGINT(), std::make_shared<Variant>(step.id))});
      }
    }
    case StepKind::kSubscript: {
      auto& type = arg->type();
      if (type->kind() == TypeKind::MAP) {
        lp::ExprPtr key;
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

        return std::make_shared<lp::CallExpr>(
            type->as<TypeKind::MAP>().childAt(1),
            "subscript",
            std::vector<lp::ExprPtr>{arg, key});
      }
      return std::make_shared<lp::CallExpr>(
          type->childAt(0),
          "subscript",
          std::vector<lp::ExprPtr>{arg, makeKey<int32_t>(INTEGER(), step.id)});
    }

    default:
      VELOX_NYI();
  }
}

std::string LogicalPlanSubfields::toString() const {
  std::stringstream out;
  out << "Nodes:";
  for (auto& pair : nodeFields) {
    out << "Node " << pair.first->id() << " = {";
    for (auto& s : pair.second.resultPaths) {
      out << s.first << " -> {";
      s.second.forEach(
          [&](auto i) { out << queryCtx()->pathById(i)->toString(); });
      out << "}\n";
    }
  }
  if (!argFields.empty()) {
    out << "Functions:";
    for (auto& pair : argFields) {
      out << "Func " << lp::ExprPrinter::toText(*pair.first) << " = {";
      for (auto& s : pair.second.resultPaths) {
        out << s.first << " -> {";
        s.second.forEach(
            [&](auto i) { out << queryCtx()->pathById(i)->toString(); });
        out << "}\n";
      }
    }
  }
  return out.str();
}

} // namespace facebook::velox::optimizer
