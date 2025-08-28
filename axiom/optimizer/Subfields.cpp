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
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/ToGraph.h"

namespace lp = facebook::velox::logical_plan;

namespace facebook::velox::optimizer {
namespace {

const RowTypePtr& lambdaArgType(const lp::Expr* expr) {
  auto* lambda = expr->asUnchecked<lp::LambdaExpr>();
  VELOX_CHECK_NOT_NULL(lambda);
  return lambda->signature();
}

PathCP stepsToPath(const std::vector<Step>& steps) {
  std::vector<Step> reverse;
  for (int32_t i = steps.size() - 1; i >= 0; --i) {
    reverse.push_back(steps[i]);
  }
  return queryCtx()->toPath(make<Path>(std::move(reverse)));
}

} // namespace

void ToGraph::markFieldAccessed(
    const lp::CallExpr& call,
    int32_t lambdaOrdinal,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl,
    std::span<const RowType* const> context,
    std::span<const LogicalContextSource> sources) {
  // The source is a lambda arg. We apply the path to the corresponding
  // container arg of the 2nd order function call that has the lambda.
  const auto* md = functionMetadata(toName(call.name()));
  const auto* lambdaInfo = md->lambdaInfo(lambdaOrdinal);
  const auto nth = lambdaInfo->argOrdinal[ordinal];

  auto callContext = context.subspan(1);
  auto callSources = sources.subspan(1);
  markSubfields(call.inputAt(nth), steps, isControl, callContext, callSources);
}

void ToGraph::markFieldAccessed(
    const lp::ProjectNode& project,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  const auto& input = project.onlyInput();
  markSubfields(
      project.expressionAt(ordinal),
      steps,
      isControl,
      std::array{input->outputType().get()},
      std::array{LogicalContextSource{.planNode = input.get()}});
}

void ToGraph::markFieldAccessed(
    const lp::AggregateNode& agg,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  const auto& input = agg.onlyInput();

  const auto inputContext = std::array{input->outputType().get()};
  const auto inputSources =
      std::array{LogicalContextSource{.planNode = input.get()}};
  std::vector<Step> subSteps;
  auto mark = [&](const lp::ExprPtr& expr) {
    markSubfields(expr, subSteps, isControl, inputContext, inputSources);
  };

  const auto& keys = agg.groupingKeys();
  if (ordinal < keys.size()) {
    mark(keys[ordinal]);
    return;
  }

  const auto& aggregate = agg.aggregateAt(ordinal - keys.size());
  for (const auto& aggregateInput : aggregate->inputs()) {
    mark(aggregateInput);
  }

  if (aggregate->filter()) {
    mark(aggregate->filter());
  }

  for (const auto& sortingField : aggregate->ordering()) {
    mark(sortingField.expression);
  }
}

void ToGraph::markFieldAccessed(
    const lp::SetNode& set,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  for (const auto& input : set.inputs()) {
    const auto inputContext = std::array{input->outputType().get()};
    const auto inputSources =
        std::array{LogicalContextSource{.planNode = input.get()}};
    markFieldAccessed(
        inputSources[0], ordinal, steps, isControl, inputContext, inputSources);
  }
}

void ToGraph::markFieldAccessed(
    const LogicalContextSource& source,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl,
    std::span<const RowType* const> context,
    std::span<const LogicalContextSource> sources) {
  if (!source.planNode) {
    markFieldAccessed(
        *source.call,
        source.lambdaOrdinal,
        ordinal,
        steps,
        isControl,
        context,
        sources);
    return;
  }

  auto* fields = isControl ? &controlSubfields_ : &payloadSubfields_;

  const auto* path = stepsToPath(steps);
  auto& paths = fields->nodeFields[source.planNode].resultPaths[ordinal];
  if (paths.contains(path->id())) {
    // Already marked.
    return;
  }
  paths.add(path->id());

  const auto kind = source.planNode->kind();
  if (kind == lp::NodeKind::kProject) {
    const auto* project = source.planNode->asUnchecked<lp::ProjectNode>();
    markFieldAccessed(*project, ordinal, steps, isControl);
    return;
  }

  if (kind == lp::NodeKind::kAggregate) {
    const auto* agg = source.planNode->asUnchecked<lp::AggregateNode>();
    markFieldAccessed(*agg, ordinal, steps, isControl);
    return;
  }

  if (kind == lp::NodeKind::kSet) {
    const auto* set = source.planNode->asUnchecked<lp::SetNode>();
    markFieldAccessed(*set, ordinal, steps, isControl);
    return;
  }

  const auto& sourceInputs = source.planNode->inputs();
  if (sourceInputs.empty()) {
    return;
  }

  const auto& fieldName = source.planNode->outputType()->nameOf(ordinal);
  for (const auto& sourceInput : sourceInputs) {
    const auto& type = sourceInput->outputType();
    if (auto maybeIdx = type->getChildIdxIfExists(fieldName)) {
      markFieldAccessed(
          {.planNode = sourceInput.get()},
          maybeIdx.value(),
          steps,
          isControl,
          context,
          sources);
      return;
    }
  }
  VELOX_FAIL("Should have found source for expr {}", fieldName);
}

std::optional<int32_t> ToGraph::stepToArg(
    const Step& step,
    const FunctionMetadata* metadata) {
  const auto begin = metadata->fieldIndexForArg.begin();
  const auto end = metadata->fieldIndexForArg.end();
  auto it = std::find(begin, end, step.id);
  if (it != end) {
    // The arg corresponding to the step is accessed.
    return metadata->argOrdinal[it - begin];
  }
  return std::nullopt;
}

namespace {

bool looksConstant(const lp::ExprPtr& expr) {
  if (expr->isConstant()) {
    return true;
  }
  if (expr->isInputReference()) {
    return false;
  }
  for (auto& input : expr->inputs()) {
    if (!looksConstant(input)) {
      return false;
    }
  }
  return true;
}
} // namespace

lp::ConstantExprPtr ToGraph::tryFoldConstant(const lp::ExprPtr expr) {
  if (expr->isConstant()) {
    return std::static_pointer_cast<const lp::ConstantExpr>(expr);
  }
  if (looksConstant(expr)) {
    auto literal = translateExpr(expr);
    if (literal->is(PlanType::kLiteralExpr)) {
      return std::make_shared<lp::ConstantExpr>(
          toTypePtr(literal->value().type),
          std::make_shared<Variant>(literal->as<Literal>()->literal()));
    }
  }
  return nullptr;
}

void ToGraph::markSubfields(
    const lp::Expr* expr,
    std::vector<Step>& steps,
    bool isControl,
    std::span<const RowType* const> context,
    std::span<const LogicalContextSource> sources) {
  if (expr->isInputReference()) {
    const auto& name = expr->asUnchecked<lp::InputReferenceExpr>()->name();
    for (auto i = 0; i < sources.size(); ++i) {
      if (auto maybeIdx = context[i]->getChildIdxIfExists(name)) {
        markFieldAccessed(
            sources[i], maybeIdx.value(), steps, isControl, context, sources);
        return;
      }
    }
    VELOX_FAIL("Field not found {}", name);
  }

  if (isSpecialForm(expr, lp::SpecialForm::kDereference)) {
    VELOX_CHECK(expr->inputAt(1)->isConstant());
    const auto* field = expr->inputAt(1)->asUnchecked<lp::ConstantExpr>();
    const auto* input = expr->inputAt(0).get();

    // Always fill both index and name for a struct getter.
    auto fieldIndex = maybeIntegerLiteral(field);
    Name name = nullptr;
    if (fieldIndex.has_value()) {
      name = toName(input->type()->asRow().nameOf(fieldIndex.value()));
    } else {
      const auto& fieldName = field->value()->value<TypeKind::VARCHAR>();
      fieldIndex = input->type()->asRow().getChildIdx(fieldName);
      name = toName(fieldName);
    }

    steps.push_back(
        {.kind = StepKind::kField, .field = name, .id = fieldIndex.value()});
    markSubfields(input, steps, isControl, context, sources);
    steps.pop_back();
    return;
  }

  if (expr->isCall()) {
    const auto& name = expr->asUnchecked<lp::CallExpr>()->name();
    if (name == "cardinality") {
      steps.push_back({.kind = StepKind::kCardinality});
      markSubfields(expr->inputAt(0), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }

    if (name == "subscript" || name == "element_at") {
      auto constant = tryFoldConstant(expr->inputAt(1));
      if (!constant) {
        std::vector<Step> subSteps;
        markSubfields(expr->inputAt(1), subSteps, isControl, context, sources);
        steps.push_back({.kind = StepKind::kSubscript, .allFields = true});
        markSubfields(expr->inputAt(0), steps, isControl, context, sources);
        steps.pop_back();
        return;
      }

      const auto& value = constant->value();
      if (value->kind() == TypeKind::VARCHAR) {
        const auto& str = value->value<TypeKind::VARCHAR>();
        steps.push_back({.kind = StepKind::kSubscript, .field = toName(str)});
      } else {
        const auto& id = integerValue(value.get());
        steps.push_back({.kind = StepKind::kSubscript, .id = id});
      }

      markSubfields(expr->inputAt(0), steps, isControl, context, sources);
      steps.pop_back();
      return;
    }

    const auto* metadata = functionMetadata(toName(name));
    if (!metadata || !metadata->processSubfields()) {
      for (const auto& input : expr->inputs()) {
        std::vector<Step> steps;
        markSubfields(input, steps, isControl, context, sources);
      }
      return;
    }

    // The function has non-default metadata. Record subfields.
    const auto* call = expr->asUnchecked<lp::CallExpr>();
    const auto* path = stepsToPath(steps);
    auto* fields = isControl ? &controlSubfields_ : &payloadSubfields_;
    if (fields->argFields[call].resultPaths[ResultAccess::kSelf].contains(
            path->id())) {
      // Already marked.
      return;
    }
    fields->argFields[call].resultPaths[ResultAccess::kSelf].add(path->id());

    // If the function is some kind of constructor, like
    // make_row_from_map or make_named_row, then a path over it
    // selects one argument. If there is no path, all arguments are
    // implicitly accessed.
    if (metadata->valuePathToArgPath && !steps.empty()) {
      auto pair = metadata->valuePathToArgPath(steps, *call);
      markSubfields(
          expr->inputAt(pair.second), pair.first, isControl, context, sources);
      return;
    }
    for (auto i = 0; i < expr->inputs().size(); ++i) {
      if (metadata->subfieldArg == i) {
        // A subfield of func is a subfield of one arg.
        markSubfields(expr->inputAt(i), steps, isControl, context, sources);
        continue;
      }

      if (!steps.empty() && steps.back().kind == StepKind::kField) {
        const auto maybeNth = stepToArg(steps.back(), metadata);
        if (maybeNth.has_value() && maybeNth.value() == i) {
          auto newSteps = steps;
          const auto* argPath = stepsToPath(newSteps);
          fields->argFields[expr].resultPaths[maybeNth.value()].add(
              argPath->id());
          newSteps.pop_back();
          markSubfields(
              expr->inputs()[maybeNth.value()],
              newSteps,
              isControl,
              context,
              sources);
          continue;
        }

        if (std::find(
                metadata->fieldIndexForArg.begin(),
                metadata->fieldIndexForArg.end(),
                i) != metadata->fieldIndexForArg.end()) {
          // The ith argument corresponds to some subfield field index
          // other than the one in path, so this argument is not
          // referenced.
          continue;
        }
      }

      if (metadata->lambdaInfo(i)) {
        const auto& argType = lambdaArgType(expr->inputAt(i).get());

        std::vector<const RowType*> newContext = {argType.get()};
        newContext.insert(newContext.end(), context.begin(), context.end());

        std::vector<LogicalContextSource> newSources = {
            {.call = call, .lambdaOrdinal = i}};
        newSources.insert(newSources.end(), sources.begin(), sources.end());

        const auto* lambda = expr->inputAt(i)->asUnchecked<lp::LambdaExpr>();
        std::vector<Step> empty;
        markSubfields(lambda->body(), empty, isControl, newContext, newSources);
        continue;
      }
      // The argument is not special, just mark through without path.
      std::vector<Step> empty;
      markSubfields(expr->inputAt(i), empty, isControl, context, sources);
    }
    return;
  }

  if (expr->isConstant()) {
    return;
  }

  if (expr->isSpecialForm()) {
    for (const auto& input : expr->inputs()) {
      std::vector<Step> steps;
      markSubfields(input, steps, isControl, context, sources);
    }
    return;
  }

  VELOX_UNREACHABLE("Unhandled expr: {}", lp::ExprPrinter::toText(*expr));
}

void ToGraph::markColumnSubfields(
    const lp::LogicalPlanNodePtr& source,
    std::span<const lp::ExprPtr> columns) {
  const auto context = std::array{source->outputType().get()};
  const auto sources =
      std::array{LogicalContextSource{.planNode = source.get()}};
  for (const auto& column : columns) {
    std::vector<Step> steps;
    markSubfields(column, steps, /* isControl */ true, context, sources);
  }
}

void ToGraph::markControl(const lp::LogicalPlanNode& node) {
  const auto kind = node.kind();
  if (kind == lp::NodeKind::kJoin) {
    const auto* join = node.asUnchecked<lp::JoinNode>();
    if (const auto& condition = join->condition()) {
      std::vector<const RowType*> context{
          join->left()->outputType().get(), join->right()->outputType().get()};
      std::vector<LogicalContextSource> sources{
          {.planNode = join->left().get()}, {.planNode = join->right().get()}};
      std::vector<Step> steps;
      markSubfields(condition, steps, /* isControl */ true, context, sources);
    }
  } else if (kind == lp::NodeKind::kFilter) {
    const auto& filter = node.asUnchecked<lp::FilterNode>();
    markColumnSubfields(node.onlyInput(), std::array{filter->predicate()});
  } else if (kind == lp::NodeKind::kAggregate) {
    const auto* agg = node.asUnchecked<lp::AggregateNode>();
    markColumnSubfields(node.onlyInput(), agg->groupingKeys());
  } else if (kind == lp::NodeKind::kSort) {
    const auto* order = node.asUnchecked<lp::SortNode>();
    std::vector<lp::ExprPtr> keys;
    for (const auto& key : order->ordering()) {
      keys.push_back(key.expression);
    }
    markColumnSubfields(node.onlyInput(), keys);
  } else if (kind == lp::NodeKind::kSet) {
    auto* set = reinterpret_cast<const lp::SetNode*>(&node);
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

  for (const auto& source : node.inputs()) {
    markControl(*source);
  }
}

void ToGraph::markAllSubfields(
    const RowType* type,
    const lp::LogicalPlanNode& node) {
  markControl(node);

  LogicalContextSource source = {.planNode = &node};

  std::vector<const RowType*> context;
  std::vector<LogicalContextSource> sources;
  for (auto i = 0; i < type->size(); ++i) {
    std::vector<Step> steps;
    markFieldAccessed(
        source, i, steps, /* isControl */ false, context, sources);
  }
}

std::vector<int32_t> ToGraph::usedChannels(const lp::LogicalPlanNode& node) {
  const auto& control = controlSubfields_.nodeFields[&node];
  const auto& payload = payloadSubfields_.nodeFields[&node];

  BitSet unique;
  std::vector<int32_t> result;
  for (const auto& [index, _] : control.resultPaths) {
    result.push_back(index);
    unique.add(index);
  }

  for (const auto& pair : payload.resultPaths) {
    if (!unique.contains(pair.first)) {
      result.push_back(pair.first);
    }
  }
  return result;
}

namespace {

template <typename T>
lp::ExprPtr makeKey(const TypePtr& type, T value) {
  return std::make_shared<lp::ConstantExpr>(
      type, std::make_shared<Variant>(value));
}
} // namespace

// static
lp::ExprPtr ToGraph::stepToLogicalPlanGetter(
    Step step,
    const lp::ExprPtr& arg) {
  const auto& argType = arg->type();
  switch (step.kind) {
    case StepKind::kField: {
      lp::ExprPtr key;
      const TypePtr* type;
      if (step.field) {
        key = makeKey(VARCHAR(), step.field);
        type = &argType->asRow().findChild(step.field);
      } else {
        key = makeKey<int32_t>(INTEGER(), step.id);
        type = &argType->childAt(step.id);
      }

      return std::make_shared<lp::SpecialFormExpr>(
          *type,
          lp::SpecialForm::kDereference,
          arg,
          makeKey(VARCHAR(), step.field));
    }

    case StepKind::kSubscript: {
      if (argType->kind() == TypeKind::ARRAY) {
        return std::make_shared<lp::CallExpr>(
            argType->childAt(0),
            "subscript",
            arg,
            makeKey<int32_t>(INTEGER(), step.id));
      }

      lp::ExprPtr key;
      switch (argType->childAt(0)->kind()) {
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
          argType->childAt(1), "subscript", arg, key);
    }

    default:
      VELOX_NYI();
  }
}

std::string PlanSubfields::toString() const {
  std::stringstream out;

  auto appendPaths = [&](const auto& resultPaths) {
    for (const auto& [index, paths] : resultPaths) {
      out << index << " -> {";
      paths.forEach(
          [&](auto i) { out << queryCtx()->pathById(i)->toString(); });
      out << "}" << std::endl;
    }
  };

  out << "Nodes: ";
  for (const auto& [node, access] : nodeFields) {
    out << "Node " << node->id() << " = {";
    appendPaths(access.resultPaths);
    out << "}" << std::endl;
  }

  if (!argFields.empty()) {
    out << "Functions: ";
    for (const auto& [expr, access] : argFields) {
      out << "Func " << lp::ExprPrinter::toText(*expr) << " = {";
      appendPaths(access.resultPaths);
      out << "}" << std::endl;
    }
  }
  return out.str();
}

} // namespace facebook::velox::optimizer
