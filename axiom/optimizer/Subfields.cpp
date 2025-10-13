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

#include <ranges>

namespace facebook::axiom::optimizer {
namespace {

namespace lp = facebook::axiom::logical_plan;

PathCP stepsToPath(std::span<const Step> steps) {
  return toPath(steps, true);
}

struct MarkFieldsAccessedContextArray {
  std::array<const velox::RowType* const, 1> rowTypes;
  std::array<const LogicalContextSource, 1> sources;

  MarkFieldsAccessedContext toCtx() const {
    return {rowTypes, sources};
  }
};

struct MarkFieldsAccessedContextVector {
  std::vector<const velox::RowType*> rowTypes;
  std::vector<LogicalContextSource> sources;

  MarkFieldsAccessedContext toCtx() const {
    return {rowTypes, sources};
  }
};

MarkFieldsAccessedContextArray fromNode(const lp::LogicalPlanNodePtr& node) {
  return {
      {node->outputType().get()},
      {LogicalContextSource{.planNode = node.get()}}};
}

MarkFieldsAccessedContextVector fromNodes(
    const std::vector<lp::LogicalPlanNodePtr>& nodes) {
  std::vector<const velox::RowType*> rowTypes;
  std::vector<LogicalContextSource> sources;
  rowTypes.reserve(nodes.size());
  sources.reserve(nodes.size());
  for (const auto& node : nodes) {
    rowTypes.push_back(node->outputType().get());
    sources.push_back(LogicalContextSource{.planNode = node.get()});
  }
  return {std::move(rowTypes), std::move(sources)};
}

} // namespace

void ToGraph::markFieldAccessed(
    const lp::ProjectNode& project,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  const auto& input = project.onlyInput();
  const auto ctx = fromNode(input);
  markSubfields(project.expressionAt(ordinal), steps, isControl, ctx.toCtx());
}

void ToGraph::markFieldAccessed(
    const lp::UnnestNode& unnest,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  const auto& input = unnest.onlyInput();
  if (ordinal < input->outputType()->size()) {
    const auto ctx = fromNode(input);
    markFieldAccessed(ctx.sources[0], ordinal, steps, isControl, ctx.toCtx());
  }
}

void ToGraph::markFieldAccessed(
    const lp::AggregateNode& agg,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl) {
  const auto& input = agg.onlyInput();

  std::vector<Step> subSteps;
  const auto ctx = fromNode(input);
  auto mark = [&](const lp::ExprPtr& expr) {
    markSubfields(expr, subSteps, isControl, ctx.toCtx());
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
    const auto ctx = fromNode(input);
    markFieldAccessed(ctx.sources[0], ordinal, steps, isControl, ctx.toCtx());
  }
}

void ToGraph::markFieldAccessed(
    const LogicalContextSource& source,
    int32_t ordinal,
    std::vector<Step>& steps,
    bool isControl,
    const MarkFieldsAccessedContext& context) {
  if (!source.planNode) {
    // The source is a lambda arg. We apply the path to the corresponding
    // container arg of the 2nd order function call that has the lambda.
    const auto* metadata = functionMetadata(toName(source.call->name()));
    const auto* lambdaInfo = metadata->lambdaInfo(source.lambdaOrdinal);
    const auto nth = lambdaInfo->argOrdinal[ordinal];

    markSubfields(
        source.call->inputAt(nth),
        steps,
        isControl,
        {context.rowTypes.subspan(1), context.sources.subspan(1)});
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

  if (kind == lp::NodeKind::kUnnest) {
    const auto* unnest = source.planNode->asUnchecked<lp::UnnestNode>();
    markFieldAccessed(*unnest, ordinal, steps, isControl);
    return;
  }

  if (kind == lp::NodeKind::kTableWrite) {
    // We cannout pushdown subfield access to TableWriteNode output.
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
          static_cast<int32_t>(maybeIdx.value()),
          steps,
          isControl,
          context);
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
  return std::ranges::all_of(expr->inputs(), looksConstant);
}

} // namespace

lp::ConstantExprPtr ToGraph::tryFoldConstant(const lp::ExprPtr& expr) {
  if (expr->isConstant()) {
    return std::static_pointer_cast<const lp::ConstantExpr>(expr);
  }

  if (looksConstant(expr)) {
    auto literal = translateExpr(expr);
    if (literal->is(PlanType::kLiteralExpr)) {
      return std::make_shared<lp::ConstantExpr>(
          toTypePtr(literal->value().type),
          std::make_shared<velox::Variant>(literal->as<Literal>()->literal()));
    }
  }
  return nullptr;
}

void ToGraph::markSubfields(
    const lp::ExprPtr& expr,
    std::vector<Step>& steps,
    bool isControl,
    const MarkFieldsAccessedContext& context) {
  if (expr->isInputReference()) {
    const auto& name = expr->asUnchecked<lp::InputReferenceExpr>()->name();
    for (auto i = 0; i < context.sources.size(); ++i) {
      if (auto maybeIdx = context.rowTypes[i]->getChildIdxIfExists(name)) {
        markFieldAccessed(
            context.sources[i],
            static_cast<int32_t>(maybeIdx.value()),
            steps,
            isControl,
            context);
        return;
      }
    }
    VELOX_FAIL("Field not found {}", name);
  }

  if (isSpecialForm(expr, lp::SpecialForm::kDereference)) {
    VELOX_CHECK(expr->inputAt(1)->isConstant());
    const auto* field = expr->inputAt(1)->asUnchecked<lp::ConstantExpr>();
    const auto& input = expr->inputAt(0);

    // Always fill both index and name for a struct getter.
    auto fieldIndex = maybeIntegerLiteral(field);
    Name name = nullptr;
    if (fieldIndex.has_value()) {
      name = toName(input->type()->asRow().nameOf(fieldIndex.value()));
    } else {
      const auto& fieldName = field->value()->value<velox::TypeKind::VARCHAR>();
      fieldIndex = input->type()->asRow().getChildIdx(fieldName);
      name = toName(fieldName);
    }

    steps.push_back(
        {.kind = StepKind::kField, .field = name, .id = fieldIndex.value()});
    markSubfields(input, steps, isControl, context);
    steps.pop_back();
    return;
  }

  if (expr->isCall()) {
    auto name = toName(expr->asUnchecked<lp::CallExpr>()->name());
    if (name == cardinality_) {
      steps.push_back({.kind = StepKind::kCardinality});
      markSubfields(expr->inputAt(0), steps, isControl, context);
      steps.pop_back();
      return;
    }

    if (name == subscript_ || name == elementAt_) {
      auto constant = tryFoldConstant(expr->inputAt(1));
      if (!constant) {
        std::vector<Step> subSteps;
        markSubfields(expr->inputAt(1), subSteps, isControl, context);

        steps.push_back({.kind = StepKind::kSubscript, .allFields = true});
        markSubfields(expr->inputAt(0), steps, isControl, context);
        steps.pop_back();
        return;
      }

      const auto& value = constant->value();
      if (value->kind() == velox::TypeKind::VARCHAR) {
        const auto& str = value->value<velox::TypeKind::VARCHAR>();
        steps.push_back({.kind = StepKind::kSubscript, .field = toName(str)});
      } else {
        const auto& id = integerValue(value.get());
        steps.push_back({.kind = StepKind::kSubscript, .id = id});
      }

      markSubfields(expr->inputAt(0), steps, isControl, context);
      steps.pop_back();
      return;
    }

    const auto* metadata = functionMetadata(toName(name));
    if (!metadata || !metadata->processSubfields()) {
      std::vector<Step> argumentSteps;
      for (const auto& input : expr->inputs()) {
        markSubfields(input, argumentSteps, isControl, context);
        VELOX_DCHECK(argumentSteps.empty());
      }
      return;
    }

    // The function has non-default metadata. Record subfields.
    const auto* call = expr->asUnchecked<lp::CallExpr>();
    const auto* path = stepsToPath(steps);
    auto* fields = isControl ? &controlSubfields_ : &payloadSubfields_;
    auto& paths = fields->argFields[call].resultPaths[ResultAccess::kSelf];
    if (paths.contains(path->id())) {
      // Already marked.
      return;
    }
    paths.add(path->id());

    // If the function is some kind of constructor, like
    // make_row_from_map or make_named_row, then a path over it
    // selects one argument. If there is no path, all arguments are
    // implicitly accessed.
    if (metadata->valuePathToArgPath && !steps.empty()) {
      auto pair = metadata->valuePathToArgPath(steps, *call);
      markSubfields(expr->inputAt(pair.second), pair.first, isControl, context);
      return;
    }
    for (auto i = 0; i < expr->inputs().size(); ++i) {
      if (metadata->subfieldArg == i) {
        // A subfield of func is a subfield of one arg.
        markSubfields(expr->inputAt(i), steps, isControl, context);
        continue;
      }

      if (!steps.empty() && steps.back().kind == StepKind::kField) {
        const auto maybeNth = stepToArg(steps.back(), metadata);
        if (maybeNth.has_value() && maybeNth.value() == i) {
          auto newSteps = steps;
          const auto* argPath = stepsToPath(newSteps);
          fields->argFields[expr.get()].resultPaths[maybeNth.value()].add(
              argPath->id());
          newSteps.pop_back();
          markSubfields(
              expr->inputs()[maybeNth.value()], newSteps, isControl, context);
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
        const auto* lambda = expr->inputAt(i)->asUnchecked<lp::LambdaExpr>();
        const auto& argType = lambda->signature();

        std::vector<const velox::RowType*> newRowTypes;
        newRowTypes.reserve(context.rowTypes.size() + 1);
        newRowTypes.push_back(argType.get());
        newRowTypes.insert(
            newRowTypes.end(),
            context.rowTypes.begin(),
            context.rowTypes.end());

        std::vector<LogicalContextSource> newSources;
        newSources.reserve(context.sources.size() + 1);
        newSources.push_back({.call = call, .lambdaOrdinal = i});
        newSources.insert(
            newSources.end(), context.sources.begin(), context.sources.end());

        std::vector<Step> lambdaSteps;
        markSubfields(
            lambda->body(), lambdaSteps, isControl, {newRowTypes, newSources});
        VELOX_DCHECK(lambdaSteps.empty());
        continue;
      }
      // The argument is not special, just mark through without path.
      std::vector<Step> argumentSteps;
      markSubfields(expr->inputAt(i), argumentSteps, isControl, context);
      VELOX_DCHECK(argumentSteps.empty());
    }
    return;
  }

  if (expr->isConstant()) {
    return;
  }

  if (expr->isSpecialForm()) {
    std::vector<Step> specialFormSteps;
    for (const auto& input : expr->inputs()) {
      markSubfields(input, specialFormSteps, isControl, context);
      VELOX_DCHECK(specialFormSteps.empty());
    }
    return;
  }

  VELOX_UNREACHABLE("Unhandled expr: {}", lp::ExprPrinter::toText(*expr));
}

void ToGraph::markColumnSubfields(
    const lp::LogicalPlanNodePtr& source,
    std::span<const lp::ExprPtr> columns,
    bool isControl) {
  const auto ctx = fromNode(source);
  std::vector<Step> steps;
  for (const auto& column : columns) {
    markSubfields(column, steps, isControl, ctx.toCtx());
    VELOX_DCHECK(steps.empty());
  }
}

void ToGraph::markControl(const lp::LogicalPlanNode& node) {
  const auto kind = node.kind();
  if (kind == lp::NodeKind::kJoin) {
    const auto* join = node.asUnchecked<lp::JoinNode>();
    if (const auto& condition = join->condition()) {
      std::vector<Step> steps;
      markSubfields(condition, steps, true, fromNodes(join->inputs()).toCtx());
    }

  } else if (kind == lp::NodeKind::kUnnest) {
    const auto& unnest = node.asUnchecked<lp::UnnestNode>();
    markColumnSubfields(node.onlyInput(), unnest->unnestExpressions());

  } else if (kind == lp::NodeKind::kFilter) {
    const auto& filter = node.asUnchecked<lp::FilterNode>();
    markColumnSubfields(node.onlyInput(), std::array{filter->predicate()});

  } else if (kind == lp::NodeKind::kTableWrite) {
    const auto& write = *node.asUnchecked<lp::TableWriteNode>();
    // All columns are needed for write, but they are all not control columns.
    markColumnSubfields(node.onlyInput(), write.columnExpressions(), false);

  } else if (kind == lp::NodeKind::kAggregate) {
    const auto& agg = *node.asUnchecked<lp::AggregateNode>();
    markColumnSubfields(node.onlyInput(), agg.groupingKeys());

  } else if (kind == lp::NodeKind::kSort) {
    const auto& order = *node.asUnchecked<lp::SortNode>();
    const auto ctx = fromNode(node.onlyInput());
    std::vector<Step> steps;
    for (const auto& key : order.ordering()) {
      markSubfields(key.expression, steps, true, ctx.toCtx());
      VELOX_DCHECK(steps.empty());
    }

  } else if (kind == lp::NodeKind::kSet) {
    const auto& set = *node.asUnchecked<lp::SetNode>();
    // If this is with a distinct every column is a control column.
    if (set.operation() != lp::SetOperation::kUnionAll) {
      std::vector<Step> steps;
      for (auto i = 0; i < set.outputType()->size(); ++i) {
        for (const auto& input : set.inputs()) {
          const auto ctx = fromNode(input);
          markFieldAccessed(ctx.sources[0], i, steps, true, ctx.toCtx());
          VELOX_CHECK(steps.empty());
        }
      }
    }
  }

  for (const auto& source : node.inputs()) {
    markControl(*source);
  }
}

void ToGraph::markAllSubfields(const lp::LogicalPlanNode& node) {
  markControl(node);

  LogicalContextSource source = {.planNode = &node};
  std::vector<Step> steps;
  for (auto i = 0; i < node.outputType()->size(); ++i) {
    markFieldAccessed(source, i, steps, false, {});
    VELOX_CHECK(steps.empty());
  }
}

std::vector<int32_t> ToGraph::usedChannels(const lp::LogicalPlanNode& node) {
  const auto& control = controlSubfields_.nodeFields[&node];
  const auto& payload = payloadSubfields_.nodeFields[&node];
  std::vector<int32_t> result;
  std::ranges::set_union(
      control.resultPaths | std::views::keys,
      payload.resultPaths | std::views::keys,
      std::back_inserter(result));
  return result;
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

} // namespace facebook::axiom::optimizer
