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

#include "axiom/optimizer/ToVelox.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "velox/core/PlanConsistencyChecker.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/vector/VariantToVector.h"

namespace facebook::axiom::optimizer {

std::string PlanAndStats::toString() const {
  return plan->toString(
      true,
      [&](const velox::core::PlanNodeId& planNodeId,
          std::string_view indentation,
          std::ostream& out) {
        auto it = prediction.find(planNodeId);
        if (it != prediction.end()) {
          out << indentation << "Estimate: " << it->second.cardinality
              << " rows, "
              << velox::succinctBytes(
                     static_cast<uint64_t>(it->second.peakMemory))
              << " peak memory" << std::endl;
        }
      });
}

ToVelox::ToVelox(
    const runner::MultiFragmentPlan::Options& options,
    const OptimizerOptions& optimizerOptions)
    : options_{options},
      optimizerOptions_{optimizerOptions},
      isSingle_{options.numWorkers == 1},
      subscript_{FunctionRegistry::instance()->subscript()} {}

namespace {

std::vector<velox::common::Subfield> columnSubfields(
    BaseTableCP table,
    int32_t id) {
  auto* optimization = queryCtx()->optimization();

  const auto columnName = queryCtx()->objectAt(id)->as<Column>()->name();

  BitSet set = table->columnSubfields(id, false, false);

  std::vector<velox::common::Subfield> subfields;
  set.forEach([&](auto id) {
    auto steps = queryCtx()->pathById(id)->steps();
    std::vector<std::unique_ptr<velox::common::Subfield::PathElement>> elements;
    elements.push_back(
        std::make_unique<velox::common::Subfield::NestedField>(columnName));
    bool first = true;
    for (auto& step : steps) {
      switch (step.kind) {
        case StepKind::kField:
          VELOX_CHECK_NOT_NULL(
              step.field, "Index subfield not suitable for pruning");
          elements.push_back(
              std::make_unique<velox::common::Subfield::NestedField>(
                  step.field));
          break;
        case StepKind::kSubscript:
          if (step.allFields) {
            elements.push_back(
                std::make_unique<velox::common::Subfield::AllSubscripts>());
            break;
          }
          if (first &&
              optimization->options().isMapAsStruct(
                  table->schemaTable->name, columnName)) {
            elements.push_back(
                std::make_unique<velox::common::Subfield::NestedField>(
                    step.field ? std::string(step.field)
                               : fmt::format("{}", step.id)));
            break;
          }
          if (step.field) {
            elements.push_back(
                std::make_unique<velox::common::Subfield::StringSubscript>(
                    step.field));
            break;
          }
          elements.push_back(
              std::make_unique<velox::common::Subfield::LongSubscript>(
                  step.id));
          break;
        case StepKind::kCardinality:
          VELOX_UNSUPPORTED();
      }
      first = false;
    }
    subfields.emplace_back(std::move(elements));
  });

  return subfields;
}

RelationOpPtr addGather(const RelationOpPtr& op) {
  if (op->distribution().distributionType.isGather) {
    return op;
  }
  if (op->relType() == RelType::kOrderBy) {
    auto order = op->distribution();
    auto final = Distribution::gather(order.orderKeys, order.orderTypes);
    auto* gather = make<Repartition>(op, final, op->columns());
    auto* orderBy = make<OrderBy>(gather, order.orderKeys, order.orderTypes);
    return orderBy;
  }
  auto* gather = make<Repartition>(op, Distribution::gather(), op->columns());
  return gather;
}

} // namespace

void ToVelox::filterUpdated(BaseTableCP table, bool updateSelectivity) {
  PlanObjectSet columnSet;
  for (auto& filter : table->columnFilters) {
    columnSet.unionSet(filter->columns());
  }
  auto leafColumns = columnSet.toObjects<Column>();

  columnAlteredTypes_.clear();

  ColumnVector topColumns;
  auto scanType = subfieldPushdownScanType(
      table, leafColumns, topColumns, columnAlteredTypes_);

  auto* optimization = queryCtx()->optimization();
  auto* evaluator = optimization->evaluator();

  std::vector<velox::core::TypedExprPtr> remainingConjuncts;
  std::vector<velox::core::TypedExprPtr> pushdownConjuncts;
  velox::ScopedVarSetter noAlias(&makeVeloxExprWithNoAlias_, true);
  velox::ScopedVarSetter getters(&getterForPushdownSubfield_, true);
  for (auto filter : table->columnFilters) {
    auto typedExpr = toTypedExpr(filter);
    try {
      auto pair = velox::exec::toSubfieldFilter(typedExpr, evaluator);
      if (!pair.second) {
        remainingConjuncts.push_back(std::move(typedExpr));
        continue;
      }
      pushdownConjuncts.push_back(typedExpr);
    } catch (const std::exception&) {
      remainingConjuncts.push_back(std::move(typedExpr));
    }
  }
  for (auto expr : table->filter) {
    remainingConjuncts.push_back(toTypedExpr(expr));
  }
  velox::core::TypedExprPtr remainingFilter;
  if (remainingConjuncts.size() == 1) {
    remainingFilter = std::move(remainingConjuncts[0]);
  } else if (!remainingConjuncts.empty()) {
    remainingFilter = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(),
        specialForm(logical_plan::SpecialForm::kAnd),
        std::move(remainingConjuncts));
  }

  columnAlteredTypes_.clear();

  auto& dataColumns = table->schemaTable->connectorTable->type();
  auto* layout = table->schemaTable->columnGroups[0]->layout;

  auto connector = layout->connector();
  auto* metadata = connector::ConnectorMetadata::metadata(connector);

  std::vector<velox::connector::ColumnHandlePtr> columns;
  for (int32_t i = 0; i < dataColumns->size(); ++i) {
    auto id = table->columnId(toName(dataColumns->nameOf(i)));
    if (!id.has_value()) {
      continue;
    }
    auto subfields = columnSubfields(table, id.value());

    columns.push_back(metadata->createColumnHandle(
        *layout, dataColumns->nameOf(i), std::move(subfields)));
  }
  auto allFilters = std::move(pushdownConjuncts);
  if (remainingFilter) {
    allFilters.push_back(remainingFilter);
  }
  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  auto handle = metadata->createTableHandle(
      *layout, columns, *evaluator, std::move(allFilters), rejectedFilters);

  setLeafHandle(table->id(), handle, std::move(rejectedFilters));
  if (updateSelectivity) {
    optimization->setLeafSelectivity(*const_cast<BaseTable*>(table), scanType);
  }
}

PlanAndStats ToVelox::toVeloxPlan(
    RelationOpPtr plan,
    const runner::MultiFragmentPlan::Options& options) {
  options_ = options;

  prediction_.clear();
  nodeHistory_.clear();

  if (options_.numWorkers > 1) {
    plan = addGather(plan);
  }

  runner::ExecutableFragment top;
  std::vector<runner::ExecutableFragment> stages;
  top.fragment.planNode = makeFragment(plan, top, stages);
  stages.push_back(std::move(top));

  for (const auto& stage : stages) {
    velox::core::PlanConsistencyChecker::check(stage.fragment.planNode);
  }

  return PlanAndStats{
      std::make_shared<runner::MultiFragmentPlan>(std::move(stages), options),
      std::move(nodeHistory_),
      std::move(prediction_)};
}

velox::RowTypePtr ToVelox::makeOutputType(const ColumnVector& columns) const {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  for (auto i = 0; i < columns.size(); ++i) {
    auto* column = columns[i];
    auto relation = column->relation();
    if (relation && relation->is(PlanType::kTableNode)) {
      auto* schemaTable = relation->as<BaseTable>()->schemaTable;
      if (!schemaTable) {
        continue;
      }

      auto runnerTable = schemaTable->connectorTable;
      if (runnerTable) {
        auto* runnerColumn = runnerTable->findColumn(std::string(
            column->topColumn() ? column->topColumn()->name()
                                : column->name()));
        VELOX_CHECK_NOT_NULL(runnerColumn);
      }
    }
    auto name = makeVeloxExprWithNoAlias_ ? std::string(column->name())
                                          : column->outputName();
    names.push_back(name);
    types.push_back(toTypePtr(columns[i]->value().type));
  }
  return ROW(std::move(names), std::move(types));
}

velox::core::TypedExprPtr ToVelox::toAnd(const ExprVector& exprs) {
  if (exprs.empty()) {
    return nullptr;
  }
  if (exprs.size() == 1) {
    return toTypedExpr(exprs[0]);
  }

  return std::make_shared<velox::core::CallTypedExpr>(
      velox::BOOLEAN(),
      specialForm(logical_plan::SpecialForm::kAnd),
      toTypedExprs(exprs));
}

namespace {

template <typename T>
velox::core::TypedExprPtr makeKey(const velox::TypePtr& type, T v) {
  return std::make_shared<velox::core::ConstantTypedExpr>(
      type, velox::Variant(v));
}

velox::core::TypedExprPtr createArrayForInList(
    const Call& call,
    const velox::TypePtr& elementType) {
  std::vector<velox::Variant> arrayElements;
  arrayElements.reserve(call.args().size() - 1);
  for (size_t i = 1; i < call.args().size(); ++i) {
    auto arg = call.args().at(i);
    VELOX_USER_CHECK(
        elementType->equivalent(*arg->value().type),
        "All elements of the IN list must have the same type got {} and {}",
        elementType->toString(),
        arg->value().type->toString());
    VELOX_USER_CHECK(arg->is(PlanType::kLiteralExpr));
    arrayElements.push_back(arg->as<Literal>()->literal());
  }
  auto arrayVector = variantToVector(
      ARRAY(elementType),
      velox::Variant::array(arrayElements),
      queryCtx()->optimization()->evaluator()->pool());
  return std::make_shared<velox::core::ConstantTypedExpr>(arrayVector);
}

velox::core::TypedExprPtr stepToGetter(
    Step step,
    velox::core::TypedExprPtr arg,
    const std::string& subscript) {
  switch (step.kind) {
    case StepKind::kField: {
      if (step.field) {
        auto& type = arg->type()->childAt(
            arg->type()->as<velox::TypeKind::ROW>().getChildIdx(step.field));
        return std::make_shared<velox::core::FieldAccessTypedExpr>(
            type, arg, step.field);
      }
      auto& type = arg->type()->childAt(step.id);
      return std::make_shared<velox::core::DereferenceTypedExpr>(
          type, arg, step.id);
    }
    case StepKind::kSubscript: {
      auto& type = arg->type();
      if (type->kind() == velox::TypeKind::MAP) {
        velox::core::TypedExprPtr key;
        switch (type->as<velox::TypeKind::MAP>().childAt(0)->kind()) {
          case velox::TypeKind::VARCHAR:
            key = makeKey(velox::VARCHAR(), step.field);
            break;
          case velox::TypeKind::BIGINT:
            key = makeKey(velox::BIGINT(), step.id);
            break;
          case velox::TypeKind::INTEGER:
            key = makeKey(velox::INTEGER(), static_cast<int32_t>(step.id));
            break;
          case velox::TypeKind::SMALLINT:
            key = makeKey(velox::SMALLINT(), static_cast<int16_t>(step.id));
            break;
          case velox::TypeKind::TINYINT:
            key = makeKey(velox::TINYINT(), static_cast<int8_t>(step.id));
            break;
          default:
            VELOX_FAIL("Unsupported key type");
        }

        return std::make_shared<velox::core::CallTypedExpr>(
            type->childAt(1), subscript, arg, key);
      }
      return std::make_shared<velox::core::CallTypedExpr>(
          type->childAt(0),
          subscript,
          arg,
          makeKey(velox::INTEGER(), static_cast<int32_t>(step.id)));
    }

    default:
      VELOX_NYI();
  }
}

} // namespace

velox::core::TypedExprPtr ToVelox::pathToGetter(
    ColumnCP column,
    PathCP path,
    velox::core::TypedExprPtr field) {
  bool first = true;
  // If this is a path over a map that is retrieved as struct, the first getter
  // becomes a struct getter.
  auto alterStep = [&](ColumnCP, const Step& step, Step& newStep) {
    auto* rel = column->relation();
    if (rel->is(PlanType::kTableNode) &&
        isMapAsStruct(
            rel->as<BaseTable>()->schemaTable->name, column->name())) {
      // This column is a map to project out as struct.
      newStep.kind = StepKind::kField;
      if (step.field) {
        newStep.field = step.field;
      } else {
        newStep.field = toName(fmt::format("{}", step.id));
      }
      return true;
    }
    return false;
  };

  for (auto& step : path->steps()) {
    Step newStep;
    if (first && alterStep(column, step, newStep)) {
      field = stepToGetter(newStep, field, subscript_.value());
      first = false;
      continue;
    }
    first = false;
    field = stepToGetter(step, field, subscript_.value());
  }
  return field;
}

std::vector<velox::core::TypedExprPtr> ToVelox::toTypedExprs(
    const ExprVector& exprs) {
  std::vector<velox::core::TypedExprPtr> typedExprs;
  typedExprs.reserve(exprs.size());
  for (auto expr : exprs) {
    typedExprs.emplace_back(toTypedExpr(expr));
  }
  return typedExprs;
}

velox::core::TypedExprPtr ToVelox::toTypedExpr(ExprCP expr) {
  auto it = projectedExprs_.find(expr);
  if (it != projectedExprs_.end()) {
    return it->second;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr: {
      auto column = expr->as<Column>();
      if (column->topColumn() && getterForPushdownSubfield_) {
        auto field = toTypedExpr(column->topColumn());
        return pathToGetter(column->topColumn(), column->path(), field);
      }
      auto name = makeVeloxExprWithNoAlias_ ? std::string(column->name())
                                            : column->outputName();
      // Check if a top level map should be retrieved as struct.
      auto it = columnAlteredTypes_.find(column);
      if (it != columnAlteredTypes_.end()) {
        return std::make_shared<velox::core::FieldAccessTypedExpr>(
            it->second, name);
      }
      return std::make_shared<velox::core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), name);
    }
    case PlanType::kCallExpr: {
      std::vector<velox::core::TypedExprPtr> inputs;
      auto call = expr->as<Call>();

      if (call->name() == SpecialFormCallNames::kIn) {
        VELOX_USER_CHECK_GE(call->args().size(), 2);
        inputs.push_back(toTypedExpr(call->args()[0]));
        inputs.push_back(createArrayForInList(*call, inputs.back()->type()));
      } else {
        for (auto arg : call->args()) {
          inputs.push_back(toTypedExpr(arg));
        }
      }

      if (auto form = SpecialFormCallNames::tryFromCallName(call->name())) {
        if (form == logical_plan::SpecialForm::kCast) {
          return std::make_shared<velox::core::CastTypedExpr>(
              toTypePtr(expr->value().type), std::move(inputs), false);
        }

        if (form == logical_plan::SpecialForm::kTryCast) {
          return std::make_shared<velox::core::CastTypedExpr>(
              toTypePtr(expr->value().type), std::move(inputs), true);
        }

        return std::make_shared<velox::core::CallTypedExpr>(
            toTypePtr(expr->value().type),
            std::move(inputs),
            specialForm(*form));
      }

      return std::make_shared<velox::core::CallTypedExpr>(
          toTypePtr(expr->value().type), std::move(inputs), call->name());
    }
    case PlanType::kFieldExpr: {
      auto* field = expr->as<Field>()->field();
      if (field) {
        return std::make_shared<velox::core::FieldAccessTypedExpr>(
            toTypePtr(expr->value().type),
            toTypedExpr(expr->as<Field>()->base()),
            field);
      }
      return std::make_shared<velox::core::DereferenceTypedExpr>(
          toTypePtr(expr->value().type),
          toTypedExpr(expr->as<Field>()->base()),
          expr->as<Field>()->index());
      break;
    }
    case PlanType::kLiteralExpr: {
      auto literal = expr->as<Literal>();
      if (literal->vector()) {
        return std::make_shared<velox::core::ConstantTypedExpr>(
            queryCtx()->toVectorPtr(literal->vector()));
      }
      // Complex constants must be vectors for constant folding to work.
      if (literal->value().type->kind() >= velox::TypeKind::ARRAY) {
        return std::make_shared<velox::core::ConstantTypedExpr>(variantToVector(
            toTypePtr(literal->value().type),
            literal->literal(),
            queryCtx()->optimization()->evaluator()->pool()));
      }
      return std::make_shared<velox::core::ConstantTypedExpr>(
          toTypePtr(literal->value().type), literal->literal());
    }
    case PlanType::kLambdaExpr: {
      auto* lambda = expr->as<Lambda>();
      std::vector<std::string> names;
      std::vector<velox::TypePtr> types;
      for (auto& c : lambda->args()) {
        names.push_back(c->toString());
        types.push_back(toTypePtr(c->value().type));
      }
      return std::make_shared<velox::core::LambdaTypedExpr>(
          ROW(std::move(names), std::move(types)), toTypedExpr(lambda->body()));
    }
    default:
      VELOX_FAIL("Cannot translate {} to TypeExpr", expr->toString());
  }
}

runner::ExecutableFragment ToVelox::newFragment() {
  runner::ExecutableFragment fragment;
  fragment.width = options_.numWorkers;
  fragment.taskPrefix = fmt::format("stage{}", ++stageCounter_);

  return fragment;
}

namespace {
velox::core::PlanNodePtr addPartialLimit(
    const velox::core::PlanNodeId& id,
    int64_t offset,
    int64_t limit,
    const velox::core::PlanNodePtr& input) {
  return std::make_shared<velox::core::LimitNode>(
      id,
      offset,
      limit,
      /* isPartial */ true,
      input);
}

velox::core::PlanNodePtr addFinalLimit(
    const velox::core::PlanNodeId& id,
    int64_t offset,
    int64_t limit,
    const velox::core::PlanNodePtr& input) {
  return std::make_shared<velox::core::LimitNode>(
      id,
      offset,
      limit,
      /* isPartial */ false,
      input);
}

velox::core::PlanNodePtr addLocalGather(
    const velox::core::PlanNodeId& id,
    const velox::core::PlanNodePtr& input) {
  return velox::core::LocalPartitionNode::gather(
      id, std::vector<velox::core::PlanNodePtr>{input});
}

velox::core::PlanNodePtr addLocalMerge(
    const velox::core::PlanNodeId& id,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& keys,
    const std::vector<velox::core::SortOrder>& sortOrder,
    const velox::core::PlanNodePtr& input) {
  return std::make_shared<velox::core::LocalMergeNode>(
      id, keys, sortOrder, std::vector<velox::core::PlanNodePtr>{input});
}

velox::core::PlanNodePtr addPartialTopN(
    const velox::core::PlanNodeId& id,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& keys,
    const std::vector<velox::core::SortOrder>& sortOrder,
    int64_t count,
    const velox::core::PlanNodePtr& input) {
  return std::make_shared<velox::core::TopNNode>(
      id,
      keys,
      sortOrder,
      count,
      /* isPartial */ true,
      input);
}

velox::core::PlanNodePtr addFinalTopN(
    const velox::core::PlanNodeId& id,
    const std::vector<velox::core::FieldAccessTypedExprPtr>& keys,
    const std::vector<velox::core::SortOrder>& sortOrder,
    int64_t count,
    const velox::core::PlanNodePtr& input) {
  return std::make_shared<velox::core::TopNNode>(
      id,
      keys,
      sortOrder,
      count,
      /* isPartial */ false,
      input);
}

velox::core::SortOrder toSortOrder(const OrderType& order) {
  return order == OrderType::kAscNullsFirst ? velox::core::kAscNullsFirst
      : order == OrderType ::kAscNullsLast  ? velox::core::kAscNullsLast
      : order == OrderType::kDescNullsFirst ? velox::core::kDescNullsFirst
                                            : velox::core::kDescNullsLast;
}

std::vector<velox::core::SortOrder> toSortOrders(
    const OrderTypeVector& orders) {
  std::vector<velox::core::SortOrder> sortOrders;
  sortOrders.reserve(orders.size());
  for (auto order : orders) {
    sortOrders.emplace_back(toSortOrder(order));
  }
  return sortOrders;
}
} // namespace

velox::core::FieldAccessTypedExprPtr ToVelox::toFieldRef(ExprCP expr) {
  VELOX_CHECK(
      expr->is(PlanType::kColumnExpr),
      "Expected column expression, but got: {} {}",
      PlanTypeName::toName(expr->type()),
      expr->toString());

  auto column = expr->as<Column>();
  return std::make_shared<velox::core::FieldAccessTypedExpr>(
      toTypePtr(column->value().type), column->outputName());
}

std::vector<velox::core::FieldAccessTypedExprPtr> ToVelox::toFieldRefs(
    const ExprVector& exprs) {
  std::vector<velox::core::FieldAccessTypedExprPtr> fields;
  fields.reserve(exprs.size());
  for (const auto& expr : exprs) {
    fields.push_back(toFieldRef(expr));
  }

  return fields;
}

velox::core::PlanNodePtr ToVelox::makeOrderBy(
    const OrderBy& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto sortOrder = toSortOrders(op.distribution().orderTypes);
  auto keys = toFieldRefs(op.distribution().orderKeys);

  if (isSingle_) {
    auto input = makeFragment(op.input(), fragment, stages);

    if (options_.numDrivers == 1) {
      if (op.limit <= 0) {
        return std::make_shared<velox::core::OrderByNode>(
            nextId(), keys, sortOrder, false, input);
      }

      auto node =
          addFinalTopN(nextId(), keys, sortOrder, op.limit + op.offset, input);

      if (op.offset > 0) {
        return addFinalLimit(nextId(), op.offset, op.limit, node);
      }

      return node;
    }

    velox::core::PlanNodePtr node;
    if (op.limit <= 0) {
      node = std::make_shared<velox::core::OrderByNode>(
          nextId(), keys, sortOrder, true, input);
    } else {
      node = addPartialTopN(
          nextId(), keys, sortOrder, op.limit + op.offset, input);
    }

    node = addLocalMerge(nextId(), keys, sortOrder, node);

    if (op.limit > 0) {
      return addFinalLimit(nextId(), op.offset, op.limit, node);
    }

    return node;
  }

  auto source = newFragment();
  auto input = makeFragment(op.input(), source, stages);

  velox::core::PlanNodePtr node;
  if (op.limit <= 0) {
    node = std::make_shared<velox::core::OrderByNode>(
        nextId(), keys, sortOrder, true, input);
  } else {
    node =
        addPartialTopN(nextId(), keys, sortOrder, op.limit + op.offset, input);
  }

  node = addLocalMerge(nextId(), keys, sortOrder, node);

  source.fragment.planNode = velox::core::PartitionedOutputNode::single(
      nextId(), node->outputType(), exchangeSerdeKind_, node);

  auto merge = std::make_shared<velox::core::MergeExchangeNode>(
      nextId(), node->outputType(), keys, sortOrder, exchangeSerdeKind_);

  fragment.width = 1;
  fragment.inputStages.emplace_back(merge->id(), source.taskPrefix);
  stages.push_back(std::move(source));

  if (op.limit > 0) {
    return addFinalLimit(nextId(), op.offset, op.limit, merge);
  }
  return merge;
}

velox::core::PlanNodePtr ToVelox::makeOffset(
    const Limit& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  if (isSingle_) {
    auto input = makeFragment(op.input(), fragment, stages);
    return addFinalLimit(nextId(), op.offset, op.limit, input);
  }

  auto source = newFragment();
  auto input = makeFragment(op.input(), source, stages);

  source.fragment.planNode = velox::core::PartitionedOutputNode::single(
      nextId(), input->outputType(), exchangeSerdeKind_, input);

  auto exchange = std::make_shared<velox::core::ExchangeNode>(
      nextId(), input->outputType(), exchangeSerdeKind_);

  auto limitNode = addFinalLimit(nextId(), op.offset, op.limit, exchange);

  fragment.width = 1;
  fragment.inputStages.emplace_back(exchange->id(), source.taskPrefix);
  stages.push_back(std::move(source));

  return limitNode;
}

velox::core::PlanNodePtr ToVelox::makeLimit(
    const Limit& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  if (op.isNoLimit()) {
    return makeOffset(op, fragment, stages);
  }

  if (isSingle_) {
    auto input = makeFragment(op.input(), fragment, stages);
    if (options_.numDrivers == 1) {
      return addFinalLimit(nextId(), op.offset, op.limit, input);
    }

    auto node = addPartialLimit(nextId(), 0, op.offset + op.limit, input);
    node = addLocalGather(nextId(), node);
    node = addFinalLimit(nextId(), op.offset, op.limit, node);

    return node;
  }

  auto source = newFragment();
  auto input = makeFragment(op.input(), source, stages);

  auto node = addPartialLimit(nextId(), 0, op.offset + op.limit, input);

  if (options_.numDrivers > 1) {
    node = addLocalGather(nextId(), node);
    node = addFinalLimit(nextId(), 0, op.offset + op.limit, node);
  }

  source.fragment.planNode = velox::core::PartitionedOutputNode::single(
      nextId(), node->outputType(), exchangeSerdeKind_, node);

  auto exchange = std::make_shared<velox::core::ExchangeNode>(
      nextId(), node->outputType(), exchangeSerdeKind_);

  auto finalLimitNode = addFinalLimit(nextId(), op.offset, op.limit, exchange);

  fragment.width = 1;
  fragment.inputStages.emplace_back(exchange->id(), source.taskPrefix);
  stages.push_back(std::move(source));

  return finalLimitNode;
}

namespace {
class HashPartitionFunctionSpec : public velox::core::PartitionFunctionSpec {
 public:
  HashPartitionFunctionSpec(
      velox::RowTypePtr inputType,
      std::vector<velox::column_index_t> keys)
      : inputType_{std::move(inputType)}, keys_{std::move(keys)} {}

  std::unique_ptr<velox::core::PartitionFunction> create(
      int numPartitions,
      bool localExchange = false) const override {
    return std::make_unique<velox::exec::HashPartitionFunction>(
        localExchange, numPartitions, inputType_, keys_);
  }

  folly::dynamic serialize() const override {
    VELOX_UNREACHABLE();
  }

  std::string toString() const override {
    return "<Verax partition function spec>";
  }

 private:
  const velox::RowTypePtr inputType_;
  const std::vector<velox::column_index_t> keys_;
};

class BroadcastPartitionFunctionSpec
    : public velox::core::PartitionFunctionSpec {
 public:
  std::unique_ptr<velox::core::PartitionFunction> create(
      int /* numPartitions */,
      bool /*localExchange*/) const override {
    return nullptr;
  }

  std::string toString() const override {
    return "broadcast";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "BroadcastPartitionFunctionSpec";
    return obj;
  }

  static velox::core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& /* obj */,
      void* /* context */) {
    return std::make_shared<BroadcastPartitionFunctionSpec>();
  }
};

template <typename ExprType>
velox::core::PartitionFunctionSpecPtr createPartitionFunctionSpec(
    const velox::RowTypePtr& inputType,
    const std::vector<ExprType>& keys,
    bool isBroadcast) {
  if (isBroadcast) {
    return std::make_shared<BroadcastPartitionFunctionSpec>();
  }

  if (keys.empty()) {
    return std::make_shared<velox::core::GatherPartitionFunctionSpec>();
  }

  std::vector<velox::column_index_t> keyIndices;
  keyIndices.reserve(keys.size());
  for (const auto& key : keys) {
    keyIndices.push_back(inputType->getChildIdx(
        dynamic_cast<const velox::core::FieldAccessTypedExpr*>(key.get())
            ->name()));
  }
  return std::make_shared<HashPartitionFunctionSpec>(
      inputType, std::move(keyIndices));
}

bool hasSubfieldPushdown(const TableScan& scan) {
  return std::ranges::any_of(
      scan.columns(), [](ColumnCP column) { return column->topColumn(); });
}

// Returns a struct with fields for skyline map keys of 'column' in
// 'baseTable'. This is the type to return from the table reader
// for the map column.
velox::RowTypePtr skylineStruct(BaseTableCP baseTable, ColumnCP column) {
  BitSet allFields;
  if (auto fields = baseTable->controlSubfields.findSubfields(column->id())) {
    allFields = *fields;
  }
  if (auto fields = baseTable->payloadSubfields.findSubfields(column->id())) {
    allFields.unionSet(*fields);
  }

  const auto numOutputs = allFields.size();
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(numOutputs);
  types.reserve(numOutputs);

  auto* ctx = queryCtx();
  auto valueType = column->value().type->childAt(1);
  allFields.forEach([&](int32_t id) {
    const auto* path = ctx->pathById(id);
    const auto& first = path->steps()[0];
    auto name =
        first.field ? std::string{first.field} : fmt::format("{}", first.id);
    names.push_back(name);
    types.push_back(valueType);
  });

  return ROW(std::move(names), std::move(types));
}
} // namespace

velox::RowTypePtr ToVelox::subfieldPushdownScanType(
    BaseTableCP baseTable,
    const ColumnVector& leafColumns,
    ColumnVector& topColumns,
    folly::F14FastMap<ColumnCP, velox::TypePtr>& typeMap) {
  PlanObjectSet top;
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  for (auto& column : leafColumns) {
    if (auto* topColumn = column->topColumn()) {
      if (top.contains(topColumn)) {
        continue;
      }
      top.add(topColumn);
      topColumns.push_back(topColumn);
      names.push_back(topColumn->name());
      if (isMapAsStruct(baseTable->schemaTable->name, topColumn->name())) {
        types.push_back(skylineStruct(baseTable, topColumn));
        typeMap[topColumn] = types.back();
      } else {
        types.push_back(toTypePtr(topColumn->value().type));
      }
    } else {
      if (top.contains(column)) {
        continue;
      }
      topColumns.push_back(column);
      names.push_back(column->name());
      types.push_back(toTypePtr(column->value().type));
    }
  }

  return ROW(std::move(names), std::move(types));
}

velox::core::PlanNodePtr ToVelox::makeSubfieldProjections(
    const TableScan& scan,
    const velox::core::PlanNodePtr& scanNode) {
  velox::ScopedVarSetter getters(&getterForPushdownSubfield_, true);
  velox::ScopedVarSetter noAlias(&makeVeloxExprWithNoAlias_, true);
  std::vector<std::string> names;
  std::vector<velox::core::TypedExprPtr> exprs;
  for (auto* column : scan.columns()) {
    names.push_back(column->outputName());
    exprs.push_back(toTypedExpr(column));
  }
  return std::make_shared<velox::core::ProjectNode>(
      nextId(), std::move(names), std::move(exprs), scanNode);
}

namespace {

void collectFieldNames(
    const velox::core::TypedExprPtr& expr,
    folly::F14FastSet<Name>& names) {
  if (expr->isFieldAccessKind()) {
    auto fieldAccess = expr->asUnchecked<velox::core::FieldAccessTypedExpr>();
    if (fieldAccess->isInputColumn()) {
      names.insert(queryCtx()->toName(fieldAccess->name()));
    }
  }

  for (auto& input : expr->inputs()) {
    collectFieldNames(input, names);
  }
}

// Combines 'conjuncts' into a single expression using AND. Rewrites inputs to
// replace column names from the table schema to correlated names used in the
// output of table scan (foo -> t1.foo). Appends columns used in 'conjuncts'
// to 'columns' unless these are already present.
velox::core::TypedExprPtr toAndWithAliases(
    std::vector<velox::core::TypedExprPtr> conjuncts,
    const BaseTable* baseTable,
    ColumnVector& columns) {
  VELOX_DCHECK(!conjuncts.empty());
  velox::core::TypedExprPtr result;
  if (conjuncts.size() == 1) {
    result = std::move(conjuncts[0]);
  } else {
    result = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(),
        std::move(conjuncts),
        specialForm(logical_plan::SpecialForm::kAnd));
  }

  folly::F14FastSet<Name> usedFieldNames;
  collectFieldNames(result, usedFieldNames);

  PlanObjectSet columnSet;
  columnSet.unionObjects(columns);

  std::unordered_map<std::string, velox::core::TypedExprPtr> mapping;
  for (const auto& column : baseTable->columns) {
    auto name = column->name();
    mapping[name] = std::make_shared<velox::core::FieldAccessTypedExpr>(
        toTypePtr(column->value().type), column->outputName());

    if (usedFieldNames.contains(name)) {
      if (!columnSet.contains(column)) {
        columns.push_back(column);
      }
      usedFieldNames.erase(name);
    }
  }

  // Verify that all fields used in 'conjuncts' are mapped to columns.
  VELOX_CHECK_EQ(0, usedFieldNames.size());

  return result->rewriteInputNames(mapping);
}

} // namespace

velox::core::PlanNodePtr ToVelox::makeScan(
    const TableScan& scan,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  columnAlteredTypes_.clear();

  const bool isSubfieldPushdown = hasSubfieldPushdown(scan);

  auto [tableHandle, rejectedFilters] = leafHandle(scan.baseTable->id());
  if (tableHandle == nullptr) {
    filterUpdated(scan.baseTable, false);
    std::tie(tableHandle, rejectedFilters) = leafHandle(scan.baseTable->id());
    VELOX_CHECK_NOT_NULL(
        tableHandle, "No table for scan {}", scan.toString(true, true));
  }

  // Add columns used by rejected filters to scan columns.
  ColumnVector allColumns = scan.columns();
  velox::core::TypedExprPtr filter;
  if (!rejectedFilters.empty()) {
    filter = toAndWithAliases(
        std::move(rejectedFilters), scan.baseTable, allColumns);
  }

  velox::RowTypePtr outputType;
  ColumnVector scanColumns;
  if (!isSubfieldPushdown) {
    scanColumns = allColumns;
    outputType = makeOutputType(allColumns);
  } else {
    outputType = subfieldPushdownScanType(
        scan.baseTable, allColumns, scanColumns, columnAlteredTypes_);
  }

  auto* connectorMetadata =
      connector::ConnectorMetadata::metadata(scan.index->layout->connector());

  velox::connector::ColumnHandleMap assignments;
  for (auto column : scanColumns) {
    std::vector<velox::common::Subfield> subfields =
        columnSubfields(scan.baseTable, column->id());
    // No correlation name in scan output if pushed down subfield projection
    // follows.
    auto scanColumnName =
        isSubfieldPushdown ? column->name() : column->outputName();
    assignments[scanColumnName] = connectorMetadata->createColumnHandle(
        *scan.index->layout, column->name(), std::move(subfields));
  }

  velox::core::PlanNodePtr result =
      std::make_shared<velox::core::TableScanNode>(
          nextId(), outputType, tableHandle, assignments);

  if (filter != nullptr) {
    result =
        std::make_shared<velox::core::FilterNode>(nextId(), filter, result);
  }

  if (isSubfieldPushdown) {
    result = makeSubfieldProjections(scan, result);
  }

  makePredictionAndHistory(result->id(), &scan);

  columnAlteredTypes_.clear();
  return result;
}

velox::core::PlanNodePtr ToVelox::makeFilter(
    const Filter& filter,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto filterNode = std::make_shared<velox::core::FilterNode>(
      nextId(),
      toAnd(filter.exprs()),
      makeFragment(filter.input(), fragment, stages));
  makePredictionAndHistory(filterNode->id(), &filter);
  return filterNode;
}

velox::core::PlanNodePtr ToVelox::makeProject(
    const Project& project,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto input = makeFragment(project.input(), fragment, stages);
  if (optimizerOptions_.parallelProjectWidth > 1) {
    auto result = maybeParallelProject(&project, input);
    if (result) {
      return result;
    }
  }

  if (project.isRedundant()) {
    return input;
  }

  const auto numOutputs = project.exprs().size();
  VELOX_DCHECK_EQ(project.columns().size(), numOutputs);

  std::vector<std::string> names;
  std::vector<velox::core::TypedExprPtr> exprs;
  names.reserve(numOutputs);
  exprs.reserve(numOutputs);
  for (auto i = 0; i < numOutputs; ++i) {
    names.push_back(project.columns()[i]->outputName());
    exprs.push_back(toTypedExpr(project.exprs()[i]));
  }

  return std::make_shared<velox::core::ProjectNode>(
      nextId(), std::move(names), std::move(exprs), std::move(input));
}

velox::core::PlanNodePtr ToVelox::makeJoin(
    const Join& join,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto left = makeFragment(join.input(), fragment, stages);
  auto right = makeFragment(join.right, fragment, stages);
  if (join.method == JoinMethod::kCross) {
    auto joinNode = std::make_shared<velox::core::NestedLoopJoinNode>(
        nextId(),
        join.joinType,
        nullptr,
        std::move(left),
        std::move(right),
        makeOutputType(join.columns()));
    if (join.filter.empty()) {
      makePredictionAndHistory(joinNode->id(), &join);
      return joinNode;
    }
    return std::make_shared<velox::core::FilterNode>(
        nextId(), toAnd(join.filter), joinNode);
  }

  auto leftKeys = toFieldRefs(join.leftKeys);
  auto rightKeys = toFieldRefs(join.rightKeys);

  auto joinNode = std::make_shared<velox::core::HashJoinNode>(
      nextId(),
      join.joinType,
      false,
      leftKeys,
      rightKeys,
      toAnd(join.filter),
      left,
      right,
      makeOutputType(join.columns()));

  makePredictionAndHistory(joinNode->id(), &join);
  return joinNode;
}

velox::core::PlanNodePtr ToVelox::makeUnnest(
    const Unnest& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto input = makeFragment(op.input(), fragment, stages);

  std::vector<std::string> unnestNames;
  unnestNames.reserve(op.unnestedColumns.size());
  for (const auto* column : op.unnestedColumns) {
    unnestNames.emplace_back(column->outputName());
  }

  return std::make_shared<velox::core::UnnestNode>(
      nextId(),
      toFieldRefs(op.replicateColumns),
      toFieldRefs(op.unnestExprs),
      std::move(unnestNames),
      std::nullopt,
      std::nullopt,
      std::move(input));
}

velox::core::PlanNodePtr ToVelox::makeAggregation(
    const Aggregation& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  auto input = makeFragment(op.input(), fragment, stages);

  const bool isRawInput =
      op.step == velox::core::AggregationNode::Step::kPartial ||
      op.step == velox::core::AggregationNode::Step::kSingle;
  const auto numKeys = op.groupingKeys.size();

  auto keys = toFieldRefs(op.groupingKeys);

  std::vector<std::string> aggregateNames;
  std::vector<velox::core::AggregationNode::Aggregate> aggregates;
  for (size_t i = 0; i < op.aggregates.size(); ++i) {
    const auto* column = op.columns()[i + numKeys];
    const auto& type = toTypePtr(column->value().type);

    aggregateNames.push_back(column->outputName());

    const auto* aggregate = op.aggregates[i];

    std::vector<velox::TypePtr> rawInputTypes;
    for (const auto& type : aggregate->rawInputType()) {
      rawInputTypes.push_back(toTypePtr(type));
    }

    if (isRawInput) {
      velox::core::FieldAccessTypedExprPtr mask;
      if (aggregate->condition()) {
        mask = toFieldRef(aggregate->condition());
      }

      auto call = std::make_shared<velox::core::CallTypedExpr>(
          type, toTypedExprs(aggregate->args()), aggregate->name());

      aggregates.push_back({
          .call = call,
          .rawInputTypes = rawInputTypes,
          .mask = mask,
          .sortingKeys = toFieldRefs(aggregate->orderKeys()),
          .sortingOrders = toSortOrders(aggregate->orderTypes()),
          .distinct = aggregate->isDistinct(),
      });
    } else {
      auto call = std::make_shared<velox::core::CallTypedExpr>(
          type,
          aggregate->name(),
          std::make_shared<velox::core::FieldAccessTypedExpr>(
              toTypePtr(aggregate->intermediateType()), aggregateNames.back()));
      aggregates.push_back({.call = call, .rawInputTypes = rawInputTypes});
    }
  }

  if (options_.numDrivers > 1 &&
      (op.step == velox::core::AggregationNode::Step::kFinal ||
       op.step == velox::core::AggregationNode::Step::kSingle)) {
    std::vector<velox::core::PlanNodePtr> inputs = {input};
    if (keys.empty()) {
      // Final agg with no grouping is single worker and has a local gather
      // before the final aggregation.
      input =
          velox::core::LocalPartitionNode::gather(nextId(), std::move(inputs));
      fragment.width = 1;
    } else {
      auto partition =
          createPartitionFunctionSpec(input->outputType(), keys, false);
      input = std::make_shared<velox::core::LocalPartitionNode>(
          nextId(),
          velox::core::LocalPartitionNode::Type::kRepartition,
          false,
          std::move(partition),
          std::move(inputs));
    }
  }

  return std::make_shared<velox::core::AggregationNode>(
      nextId(),
      op.step,
      keys,
      std::vector<velox::core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      false,
      input);
}

velox::core::PlanNodePtr ToVelox::makeRepartition(
    const Repartition& repartition,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages,
    std::shared_ptr<velox::core::ExchangeNode>& exchange) {
  auto source = newFragment();
  auto sourcePlan = makeFragment(repartition.input(), source, stages);

  auto keys = toTypedExprs(repartition.distribution().partition);

  const auto& distribution = repartition.distribution();
  if (distribution.distributionType.isGather) {
    fragment.width = 1;
  }

  auto partitionFunctionFactory = createPartitionFunctionSpec(
      sourcePlan->outputType(), keys, distribution.isBroadcast);

  source.fragment.planNode =
      std::make_shared<velox::core::PartitionedOutputNode>(
          nextId(),
          distribution.isBroadcast
              ? velox::core::PartitionedOutputNode::Kind::kBroadcast
              : velox::core::PartitionedOutputNode::Kind::kPartitioned,
          keys,
          keys.empty() ? 1 : fragment.width,
          false,
          std::move(partitionFunctionFactory),
          makeOutputType(repartition.columns()),
          exchangeSerdeKind_,
          sourcePlan);

  if (exchange == nullptr) {
    exchange = std::make_shared<velox::core::ExchangeNode>(
        nextId(), sourcePlan->outputType(), exchangeSerdeKind_);
  }
  fragment.inputStages.emplace_back(exchange->id(), source.taskPrefix);
  stages.push_back(std::move(source));
  return exchange;
}

velox::core::PlanNodePtr ToVelox::makeUnionAll(
    const UnionAll& unionAll,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  // If no inputs have a repartition, this is a local exchange. If
  // some have repartition and more than one have no repartition,
  // this is a local exchange with a remote exchaneg as input. All the
  // inputs with repartition go to one remote exchange.
  std::vector<velox::core::PlanNodePtr> localSources;
  std::shared_ptr<velox::core::ExchangeNode> exchange;
  for (const auto& input : unionAll.inputs) {
    if (input->relType() == RelType::kRepartition) {
      makeRepartition(*input->as<Repartition>(), fragment, stages, exchange);
    } else {
      localSources.push_back(makeFragment(input, fragment, stages));
    }
  }

  if (localSources.empty()) {
    return exchange;
  }

  if (exchange) {
    localSources.push_back(exchange);
  }

  return std::make_shared<velox::core::LocalPartitionNode>(
      nextId(),
      velox::core::LocalPartitionNode::Type::kRepartition,
      /* scaleWriter */ false,
      std::make_shared<velox::exec::RoundRobinPartitionFunctionSpec>(),
      localSources);
}

velox::core::PlanNodePtr ToVelox::makeValues(
    const Values& values,
    runner::ExecutableFragment& fragment) {
  fragment.width = 1;
  const auto& newColumns = values.columns();
  const auto newType = makeOutputType(newColumns);
  VELOX_DCHECK_EQ(newColumns.size(), newType->size());

  const auto& type = values.valuesTable.values.outputType();
  const auto& data = values.valuesTable.values.data();
  std::vector<velox::RowVectorPtr> newValues;
  if (auto* rows = std::get_if<std::vector<velox::Variant>>(&data)) {
    auto* pool = queryCtx()->optimization()->evaluator()->pool();

    newValues.reserve(rows->size());
    for (const auto& row : *rows) {
      newValues.emplace_back(std::dynamic_pointer_cast<velox::RowVector>(
          velox::BaseVector::wrappedVectorShared(
              variantToVector(type, row, pool))));
    }

  } else {
    const auto& oldValues = std::get<std::vector<velox::RowVectorPtr>>(data);
    newValues.reserve(oldValues.size());

    VELOX_DCHECK(!oldValues.empty());
    const auto oldType = oldValues.front()->rowType();

    std::vector<uint32_t> oldColumnIdxs;
    oldColumnIdxs.reserve(newColumns.size());
    for (const auto& column : newColumns) {
      auto oldColumnIdx = oldType->getChildIdx(column->name());
      oldColumnIdxs.emplace_back(oldColumnIdx);
    }

    for (const auto& oldValue : oldValues) {
      const auto& oldChildren = oldValue->children();
      std::vector<velox::VectorPtr> newChildren;
      newChildren.reserve(oldColumnIdxs.size());
      for (const auto columnIdx : oldColumnIdxs) {
        newChildren.emplace_back(oldChildren[columnIdx]);
      }

      auto newValue = std::make_shared<velox::RowVector>(
          oldValue->pool(),
          newType,
          oldValue->nulls(),
          oldValue->size(),
          std::move(newChildren),
          oldValue->getNullCount());
      newValues.emplace_back(std::move(newValue));
    }
  }

  auto valuesNode =
      std::make_shared<velox::core::ValuesNode>(nextId(), std::move(newValues));

  makePredictionAndHistory(valuesNode->id(), &values);

  return valuesNode;
}

void ToVelox::makePredictionAndHistory(
    const velox::core::PlanNodeId& id,
    const RelationOp* op) {
  nodeHistory_[id] = op->historyKey();
  prediction_[id] = NodePrediction{
      .cardinality = op->cost().inputCardinality * op->cost().fanout};
}

velox::core::PlanNodePtr ToVelox::makeFragment(
    const RelationOpPtr& op,
    runner::ExecutableFragment& fragment,
    std::vector<runner::ExecutableFragment>& stages) {
  switch (op->relType()) {
    case RelType::kProject:
      return makeProject(*op->as<Project>(), fragment, stages);
    case RelType::kFilter:
      return makeFilter(*op->as<Filter>(), fragment, stages);
    case RelType::kAggregation:
      return makeAggregation(*op->as<Aggregation>(), fragment, stages);
    case RelType::kOrderBy:
      return makeOrderBy(*op->as<OrderBy>(), fragment, stages);
    case RelType::kLimit:
      return makeLimit(*op->as<Limit>(), fragment, stages);
    case RelType::kRepartition: {
      std::shared_ptr<velox::core::ExchangeNode> ignore;
      return makeRepartition(*op->as<Repartition>(), fragment, stages, ignore);
    }
    case RelType::kTableScan:
      return makeScan(*op->as<TableScan>(), fragment, stages);
    case RelType::kJoin:
      return makeJoin(*op->as<Join>(), fragment, stages);
    case RelType::kHashBuild:
      return makeFragment(op->input(), fragment, stages);
    case RelType::kUnionAll:
      return makeUnionAll(*op->as<UnionAll>(), fragment, stages);
    case RelType::kValues:
      return makeValues(*op->as<Values>(), fragment);
    case RelType::kUnnest:
      return makeUnnest(*op->as<Unnest>(), fragment, stages);
    default:
      VELOX_FAIL(
          "Unsupported RelationOp {}", static_cast<int32_t>(op->relType()));
  }
  return nullptr;
}

// Debug helper functions. Must be extern to be callable from debugger.

extern std::string veloxToString(const velox::core::PlanNode* plan) {
  return plan->toString(true, true);
}

extern std::string planString(const runner::MultiFragmentPlan* plan) {
  return plan->toString(true);
}

} // namespace facebook::axiom::optimizer
