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
#include "axiom/optimizer/Plan.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/vector/VariantToVector.h"

using namespace facebook::velox::exec;
using namespace facebook::axiom::runner;

namespace facebook::velox::optimizer {

namespace {

std::vector<common::Subfield> columnSubfields(BaseTableCP table, int32_t id) {
  auto* optimization = queryCtx()->optimization();

  const auto columnName = queryCtx()->objectAt(id)->as<Column>()->name();

  BitSet set = table->columnSubfields(id, false, false);

  std::vector<common::Subfield> subfields;
  set.forEach([&](auto id) {
    auto steps = queryCtx()->pathById(id)->steps();
    std::vector<std::unique_ptr<common::Subfield::PathElement>> elements;
    elements.push_back(
        std::make_unique<common::Subfield::NestedField>(columnName));
    bool first = true;
    for (auto& step : steps) {
      switch (step.kind) {
        case StepKind::kField:
          VELOX_CHECK_NOT_NULL(
              step.field, "Index subfield not suitable for pruning");
          elements.push_back(
              std::make_unique<common::Subfield::NestedField>(step.field));
          break;
        case StepKind::kSubscript:
          if (step.allFields) {
            elements.push_back(
                std::make_unique<common::Subfield::AllSubscripts>());
            break;
          }
          if (first &&
              optimization->options().isMapAsStruct(
                  table->schemaTable->name, columnName)) {
            elements.push_back(std::make_unique<common::Subfield::NestedField>(
                step.field ? std::string(step.field)
                           : fmt::format("{}", step.id)));
            break;
          }
          if (step.field) {
            elements.push_back(
                std::make_unique<common::Subfield::StringSubscript>(
                    step.field));
            break;
          }
          elements.push_back(
              std::make_unique<common::Subfield::LongSubscript>(step.id));
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
    Distribution final = Distribution::gather(order.order, order.orderType);
    auto* gather = make<Repartition>(op, final, op->columns());
    auto* orderBy = make<OrderBy>(gather, order.order, order.orderType);
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
  ColumnVector leafColumns;
  columnSet.forEach([&](auto obj) {
    leafColumns.push_back(reinterpret_cast<const Column*>(obj));
  });

  columnAlteredTypes_.clear();

  ColumnVector topColumns;
  auto scanType = subfieldPushdownScanType(
      table, leafColumns, topColumns, columnAlteredTypes_);

  auto* optimization = queryCtx()->optimization();
  auto* evaluator = optimization->evaluator();

  std::vector<core::TypedExprPtr> remainingConjuncts;
  std::vector<core::TypedExprPtr> pushdownConjuncts;
  ScopedVarSetter noAlias(&makeVeloxExprWithNoAlias_, true);
  ScopedVarSetter getters(&getterForPushdownSubfield_, true);
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
  core::TypedExprPtr remainingFilter;
  for (const auto& conjunct : remainingConjuncts) {
    if (!remainingFilter) {
      remainingFilter = conjunct;
    } else {
      remainingFilter = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{remainingFilter, conjunct},
          "and");
    }
  }

  columnAlteredTypes_.clear();

  auto& dataColumns = table->schemaTable->connectorTable->rowType();
  auto* layout = table->schemaTable->columnGroups[0]->layout;
  auto connector = layout->connector();
  std::vector<connector::ColumnHandlePtr> columns;
  for (int32_t i = 0; i < dataColumns->size(); ++i) {
    auto id = table->columnId(toName(dataColumns->nameOf(i)));
    if (!id.has_value()) {
      continue;
    }
    auto subfields = columnSubfields(table, id.value());

    columns.push_back(connector->metadata()->createColumnHandle(
        *layout, dataColumns->nameOf(i), std::move(subfields)));
  }
  auto allFilters = std::move(pushdownConjuncts);
  if (remainingFilter) {
    allFilters.push_back(remainingFilter);
  }
  std::vector<core::TypedExprPtr> rejectedFilters;
  auto handle = connector->metadata()->createTableHandle(
      *layout, columns, *evaluator, std::move(allFilters), rejectedFilters);

  setLeafHandle(table->id(), handle, std::move(rejectedFilters));
  if (updateSelectivity) {
    optimization->setLeafSelectivity(*const_cast<BaseTable*>(table), scanType);
  }
}

PlanAndStats ToVelox::toVeloxPlan(
    RelationOpPtr plan,
    const MultiFragmentPlan::Options& options) {
  options_ = options;

  prediction_.clear();
  nodeHistory_.clear();

  if (options_.numWorkers > 1) {
    plan = addGather(plan);
  }

  ExecutableFragment top;
  std::vector<ExecutableFragment> stages;
  top.fragment.planNode = makeFragment(std::move(plan), top, stages);

  stages.push_back(std::move(top));
  return PlanAndStats{
      std::make_shared<axiom::runner::MultiFragmentPlan>(
          std::move(stages), options),
      std::move(nodeHistory_),
      std::move(prediction_)};
}

RowTypePtr ToVelox::makeOutputType(const ColumnVector& columns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
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
                                          : outputName(column);
    names.push_back(name);
    types.push_back(toTypePtr(columns[i]->value().type));
  }
  return ROW(std::move(names), std::move(types));
}

core::TypedExprPtr ToVelox::toAnd(const ExprVector& exprs) {
  core::TypedExprPtr result;
  for (auto expr : exprs) {
    auto conjunct = toTypedExpr(expr);
    if (!result) {
      result = conjunct;
    } else {
      result = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), std::vector<core::TypedExprPtr>{result, conjunct}, "and");
    }
  }
  return result;
}

namespace {

template <typename T>
core::TypedExprPtr makeKey(const TypePtr& type, T v) {
  return std::make_shared<core::ConstantTypedExpr>(type, variant(v));
}

core::TypedExprPtr createArrayForInList(
    const Call& call,
    const TypePtr& elementType) {
  std::vector<variant> arrayElements;
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
      variant::array(arrayElements),
      queryCtx()->optimization()->evaluator()->pool());
  return std::make_shared<core::ConstantTypedExpr>(arrayVector);
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
            type->as<TypeKind::MAP>().childAt(1),
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

core::TypedExprPtr
ToVelox::pathToGetter(ColumnCP column, PathCP path, core::TypedExprPtr field) {
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
      field = stepToGetter(newStep, field);
      first = false;
      continue;
    }
    first = false;
    field = stepToGetter(step, field);
  }
  return field;
}

core::TypedExprPtr ToVelox::toTypedExpr(ExprCP expr) {
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
                                            : outputName(column);
      // Check if a top level map should be retrieved as struct.
      auto it = columnAlteredTypes_.find(column);
      if (it != columnAlteredTypes_.end()) {
        return std::make_shared<core::FieldAccessTypedExpr>(it->second, name);
      }
      return std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), name);
    }
    case PlanType::kCallExpr: {
      std::vector<core::TypedExprPtr> inputs;
      auto call = expr->as<Call>();

      if (call->name() == toName("in")) {
        VELOX_USER_CHECK_GE(call->args().size(), 2);
        inputs.push_back(toTypedExpr(call->args().at(0)));
        inputs.push_back(createArrayForInList(*call, inputs.back()->type()));
      } else {
        for (auto arg : call->args()) {
          inputs.push_back(toTypedExpr(arg));
        }
      }

      if (call->name() == toName("cast")) {
        return std::make_shared<core::CastTypedExpr>(
            toTypePtr(expr->value().type), std::move(inputs), false);
      }

      return std::make_shared<core::CallTypedExpr>(
          toTypePtr(expr->value().type), std::move(inputs), call->name());
    }
    case PlanType::kFieldExpr: {
      auto* field = expr->as<Field>()->field();
      if (field) {
        return std::make_shared<core::FieldAccessTypedExpr>(
            toTypePtr(expr->value().type),
            toTypedExpr(expr->as<Field>()->base()),
            field);
      }
      return std::make_shared<core::DereferenceTypedExpr>(
          toTypePtr(expr->value().type),
          toTypedExpr(expr->as<Field>()->base()),
          expr->as<Field>()->index());
      break;
    }
    case PlanType::kLiteralExpr: {
      auto literal = expr->as<Literal>();
      if (literal->vector()) {
        return std::make_shared<core::ConstantTypedExpr>(
            queryCtx()->toVectorPtr(literal->vector()));
      }
      // Complex constants must be vectors for constant folding to work.
      if (literal->value().type->kind() >= TypeKind::ARRAY) {
        return std::make_shared<core::ConstantTypedExpr>(variantToVector(
            toTypePtr(literal->value().type),
            literal->literal(),
            queryCtx()->optimization()->evaluator()->pool()));
      }
      return std::make_shared<core::ConstantTypedExpr>(
          toTypePtr(literal->value().type), literal->literal());
    }
    case PlanType::kLambdaExpr: {
      auto* lambda = expr->as<Lambda>();
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      for (auto& c : lambda->args()) {
        names.push_back(c->toString());
        types.push_back(toTypePtr(c->value().type));
      }
      return std::make_shared<core::LambdaTypedExpr>(
          ROW(std::move(names), std::move(types)), toTypedExpr(lambda->body()));
    }
    default:
      VELOX_FAIL("Cannot translate {} to TypeExpr", expr->toString());
  }
}

namespace {

// Translates ExprPtrs to FieldAccessTypedExprs. Maintains a set of
// projections and produces a ProjectNode to evaluate distinct
// expressions for non-column Exprs given to toFieldref() and
// related functions.
class TempProjections {
 public:
  TempProjections(ToVelox& tv, const RelationOp& input)
      : toVelox_(tv), input_(input) {
    for (auto& column : input_.columns()) {
      exprChannel_[column] = nextChannel_++;
      names_.push_back(ToVelox::outputName(column));
      fieldRefs_.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(column->value().type), names_.back()));
    }
    exprs_.insert(exprs_.begin(), fieldRefs_.begin(), fieldRefs_.end());
  }

  core::FieldAccessTypedExprPtr toFieldRef(
      ExprCP expr,
      const std::string* optName = nullptr) {
    auto it = exprChannel_.find(expr);
    if (it == exprChannel_.end()) {
      VELOX_CHECK(expr->type() != PlanType::kColumnExpr);
      exprChannel_[expr] = nextChannel_++;
      exprs_.push_back(queryCtx()->optimization()->toTypedExpr(expr));
      names_.push_back(
          optName ? *optName : fmt::format("__r{}", nextChannel_ - 1));
      fieldRefs_.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), names_.back()));
      return fieldRefs_.back();
    }
    auto fieldRef = fieldRefs_[it->second];
    if (optName && *optName != fieldRef->name()) {
      auto aliasFieldRef = std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), *optName);
      names_.push_back(*optName);
      exprs_.push_back(fieldRef);
      fieldRefs_.push_back(aliasFieldRef);
      exprChannel_[expr] = nextChannel_++;
      return aliasFieldRef;
    }
    return fieldRef;
  }

  template <typename Result = core::FieldAccessTypedExprPtr>
  std::vector<Result> toFieldRefs(
      const ExprVector& exprs,
      const std::vector<std::string>* optNames = nullptr) {
    std::vector<Result> result;
    result.reserve(exprs.size());
    for (auto i = 0; i < exprs.size(); ++i) {
      result.push_back(
          toFieldRef(exprs[i], optNames ? &(*optNames)[i] : nullptr));
    }
    return result;
  }

  core::PlanNodePtr maybeProject(core::PlanNodePtr inputNode) {
    if (nextChannel_ == input_.columns().size()) {
      return inputNode;
    }

    return std::make_shared<core::ProjectNode>(
        toVelox_.nextId(), std::move(names_), std::move(exprs_), inputNode);
  }

 private:
  ToVelox& toVelox_;
  const RelationOp& input_;
  int32_t nextChannel_{0};
  std::vector<core::FieldAccessTypedExprPtr> fieldRefs_;
  std::vector<std::string> names_;
  std::vector<core::TypedExprPtr> exprs_;
  std::unordered_map<ExprCP, int32_t> exprChannel_;
};
} // namespace

axiom::runner::ExecutableFragment ToVelox::newFragment() {
  ExecutableFragment fragment;
  fragment.width = options_.numWorkers;
  fragment.taskPrefix = fmt::format("stage{}", ++stageCounter_);

  return fragment;
}

namespace {
core::PlanNodePtr addPartialLimit(
    const core::PlanNodeId& id,
    int64_t offset,
    int64_t limit,
    const core::PlanNodePtr& input) {
  return std::make_shared<core::LimitNode>(
      id,
      offset,
      limit,
      /* isPartial */ true,
      input);
}

core::PlanNodePtr addFinalLimit(
    const core::PlanNodeId& id,
    int64_t offset,
    int64_t limit,
    const core::PlanNodePtr& input) {
  return std::make_shared<core::LimitNode>(
      id,
      offset,
      limit,
      /* isPartial */ false,
      input);
}

core::PlanNodePtr addLocalGather(
    const core::PlanNodeId& id,
    const core::PlanNodePtr& input) {
  return core::LocalPartitionNode::gather(
      id, std::vector<core::PlanNodePtr>{input});
}

core::PlanNodePtr addLocalMerge(
    const core::PlanNodeId& id,
    const std::vector<core::FieldAccessTypedExprPtr>& keys,
    const std::vector<core::SortOrder>& sortOrder,
    const core::PlanNodePtr& input) {
  return std::make_shared<core::LocalMergeNode>(
      id, keys, sortOrder, std::vector<core::PlanNodePtr>{input});
}

core::PlanNodePtr addPartialTopN(
    const core::PlanNodeId& id,
    const std::vector<core::FieldAccessTypedExprPtr>& keys,
    const std::vector<core::SortOrder>& sortOrder,
    int64_t count,
    const core::PlanNodePtr& input) {
  return std::make_shared<core::TopNNode>(
      id,
      keys,
      sortOrder,
      count,
      /* isPartial */ true,
      input);
}

core::PlanNodePtr addFinalTopN(
    const core::PlanNodeId& id,
    const std::vector<core::FieldAccessTypedExprPtr>& keys,
    const std::vector<core::SortOrder>& sortOrder,
    int64_t count,
    const core::PlanNodePtr& input) {
  return std::make_shared<core::TopNNode>(
      id,
      keys,
      sortOrder,
      count,
      /* isPartial */ false,
      input);
}

core::SortOrder toSortOrder(const OrderType& order) {
  return order == OrderType::kAscNullsFirst ? core::kAscNullsFirst
      : order == OrderType ::kAscNullsLast  ? core::kAscNullsLast
      : order == OrderType::kDescNullsFirst ? core::kDescNullsFirst
                                            : core::kDescNullsLast;
}
} // namespace

core::PlanNodePtr ToVelox::makeOrderBy(
    const OrderBy& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
  std::vector<core::SortOrder> sortOrder;
  sortOrder.reserve(op.distribution().orderType.size());
  for (auto order : op.distribution().orderType) {
    sortOrder.push_back(toSortOrder(order));
  }

  if (isSingle_) {
    auto input = makeFragment(op.input(), fragment, stages);

    TempProjections projections(*this, *op.input());
    auto keys = projections.toFieldRefs(op.distribution().order);
    auto project = projections.maybeProject(input);

    if (options_.numDrivers == 1) {
      if (op.limit <= 0) {
        return std::make_shared<core::OrderByNode>(
            nextId(), keys, sortOrder, false, project);
      }

      auto node = addFinalTopN(
          nextId(), keys, sortOrder, op.limit + op.offset, project);

      if (op.offset > 0) {
        return addFinalLimit(nextId(), op.offset, op.limit, node);
      }

      return node;
    }

    core::PlanNodePtr node;
    if (op.limit <= 0) {
      node = std::make_shared<core::OrderByNode>(
          nextId(), keys, sortOrder, true, project);
    } else {
      node = addPartialTopN(
          nextId(), keys, sortOrder, op.limit + op.offset, project);
    }

    node = addLocalMerge(nextId(), keys, sortOrder, node);

    if (op.limit > 0) {
      return addFinalLimit(nextId(), op.offset, op.limit, node);
    }

    return node;
  }

  auto source = newFragment();
  auto input = makeFragment(op.input(), source, stages);

  TempProjections projections(*this, *op.input());
  auto keys = projections.toFieldRefs(op.distribution().order);
  auto project = projections.maybeProject(input);

  core::PlanNodePtr node;
  if (op.limit <= 0) {
    node = std::make_shared<core::OrderByNode>(
        nextId(), keys, sortOrder, true, project);
  } else {
    node = addPartialTopN(
        nextId(), keys, sortOrder, op.limit + op.offset, project);
  }

  node = addLocalMerge(nextId(), keys, sortOrder, node);

  source.fragment.planNode = core::PartitionedOutputNode::single(
      nextId(), node->outputType(), exchangeSerdeKind_, node);

  auto merge = std::make_shared<core::MergeExchangeNode>(
      nextId(), node->outputType(), keys, sortOrder, exchangeSerdeKind_);

  fragment.width = 1;
  fragment.inputStages.push_back(InputStage{merge->id(), source.taskPrefix});
  stages.push_back(std::move(source));

  if (op.limit > 0) {
    return addFinalLimit(nextId(), op.offset, op.limit, merge);
  }
  return merge;
}

velox::core::PlanNodePtr ToVelox::makeOffset(
    const Limit& op,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  if (isSingle_) {
    auto input = makeFragment(op.input(), fragment, stages);
    return addFinalLimit(nextId(), op.offset, op.limit, input);
  }

  auto source = newFragment();
  auto input = makeFragment(op.input(), source, stages);

  source.fragment.planNode = core::PartitionedOutputNode::single(
      nextId(), input->outputType(), exchangeSerdeKind_, input);

  auto exchange = std::make_shared<core::ExchangeNode>(
      nextId(), input->outputType(), exchangeSerdeKind_);

  auto limitNode = addFinalLimit(nextId(), op.offset, op.limit, exchange);

  fragment.width = 1;
  fragment.inputStages.push_back(InputStage{exchange->id(), source.taskPrefix});
  stages.push_back(std::move(source));

  return limitNode;
}

core::PlanNodePtr ToVelox::makeLimit(
    const Limit& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
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

  source.fragment.planNode = core::PartitionedOutputNode::single(
      nextId(), node->outputType(), exchangeSerdeKind_, node);

  auto exchange = std::make_shared<core::ExchangeNode>(
      nextId(), node->outputType(), exchangeSerdeKind_);

  auto finalLimitNode = addFinalLimit(nextId(), op.offset, op.limit, exchange);

  fragment.width = 1;
  fragment.inputStages.push_back(InputStage{exchange->id(), source.taskPrefix});
  stages.push_back(std::move(source));

  return finalLimitNode;
}

namespace {
class HashPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  HashPartitionFunctionSpec(
      RowTypePtr inputType,
      std::vector<column_index_t> keys)
      : inputType_{std::move(inputType)}, keys_{std::move(keys)} {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool localExchange = false) const override {
    return std::make_unique<exec::HashPartitionFunction>(
        localExchange, numPartitions, inputType_, keys_);
  }

  folly::dynamic serialize() const override {
    VELOX_UNREACHABLE();
  }

  std::string toString() const override {
    return "<Verax partition function spec>";
  }

 private:
  const RowTypePtr inputType_;
  const std::vector<column_index_t> keys_;
};

class BroadcastPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  std::unique_ptr<core::PartitionFunction> create(
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

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& /* obj */,
      void* /* context */) {
    return std::make_shared<BroadcastPartitionFunctionSpec>();
  }
};

template <typename ExprType>
core::PartitionFunctionSpecPtr createPartitionFunctionSpec(
    const RowTypePtr& inputType,
    const std::vector<ExprType>& keys,
    bool isBroadcast) {
  if (isBroadcast) {
    return std::make_shared<BroadcastPartitionFunctionSpec>();
  }

  if (keys.empty()) {
    return std::make_shared<core::GatherPartitionFunctionSpec>();
  }

  std::vector<column_index_t> keyIndices;
  keyIndices.reserve(keys.size());
  for (const auto& key : keys) {
    keyIndices.push_back(inputType->getChildIdx(
        dynamic_cast<const core::FieldAccessTypedExpr*>(key.get())->name()));
  }
  return std::make_shared<HashPartitionFunctionSpec>(
      inputType, std::move(keyIndices));
}

bool hasSubfieldPushdown(const TableScan& scan) {
  for (auto& column : scan.columns()) {
    if (column->topColumn()) {
      return true;
    }
  }
  return false;
}

// Returns a struct with fields for skyline map keys of 'column' in
// 'baseTable'. This is the type to return from the table reader
// for the map column.
RowTypePtr skylineStruct(BaseTableCP baseTable, ColumnCP column) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<std::string> distinct;
  auto valueType = column->value().type->childAt(1);

  auto* ctx = queryCtx();
  auto addTopFields = [&](const BitSet& paths) {
    paths.forEach([&](int32_t id) {
      auto path = ctx->pathById(id);
      auto& first = path->steps()[0];
      std::string name =
          first.field ? std::string(first.field) : fmt::format("{}", first.id);
      if (!distinct.count(name)) {
        distinct.insert(name);
        names.push_back(name);
        types.push_back(valueType);
      }
    });
  };

  auto fields = baseTable->controlSubfields.findSubfields(column->id());
  if (fields.has_value()) {
    addTopFields(fields.value());
  }
  fields = baseTable->payloadSubfields.findSubfields(column->id());
  if (fields.has_value()) {
    addTopFields(fields.value());
  }

  return ROW(std::move(names), std::move(types));
}
} // namespace

RowTypePtr ToVelox::scanOutputType(
    const TableScan& scan,
    ColumnVector& scanColumns,
    std::unordered_map<ColumnCP, TypePtr>& typeMap) {
  if (!hasSubfieldPushdown(scan)) {
    scanColumns = scan.columns();
    return makeOutputType(scan.columns());
  }
  return subfieldPushdownScanType(
      scan.baseTable, scan.columns(), scanColumns, typeMap);
}

RowTypePtr ToVelox::subfieldPushdownScanType(
    BaseTableCP baseTable,
    const ColumnVector& leafColumns,
    ColumnVector& topColumns,
    std::unordered_map<ColumnCP, TypePtr>& typeMap) {
  PlanObjectSet top;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
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
      topColumns.push_back(column);
      names.push_back(column->name());
      types.push_back(toTypePtr(column->value().type));
    }
  }

  return ROW(std::move(names), std::move(types));
}

core::PlanNodePtr ToVelox::makeSubfieldProjections(
    const TableScan& scan,
    const std::shared_ptr<const core::TableScanNode>& scanNode) {
  ScopedVarSetter getters(&getterForPushdownSubfield_, true);
  ScopedVarSetter noAlias(&makeVeloxExprWithNoAlias_, true);
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> exprs;
  for (auto* column : scan.columns()) {
    names.push_back(outputName(column));
    exprs.push_back(toTypedExpr(column));
  }
  return std::make_shared<core::ProjectNode>(
      nextId(), std::move(names), std::move(exprs), scanNode);
}

namespace {
core::TypedExprPtr toAndWithAliases(
    const std::vector<core::TypedExprPtr>& exprs,
    const BaseTable* baseTable) {
  auto result = std::make_shared<core::CallTypedExpr>(BOOLEAN(), exprs, "and");

  std::unordered_map<std::string, core::TypedExprPtr> mapping;
  for (const auto& column : baseTable->columns) {
    mapping[column->name()] = std::make_shared<core::FieldAccessTypedExpr>(
        toTypePtr(column->value().type), ToVelox::outputName(column));
  }
  return result->rewriteInputNames(mapping);
}
} // namespace

velox::core::PlanNodePtr ToVelox::makeScan(
    const TableScan& scan,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  columnAlteredTypes_.clear();
  bool isSubfieldPushdown = hasSubfieldPushdown(scan);
  auto handlePair = leafHandle(scan.baseTable->id());
  if (!handlePair.first) {
    queryCtx()->optimization()->filterUpdated(scan.baseTable, false);
    handlePair = leafHandle(scan.baseTable->id());
    VELOX_CHECK_NOT_NULL(
        handlePair.first, "No table for scan {}", scan.toString(true, true));
  }

  ColumnVector scanColumns;
  auto outputType = scanOutputType(scan, scanColumns, columnAlteredTypes_);
  connector::ColumnHandleMap assignments;
  for (auto column : scanColumns) {
    // TODO: Make assignments have a ConnectorTableHandlePtr instead of
    // non-const shared_ptr.
    std::vector<common::Subfield> subfields =
        columnSubfields(scan.baseTable, column->id());
    // No correlation name in scan output if pushed down subfield projection
    // follows.
    auto scanColumnName =
        isSubfieldPushdown ? column->name() : outputName(column);
    assignments[scanColumnName] =
        std::const_pointer_cast<connector::ColumnHandle>(
            scan.index->layout->connector()->metadata()->createColumnHandle(
                *scan.index->layout, column->name(), std::move(subfields)));
  }

  auto scanNode = std::make_shared<core::TableScanNode>(
      nextId(), outputType, handlePair.first, assignments);

  core::PlanNodePtr result = scanNode;
  if (hasSubfieldPushdown(scan)) {
    result = makeSubfieldProjections(scan, scanNode);
  }

  if (!handlePair.second.empty()) {
    result = std::make_shared<core::FilterNode>(
        nextId(), toAndWithAliases(handlePair.second, scan.baseTable), result);
    makePredictionAndHistory(result->id(), &scan);
  } else {
    makePredictionAndHistory(scanNode->id(), &scan);
  }

  fragment.scans.push_back(scanNode);

  columnAlteredTypes_.clear();
  return result;
}

velox::core::PlanNodePtr ToVelox::makeFilter(
    const Filter& filter,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  auto filterNode = std::make_shared<core::FilterNode>(
      nextId(),
      toAnd(filter.exprs()),
      makeFragment(filter.input(), fragment, stages));
  makePredictionAndHistory(filterNode->id(), &filter);
  return filterNode;
}

velox::core::PlanNodePtr ToVelox::makeProject(
    const Project& project,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  auto input = makeFragment(project.input(), fragment, stages);
  if (optimizerOptions_.parallelProjectWidth > 1) {
    auto result = maybeParallelProject(&project, input);
    if (result) {
      return result;
    }
  }

  bool redundant = true;
  for (auto i = 0; i < project.exprs().size(); ++i) {
    auto expr = project.exprs()[i];
    if (expr->type() != PlanType::kColumnExpr) {
      redundant = false;
      break;
    }

    auto column = project.columns()[i];

    // TODO Why expr->sameOrEquals(*column) doesn't work?
    if (input->outputType()->nameOf(i) != outputName(column)) {
      redundant = false;
      break;
    }
  }

  if (redundant) {
    return input;
  }

  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> exprs;
  for (auto i = 0; i < project.exprs().size(); ++i) {
    auto column = project.columns()[i];
    names.push_back(outputName(column));
    exprs.push_back(toTypedExpr(project.exprs()[i]));
  }

  return std::make_shared<core::ProjectNode>(
      nextId(), std::move(names), std::move(exprs), input);
}

velox::core::PlanNodePtr ToVelox::makeJoin(
    const Join& join,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  TempProjections leftProjections(*this, *join.input());
  TempProjections rightProjections(*this, *join.right);
  auto left = makeFragment(join.input(), fragment, stages);
  auto right = makeFragment(join.right, fragment, stages);
  if (join.method == JoinMethod::kCross) {
    auto joinNode = std::make_shared<core::NestedLoopJoinNode>(
        nextId(),
        join.joinType,
        nullptr,
        leftProjections.maybeProject(left),
        rightProjections.maybeProject(right),
        makeOutputType(join.columns()));
    if (join.filter.empty()) {
      makePredictionAndHistory(joinNode->id(), &join);
      return joinNode;
    }
    return std::make_shared<core::FilterNode>(
        nextId(), toAnd(join.filter), joinNode);
  }

  auto leftKeys = leftProjections.toFieldRefs(join.leftKeys);
  auto rightKeys = rightProjections.toFieldRefs(join.rightKeys);

  auto joinNode = std::make_shared<core::HashJoinNode>(
      nextId(),
      join.joinType,
      false,
      leftKeys,
      rightKeys,
      toAnd(join.filter),
      leftProjections.maybeProject(left),
      rightProjections.maybeProject(right),
      makeOutputType(join.columns()));
  makePredictionAndHistory(joinNode->id(), &join);
  return joinNode;
}

core::PlanNodePtr ToVelox::makeAggregation(
    Aggregation& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
  auto input = makeFragment(op.input(), fragment, stages);

  const bool isRawInput = op.step == core::AggregationNode::Step::kPartial ||
      op.step == core::AggregationNode::Step::kSingle;
  const int32_t numKeys = op.groupingKeys.size();

  TempProjections projections(*this, *op.input());
  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;
  for (auto i = 0; i < op.aggregates.size(); ++i) {
    const auto* column = op.columns()[i + numKeys];
    const auto& type = toTypePtr(column->value().type);

    aggregateNames.push_back(outputName(column));

    const auto* aggregate = op.aggregates[i];

    std::vector<TypePtr> rawInputTypes;
    for (auto type : aggregate->rawInputType()) {
      rawInputTypes.push_back(toTypePtr(type));
    }

    if (isRawInput) {
      core::FieldAccessTypedExprPtr mask;
      if (aggregate->condition()) {
        mask = projections.toFieldRef(aggregate->condition());
      }
      auto call = std::make_shared<core::CallTypedExpr>(
          type,
          projections.toFieldRefs<core::TypedExprPtr>(aggregate->args()),
          aggregate->name());
      aggregates.push_back({call, rawInputTypes, mask, {}, {}, false});
    } else {
      auto call = std::make_shared<core::CallTypedExpr>(
          type,
          std::vector<core::TypedExprPtr>{
              std::make_shared<core::FieldAccessTypedExpr>(
                  toTypePtr(aggregate->intermediateType()),
                  aggregateNames.back())},
          aggregate->name());
      aggregates.push_back(
          {call, rawInputTypes, /* mask */ nullptr, {}, {}, false});
    }
  }

  std::vector<std::string> keyNames;
  keyNames.reserve(op.groupingKeys.size());
  for (auto i = 0; i < op.groupingKeys.size(); ++i) {
    keyNames.push_back(outputName(op.columns()[i]));
  }

  auto keys = projections.toFieldRefs(op.groupingKeys, &keyNames);
  auto project = projections.maybeProject(input);
  if (options_.numDrivers > 1 &&
      (op.step == core::AggregationNode::Step::kFinal ||
       op.step == core::AggregationNode::Step::kSingle)) {
    std::vector<core::PlanNodePtr> inputs = {project};
    if (keys.empty()) {
      // Final agg with no grouping is single worker and has a local gather
      // before the final aggregation.
      project = core::LocalPartitionNode::gather(nextId(), std::move(inputs));
      fragment.width = 1;
    } else {
      auto partition =
          createPartitionFunctionSpec(project->outputType(), keys, false);
      project = std::make_shared<core::LocalPartitionNode>(
          nextId(),
          core::LocalPartitionNode::Type::kRepartition,
          false,
          std::move(partition),
          std::move(inputs));
    }
  }

  return std::make_shared<core::AggregationNode>(
      nextId(),
      op.step,
      keys,
      std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      false,
      project);
}

velox::core::PlanNodePtr ToVelox::makeRepartition(
    const Repartition& repartition,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages,
    std::shared_ptr<core::ExchangeNode>& exchange) {
  auto source = newFragment();
  auto sourcePlan = makeFragment(repartition.input(), source, stages);

  TempProjections project(*this, *repartition.input());
  auto keys = project.toFieldRefs<core::TypedExprPtr>(
      repartition.distribution().partition);
  auto& distribution = repartition.distribution();
  if (distribution.distributionType.isGather) {
    fragment.width = 1;
  }

  auto partitioningInput = project.maybeProject(sourcePlan);

  auto partitionFunctionFactory = createPartitionFunctionSpec(
      partitioningInput->outputType(), keys, distribution.isBroadcast);
  if (distribution.isBroadcast) {
    source.numBroadcastDestinations = fragment.width;
  }
  source.fragment.planNode = std::make_shared<core::PartitionedOutputNode>(
      nextId(),
      distribution.isBroadcast
          ? core::PartitionedOutputNode::Kind::kBroadcast
          : core::PartitionedOutputNode::Kind::kPartitioned,
      keys,
      keys.empty() ? 1 : fragment.width,
      false,
      std::move(partitionFunctionFactory),
      makeOutputType(repartition.columns()),
      exchangeSerdeKind_,
      partitioningInput);

  if (exchange == nullptr) {
    exchange = std::make_shared<core::ExchangeNode>(
        nextId(), sourcePlan->outputType(), exchangeSerdeKind_);
  }
  fragment.inputStages.push_back(InputStage{exchange->id(), source.taskPrefix});
  stages.push_back(std::move(source));
  return exchange;
}

velox::core::PlanNodePtr ToVelox::makeUnionAll(
    const UnionAll& unionAll,
    axiom::runner::ExecutableFragment& fragment,
    std::vector<axiom::runner::ExecutableFragment>& stages) {
  // If no inputs have a repartition, this is a local exchange. If
  // some have repartition and more than one have no repartition,
  // this is a local exchange with a remote exchaneg as input. All the
  // inputs with repartition go to one remote exchange.
  std::vector<core::PlanNodePtr> localSources;
  std::shared_ptr<core::ExchangeNode> exchange;
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

  return std::make_shared<core::LocalPartitionNode>(
      nextId(),
      core::LocalPartitionNode::Type::kRepartition,
      /* scaleWriter */ false,
      std::make_shared<exec::RoundRobinPartitionFunctionSpec>(),
      localSources);
}

core::PlanNodePtr ToVelox::makeValues(
    const Values& values,
    ExecutableFragment& fragment) {
  fragment.width = 1;
  const auto& newColumns = values.columns();
  const auto newType = makeOutputType(newColumns);
  VELOX_DCHECK_EQ(newColumns.size(), newType->size());

  const auto& data = values.valuesTable.values.data();
  std::vector<RowVectorPtr> newValues;
  if ([[maybe_unused]] auto* row = std::get_if<std::vector<Variant>>(&data)) {
    [[maybe_unused]] auto& newValue = newValues.emplace_back();
    VELOX_NYI("Translate rows from vector<Variant> to RowVector");
  } else {
    const auto& oldValues = std::get<std::vector<RowVectorPtr>>(data);
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
      std::vector<VectorPtr> newChildren;
      newChildren.reserve(oldColumnIdxs.size());
      for (const auto columnIdx : oldColumnIdxs) {
        newChildren.emplace_back(oldChildren[columnIdx]);
      }

      auto newValue = std::make_shared<RowVector>(
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
      std::make_shared<core::ValuesNode>(nextId(), std::move(newValues));

  makePredictionAndHistory(valuesNode->id(), &values);

  return valuesNode;
}

void ToVelox::makePredictionAndHistory(
    const core::PlanNodeId& id,
    const RelationOp* op) {
  nodeHistory_[id] = op->historyKey();
  prediction_[id] = NodePrediction{
      .cardinality = op->cost().inputCardinality * op->cost().fanout};
}

core::PlanNodePtr ToVelox::makeFragment(
    const RelationOpPtr& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
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
      std::shared_ptr<core::ExchangeNode> ignore;
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
    default:
      VELOX_FAIL(
          "Unsupported RelationOp {}", static_cast<int32_t>(op->relType()));
  }
  return nullptr;
}

/// Debugging helper functions. Must be in a namespace to be
/// callable from debugger.
std::string veloxToString(const core::PlanNode* plan) {
  return plan->toString(true, true);
}

std::string planString(MultiFragmentPlan* plan) {
  return plan->toString(true);
}

} // namespace facebook::velox::optimizer
