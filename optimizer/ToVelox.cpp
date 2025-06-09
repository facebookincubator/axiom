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

#include "optimizer/Plan.h" //@manual
#include "optimizer/PlanUtils.h" //@manual
#include "velox/core/PlanNode.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::optimizer {

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::runner;

std::vector<common::Subfield> columnSubfields(BaseTableCP table, int32_t id) {
  BitSet set = table->columnSubfields(id, false, false);
  auto* optimization = queryCtx()->optimization();
  auto columnName = queryCtx()->objectAt(id)->as<Column>()->name();
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
              optimization->isMapAsStruct(
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

void filterUpdated(BaseTableCP table, bool updateSelectivity) {
  auto ctx = queryCtx();
  auto optimization = ctx->optimization();

  PlanObjectSet columnSet;
  for (auto& filter : table->columnFilters) {
    columnSet.unionSet(filter->columns());
  }
  ColumnVector leafColumns;
  columnSet.forEach([&](auto obj) {
    leafColumns.push_back(reinterpret_cast<const Column*>(obj));
  });
  optimization->columnAlteredTypes().clear();
  ColumnVector topColumns;
  auto scanType = optimization->subfieldPushdownScanType(
      table, leafColumns, topColumns, optimization->columnAlteredTypes());

  std::vector<core::TypedExprPtr> remainingConjuncts;
  std::vector<core::TypedExprPtr> pushdownConjuncts;
  ScopedVarSetter noAlias(&optimization->makeVeloxExprWithNoAlias(), true);
  ScopedVarSetter getters(&optimization->getterForPushdownSubfield(), true);
  for (auto filter : table->columnFilters) {
    auto typedExpr = optimization->toTypedExpr(filter);
    try {
      auto evaluator = optimization->evaluator();
      auto pair = velox::exec::toSubfieldFilter(typedExpr, evaluator);
      if (!pair.second) {
        remainingConjuncts.push_back(std::move(typedExpr));
        continue;
      }
      pushdownConjuncts.push_back(typedExpr);
    } catch (const std::exception& e) {
      remainingConjuncts.push_back(std::move(typedExpr));
    }
  }
  for (auto expr : table->filter) {
    remainingConjuncts.push_back(optimization->toTypedExpr(expr));
  }
  core::TypedExprPtr remainingFilter;
  for (auto conjunct : remainingConjuncts) {
    if (!remainingFilter) {
      remainingFilter = conjunct;
    } else {
      remainingFilter = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>{remainingFilter, conjunct},
          "and");
    }
  }
  optimization->columnAlteredTypes().clear();
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
      *layout,
      columns,
      *optimization->evaluator(),
      std::move(allFilters),
      rejectedFilters);

  optimization->setLeafHandle(table->id(), handle, std::move(rejectedFilters));
  if (updateSelectivity) {
    optimization->setLeafSelectivity(*const_cast<BaseTable*>(table), scanType);
  }
}

core::PlanNodeId Optimization::nextId(const RelationOp& op) {
  return idGenerator_.next();
}

RelationOpPtr addGather(RelationOpPtr op) {
  if (op->distribution().distributionType.isGather) {
    return op;
  }
  if (op->relType() == RelType::kOrderBy) {
    auto order = op->distribution();
    Distribution final = Distribution::gather(
        op->distribution().distributionType, order.order, order.orderType);
    auto* gather = make<Repartition>(op, final, op->columns());
    auto* orderBy = make<OrderBy>(gather, order.order, order.orderType);
    return orderBy;
  }
  auto* gather = make<Repartition>(
      op,
      Distribution::gather(op->distribution().distributionType),
      op->columns());
  return gather;
}

PlanAndStats Optimization::toVeloxPlan(
    RelationOpPtr plan,
    const MultiFragmentPlan::Options& options) {
  options_ = options;
  std::vector<ExecutableFragment> stages;
  prediction_.clear();
  nodeHistory_.clear();
  if (options_.numWorkers > 1) {
    plan = addGather(plan);
  }
  ExecutableFragment top;
  top.fragment.planNode = makeFragment(plan, top, stages);
  stages.push_back(std::move(top));
  return PlanAndStats{
      std::make_shared<velox::runner::MultiFragmentPlan>(
          std::move(stages), options),
      std::move(nodeHistory_),
      std::move(prediction_)};
}

RowTypePtr Optimization::makeOutputType(const ColumnVector& columns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < columns.size(); ++i) {
    auto* column = columns[i];
    auto relation = column->relation();
    if (relation && relation->type() == PlanType::kTable) {
      auto* schemaTable = relation->as<BaseTable>()->schemaTable;
      if (!schemaTable) {
        continue;
      }

      auto* runnerTable = schemaTable->connectorTable;
      if (runnerTable) {
        auto* runnerColumn = runnerTable->findColumn(std::string(
            column->topColumn() ? column->topColumn()->name()
                                : column->name()));
        VELOX_CHECK_NOT_NULL(runnerColumn);
      }
    }
    auto name = makeVeloxExprWithNoAlias_ ? std::string(column->name())
                                          : column->toString();
    names.push_back(name);
    types.push_back(toTypePtr(columns[i]->value().type));
  }
  return ROW(std::move(names), std::move(types));
}

core::TypedExprPtr Optimization::toAnd(const ExprVector& exprs) {
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

bool Optimization::isMapAsStruct(Name table, Name column) {
  auto it = opts_.mapAsStruct.find(table);
  if (it == opts_.mapAsStruct.end()) {
    return false;
  }
  return (
      std::find(it->second.begin(), it->second.end(), column) !=
      it->second.end());
}

core::TypedExprPtr Optimization::pathToGetter(
    ColumnCP column,
    PathCP path,
    core::TypedExprPtr field) {
  bool first = true;
  // If this is a path over a map that is retrieved as struct, the first getter
  // becomes a struct getter.
  auto alterStep = [&](ColumnCP, const Step& step, Step& newStep) {
    auto* rel = column->relation();
    if (rel->type() == PlanType::kTable &&
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

core::TypedExprPtr Optimization::toTypedExpr(ExprCP expr) {
  auto it = projectedExprs_.find(expr);
  if (it != projectedExprs_.end()) {
    return it->second;
  }

  switch (expr->type()) {
    case PlanType::kColumn: {
      auto column = expr->as<Column>();
      if (column->topColumn() && getterForPushdownSubfield_) {
        auto field = toTypedExpr(column->topColumn());
        return pathToGetter(column->topColumn(), column->path(), field);
      }
      auto name = makeVeloxExprWithNoAlias_ ? std::string(column->name())
                                            : column->toString();
      // Check if a top level map should be retrieved as struct.
      auto it = columnAlteredTypes_.find(column);
      if (it != columnAlteredTypes_.end()) {
        return std::make_shared<core::FieldAccessTypedExpr>(it->second, name);
      }
      return std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), name);
    }
    case PlanType::kCall: {
      std::vector<core::TypedExprPtr> inputs;
      auto call = expr->as<Call>();
      for (auto arg : call->args()) {
        inputs.push_back(toTypedExpr(arg));
      }
      if (call->name() == toName("cast")) {
        return std::make_shared<core::CastTypedExpr>(
            toTypePtr(expr->value().type), std::move(inputs), false);
      }
      return std::make_shared<core::CallTypedExpr>(
          toTypePtr(expr->value().type), std::move(inputs), call->name());
    }
    case PlanType::kField: {
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
    case PlanType::kLiteral: {
      auto literal = expr->as<Literal>();
      if (literal->vector()) {
        return std::make_shared<core::ConstantTypedExpr>(
            queryCtx()->toVectorPtr(literal->vector()));
      }
      return std::make_shared<core::ConstantTypedExpr>(
          toTypePtr(literal->value().type), literal->literal());
    }
    case PlanType::kLambda: {
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

// Translates ExprPtrs to FieldAccessTypedExprs. Maintains a set of
// projections and produces a ProjectNode to evaluate distinct
// expressions for non-column Exprs given to toFieldref() and
// related functions.
class TempProjections {
 public:
  TempProjections(Optimization& optimization, const RelationOp& input)
      : optimization_(optimization), input_(input) {
    for (auto& column : input_.columns()) {
      exprChannel_[column] = nextChannel_++;
      names_.push_back(column->toString());
      fieldRefs_.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(column->value().type), column->toString()));
    }
    exprs_.insert(exprs_.begin(), fieldRefs_.begin(), fieldRefs_.end());
  }

  core::FieldAccessTypedExprPtr toFieldRef(
      ExprCP expr,
      const std::string* optName = nullptr) {
    auto it = exprChannel_.find(expr);
    if (it == exprChannel_.end()) {
      VELOX_CHECK(expr->type() != PlanType::kColumn);
      exprChannel_[expr] = nextChannel_++;
      exprs_.push_back(optimization_.toTypedExpr(expr));
      names_.push_back(
          optName ? *optName : fmt::format("__r{}", nextChannel_ - 1));
      fieldRefs_.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          toTypePtr(expr->value().type), names_.back()));
      return fieldRefs_.back();
    }
    return fieldRefs_[it->second];
  }

  template <typename Result = core::FieldAccessTypedExprPtr>
  std::vector<Result> toFieldRefs(
      const ExprVector& exprs,
      const std::vector<std::string>* optNames = nullptr) {
    std::vector<Result> result;
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
        optimization_.idGenerator().next(),
        std::move(names_),
        std::move(exprs_),
        inputNode);
  }

 private:
  Optimization& optimization_;
  const RelationOp& input_;
  int32_t nextChannel_{0};
  std::vector<core::FieldAccessTypedExprPtr> fieldRefs_;
  std::vector<std::string> names_;
  std::vector<core::TypedExprPtr> exprs_;
  std::unordered_map<ExprCP, int32_t> exprChannel_;
};

core::PlanNodePtr Optimization::makeOrderBy(
    OrderBy& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
  if (root_->limit > 0) {
    toVeloxLimit_ = root_->limit;
    toVeloxOffset_ = root_->offset;
  }
  ExecutableFragment source;
  source.width = options_.numWorkers;
  source.taskPrefix = fmt::format("stage{}", ++stageCounter_);
  auto input = makeFragment(op.input(), source, stages);
  TempProjections projections(*this, *op.input());
  std::vector<core::SortOrder> sortOrder;
  for (auto order : op.distribution().orderType) {
    sortOrder.push_back(
        order == OrderType::kAscNullsFirst       ? core::SortOrder(true, true)
            : order == OrderType ::kAscNullsLast ? core::SortOrder(true, false)
            : order == OrderType::kDescNullsFirst
            ? core::SortOrder(false, true)
            : core::SortOrder(false, false));
  }
  auto keys = projections.toFieldRefs(op.distribution().order);
  auto project = projections.maybeProject(input);
  core::PlanNodePtr orderByNode;
  if (toVeloxLimit_ <= 0) {
    orderByNode = std::make_shared<core::OrderByNode>(
        nextId(op), keys, sortOrder, true, project);
  } else {
    orderByNode = std::make_shared<core::TopNNode>(
        nextId(op),
        keys,
        sortOrder,
        toVeloxLimit_ + toVeloxOffset_,
        true,
        project);
  }
  auto localMerge = std::make_shared<core::LocalMergeNode>(
      idGenerator_.next(),
      keys,
      sortOrder,
      std::vector<core::PlanNodePtr>{orderByNode});

  source.fragment.planNode = std::make_shared<core::PartitionedOutputNode>(
      idGenerator_.next(),
      core::PartitionedOutputNode::Kind::kPartitioned,
      std::vector<core::TypedExprPtr>{},
      1,
      false,
      std::make_shared<core::GatherPartitionFunctionSpec>(),
      localMerge->outputType(),
      VectorSerde::Kind::kPresto,
      localMerge);

  core::PlanNodePtr merge = std::make_shared<core::MergeExchangeNode>(
      idGenerator_.next(),
      localMerge->outputType(),
      keys,
      sortOrder,
      VectorSerde::Kind::kPresto);
  fragment.width = 1;
  fragment.inputStages.push_back(InputStage{merge->id(), source.taskPrefix});
  stages.push_back(std::move(source));
  if (toVeloxLimit_ > 0 || toVeloxOffset_ != 0) {
    return std::make_shared<core::LimitNode>(
        idGenerator().next(), toVeloxOffset_, toVeloxLimit_, false, merge);
  }
  return merge;
}

class HashPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  HashPartitionFunctionSpec(
      RowTypePtr inputType,
      std::vector<column_index_t> keys)
      : inputType_{inputType}, keys_{keys} {}

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
  } else {
    std::vector<column_index_t> keyIndices;
    keyIndices.reserve(keys.size());
    for (const auto& key : keys) {
      keyIndices.push_back(inputType->getChildIdx(
          dynamic_cast<const core::FieldAccessTypedExpr*>(key.get())->name()));
    }
    return std::make_shared<HashPartitionFunctionSpec>(
        inputType, std::move(keyIndices));
  }
}

bool hasSubfieldPushdown(TableScan* scan) {
  for (auto& column : scan->columns()) {
    if (column->topColumn()) {
      return true;
    }
  }
  return false;
}

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

RowTypePtr Optimization::scanOutputType(
    TableScan* scan,
    ColumnVector& scanColumns,
    std::unordered_map<ColumnCP, TypePtr>& typeMap) {
  if (!hasSubfieldPushdown(scan)) {
    scanColumns = scan->columns();
    return makeOutputType(scan->columns());
  }
  return subfieldPushdownScanType(
      scan->baseTable, scan->columns(), scanColumns, typeMap);
}

RowTypePtr Optimization::subfieldPushdownScanType(
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

core::PlanNodePtr Optimization::makeSubfieldProjections(
    TableScan* scan,
    const std::shared_ptr<const core::TableScanNode>& scanNode) {
  ScopedVarSetter getters(&getterForPushdownSubfield(), true);
  ScopedVarSetter noAlias(&makeVeloxExprWithNoAlias(), true);
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> exprs;
  for (auto* column : scan->columns()) {
    names.push_back(column->toString());
    exprs.push_back(toTypedExpr(column));
  }
  return std::make_shared<core::ProjectNode>(
      idGenerator_.next(), std::move(names), std::move(exprs), scanNode);
}

core::PlanNodePtr Optimization::makeAggregation(
    Aggregation& op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
  auto input = makeFragment(op.input(), fragment, stages);
  TempProjections projections(*this, *op.input());

  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;
  bool isRawInput = op.step == core::AggregationNode::Step::kPartial ||
      op.step == core::AggregationNode::Step::kSingle;
  int32_t numKeys = op.grouping.size();
  for (auto i = 0; i < op.aggregates.size(); ++i) {
    aggregateNames.push_back(op.columns()[i + numKeys]->toString());

    auto aggregate = op.aggregates[i];
    core::FieldAccessTypedExprPtr mask;
    std::vector<TypePtr> rawInputTypes;
    for (auto type : aggregate->rawInputType()) {
      rawInputTypes.push_back(toTypePtr(type));
    }
    if (isRawInput) {
      if (aggregate->condition()) {
        mask = projections.toFieldRef(aggregate->condition());
      }
      auto call = std::make_shared<core::CallTypedExpr>(
          toTypePtr(op.columns()[numKeys + i]->value().type),
          projections.toFieldRefs<core::TypedExprPtr>(aggregate->args()),
          aggregate->name());
      aggregates.push_back({call, rawInputTypes, mask, {}, {}, false});
    } else {
      auto call = std::make_shared<core::CallTypedExpr>(
          toTypePtr(op.columns()[numKeys + i]->value().type),
          std::vector<core::TypedExprPtr>{
              std::make_shared<core::FieldAccessTypedExpr>(
                  toTypePtr(aggregate->intermediateType()),
                  aggregateNames.back())},
          aggregate->name());
      aggregates.push_back({call, rawInputTypes, mask, {}, {}, false});
    }
  }
  std::vector<std::string> keyNames;
  for (auto i = 0; i < op.grouping.size(); ++i) {
    keyNames.push_back(op.intermediateColumns[i]->toString());
  }
  auto keys = projections.toFieldRefs(op.grouping, &keyNames);
  auto project = projections.maybeProject(input);
  if (options_.numDrivers > 1 &&
      (op.step == core::AggregationNode::Step::kFinal ||
       op.step == core::AggregationNode::Step::kSingle)) {
    if (keys.empty()) {
      // Final agg with no grouping is single worker and has a local gather
      // before the final aggregation.
      auto partition =
          createPartitionFunctionSpec(project->outputType(), keys, false);
      std::vector<core::PlanNodePtr> inputs = {project};
      project = std::make_shared<core::LocalPartitionNode>(
          nextId(op),
          core::LocalPartitionNode::Type::kGather,
          false,
          std::move(partition),
          std::move(inputs));
      fragment.width = 1;
    } else {
      auto partition =
          createPartitionFunctionSpec(project->outputType(), keys, false);
      std::vector<core::PlanNodePtr> inputs = {project};
      project = std::make_shared<core::LocalPartitionNode>(
          nextId(op),
          core::LocalPartitionNode::Type::kRepartition,
          false,
          std::move(partition),
          std::move(inputs));
    }
  }
  auto r = new core::AggregationNode(
      nextId(op),
      op.step,
      keys,
      {},
      aggregateNames,
      aggregates,
      false,
      project);
  core::PlanNodePtr ptr;
  ptr.reset(r);
  return ptr;
}
void Optimization::makePredictionAndHistory(
    const core::PlanNodeId& id,
    const RelationOp* op) {
  nodeHistory_[id] = op->historyKey();
  prediction_[id] = NodePrediction{
      .cardinality = op->cost().inputCardinality * op->cost().fanout};
}

core::PlanNodePtr Optimization::makeFragment(
    RelationOpPtr op,
    ExecutableFragment& fragment,
    std::vector<ExecutableFragment>& stages) {
  switch (op->relType()) {
    case RelType::kProject: {
      auto input = makeFragment(op->input(), fragment, stages);
      auto project = op->as<Project>();
      if (opts_.parallelProjectWidth > 1) {
        auto result = maybeParallelProject(project, input);
        if (result) {
          return result;
        }
      }
      std::vector<std::string> names;
      std::vector<core::TypedExprPtr> exprs;
      for (auto i = 0; i < project->exprs().size(); ++i) {
        names.push_back(project->columns()[i]->toString());
        exprs.push_back(toTypedExpr(project->exprs()[i]));
      }
      return std::make_shared<core::ProjectNode>(
          nextId(*project), std::move(names), std::move(exprs), input);
    }
    case RelType::kFilter: {
      auto filter = op->as<Filter>();
      auto filterNode = std::make_shared<core::FilterNode>(
          idGenerator_.next(),
          toAnd(filter->exprs()),
          makeFragment(filter->input(), fragment, stages));
      makePredictionAndHistory(filterNode->id(), filter);
      return filterNode;
    }
    case RelType::kAggregation: {
      return makeAggregation(*op->as<Aggregation>(), fragment, stages);
    }
    case RelType::kOrderBy: {
      return makeOrderBy(*op->as<OrderBy>(), fragment, stages);
    }
    case RelType::kRepartition: {
      ExecutableFragment source;
      source.width = options_.numWorkers;
      source.taskPrefix = fmt::format("stage{}", ++stageCounter_);
      auto sourcePlan = makeFragment(op->input(), source, stages);
      TempProjections project(*this, *op->input());

      auto repartition = op->as<Repartition>();
      auto keys = project.toFieldRefs<core::TypedExprPtr>(
          repartition->distribution().partition);
      auto& distribution = repartition->distribution();
      if (distribution.distributionType.isGather) {
        fragment.width = 1;
      }
      auto partitioningInput = project.maybeProject(sourcePlan);
      auto partitionFunctionFactory = createPartitionFunctionSpec(
          partitioningInput->outputType(), keys, distribution.isBroadcast);
      if (distribution.isBroadcast) {
        source.numBroadcastDestinations = options_.numWorkers;
      }
      source.fragment.planNode = std::make_shared<core::PartitionedOutputNode>(
          nextId(*op),
          distribution.isBroadcast
              ? core::PartitionedOutputNode::Kind::kBroadcast
              : core::PartitionedOutputNode::Kind::kPartitioned,
          keys,
          (keys.empty()) ? 1 : options_.numWorkers,
          false,
          std::move(partitionFunctionFactory),
          makeOutputType(repartition->columns()),
          VectorSerde::Kind::kPresto,
          partitioningInput);
      auto exchange = std::make_shared<core::ExchangeNode>(
          idGenerator_.next(),
          sourcePlan->outputType(),
          VectorSerde::Kind::kPresto);
      fragment.inputStages.push_back(
          InputStage{exchange->id(), source.taskPrefix});
      stages.push_back(std::move(source));
      return exchange;
    }
    case RelType::kTableScan: {
      columnAlteredTypes_.clear();
      auto scan = op->as<TableScan>();
      bool isSubfieldPushdown = hasSubfieldPushdown(scan);
      auto handlePair = leafHandle(scan->baseTable->id());
      if (!handlePair.first) {
        filterUpdated(scan->baseTable, false);
        handlePair = leafHandle(scan->baseTable->id());
        VELOX_CHECK_NOT_NULL(
            handlePair.first,
            "No table for scan {}",
            scan->toString(true, true));
      }
      ColumnVector scanColumns;
      auto outputType = scanOutputType(scan, scanColumns, columnAlteredTypes_);
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
          assignments;
      for (auto column : scanColumns) {
        // TODO: Make assignments have a ConnectorTableHandlePtr instead of
        // non-const shared_ptr.
        std::vector<common::Subfield> subfields =
            columnSubfields(scan->baseTable, column->id());
        // No correlation name in scan output if pushed down subfield projection
        // follows.
        auto scanColumnName =
            isSubfieldPushdown ? column->name() : column->toString();
        assignments[scanColumnName] = std::const_pointer_cast<
            connector::ColumnHandle>(
            scan->index->layout->connector()->metadata()->createColumnHandle(
                *scan->index->layout, column->name(), std::move(subfields)));
      }
      auto scanNode = std::make_shared<core::TableScanNode>(
          nextId(*op),
          outputType,
          std::const_pointer_cast<connector::ConnectorTableHandle>(
              handlePair.first),
          assignments);
      VELOX_CHECK(handlePair.second.empty(), "Expecting no rejected filters");
      makePredictionAndHistory(scanNode->id(), scan);
      fragment.scans.push_back(scanNode);
      if (hasSubfieldPushdown(scan)) {
        auto result = makeSubfieldProjections(scan, scanNode);
        columnAlteredTypes_.clear();
        return result;
      }
      columnAlteredTypes_.clear();
      return scanNode;
    }
    case RelType::kJoin: {
      auto join = op->as<Join>();
      TempProjections leftProjections(*this, *op->input());
      TempProjections rightProjections(*this, *join->right);
      auto left = makeFragment(op->input(), fragment, stages);
      auto right = makeFragment(join->right, fragment, stages);
      if (join->method == JoinMethod::kCross) {
        auto joinNode = std::make_shared<core::NestedLoopJoinNode>(
            nextId(*join),
            join->joinType,
            nullptr,
            leftProjections.maybeProject(left),
            rightProjections.maybeProject(right),
            makeOutputType(join->columns()));
        if (join->filter.empty()) {
          makePredictionAndHistory(joinNode->id(), join);
          return joinNode;
        }
        return std::make_shared<core::FilterNode>(
            idGenerator().next(), toAnd(join->filter), joinNode);
      }
      auto leftKeys = leftProjections.toFieldRefs(join->leftKeys);
      auto rightKeys = rightProjections.toFieldRefs(join->rightKeys);
      auto joinNode = std::make_shared<core::HashJoinNode>(
          nextId(*join),
          join->joinType,
          false,
          leftKeys,
          rightKeys,
          toAnd(join->filter),
          leftProjections.maybeProject(left),
          rightProjections.maybeProject(right),
          makeOutputType(join->columns()));
      makePredictionAndHistory(joinNode->id(), join);
      return joinNode;
    }
    case RelType::kHashBuild:
      return makeFragment(op->input(), fragment, stages);
    default:
      VELOX_FAIL(
          "Unsupported RelationOp {}", static_cast<int32_t>(op->relType()));
  }
  return nullptr;
}

std::string planString(MultiFragmentPlan* plan) {
  return plan->toString(true);
}

} // namespace facebook::velox::optimizer
