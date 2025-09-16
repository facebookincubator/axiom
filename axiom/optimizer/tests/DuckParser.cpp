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

#include "axiom/optimizer/tests/DuckParser.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/parse/DuckLogicalOperator.h"
#include "velox/parse/PlanNodeIdGenerator.h"

#include <duckdb.hpp> // @manual
#include <duckdb/main/connection.hpp> // @manual
#include <duckdb/planner/expression/bound_aggregate_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_cast_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_comparison_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_constant_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_function_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_reference_expression.hpp> // @manual
#include <duckdb/planner/operator/logical_dummy_scan.hpp> // @manual

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class ColumnNameGenerator {
 public:
  std::string next(const std::string& prefix = "_c") {
    if (names_.count(prefix)) {
      auto name = fmt::format("{}{}", prefix, nextId_++);
      names_.insert(name);
      return name;
    }

    names_.insert(prefix);
    return prefix;
  }

 private:
  folly::F14FastSet<std::string> names_;
  int nextId_{0};
};

struct QueryContext {
  const std::string connectorId;
  core::PlanNodeIdGenerator planNodeIdGenerator;
  ColumnNameGenerator columnNameGenerator;

  explicit QueryContext(const std::string& _connectorId)
      : connectorId(_connectorId) {}

  std::string nextNodeId() {
    return planNodeIdGenerator.next();
  }

  std::string nextColumnName() {
    return columnNameGenerator.next();
  }

  std::string nextColumnName(const std::string& prefix) {
    return columnNameGenerator.next(prefix);
  }
};

std::optional<int64_t> extractInteger(const lp::ConstantExpr& constant) {
  const auto& value = *constant.value();
  if (value.isNull()) {
    return std::nullopt;
  }

  const auto typeKind = value.kind();

  switch (typeKind) {
    case TypeKind::BIGINT:
      return value.value<int64_t>();
    case TypeKind::INTEGER:
      return value.value<int32_t>();
    case TypeKind::VARCHAR:
      try {
        return std::stol(value.value<std::string>());
      } catch (...) {
        return std::nullopt;
      }
    default:
      return std::nullopt;
  }
}

lp::ConstantExprPtr tryParseInterval(
    const std::string& functionName,
    const lp::ExprPtr& input) {
  std::optional<int64_t> value;

  if (input->isConstant()) {
    const auto* constant = input->asUnchecked<lp::ConstantExpr>();
    value = extractInteger(*constant);
  } else if (input->isSpecialForm()) {
    const auto* specialForm = input->asUnchecked<lp::SpecialFormExpr>();
    if (specialForm->form() == lp::SpecialForm::kCast) {
      const auto& arg = *specialForm->inputAt(0);
      if (arg.isConstant()) {
        const auto* constant = arg.asUnchecked<lp::ConstantExpr>();
        value = extractInteger(*constant);
      }
    }
  }

  if (!value.has_value()) {
    return nullptr;
  }

  int64_t multiplier;

  if (functionName == "to_days") {
    multiplier = 24 * 60 * 60 * 1'000;
  } else if (functionName == "to_hours") {
    multiplier = 60 * 60 * 1'000;
  } else if (functionName == "to_minutes") {
    multiplier = 60 * 1'000;
  } else if (functionName == "to_seconds") {
    multiplier = 1'000;
  } else if (functionName == "to_milliseconds") {
    multiplier = 1;
  }
  // The other two options are years and months. They are expressed in terms of
  // number of months, and return a different type (INTERVAL_YEAR_MONTH).
  else {
    if (functionName == "to_years") {
      multiplier = 12;
    } else if (functionName == "to_months") {
      multiplier = 1;
    } else {
      return nullptr;
    }

    return std::make_shared<lp::ConstantExpr>(
        INTERVAL_YEAR_MONTH(),
        std::make_shared<Variant>((int32_t)(value.value() * multiplier)));
  }

  return std::make_shared<lp::ConstantExpr>(
      INTERVAL_DAY_TIME(),
      std::make_shared<Variant>(value.value() * multiplier));
}

std::string mapScalarFunctionName(const std::string& name) {
  static const folly::F14FastMap<std::string, std::string> kMapping = {
      {"+", "plus"},
      {"-", "minus"},
      {"*", "multiply"},
      {"/", "divide"},
      {"%", "mod"},
      {"~~", "like"},
      {"!~~", "not_like"},
      {"list_value", "array_constructor"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

std::string mapAggregateFunctionName(const std::string& name) {
  static const folly::F14FastMap<std::string, std::string> kMapping = {
      {"count_star", "count"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalDummyScan& logicalDummyScan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < logicalDummyScan.types.size(); ++i) {
    names.push_back(queryContext.nextColumnName());
    types.push_back(duckdb::toVeloxType(logicalDummyScan.types[i]));
  }

  auto rowType = ROW(std::move(names), std::move(types));

  std::vector<RowVectorPtr> vectors = {std::make_shared<RowVector>(
      pool, rowType, nullptr, 1, std::vector<VectorPtr>{})};

  return std::make_shared<lp::ValuesNode>(
      queryContext.nextNodeId(), std::move(vectors));
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalGet& logicalGet,
    memory::MemoryPool* pool,
    const std::vector<lp::LogicalPlanNodePtr>& sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(logicalGet.function.name, "seq_scan");
  VELOX_CHECK_EQ(0, sources.size());

  std::vector<std::string> columnNames;

  std::vector<std::string> names;
  std::vector<TypePtr> types;

  constexpr uint64_t kNone = ~0UL;

  const auto& columnIds = logicalGet.column_ids;
  for (auto i = 0; i < columnIds.size(); ++i) {
    if (columnIds[i] == kNone) {
      continue;
    }
    names.push_back(
        queryContext.nextColumnName(logicalGet.names[columnIds[i]]));
    types.push_back(
        duckdb::toVeloxType(logicalGet.returned_types[columnIds[i]]));
    columnNames.push_back(logicalGet.names[columnIds[i]]);
  }

  auto rowType = ROW(std::move(names), std::move(types));

  auto tableName = logicalGet.function.to_string(logicalGet.bind_data.get());

  return std::make_shared<lp::TableScanNode>(
      queryContext.nextNodeId(),
      rowType,
      queryContext.connectorId,
      tableName,
      columnNames);
}

lp::ExprPtr toExpr(::duckdb::Expression& expression, const TypePtr& inputType);

std::vector<lp::ExprPtr> toExprs(
    const ::duckdb::vector<::duckdb::unique_ptr<::duckdb::Expression>>&
        duckExprs,
    const TypePtr& inputType) {
  std::vector<lp::ExprPtr> exprs;
  exprs.reserve(duckExprs.size());
  for (const auto& duckExpr : duckExprs) {
    exprs.push_back(toExpr(*duckExpr, inputType));
  }

  return exprs;
}

lp::ExprPtr toLogicalComparisonExpression(
    const std::string& name,
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  auto* comparison =
      dynamic_cast<::duckdb::BoundComparisonExpression*>(&expression);

  return std::make_shared<lp::CallExpr>(
      BOOLEAN(),
      name,
      toExpr(*comparison->left, inputType),
      toExpr(*comparison->right, inputType));
}

lp::ExprPtr toExpr(::duckdb::Expression& expression, const TypePtr& inputType) {
  switch (expression.type) {
    case ::duckdb::ExpressionType::VALUE_CONSTANT: {
      auto* constant =
          dynamic_cast<::duckdb::BoundConstantExpression*>(&expression);
      return std::make_shared<lp::ConstantExpr>(
          duckdb::toVeloxType(constant->return_type),
          std::make_shared<Variant>(
              duckdb::duckValueToVariant(constant->value)));
    }
    case ::duckdb::ExpressionType::COMPARE_EQUAL:
      return toLogicalComparisonExpression("eq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return toLogicalComparisonExpression("neq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return toLogicalComparisonExpression("gt", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return toLogicalComparisonExpression("gte", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_LESSTHAN:
      return toLogicalComparisonExpression("lt", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return toLogicalComparisonExpression("lte", expression, inputType);

    case ::duckdb::ExpressionType::OPERATOR_CAST: {
      auto* cast = dynamic_cast<::duckdb::BoundCastExpression*>(&expression);
      const auto form =
          cast->try_cast ? lp::SpecialForm::kTryCast : lp::SpecialForm::kCast;
      return std::make_shared<lp::SpecialFormExpr>(
          duckdb::toVeloxType(cast->return_type),
          form,
          std::vector<lp::ExprPtr>{toExpr(*cast->child, inputType)});
    }
    case ::duckdb::ExpressionType::BOUND_FUNCTION: {
      auto* func =
          dynamic_cast<::duckdb::BoundFunctionExpression*>(&expression);

      const auto inputs = toExprs(func->children, inputType);

      auto name = mapScalarFunctionName(func->function.name);

      if (inputs.size() == 1) {
        if (auto interval = tryParseInterval(name, inputs[0])) {
          return interval;
        }
      }

      bool negate = false;
      if (name == "not_like") {
        name = "like";
        negate = true;
      }
      auto call = std::make_shared<lp::CallExpr>(
          duckdb::toVeloxType(func->function.return_type), name, inputs);
      if (negate) {
        return std::make_shared<lp::CallExpr>(
            BOOLEAN(), "not", std::vector<lp::ExprPtr>{call});
      }
      return call;
    }
    case ::duckdb::ExpressionType::BOUND_REF: {
      auto* ref =
          dynamic_cast<::duckdb::BoundReferenceExpression*>(&expression);
      return std::make_shared<lp::InputReferenceExpr>(
          duckdb::toVeloxType(ref->return_type),
          inputType->asRow().nameOf(ref->index));
    }
    case ::duckdb::ExpressionType::BOUND_AGGREGATE: {
      auto* agg =
          dynamic_cast<::duckdb::BoundAggregateExpression*>(&expression);

      const auto inputs = toExprs(agg->children, inputType);

      return std::make_shared<lp::CallExpr>(
          duckdb::toVeloxType(agg->return_type),
          mapAggregateFunctionName(agg->function.name),
          inputs);
    }
    default:
      VELOX_NYI(
          "Expression type {} is not supported yet: {}",
          ::duckdb::ExpressionTypeToString(expression.type),
          expression.ToString());
  }
}

lp::ExprPtr makeAnd(const lp::ExprPtr& left, const lp::ExprPtr& right) {
  return std::make_shared<lp::SpecialFormExpr>(
      BOOLEAN(), lp::SpecialForm::kAnd, left, right);
}

lp::ExprPtr combineConjuncts(
    const ::duckdb::vector<::duckdb::unique_ptr<::duckdb::Expression>>&
        conjuncts,
    const RowTypePtr& inputRowType) {
  lp::ExprPtr combined;
  for (const auto& conjunct : conjuncts) {
    auto expr = toExpr(*conjunct, inputRowType);
    if (!combined) {
      combined = expr;
    } else {
      combined = makeAnd(combined, expr);
    }
  }

  return combined;
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalFilter& duckFilter,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  auto predicate =
      combineConjuncts(duckFilter.expressions, sources[0]->outputType());

  return std::make_shared<lp::FilterNode>(
      queryContext.nextNodeId(), sources[0], predicate);
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalProjection& logicalProjection,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  const auto projections =
      toExprs(logicalProjection.expressions, sources[0]->outputType());

  std::vector<std::string> names;
  names.reserve(projections.size());
  for (auto i = 0; i < projections.size(); ++i) {
    names.push_back(queryContext.nextColumnName("_p"));
  }
  return std::make_shared<lp::ProjectNode>(
      queryContext.nextNodeId(), sources[0], names, projections);
}

namespace {
std::string translateAggregateName(const std::string& name) {
  // first(x) is used to get one element of a set. The closes Velox
  // counterpart is arbitrary, which usually returns the first value it sees.
  if (name == "first") {
    return "arbitrary";
  }
  return name;
}
} // namespace

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalAggregate& logicalAggregate,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<std::string> outputNames;
  outputNames.reserve(
      logicalAggregate.groups.size() + logicalAggregate.expressions.size());

  auto groupingKeys =
      toExprs(logicalAggregate.groups, sources[0]->outputType());

  for (const auto& key : groupingKeys) {
    if (key->isInputReference()) {
      const auto* inputReference = key->asUnchecked<lp::InputReferenceExpr>();
      outputNames.push_back(inputReference->name());
    } else {
      queryContext.nextColumnName("_gk");
    }
  }

  std::vector<lp::AggregateExprPtr> aggregates;
  for (auto& expression : logicalAggregate.expressions) {
    const auto expr = toExpr(*expression, sources[0]->outputType());
    const auto* call = expr->asUnchecked<lp::CallExpr>();

    auto type = call->type();
    if (expression->return_type.InternalType() ==
        ::duckdb::PhysicalType::INT128) {
      type = BIGINT();
    }

    const auto aggName = translateAggregateName(call->name());
    aggregates.push_back(
        std::make_shared<lp::AggregateExpr>(type, aggName, call->inputs()));

    outputNames.push_back(queryContext.nextColumnName("_a"));
  }

  return std::make_shared<lp::AggregateNode>(
      queryContext.nextNodeId(),
      sources[0],
      groupingKeys,
      std::vector<lp::AggregateNode::GroupingSet>{},
      aggregates,
      outputNames);
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalDistinct& logicalDistinct,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<std::string> outputNames;
  outputNames.reserve(logicalDistinct.distinct_targets.size());

  auto groupingKeys =
      toExprs(logicalDistinct.distinct_targets, sources[0]->outputType());

  for (const auto& key : groupingKeys) {
    if (key->isInputReference()) {
      const auto* inputReference = key->asUnchecked<lp::InputReferenceExpr>();
      outputNames.push_back(inputReference->name());
    } else {
      queryContext.nextColumnName("_gk");
    }
  }

  return std::make_shared<lp::AggregateNode>(
      queryContext.nextNodeId(),
      sources[0],
      groupingKeys,
      std::vector<lp::AggregateNode::GroupingSet>{},
      std::vector<lp::AggregateExprPtr>{},
      outputNames);
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalOrder& logicalOrder,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  const auto& source = sources[0];

  std::vector<lp::SortingField> ordering;
  for (const auto& order : logicalOrder.orders) {
    const bool ascending = order.type == ::duckdb::OrderType::ASCENDING ||
        order.type == ::duckdb::OrderType::ORDER_DEFAULT;

    const bool nullsFirst =
        order.null_order == ::duckdb::OrderByNullType::NULLS_FIRST ||
        order.null_order == ::duckdb::OrderByNullType::ORDER_DEFAULT;

    ordering.push_back(
        {toExpr(*order.expression, source->outputType()),
         lp::SortOrder{ascending, nullsFirst}});
  }

  return std::make_shared<lp::SortNode>(
      queryContext.nextNodeId(), source, ordering);
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalCrossProduct& logicalCrossProduct,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  return std::make_shared<lp::JoinNode>(
      queryContext.nextNodeId(),
      sources[0],
      sources[1],
      lp::JoinType::kInner,
      /* condition */ nullptr);
}

lp::JoinType toJoinType(::duckdb::JoinType& join) {
  switch (join) {
    case ::duckdb::JoinType::INNER:
    case ::duckdb::JoinType::SINGLE:
      return lp::JoinType::kInner;
    case ::duckdb::JoinType::LEFT:
      return lp::JoinType::kLeft;
    case ::duckdb::JoinType::RIGHT:
      return lp::JoinType::kRight;
    case ::duckdb::JoinType::OUTER:
      return lp::JoinType::kFull;
    default:
      VELOX_NYI("Unsupported Duck join type: {}", static_cast<int32_t>(join));
  }
}

std::shared_ptr<const RowType> joinInputType(
    const std::vector<lp::LogicalPlanNodePtr>& sources) {
  return sources[0]->outputType()->unionWith(sources[1]->outputType());
  ;
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalComparisonJoin& join,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  auto joinType = toJoinType(join.join_type);

  const auto& leftType = sources[0]->outputType();
  const auto& rightType = sources[1]->outputType();

  lp::ExprPtr filter;
  if (!join.expressions.empty()) {
    VELOX_CHECK_EQ(join.expressions.size(), 1);
    const auto joinInputType = leftType->unionWith(rightType);
    filter = toExpr(*join.expressions.front(), joinInputType);
  }

  for (auto& condition : join.conditions) {
    VELOX_CHECK(
        condition.comparison == ::duckdb::ExpressionType::COMPARE_EQUAL ||
        condition.comparison ==
            ::duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM);

    auto leftKey = toExpr(*condition.left, leftType);
    auto rightKey = toExpr(*condition.right, rightType);

    auto conjunct =
        std::make_shared<lp::CallExpr>(BOOLEAN(), "eq", leftKey, rightKey);
    if (!filter) {
      filter = conjunct;
    } else {
      filter = makeAnd(filter, conjunct);
    }
  }

  return std::make_shared<lp::JoinNode>(
      queryContext.nextNodeId(), sources[0], sources[1], joinType, filter);
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalAnyJoin& join,
    memory::MemoryPool* pool,
    std::vector<lp::LogicalPlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  auto joinType = toJoinType(join.join_type);

  lp::ExprPtr filter;
  if (join.condition) {
    const auto inputType = joinInputType(sources);
    filter = toExpr(*join.condition, inputType);
  }

  return std::make_shared<lp::JoinNode>(
      queryContext.nextNodeId(),
      sources[0],
      sources[1],
      joinType,
      std::move(filter));
}

lp::LogicalPlanNodePtr toPlanNode(
    ::duckdb::LogicalOperator& plan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<lp::LogicalPlanNodePtr> sources;
  for (auto& child : plan.children) {
    sources.push_back(toPlanNode(*child, pool, queryContext));
    VELOX_CHECK_NOT_NULL(
        sources.back(), "Null plan for: {}", child->ToString());
  }

  switch (plan.type) {
    case ::duckdb::LogicalOperatorType::LOGICAL_DUMMY_SCAN:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalDummyScan&>(plan), pool, queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_GET:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalGet&>(plan),
          pool,
          sources,
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_FILTER:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalFilter&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalProjection&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalAggregate&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_DISTINCT: {
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalDistinct&>(plan),
          pool,
          std::move(sources),
          queryContext);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalCrossProduct&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalOrder&>(plan),
          pool,
          std::move(sources),
          queryContext);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
      auto& limit = dynamic_cast<const ::duckdb::LogicalLimit&>(plan);
      return std::make_shared<lp::LimitNode>(
          queryContext.nextNodeId(),
          sources[0],
          limit.offset_val,
          limit.limit_val);
    }
    case ::duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalComparisonJoin&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
      return toPlanNode(
          dynamic_cast<::duckdb::LogicalAnyJoin&>(plan),
          pool,
          std::move(sources),
          queryContext);
    default:
      VELOX_NYI(
          "Plan node is not supported yet: {}",
          ::duckdb::LogicalOperatorToString(plan.type));
  }
}

static void customScalarFunction(
    ::duckdb::DataChunk& args,
    ::duckdb::ExpressionState& state,
    ::duckdb::Vector& result) {
  VELOX_UNREACHABLE();
}

static ::duckdb::idx_t customAggregateState() {
  VELOX_UNREACHABLE();
}

static void customAggregateInitialize(::duckdb::data_ptr_t) {
  VELOX_UNREACHABLE();
}

static void customAggregateUpdate(
    ::duckdb::Vector inputs[],
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t input_count,
    ::duckdb::Vector& state,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateCombine(
    ::duckdb::Vector& state,
    ::duckdb::Vector& combined,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateFinalize(
    ::duckdb::Vector& state,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::Vector& result,
    ::duckdb::idx_t count,
    ::duckdb::idx_t offset) {
  VELOX_UNREACHABLE();
}

} // namespace

void DuckParser::registerTable(
    const std::string& name,
    const RowTypePtr& type) {
  VELOX_CHECK_EQ(
      tables_.count(name), 0, "Table is already registered: {}", name);

  auto createTableSql = duckdb::makeCreateTableSql(name, *type);
  auto res = conn_.Query(createTableSql);
}

void DuckParser::registerScalarFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateVectorizedFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customScalarFunction);
}

void DuckParser::registerAggregateFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateAggregateFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customAggregateState,
      customAggregateInitialize,
      customAggregateUpdate,
      customAggregateCombine,
      customAggregateFinalize);
}

SqlStatementPtr DuckParser::parse(const std::string& sql) {
  // Disable the optimizer. Otherwise, the filter over table scan gets pushdown
  // as a callback that is impossible to recover.
  conn_.Query("PRAGMA disable_optimizer");

  auto plan = conn_.ExtractPlan(sql);

  QueryContext queryContext{connectorId_};

  if (plan->type == ::duckdb::LogicalOperatorType::LOGICAL_EXPLAIN) {
    auto select = std::make_shared<SelectStatement>(
        toPlanNode(*plan->children[0], pool_, queryContext));
    return std::make_shared<ExplainStatement>(select);
  }

  return std::make_shared<SelectStatement>(
      toPlanNode(*plan, pool_, queryContext));
}

} // namespace facebook::axiom::optimizer::test
