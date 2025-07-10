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

#include "velox/frontend/logical_plan/PlanBuilder.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/optimizer/connectors/ConnectorMetadata.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::logical_plan {

namespace {

std::string toString(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  std::ostringstream signature;
  signature << functionName << "(";
  for (auto i = 0; i < argTypes.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << argTypes[i]->toString();
  }
  signature << ")";
  return signature.str();
}

std::string toString(
    const std::vector<const exec::FunctionSignature*>& signatures) {
  std::stringstream out;
  for (auto i = 0; i < signatures.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signatures[i]->toString();
  }
  return out.str();
}

TypePtr resolveScalarFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  if (auto type = resolveFunction(name, argTypes)) {
    return type;
  }

  auto allSignatures = getFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Scalar function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(functionSignatures));
  }
}

ExprPtr tryResolveSpecialForm(
    const std::string& name,
    const core::ExprPtr& expr,
    const std::vector<ExprPtr>& resolvedInputs) {
  if (name == "and") {
    return std::make_shared<SpecialFormExpr>(
        BOOLEAN(), SpecialForm::kAnd, resolvedInputs);
  }

  if (name == "or") {
    return std::make_shared<SpecialFormExpr>(
        BOOLEAN(), SpecialForm::kOr, resolvedInputs);
  }

  if (name == "try") {
    VELOX_USER_CHECK_EQ(resolvedInputs.size(), 1, "TRY must have one argument");
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(0)->type(), SpecialForm::kTry, resolvedInputs);
  }

  if (name == "coalesce") {
    VELOX_USER_CHECK_GE(
        resolvedInputs.size(), 2, "COALESCE must have at least two arguments");
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(0)->type(), SpecialForm::kCoalesce, resolvedInputs);
  }

  if (name == "if") {
    VELOX_USER_CHECK_GE(
        resolvedInputs.size(), 2, "IF must have at least two arguments");
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(1)->type(), SpecialForm::kIf, resolvedInputs);
  }

  if (name == "switch") {
    VELOX_USER_CHECK_GE(
        resolvedInputs.size(), 2, "SWITCH must have at least two arguments");
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(1)->type(), SpecialForm::kSwitch, resolvedInputs);
  }

  return nullptr;
}

ExprPtr resolveTypes(
    const core::ExprPtr& expr,
    const RowTypePtr& inputRowType) {
  if (const auto* fieldAccess =
          dynamic_cast<const core::FieldAccessExpr*>(expr.get())) {
    const auto& name = fieldAccess->name();

    if (fieldAccess->isRootColumn()) {
      return std::make_shared<InputReferenceExpr>(
          inputRowType->findChild(name), name);
    }

    auto input = resolveTypes(fieldAccess->input(), inputRowType);

    return std::make_shared<SpecialFormExpr>(
        input->type()->asRow().findChild(name),
        SpecialForm::kDereference,
        std::vector<ExprPtr>{
            input, std::make_shared<ConstantExpr>(VARCHAR(), name)});
  }

  if (const auto& constant =
          dynamic_cast<const core::ConstantExpr*>(expr.get())) {
    return std::make_shared<ConstantExpr>(constant->type(), constant->value());
  }

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveTypes(input, inputRowType));
  }

  if (const auto* call = dynamic_cast<const core::CallExpr*>(expr.get())) {
    const auto& name = call->name();

    if (auto specialForm = tryResolveSpecialForm(name, expr, inputs)) {
      return specialForm;
    }

    std::vector<TypePtr> inputTypes;
    inputTypes.reserve(inputs.size());
    for (const auto& input : inputs) {
      inputTypes.push_back(input->type());
    }

    auto type = resolveScalarFunction(name, inputTypes);

    return std::make_shared<CallExpr>(type, name, inputs);
  }

  if (const auto* cast = dynamic_cast<const core::CastExpr*>(expr.get())) {
    return std::make_shared<SpecialFormExpr>(
        cast->type(),
        cast->isTryCast() ? SpecialForm::kTryCast : SpecialForm::kCast,
        inputs);
  }

  VELOX_NYI("Can't resolve {}", expr->toString());
}

AggregateExprPtr resolveAggregate(
    const core::ExprPtr& expr,
    const RowTypePtr& inputRowType) {
  const auto* call = dynamic_cast<const core::CallExpr*>(expr.get());
  VELOX_USER_CHECK_NOT_NULL(call, "Aggregate must be a call expression");

  const auto& name = call->name();

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveTypes(input, inputRowType));
  }

  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (const auto& input : inputs) {
    inputTypes.push_back(input->type());
  }

  if (auto type = exec::resolveAggregateFunction(name, inputTypes).second) {
    return std::make_shared<AggregateExpr>(type, name, inputs);
  }

  auto allSignatures = exec::getAggregateFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Aggregate function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Aggregate function signature is not supported: {}. Supported signatures: {}.",
        toString(name, inputTypes),
        toString(functionSignatures));
  }
}

} // namespace

PlanBuilder& PlanBuilder::values(
    const RowTypePtr& rowType,
    std::vector<Variant> rows) {
  VELOX_USER_CHECK_NULL(node_, "Values node must be the leaf node");

  node_ = std::make_shared<ValuesNode>(nextId(), rowType, std::move(rows));

  return *this;
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& connectorId,
    const std::string& tableName,
    const std::vector<std::string>& columnNames) {
  VELOX_USER_CHECK_NULL(node_, "Table scan node must be the leaf node");

  auto* metadata = connector::getConnector(connectorId)->metadata();
  auto* table = metadata->findTable(tableName);
  const auto& schema = table->rowType();

  const auto numColumns = columnNames.size();

  std::vector<TypePtr> columnTypes;
  columnTypes.reserve(numColumns);

  for (const auto& name : columnNames) {
    columnTypes.push_back(schema->findChild(name));
  }

  node_ = std::make_shared<TableScanNode>(
      nextId(),
      ROW(columnNames, columnTypes),
      connectorId,
      tableName,
      columnNames);

  return *this;
}

PlanBuilder& PlanBuilder::filter(const std::string& predicate) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Filter node cannot be a leaf node");

  auto untypedExpr = parse::parseExpr(predicate, parseOptions_);
  auto expr = resolveTypes(untypedExpr, node_->outputType());

  node_ = std::make_shared<FilterNode>(nextId(), node_, expr);

  return *this;
}

PlanBuilder& PlanBuilder::project(const std::vector<std::string>& projections) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Project node cannot be a leaf node");

  std::vector<std::string> aliases;
  std::vector<ExprPtr> exprs;

  aliases.reserve(projections.size());
  exprs.reserve(projections.size());

  for (const auto& sql : projections) {
    auto untypedExpr = parse::parseExpr(sql, parseOptions_);
    auto expr = resolveTypes(untypedExpr, node_->outputType());

    if (expr->isInputReference()) {
      aliases.push_back(expr->asUnchecked<InputReferenceExpr>()->name());
    } else {
      VELOX_USER_CHECK(
          untypedExpr->alias().has_value(),
          "Non-trivial projections must specify an alias: {}",
          sql);

      aliases.push_back(untypedExpr->alias().value());
    }

    exprs.push_back(expr);
  }

  node_ = std::make_shared<ProjectNode>(nextId(), node_, aliases, exprs);
  return *this;
}

PlanBuilder& PlanBuilder::with(const std::vector<std::string>& projections) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Project node cannot be a leaf node");

  std::vector<std::string> allProjections = node_->outputType()->names();
  allProjections.insert(
      allProjections.end(), projections.begin(), projections.end());

  return project(allProjections);
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Aggregate node cannot be a leaf node");

  std::vector<std::string> aliases;
  aliases.reserve(groupingKeys.size() + aggregates.size());

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(groupingKeys.size());

  for (const auto& key : groupingKeys) {
    auto untypedExpr = parse::parseExpr(key, parseOptions_);
    auto expr = resolveTypes(untypedExpr, node_->outputType());

    if (expr->isInputReference()) {
      aliases.push_back(expr->asUnchecked<InputReferenceExpr>()->name());
    } else {
      VELOX_USER_CHECK(
          untypedExpr->alias().has_value(),
          "Non-trivial grouping keys must specify an alias: {}",
          key);

      aliases.push_back(untypedExpr->alias().value());
    }

    keyExprs.push_back(expr);
  }

  std::vector<AggregateExprPtr> exprs;
  exprs.reserve(aggregates.size());

  for (const auto& sql : aggregates) {
    auto untypedExpr = parse::parseExpr(sql, parseOptions_);
    auto expr = resolveAggregate(untypedExpr, node_->outputType());

    VELOX_USER_CHECK(
        untypedExpr->alias().has_value(),
        "Aggregates must specify an alias: {}",
        sql);

    aliases.push_back(untypedExpr->alias().value());

    exprs.push_back(expr);
  }

  node_ = std::make_shared<AggregateNode>(
      nextId(),
      node_,
      keyExprs,
      std::vector<AggregateNode::GroupingSet>{},
      exprs,
      aliases);

  return *this;
}

PlanBuilder& PlanBuilder::join(
    const LogicalPlanNodePtr& right,
    const std::string& condition,
    JoinType joinType) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Join node cannot be a leaf node");

  auto inputRowType = node_->outputType()->unionWith(right->outputType());

  auto untypedExpr = parse::parseExpr(condition, parseOptions_);
  auto expr = resolveTypes(untypedExpr, inputRowType);

  node_ = std::make_shared<JoinNode>(nextId(), node_, right, joinType, expr);

  return *this;
}

PlanBuilder& PlanBuilder::sort(const std::vector<std::string>& sortingKeys) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sort node cannot be a leaf node");

  std::vector<SortingField> sortingFields;
  sortingFields.reserve(sortingKeys.size());

  for (const auto& key : sortingKeys) {
    auto orderBy = parse::parseOrderByExpr(key);
    auto expr = resolveTypes(orderBy.expr, node_->outputType());

    sortingFields.push_back(
        SortingField(expr, {orderBy.ascending, orderBy.nullsFirst}));
  }

  node_ = std::make_shared<SortNode>(nextId(), node_, sortingFields);

  return *this;
}

PlanBuilder& PlanBuilder::limit(int32_t offset, int32_t count) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Limit node cannot be a leaf node");

  node_ = std::make_shared<LimitNode>(nextId(), node_, offset, count);

  return *this;
}

} // namespace facebook::velox::logical_plan
