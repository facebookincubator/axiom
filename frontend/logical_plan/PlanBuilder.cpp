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

#include "logical_plan/PlanBuilder.h" //@manual
#include "optimizer/connectors/ConnectorMetadata.h" //@manual
#include "velox/connectors/Connector.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::logical_plan {

PlanBuilder& PlanBuilder::values(
    const RowTypePtr& rowType,
    std::vector<Variant> rows) {
  VELOX_USER_CHECK_NULL(node_, "Values node must be the leaf node");

  outputMapping_ = std::make_shared<NameMappings>();

  const auto numColumns = rowType->size();
  std::vector<std::string> outputNames;
  outputNames.reserve(numColumns);
  for (const auto& name : rowType->names()) {
    outputNames.push_back(newName(name));
    outputMapping_->add(name, outputNames.back());
  }

  node_ = std::make_shared<ValuesNode>(
      nextId(), ROW(outputNames, rowType->children()), std::move(rows));

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

  std::vector<std::string> outputNames;
  outputNames.reserve(numColumns);

  outputMapping_ = std::make_shared<NameMappings>();

  for (const auto& name : columnNames) {
    columnTypes.push_back(schema->findChild(name));

    outputNames.push_back(newName(name));
    outputMapping_->add(name, outputNames.back());
  }

  node_ = std::make_shared<TableScanNode>(
      nextId(),
      ROW(columnNames, columnTypes),
      connectorId,
      tableName,
      outputNames);

  return *this;
}

PlanBuilder& PlanBuilder::filter(const std::string& predicate) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Filter node cannot be a leaf node");

  auto untypedExpr = parse::parseExpr(predicate, parseOptions_);
  auto expr = resolveScalarTypes(untypedExpr);

  node_ = std::make_shared<FilterNode>(nextId(), node_, expr);

  return *this;
}

namespace {
std::optional<std::string> tryGetRootName(const core::ExprPtr& expr) {
  if (const auto* fieldAccess =
          dynamic_cast<const core::FieldAccessExpr*>(expr.get())) {
    if (fieldAccess->isRootColumn()) {
      return fieldAccess->name();
    }
  }

  return std::nullopt;
}
} // namespace

void PlanBuilder::resolveProjections(
    const std::vector<std::string>& projections,
    std::vector<std::string>& outputNames,
    std::vector<ExprPtr>& exprs,
    NameMappings& mappings) {
  for (const auto& sql : projections) {
    auto untypedExpr = parse::parseExpr(sql, parseOptions_);
    auto expr = resolveScalarTypes(untypedExpr);

    if (untypedExpr->alias().has_value()) {
      const auto& alias = untypedExpr->alias().value();
      outputNames.push_back(newName(alias));
      mappings.add(alias, outputNames.back());
    } else if (expr->isInputReference()) {
      // Identity projection
      const auto& id = expr->asUnchecked<InputReferenceExpr>()->name();
      outputNames.push_back(id);

      const auto names = outputMapping_->reverseLookup(id);
      VELOX_USER_CHECK(!names.empty());

      for (const auto& name : names) {
        mappings.add(name, id);
      }
    } else {
      outputNames.push_back(newName("expr"));
    }

    exprs.push_back(expr);
  }
}

PlanBuilder& PlanBuilder::project(const std::vector<std::string>& projections) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Project node cannot be a leaf node");

  std::vector<std::string> outputNames;
  outputNames.reserve(projections.size());

  std::vector<ExprPtr> exprs;
  exprs.reserve(projections.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  resolveProjections(projections, outputNames, exprs, *newOutputMapping);

  node_ = std::make_shared<ProjectNode>(nextId(), node_, outputNames, exprs);
  outputMapping_ = newOutputMapping;

  return *this;
}

PlanBuilder& PlanBuilder::with(const std::vector<std::string>& projections) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Project node cannot be a leaf node");

  std::vector<std::string> outputNames;
  outputNames.reserve(projections.size());

  std::vector<ExprPtr> exprs;
  exprs.reserve(projections.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  const auto& inputType = node_->outputType();

  for (auto i = 0; i < inputType->size(); i++) {
    const auto& id = inputType->nameOf(i);

    outputNames.push_back(id);

    const auto names = outputMapping_->reverseLookup(id);
    for (const auto& name : names) {
      newOutputMapping->add(name, id);
    }

    exprs.push_back(
        std::make_shared<InputReferenceExpr>(inputType->childAt(i), id));
  }

  resolveProjections(projections, outputNames, exprs, *newOutputMapping);

  node_ = std::make_shared<ProjectNode>(nextId(), node_, outputNames, exprs);
  outputMapping_ = newOutputMapping;

  return *this;
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Aggregate node cannot be a leaf node");

  std::vector<std::string> outputNames;
  outputNames.reserve(groupingKeys.size() + aggregates.size());

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(groupingKeys.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  resolveProjections(groupingKeys, outputNames, keyExprs, *newOutputMapping);

  std::vector<AggregateExprPtr> exprs;
  exprs.reserve(aggregates.size());

  for (const auto& sql : aggregates) {
    auto untypedExpr = parse::parseExpr(sql, parseOptions_);
    auto expr = resolveAggregateTypes(untypedExpr);

    if (untypedExpr->alias().has_value()) {
      const auto& alias = untypedExpr->alias().value();
      outputNames.push_back(newName(alias));
      newOutputMapping->add(alias, outputNames.back());
    } else {
      outputNames.push_back(newName(expr->name()));
    }

    exprs.push_back(expr);
  }

  node_ = std::make_shared<AggregateNode>(
      nextId(),
      node_,
      keyExprs,
      std::vector<AggregateNode::GroupingSet>{},
      exprs,
      outputNames);

  outputMapping_ = newOutputMapping;

  return *this;
}

namespace {

ExprPtr resolveJoinInputName(
    const std::optional<std::string>& alias,
    const std::string& name,
    const NameMappings& mapping,
    const RowTypePtr& inputRowType) {
  if (alias.has_value()) {
    if (auto id = mapping.lookup(alias.value(), name)) {
      return std::make_shared<InputReferenceExpr>(
          inputRowType->findChild(id.value()), id.value());
    }

    return nullptr;
  }

  if (auto id = mapping.lookup(name)) {
    return std::make_shared<InputReferenceExpr>(
        inputRowType->findChild(id.value()), id.value());
  }

  VELOX_USER_FAIL(
      "Cannot resolve column in join input: {} not found in [{}]",
      NameMappings::QualifiedName{alias, name}.toString(),
      mapping.toString());
}

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

ExprPtr resolveScalarTypesImpl(
    const core::ExprPtr& expr,
    const std::function<ExprPtr(
        const std::optional<std::string>& alias,
        const std::string& fieldName)>& inputNameResolver) {
  if (const auto* fieldAccess =
          dynamic_cast<const core::FieldAccessExpr*>(expr.get())) {
    const auto& name = fieldAccess->name();

    if (fieldAccess->isRootColumn()) {
      return inputNameResolver(std::nullopt, name);
    }

    if (auto rootName = tryGetRootName(fieldAccess->input())) {
      if (auto resolved = inputNameResolver(rootName, name)) {
        return resolved;
      }
    }

    auto input =
        resolveScalarTypesImpl(fieldAccess->input(), inputNameResolver);

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
    inputs.push_back(resolveScalarTypesImpl(input, inputNameResolver));
  }

  if (const auto* call = dynamic_cast<const core::CallExpr*>(expr.get())) {
    const auto& name = call->name();

    if (auto specialForm = tryResolveSpecialForm(name, inputs)) {
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

AggregateExprPtr resolveAggregateTypesImpl(
    const core::ExprPtr& expr,
    const std::function<ExprPtr(
        const std::optional<std::string>& alias,
        const std::string& fieldName)>& inputNameResolver) {
  const auto* call = dynamic_cast<const core::CallExpr*>(expr.get());
  VELOX_USER_CHECK_NOT_NULL(call, "Aggregate must be a call expression");

  const auto& name = call->name();

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveScalarTypesImpl(input, inputNameResolver));
  }

  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (const auto& input : inputs) {
    inputTypes.push_back(input->type());
  }

  if (auto type = exec::resolveAggregateFunction(name, inputTypes).first) {
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

PlanBuilder& PlanBuilder::join(
    const PlanBuilder& right,
    const std::string& condition,
    JoinType joinType) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Join node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(right.node_);

  // User-facing column names may have duplicates between left and right side.
  // Columns that are unique can be referenced as is. Columns that are not
  // unique must be referenced using an alias.
  outputMapping_->merge(*right.outputMapping_);

  auto inputRowType = node_->outputType()->unionWith(right.node_->outputType());

  auto untypedExpr = parse::parseExpr(condition, parseOptions_);
  auto expr = resolveScalarTypesImpl(
      untypedExpr, [&](const auto& alias, const auto& name) {
        return resolveJoinInputName(alias, name, *outputMapping_, inputRowType);
      });

  node_ =
      std::make_shared<JoinNode>(nextId(), node_, right.node_, joinType, expr);

  return *this;
}

PlanBuilder& PlanBuilder::sort(const std::vector<std::string>& sortingKeys) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sort node cannot be a leaf node");

  std::vector<SortingField> sortingFields;
  sortingFields.reserve(sortingKeys.size());

  for (const auto& key : sortingKeys) {
    auto orderBy = parse::parseOrderByExpr(key);
    auto expr = resolveScalarTypes(orderBy.expr);

    sortingFields.push_back(
        SortingField{expr, SortOrder(orderBy.ascending, orderBy.nullsFirst)});
  }

  node_ = std::make_shared<SortNode>(nextId(), node_, sortingFields);

  return *this;
}

PlanBuilder& PlanBuilder::limit(int32_t offset, int32_t count) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Limit node cannot be a leaf node");

  node_ = std::make_shared<LimitNode>(nextId(), node_, offset, count);

  return *this;
}

ExprPtr PlanBuilder::resolveInputName(
    const std::optional<std::string>& alias,
    const std::string& name) const {
  if (alias.has_value()) {
    if (auto id = outputMapping_->lookup(alias.value(), name)) {
      return std::make_shared<InputReferenceExpr>(
          node_->outputType()->findChild(id.value()), id.value());
    }

    return nullptr;
  }

  if (auto id = outputMapping_->lookup(name)) {
    return std::make_shared<InputReferenceExpr>(
        node_->outputType()->findChild(id.value()), id.value());
  }

  VELOX_USER_FAIL(
      "Cannot resolve column: {} not in [{}]",
      NameMappings::QualifiedName{alias, name}.toString(),
      outputMapping_->toString());
}

ExprPtr PlanBuilder::resolveScalarTypes(const core::ExprPtr& expr) const {
  return resolveScalarTypesImpl(expr, [&](const auto& alias, const auto& name) {
    return resolveInputName(alias, name);
  });
}

AggregateExprPtr PlanBuilder::resolveAggregateTypes(
    const core::ExprPtr& expr) const {
  return resolveAggregateTypesImpl(
      expr, [&](const auto& alias, const auto& name) {
        return resolveInputName(alias, name);
      });
}

PlanBuilder& PlanBuilder::as(const std::string& alias) {
  outputMapping_->setAlias(alias);
  return *this;
}

std::string PlanBuilder::newName(const std::string& hint) {
  return nameAllocator_->newName(hint);
}

LogicalPlanNodePtr PlanBuilder::build() {
  VELOX_USER_CHECK_NOT_NULL(node_);

  // Use user-specified names for the output. Should we add an OutputNode?

  const auto names = outputMapping_->uniqueNames();

  bool needRename = false;

  const auto& rowType = node_->outputType();

  std::vector<std::string> outputNames;
  outputNames.reserve(rowType->size());

  std::vector<ExprPtr> exprs;
  exprs.reserve(rowType->size());

  for (auto i = 0; i < rowType->size(); i++) {
    const auto& id = rowType->nameOf(i);

    auto it = names.find(id);
    if (it != names.end()) {
      outputNames.push_back(it->second);
    } else {
      outputNames.push_back(id);
    }

    if (id != outputNames.back()) {
      needRename = true;
    }

    exprs.push_back(
        std::make_shared<InputReferenceExpr>(rowType->childAt(i), id));
  }

  if (needRename) {
    return std::make_shared<ProjectNode>(nextId(), node_, outputNames, exprs);
  }

  return node_;
}

std::string NameAllocator::newName(const std::string& hint) {
  VELOX_CHECK(!hint.empty(), "Hint cannot be empty");

  // Strip suffix past '_'.
  std::string prefix = hint;

  auto pos = prefix.rfind('_');
  if (pos != std::string::npos) {
    prefix = prefix.substr(0, pos);
  }

  std::string name = prefix;
  do {
    if (names_.insert(name).second) {
      return name;
    }
    name = fmt::format("{}_{}", prefix, nextId_++);
  } while (true);
}

std::optional<std::string> NameMappings::lookup(const std::string& name) const {
  auto it = mappings_.find(QualifiedName{.alias = {}, .name = name});
  if (it != mappings_.end()) {
    return it->second;
  }

  return std::nullopt;
}

std::optional<std::string> NameMappings::lookup(
    const std::string& alias,
    const std::string& name) const {
  auto it = mappings_.find(QualifiedName{.alias = alias, .name = name});
  if (it != mappings_.end()) {
    return it->second;
  }

  return std::nullopt;
}

std::vector<NameMappings::QualifiedName> NameMappings::reverseLookup(
    const std::string& id) const {
  std::vector<QualifiedName> names;
  for (const auto& [key, value] : mappings_) {
    if (value == id) {
      names.push_back(key);
    }
  }

  VELOX_CHECK_LE(names.size(), 2);
  if (names.size() == 2) {
    VELOX_CHECK_EQ(names[0].name, names[1].name);
    VELOX_CHECK_NE(names[0].alias.has_value(), names[1].alias.has_value());
  }

  return names;
}

void NameMappings::setAlias(const std::string& alias) {
  std::vector<std::pair<std::string, std::string>> names;
  for (auto it = mappings_.begin(); it != mappings_.end();) {
    if (it->first.alias.has_value()) {
      it = mappings_.erase(it);
    } else {
      names.emplace_back(it->first.name, it->second);
      ++it;
    }
  }

  for (auto& [name, id] : names) {
    mappings_.emplace(
        QualifiedName{.alias = alias, .name = std::move(name)}, std::move(id));
  }
}

void NameMappings::merge(const NameMappings& other) {
  for (const auto& [name, id] : other.mappings_) {
    if (mappings_.count(name) != 0) {
      VELOX_CHECK(!name.alias.has_value());
      mappings_.erase(name);
    } else {
      mappings_.emplace(name, id);
    }
  }
}

std::unordered_map<std::string, std::string> NameMappings::uniqueNames() const {
  std::unordered_map<std::string, std::string> names;
  for (const auto& [name, id] : mappings_) {
    if (!name.alias.has_value()) {
      names.emplace(id, name.name);
    }
  }
  return names;
}

std::string NameMappings::toString() const {
  bool first = true;
  std::stringstream out;
  for (const auto& [name, id] : mappings_) {
    if (!first) {
      out << ", ";
    } else {
      first = false;
    }
    out << name.toString() << " -> " << id;
  }
  return out.str();
}

size_t NameMappings::QualifiedNameHasher::operator()(
    const QualifiedName& value) const {
  size_t h1 = 0;
  if (value.alias.has_value()) {
    h1 = std::hash<std::string>()(value.alias.value());
  }

  size_t h2 = std::hash<std::string>()(value.name);

  return h1 ^ (h2 << 1);
}

} // namespace facebook::velox::logical_plan
