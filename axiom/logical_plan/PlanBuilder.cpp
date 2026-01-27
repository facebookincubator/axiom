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
#include "axiom/logical_plan/PlanBuilder.h"
#include <velox/common/base/Exceptions.h>
#include <vector>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/NameMappings.h"
#include "velox/type/TypeCoercer.h"

namespace facebook::axiom::logical_plan {

PlanBuilder& PlanBuilder::values(
    const velox::RowTypePtr& rowType,
    std::vector<velox::Variant> rows) {
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
      nextId(),
      ROW(std::move(outputNames), rowType->children()),
      std::move(rows));

  return *this;
}

PlanBuilder& PlanBuilder::values(
    const std::vector<velox::RowVectorPtr>& values) {
  VELOX_USER_CHECK_NULL(node_, "Values node must be the leaf node");

  outputMapping_ = std::make_shared<NameMappings>();

  auto rowType = values.empty() ? velox::ROW({}) : values.front()->rowType();
  const auto numColumns = rowType->size();
  std::vector<std::string> outputNames;
  outputNames.reserve(numColumns);
  for (const auto& name : rowType->names()) {
    outputNames.push_back(newName(name));
    outputMapping_->add(name, outputNames.back());
  }
  rowType = ROW(std::move(outputNames), rowType->children());

  std::vector<velox::RowVectorPtr> newValues;
  newValues.reserve(values.size());
  for (const auto& value : values) {
    VELOX_USER_CHECK_NOT_NULL(value);
    VELOX_USER_CHECK(
        value->rowType()->equivalent(*rowType),
        "All values must have the equilent type: {} vs. {}",
        value->rowType()->toString(),
        rowType->toString());
    auto newValue = std::make_shared<velox::RowVector>(
        value->pool(),
        rowType,
        value->nulls(),
        static_cast<size_t>(value->size()),
        value->children(),
        value->getNullCount());
    newValues.emplace_back(std::move(newValue));
  }

  node_ = std::make_shared<ValuesNode>(nextId(), std::move(newValues));

  return *this;
}

namespace {
ExprPtr applyCoercion(const ExprPtr& input, const velox::TypePtr& type) {
  if (input->isSpecialForm() &&
      input->as<SpecialFormExpr>()->form() == SpecialForm::kCast) {
    return std::make_shared<SpecialFormExpr>(
        type, SpecialForm::kCast, input->inputAt(0));
  }

  return std::make_shared<SpecialFormExpr>(type, SpecialForm::kCast, input);
}

std::vector<velox::TypePtr> toTypes(const std::vector<ExprPtr>& exprs) {
  std::vector<velox::TypePtr> types;
  types.reserve(exprs.size());
  for (auto& expr : exprs) {
    types.push_back(expr->type());
  }

  return types;
}
} // namespace

PlanBuilder& PlanBuilder::values(
    const std::vector<std::string>& names,
    const std::vector<std::vector<ExprApi>>& values) {
  VELOX_USER_CHECK_NULL(node_, "Values node must be the leaf node");

  if (values.empty()) {
    node_ = std::make_shared<ValuesNode>(nextId(), ValuesNode::Vectors{});
    return *this;
  }

  const auto numColumns = names.size();
  const auto numRows = values.size();

  std::vector<std::vector<ExprPtr>> exprs;
  exprs.reserve(numRows);
  for (const auto& row : values) {
    VELOX_USER_CHECK_EQ(numColumns, row.size());

    std::vector<ExprPtr> valueExprs;
    valueExprs.reserve(numColumns);
    for (const auto& expr : row) {
      valueExprs.emplace_back(resolveScalarTypes(expr.expr()));
    }

    exprs.emplace_back(std::move(valueExprs));
  }

  auto types = toTypes(exprs.front());

  outputMapping_ = std::make_shared<NameMappings>();

  std::vector<std::string> outputNames;
  if (numColumns > 0) {
    outputNames.reserve(numColumns);
    for (const auto& name : names) {
      outputNames.push_back(newName(name));
      outputMapping_->add(name, outputNames.back());
    }
  }

  if (enableCoercions_) {
    for (auto i = 0; i < numRows; ++i) {
      auto& row = exprs[i];
      for (auto j = 0; j < numColumns; ++j) {
        const auto& type = row[j]->type();
        if (types[j]->equivalent(*type)) {
          continue;
        }

        if (velox::TypeCoercer::coercible(type, types[j])) {
          row[j] = applyCoercion(row[j], types[j]);
        } else if (velox::TypeCoercer::coercible(types[j], type)) {
          types[j] = type;

          for (auto k = 0; k < i; ++k) {
            exprs[k][j] = applyCoercion(exprs[k][j], types[j]);
          }
        }
      }
    }
  }

  node_ = std::make_shared<ValuesNode>(
      nextId(),
      ROW(std::move(outputNames), std::move(types)),
      std::move(exprs));

  return *this;
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& tableName,
    bool includeHiddenColumns) {
  VELOX_USER_CHECK(defaultConnectorId_.has_value());
  return tableScan(
      defaultConnectorId_.value(), tableName, includeHiddenColumns);
}

PlanBuilder& PlanBuilder::from(const std::vector<std::string>& tableNames) {
  VELOX_USER_CHECK_NULL(node_, "Table scan node must be the leaf node");
  VELOX_USER_CHECK(!tableNames.empty());

  tableScan(tableNames.front());

  Context context{defaultConnectorId_};
  context.planNodeIdGenerator = planNodeIdGenerator_;
  context.nameAllocator = nameAllocator_;

  for (auto i = 1; i < tableNames.size(); ++i) {
    crossJoin(PlanBuilder(context).tableScan(tableNames.at(i)));
  }

  return *this;
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& connectorId,
    const std::string& tableName,
    bool includeHiddenColumns) {
  VELOX_USER_CHECK_NULL(node_, "Table scan node must be the leaf node");

  auto* metadata = connector::ConnectorMetadata::metadata(connectorId);
  auto table = metadata->findTable(tableName);
  VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);

  // Table::type() returns visible columns only.
  // Table::allColumns() returns all columns, including hidden ones.
  const auto& schema = table->type();
  const auto& allColumns = table->allColumns();

  const auto numColumns =
      includeHiddenColumns ? allColumns.size() : schema->size();

  std::vector<velox::TypePtr> columnTypes;
  columnTypes.reserve(numColumns);

  std::vector<std::string> outputNames;
  outputNames.reserve(numColumns);

  std::vector<std::string> originalNames;
  originalNames.reserve(numColumns);

  outputMapping_ = std::make_shared<NameMappings>();

  auto addColumn = [&](const auto& name, const auto& type) {
    columnTypes.push_back(type);

    originalNames.push_back(name);
    outputNames.push_back(newName(name));
    outputMapping_->add(name, outputNames.back());
  };

  for (auto i = 0; i < schema->size(); ++i) {
    addColumn(schema->nameOf(i), schema->childAt(i));
  }

  if (includeHiddenColumns) {
    for (const auto* column : allColumns) {
      if (column->hidden()) {
        addColumn(column->name(), column->type());
        outputMapping_->markHidden(outputNames.back());
      }
    }
  }

  node_ = std::make_shared<TableScanNode>(
      nextId(),
      ROW(std::move(outputNames), std::move(columnTypes)),
      connectorId,
      tableName,
      std::move(originalNames));

  return *this;
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& tableName,
    const std::vector<std::string>& columnNames) {
  VELOX_USER_CHECK(defaultConnectorId_.has_value());
  return tableScan(defaultConnectorId_.value(), tableName, columnNames);
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& connectorId,
    const std::string& tableName,
    const std::vector<std::string>& columnNames) {
  VELOX_USER_CHECK_NULL(node_, "Table scan node must be the leaf node");

  auto* metadata = connector::ConnectorMetadata::metadata(connectorId);
  auto table = metadata->findTable(tableName);
  VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);
  const auto& schema = table->type();

  const auto numColumns = columnNames.size();

  std::vector<velox::TypePtr> columnTypes;
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
      ROW(outputNames, columnTypes),
      connectorId,
      tableName,
      columnNames);

  return *this;
}

PlanBuilder& PlanBuilder::dropHiddenColumns() {
  const auto size = numOutput();
  const auto& inputType = node_->outputType();

  bool hasHiddenColumns = false;
  for (const auto& name : inputType->names()) {
    if (outputMapping_->isHidden(name)) {
      hasHiddenColumns = true;
      break;
    }
  }

  if (!hasHiddenColumns) {
    return *this;
  }

  std::vector<std::string> outputNames;
  outputNames.reserve(size);

  std::vector<ExprPtr> exprs;
  exprs.reserve(size);

  auto newOutputMapping = std::make_shared<NameMappings>();

  for (auto i = 0; i < inputType->size(); i++) {
    const auto& id = inputType->nameOf(i);

    if (outputMapping_->isHidden(id)) {
      continue;
    }

    outputNames.push_back(id);

    const auto names = outputMapping_->reverseLookup(id);
    for (const auto& name : names) {
      newOutputMapping->add(name, id);
    }

    exprs.push_back(
        std::make_shared<InputReferenceExpr>(inputType->childAt(i), id));
  }

  node_ = std::make_shared<ProjectNode>(
      nextId(), std::move(node_), std::move(outputNames), std::move(exprs));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

PlanBuilder& PlanBuilder::filter(const std::string& predicate) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Filter node cannot be a leaf node");

  auto untypedExpr = sqlParser_->parseExpr(predicate);
  return filter(untypedExpr);
}

PlanBuilder& PlanBuilder::filter(const ExprApi& predicate) {
  auto expr = resolveScalarTypes(predicate.expr());

  node_ = std::make_shared<FilterNode>(nextId(), node_, std::move(expr));

  return *this;
}

std::vector<ExprApi> PlanBuilder::parse(const std::vector<std::string>& exprs) {
  std::vector<ExprApi> untypedExprs;
  untypedExprs.reserve(exprs.size());
  for (const auto& sql : exprs) {
    untypedExprs.emplace_back(sqlParser_->parseExpr(sql));
  }

  return untypedExprs;
}

void PlanBuilder::resolveProjections(
    const std::vector<ExprApi>& projections,
    std::vector<std::string>& outputNames,
    std::vector<ExprPtr>& exprs,
    NameMappings& mappings) {
  std::unordered_set<std::string> identities;
  for (const auto& untypedExpr : projections) {
    auto expr = resolveScalarTypes(untypedExpr.expr());

    const auto& alias = untypedExpr.name();

    if (expr->isInputReference()) {
      // Identity projection
      const auto& id = expr->as<InputReferenceExpr>()->name();
      if (!alias.has_value() || id == alias.value()) {
        if (identities.emplace(id).second) {
          outputNames.push_back(id);

          const auto names = outputMapping_->reverseLookup(id);
          VELOX_USER_CHECK(!names.empty());

          for (const auto& name : names) {
            mappings.add(name, id);
          }
        } else {
          outputNames.push_back(newName(id));
        }
      } else {
        outputNames.push_back(newName(alias.value()));
        mappings.add(alias.value(), outputNames.back());
      }
    } else if (alias.has_value()) {
      outputNames.push_back(newName(alias.value()));
      mappings.add(alias.value(), outputNames.back());
    } else {
      outputNames.push_back(newName("expr"));
    }

    exprs.push_back(std::move(expr));
  }
}

PlanBuilder& PlanBuilder::project(const std::vector<ExprApi>& projections) {
  if (!node_) {
    values(velox::ROW({}), {velox::Variant::row({})});
  }

  std::vector<std::string> outputNames;
  outputNames.reserve(projections.size());

  std::vector<ExprPtr> exprs;
  exprs.reserve(projections.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  resolveProjections(projections, outputNames, exprs, *newOutputMapping);

  node_ = std::make_shared<ProjectNode>(
      nextId(), std::move(node_), std::move(outputNames), std::move(exprs));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

PlanBuilder& PlanBuilder::with(const std::vector<ExprApi>& projections) {
  if (!node_) {
    values(velox::ROW({}), {velox::Variant::row({})});
  }

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

  node_ = std::make_shared<ProjectNode>(
      nextId(), std::move(node_), std::move(outputNames), std::move(exprs));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  std::vector<AggregateOptions> options;
  options.reserve(aggregates.size());
  for (const auto& sql : aggregates) {
    auto aggregateExpr = velox::duckdb::parseAggregateExpr(sql, {});

    std::vector<SortKey> sortingKeys;
    sortingKeys.reserve(aggregateExpr.orderBy.size());
    for (const auto& orderBy : aggregateExpr.orderBy) {
      sortingKeys.emplace_back(
          SortKey{
              ExprApi(orderBy.expr), orderBy.ascending, orderBy.nullsFirst});
    }

    options.emplace_back(
        std::move(aggregateExpr.maskExpr),
        std::move(sortingKeys),
        aggregateExpr.distinct);
  }

  return aggregate(parse(groupingKeys), parse(aggregates), options);
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<ExprApi>& groupingKeys,
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Aggregate node cannot be a leaf node");

  std::vector<std::string> outputNames;
  outputNames.reserve(groupingKeys.size() + aggregates.size());

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(groupingKeys.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  resolveProjections(groupingKeys, outputNames, keyExprs, *newOutputMapping);

  std::vector<AggregateExprPtr> exprs;
  exprs.reserve(aggregates.size());

  VELOX_USER_CHECK(options.size() == aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    const auto& aggregate = aggregates[i];

    ExprPtr filter;
    if (options[i].filter != nullptr) {
      filter = resolveScalarTypes(options[i].filter);
    }

    std::vector<SortingField> sortingFields;
    sortingFields.reserve(options[i].orderBy.size());
    for (const auto& key : options[i].orderBy) {
      auto expr = resolveScalarTypes(key.expr.expr());

      sortingFields.push_back(
          SortingField{expr, SortOrder(key.ascending, key.nullsFirst)});
    }

    AggregateExprPtr expr;
    expr = resolveAggregateTypes(
        aggregate.expr(), filter, sortingFields, options[i].distinct);

    if (aggregate.name().has_value()) {
      const auto& alias = aggregate.name().value();
      outputNames.push_back(newName(alias));
      newOutputMapping->add(alias, outputNames.back());
    } else {
      outputNames.push_back(newName(expr->name()));
    }

    exprs.emplace_back(std::move(expr));
  }

  node_ = std::make_shared<AggregateNode>(
      nextId(),
      std::move(node_),
      std::move(keyExprs),
      std::vector<AggregateNode::GroupingSet>{},
      std::move(exprs),
      std::move(outputNames));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

PlanBuilder& PlanBuilder::distinct() {
  VELOX_USER_CHECK_NOT_NULL(
      node_, "Distinct aggregation node cannot be a leaf node");

  const auto& inputType = node_->outputType();

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(inputType->size());
  for (auto i = 0; i < inputType->size(); i++) {
    keyExprs.push_back(
        std::make_shared<InputReferenceExpr>(
            inputType->childAt(i), inputType->nameOf(i)));
  }

  node_ = std::make_shared<AggregateNode>(
      nextId(),
      std::move(node_),
      std::move(keyExprs),
      std::vector<AggregateNode::GroupingSet>{},
      std::vector<AggregateExprPtr>{},
      inputType->names());

  return *this;
}

PlanBuilder& PlanBuilder::unnest(
    const std::vector<ExprApi>& unnestExprs,
    bool withOrdinality,
    const std::optional<std::string>& alias,
    const std::vector<std::string>& unnestAliases) {
  if (!node_) {
    values(velox::ROW({}), {velox::Variant::row({})});
  }

  auto newOutputMapping = outputMapping_;

  size_t index = 0;

  auto addOutputMapping = [&](const std::string& name, const std::string& id) {
    if (!newOutputMapping->lookup(name)) {
      newOutputMapping->add(name, id);
    }
    newOutputMapping->add({.alias = alias, .name = name}, id);
    ++index;
  };

  std::vector<ExprPtr> exprs;
  std::vector<std::vector<std::string>> outputNames;
  for (const auto& unnestExpr : unnestExprs) {
    auto expr = resolveScalarTypes(unnestExpr.expr());
    exprs.push_back(expr);

    if (!unnestExpr.unnestedAliases().empty()) {
      outputNames.emplace_back();
      for (const std::string& alias : unnestExpr.unnestedAliases()) {
        outputNames.back().emplace_back(newName(alias));
        newOutputMapping->add(alias, outputNames.back().back());
      }
    } else {
      switch (expr->type()->kind()) {
        case velox::TypeKind::ARRAY:
          if (!unnestAliases.empty()) {
            VELOX_USER_CHECK_LT(index, unnestAliases.size());

            const auto& outputName = unnestAliases.at(index);
            outputNames.emplace_back(
                std::vector<std::string>{newName(outputName)});

            addOutputMapping(outputName, outputNames.back().back());
          } else {
            outputNames.emplace_back(std::vector<std::string>{newName("e")});
          }
          break;

        case velox::TypeKind::MAP:
          if (!unnestAliases.empty()) {
            VELOX_USER_CHECK_LT(index, unnestAliases.size());

            const auto& keyName = unnestAliases.at(index);
            const auto& valueName = unnestAliases.at(index + 1);
            outputNames.emplace_back(
                std::vector<std::string>{newName(keyName), newName(valueName)});

            addOutputMapping(keyName, outputNames.back().at(0));
            addOutputMapping(valueName, outputNames.back().at(1));
          } else {
            outputNames.emplace_back(
                std::vector<std::string>{newName("k"), newName("v")});
          }
          break;

        default:
          VELOX_USER_FAIL(
              "Unsupported type to unnest: {}", expr->type()->toString());
      }
    }
  }

  std::optional<std::string> ordinalityName;
  if (withOrdinality) {
    ordinalityName = newName("ordinality");
  }

  bool flattenArrayOfRows = false;

  node_ = std::make_shared<UnnestNode>(
      nextId(),
      std::move(node_),
      std::move(exprs),
      std::move(outputNames),
      std::move(ordinalityName),
      flattenArrayOfRows);

  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

PlanBuilder& PlanBuilder::join(
    const PlanBuilder& right,
    const std::string& condition,
    JoinType joinType) {
  std::optional<ExprApi> conditionExpr;
  if (!condition.empty()) {
    conditionExpr = sqlParser_->parseExpr(condition);
  }

  return join(right, conditionExpr, joinType);
}

namespace {

ExprPtr resolveJoinInputName(
    const std::optional<std::string>& alias,
    const std::string& name,
    const NameMappings& mapping,
    const velox::RowTypePtr& inputRowType) {
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
} // namespace

PlanBuilder& PlanBuilder::join(
    const PlanBuilder& right,
    const std::optional<ExprApi>& condition,
    JoinType joinType) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Join node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(right.node_);

  // User-facing column names may have duplicates between left and right side.
  // Columns that are unique can be referenced as is. Columns that are not
  // unique must be referenced using an alias.
  outputMapping_->merge(*right.outputMapping_);

  auto inputRowType = node_->outputType()->unionWith(right.node_->outputType());

  ExprPtr expr;
  if (condition.has_value()) {
    expr = resolver_.resolveScalarTypes(
        condition->expr(), [&](const auto& alias, const auto& name) {
          return resolveJoinInputName(
              alias, name, *outputMapping_, inputRowType);
        });
  }

  node_ = std::make_shared<JoinNode>(
      nextId(), std::move(node_), right.node_, joinType, std::move(expr));

  return *this;
}

PlanBuilder& PlanBuilder::unionAll(const PlanBuilder& other) {
  VELOX_USER_CHECK_NOT_NULL(node_, "UnionAll node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(other.node_);

  node_ = std::make_shared<SetNode>(
      nextId(),
      std::vector<LogicalPlanNodePtr>{node_, other.node_},
      SetOperation::kUnionAll);

  return *this;
}

PlanBuilder& PlanBuilder::intersect(const PlanBuilder& other) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Intersect node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(other.node_);

  node_ = std::make_shared<SetNode>(
      nextId(),
      std::vector<LogicalPlanNodePtr>{std::move(node_), other.node_},
      SetOperation::kIntersect);

  return *this;
}

PlanBuilder& PlanBuilder::except(const PlanBuilder& other) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Intersect node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(other.node_);

  node_ = std::make_shared<SetNode>(
      nextId(),
      std::vector<LogicalPlanNodePtr>{std::move(node_), other.node_},
      SetOperation::kExcept);

  return *this;
}

PlanBuilder& PlanBuilder::setOperation(
    SetOperation op,
    const PlanBuilder& other) {
  return setOperation(op, {std::move(*this), other});
}

PlanBuilder& PlanBuilder::setOperation(
    SetOperation op,
    const std::vector<PlanBuilder>& inputs) {
  VELOX_USER_CHECK_NULL(node_, "setOperation must be a leaf");
  VELOX_USER_CHECK_GE(
      inputs.size(), 2, "Set operation requires at least 2 inputs");

  outputMapping_ = inputs.front().outputMapping_;

  std::vector<LogicalPlanNodePtr> nodes;
  nodes.reserve(inputs.size());
  for (const auto& builder : inputs) {
    VELOX_CHECK_NOT_NULL(builder.node_);
    nodes.push_back(builder.node_);
  }

  if (enableCoercions_) {
    // Apply type coercion: find common supertype for each column.
    const auto firstRowType = nodes[0]->outputType();
    auto targetTypes = firstRowType->children();

    for (size_t i = 1; i < nodes.size(); ++i) {
      const auto& rowType = nodes[i]->outputType();

      VELOX_USER_CHECK_EQ(
          firstRowType->size(),
          rowType->size(),
          "Output schemas of all inputs to a Set operation must have same number of columns");

      for (uint32_t j = 0; j < firstRowType->size(); ++j) {
        const auto& currentType = targetTypes[j];
        const auto& nextType = rowType->childAt(j);

        if (currentType->equivalent(*nextType)) {
          continue;
        }

        auto commonType =
            velox::TypeCoercer::leastCommonSuperType(currentType, nextType);
        VELOX_USER_CHECK_NOT_NULL(
            commonType,
            "Output schemas of all inputs to a Set operation must match: {} vs. {} at {}.{}",
            currentType->toSummaryString(),
            nextType->toSummaryString(),
            j,
            firstRowType->nameOf(j));

        targetTypes[j] = commonType;
      }
    }

    auto targetRowType =
        velox::ROW(folly::copy(firstRowType->names()), std::move(targetTypes));

    // Add cast projections where needed.
    for (auto& node : nodes) {
      const auto& inputRowType = node->outputType();
      std::vector<uint32_t> indicesToCast;
      for (uint32_t i = 0; i < inputRowType->size(); ++i) {
        if (*inputRowType->childAt(i) != *targetRowType->childAt(i)) {
          indicesToCast.push_back(i);
        }
      }

      if (!indicesToCast.empty()) {
        std::vector<ExprPtr> exprs;
        exprs.reserve(inputRowType->size());

        size_t castIdx = 0;
        for (uint32_t i = 0; i < inputRowType->size(); ++i) {
          const auto& inputType = inputRowType->childAt(i);
          const auto& name = inputRowType->nameOf(i);

          auto inputRef = std::make_shared<InputReferenceExpr>(inputType, name);

          if (castIdx < indicesToCast.size() && indicesToCast[castIdx] == i) {
            exprs.push_back(
                std::make_shared<SpecialFormExpr>(
                    targetRowType->childAt(i), SpecialForm::kCast, inputRef));
            ++castIdx;
          } else {
            exprs.push_back(inputRef);
          }
        }

        node = std::make_shared<ProjectNode>(
            nextId(), std::move(node), inputRowType->names(), std::move(exprs));
      }
    }
  }

  node_ = std::make_shared<SetNode>(nextId(), std::move(nodes), op);
  return *this;
}

PlanBuilder& PlanBuilder::sort(const std::vector<std::string>& sortingKeys) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sort node cannot be a leaf node");

  std::vector<SortingField> sortingFields;
  sortingFields.reserve(sortingKeys.size());

  for (const auto& key : sortingKeys) {
    auto orderBy = sqlParser_->parseOrderByExpr(key);
    auto expr = resolveScalarTypes(orderBy.expr);

    sortingFields.push_back(
        SortingField{expr, SortOrder(orderBy.ascending, orderBy.nullsFirst)});
  }

  node_ = std::make_shared<SortNode>(
      nextId(), std::move(node_), std::move(sortingFields));

  return *this;
}

PlanBuilder& PlanBuilder::sort(const std::vector<SortKey>& sortingKeys) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sort node cannot be a leaf node");

  std::vector<SortingField> sortingFields;
  sortingFields.reserve(sortingKeys.size());

  for (const auto& key : sortingKeys) {
    auto expr = resolveScalarTypes(key.expr.expr());

    sortingFields.push_back(
        SortingField{expr, SortOrder(key.ascending, key.nullsFirst)});
  }

  node_ = std::make_shared<SortNode>(
      nextId(), std::move(node_), std::move(sortingFields));

  return *this;
}

PlanBuilder& PlanBuilder::limit(int64_t offset, int64_t count) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Limit node cannot be a leaf node");

  node_ =
      std::make_shared<LimitNode>(nextId(), std::move(node_), offset, count);

  return *this;
}

PlanBuilder& PlanBuilder::offset(int64_t offset) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Offset node cannot be a leaf node");

  node_ = std::make_shared<LimitNode>(
      nextId(), std::move(node_), offset, std::numeric_limits<int64_t>::max());

  return *this;
}

PlanBuilder& PlanBuilder::tableWrite(
    std::string connectorId,
    std::string tableName,
    WriteKind kind,
    std::vector<std::string> columnNames,
    const std::vector<ExprApi>& columnExprs,
    folly::F14FastMap<std::string, std::string> options) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Table write node cannot be a leaf node");
  VELOX_USER_CHECK_GT(columnNames.size(), 0);
  VELOX_USER_CHECK_EQ(columnNames.size(), columnExprs.size());

  std::vector<ExprPtr> columnExpressions;
  columnExpressions.reserve(columnExprs.size());
  for (const auto& expr : columnExprs) {
    columnExpressions.push_back(resolveScalarTypes(expr.expr()));
  }

  if (kind == WriteKind::kInsert) {
    // Check input types.
    auto* metadata = connector::ConnectorMetadata::metadata(connectorId);
    auto table = metadata->findTable(tableName);
    VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);
    const auto& schema = table->type();

    for (auto i = 0; i < columnNames.size(); i++) {
      const auto& name = columnNames[i];
      const auto index = schema->getChildIdxIfExists(name);
      VELOX_USER_CHECK(
          index.has_value(),
          "Column not found: '{}' in table '{}'",
          name,
          tableName);

      const auto& inputType = columnExpressions[i]->type();
      const auto& schemaType = schema->childAt(index.value());

      if (!schemaType->equivalent(*inputType)) {
        if (enableCoercions_ &&
            velox::TypeCoercer::coercible(inputType, schemaType)) {
          columnExpressions[i] =
              applyCoercion(columnExpressions[i], schemaType);
        } else {
          VELOX_USER_FAIL(
              "Wrong column type: {} vs. {}, column '{}' in table '{}'",
              inputType->toString(),
              schemaType->toString(),
              name,
              tableName);
        }
      }
    }
  }

  node_ = std::make_shared<TableWriteNode>(
      nextId(),
      std::move(node_),
      std::move(connectorId),
      std::move(tableName),
      kind,
      std::move(columnNames),
      std::move(columnExpressions),
      std::move(options));

  return *this;
}

PlanBuilder& PlanBuilder::tableWrite(
    std::string tableName,
    WriteKind kind,
    std::vector<std::string> columnNames,
    folly::F14FastMap<std::string, std::string> options) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Table write node cannot be a leaf node");
  VELOX_USER_CHECK(defaultConnectorId_.has_value());

  return tableWrite(
      defaultConnectorId_.value(),
      std::move(tableName),
      kind,
      std::move(columnNames),
      findOrAssignOutputNames(),
      std::move(options));
}

PlanBuilder& PlanBuilder::sample(
    double percentage,
    SampleNode::SampleMethod sampleMethod) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sample node cannot be a leaf node");
  VELOX_USER_CHECK_GE(percentage, 0.0, "Sample percentage must be >= 0");
  VELOX_USER_CHECK_LE(percentage, 100.0, "Sample percentage must be <= 100");

  node_ = std::make_shared<SampleNode>(
      nextId(),
      std::move(node_),
      std::make_shared<ConstantExpr>(
          velox::DOUBLE(), std::make_shared<velox::Variant>(percentage)),
      sampleMethod);

  return *this;
}

PlanBuilder& PlanBuilder::sample(
    const ExprApi& percentage,
    SampleNode::SampleMethod sampleMethod) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Sample node cannot be a leaf node");

  auto expr = resolveScalarTypes(percentage.expr());

  if (!expr->type()->isDouble()) {
    if (enableCoercions_) {
      if (velox::TypeCoercer::coercible(expr->type(), velox::DOUBLE())) {
        expr = applyCoercion(expr, velox::DOUBLE());
      } else {
        VELOX_USER_FAIL(
            "Sample percentage must be coercible to double: {}",
            expr->toString());
      }
    } else {
      VELOX_USER_FAIL(
          "Sample percentage must be a double: {}", expr->toString());
    }
  }

  node_ = std::make_shared<SampleNode>(
      nextId(), std::move(node_), std::move(expr), sampleMethod);

  return *this;
}

ExprPtr PlanBuilder::resolveInputName(
    const std::optional<std::string>& alias,
    const std::string& name) const {
  if (outputMapping_ == nullptr) {
    VELOX_CHECK_NOT_NULL(outerScope_);
    return outerScope_(alias, name);
  }

  if (alias.has_value()) {
    if (auto id = outputMapping_->lookup(alias.value(), name)) {
      return std::make_shared<InputReferenceExpr>(
          node_->outputType()->findChild(id.value()), id.value());
    }

    if (outerScope_ != nullptr) {
      // TODO Figure out how to handle dereference.
      return outerScope_(alias, name);
    }

    return nullptr;
  }

  if (auto id = outputMapping_->lookup(name)) {
    return std::make_shared<InputReferenceExpr>(
        node_->outputType()->findChild(id.value()), id.value());
  }

  if (outerScope_ != nullptr) {
    return outerScope_(alias, name);
  }

  VELOX_USER_FAIL(
      "Cannot resolve column: {} not in [{}]",
      NameMappings::QualifiedName{alias, name}.toString(),
      outputMapping_->toString());
}

ExprPtr PlanBuilder::resolveScalarTypes(
    const velox::core::ExprPtr& expr) const {
  return resolver_.resolveScalarTypes(
      expr, [&](const auto& alias, const auto& name) {
        return resolveInputName(alias, name);
      });
}

AggregateExprPtr PlanBuilder::resolveAggregateTypes(
    const velox::core::ExprPtr& expr,
    const ExprPtr& filter,
    const std::vector<SortingField>& ordering,
    bool distinct) const {
  return resolver_.resolveAggregateTypes(
      expr,
      [&](const auto& alias, const auto& name) {
        return resolveInputName(alias, name);
      },
      filter,
      ordering,
      distinct);
}

PlanBuilder& PlanBuilder::as(const std::string& alias) {
  outputMapping_->setAlias(alias);
  return *this;
}

std::string PlanBuilder::newName(const std::string& hint) {
  return nameAllocator_->newName(hint);
}

size_t PlanBuilder::numOutput() const {
  VELOX_CHECK_NOT_NULL(node_);
  return node_->outputType()->size();
}

namespace {
std::optional<std::string> pickName(
    const std::vector<NameMappings::QualifiedName>& names) {
  if (names.empty()) {
    return std::nullopt;
  }

  // Prefer non-aliased name.
  for (const auto& name : names) {
    if (!name.alias.has_value()) {
      return name.name;
    }
  }

  return names.front().name;
}
} // namespace

std::vector<std::optional<std::string>> PlanBuilder::outputNames() const {
  auto size = numOutput();

  std::vector<std::optional<std::string>> names;
  names.reserve(size);

  for (auto i = 0; i < size; i++) {
    const auto id = node_->outputType()->nameOf(i);
    names.push_back(pickName(outputMapping_->reverseLookup(id)));
  }

  return names;
}

std::vector<velox::TypePtr> PlanBuilder::outputTypes() const {
  VELOX_CHECK_NOT_NULL(node_);
  return node_->outputType()->children();
}

std::vector<std::string> PlanBuilder::findOrAssignOutputNames(
    bool includeHiddenColumns,
    const std::optional<std::string>& alias) const {
  const auto size = numOutput();
  const auto& inputType = node_->outputType();

  std::vector<std::string> names;
  names.reserve(size);

  folly::F14FastSet<std::string> allowedIds;
  if (alias.has_value()) {
    allowedIds = outputMapping_->idsWithAlias(alias.value());
    VELOX_USER_CHECK(!allowedIds.empty(), "Alias not found: {}", alias.value());
  }

  for (auto i = 0; i < size; i++) {
    const auto& id = inputType->nameOf(i);

    if (!includeHiddenColumns && outputMapping_->isHidden(id)) {
      continue;
    }

    if (alias.has_value() && !allowedIds.contains(id)) {
      continue;
    }

    names.push_back(findOrAssignOutputNameAt(i));
  }

  return names;
}

std::string PlanBuilder::findOrAssignOutputNameAt(size_t index) const {
  const auto size = numOutput();
  VELOX_CHECK_LT(index, size, "{}", node_->outputType()->toString());

  const auto& id = node_->outputType()->nameOf(index);

  if (auto name = pickName(outputMapping_->reverseLookup(id))) {
    return name.value();
  }

  // Assign a name to the output column.
  outputMapping_->add(id, id);
  return id;
}

LogicalPlanNodePtr PlanBuilder::build(bool useIds) {
  VELOX_USER_CHECK_NOT_NULL(node_);
  VELOX_USER_CHECK_NOT_NULL(outputMapping_);

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
    const auto& type = rowType->childAt(i);

    if (useIds) {
      exprs.push_back(std::make_shared<InputReferenceExpr>(type, id));
    } else {
      auto it = names.find(id);
      if (it != names.end()) {
        outputNames.push_back(it->second);
      } else {
        outputNames.push_back(id);
      }

      if (id != outputNames.back()) {
        needRename = true;
      }

      exprs.push_back(std::make_shared<InputReferenceExpr>(type, id));
    }
  }

  if (needRename) {
    return std::make_shared<ProjectNode>(nextId(), node_, outputNames, exprs);
  }

  return node_;
}

} // namespace facebook::axiom::logical_plan
