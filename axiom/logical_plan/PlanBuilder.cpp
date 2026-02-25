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
#include <numeric>
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

ExprPtr makeInputRef(const velox::TypePtr& type, const std::string& name) {
  return std::make_shared<InputReferenceExpr>(type, name);
}

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

    newOutputMapping->copyUserName(id, *outputMapping_);

    exprs.push_back(makeInputRef(inputType->childAt(i), id));
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

namespace {

WindowExpr::BoundType toBoundType(velox::parse::BoundType type) {
  switch (type) {
    case velox::parse::BoundType::kCurrentRow:
      return WindowExpr::BoundType::kCurrentRow;
    case velox::parse::BoundType::kUnboundedPreceding:
      return WindowExpr::BoundType::kUnboundedPreceding;
    case velox::parse::BoundType::kUnboundedFollowing:
      return WindowExpr::BoundType::kUnboundedFollowing;
    case velox::parse::BoundType::kPreceding:
      return WindowExpr::BoundType::kPreceding;
    case velox::parse::BoundType::kFollowing:
      return WindowExpr::BoundType::kFollowing;
  }
  VELOX_UNREACHABLE();
}

WindowSpec toWindowSpec(const velox::parse::WindowExpr& parsed) {
  WindowSpec spec;

  std::vector<ExprApi> partitionKeys;
  partitionKeys.reserve(parsed.partitionBy.size());
  for (const auto& key : parsed.partitionBy) {
    partitionKeys.emplace_back(key);
  }
  if (!partitionKeys.empty()) {
    spec.partitionBy(std::move(partitionKeys));
  }

  std::vector<SortKey> orderByKeys;
  orderByKeys.reserve(parsed.orderBy.size());
  for (const auto& ob : parsed.orderBy) {
    orderByKeys.emplace_back(ExprApi(ob.expr), ob.ascending, ob.nullsFirst);
  }
  if (!orderByKeys.empty()) {
    spec.orderBy(std::move(orderByKeys));
  }

  std::optional<ExprApi> startValue;
  if (parsed.frame.startValue) {
    startValue = ExprApi(parsed.frame.startValue);
  }
  std::optional<ExprApi> endValue;
  if (parsed.frame.endValue) {
    endValue = ExprApi(parsed.frame.endValue);
  }

  switch (parsed.frame.type) {
    case velox::parse::WindowType::kRows:
      spec.rows(
          toBoundType(parsed.frame.startType),
          std::move(startValue),
          toBoundType(parsed.frame.endType),
          std::move(endValue));
      break;
    case velox::parse::WindowType::kRange:
      spec.range(
          toBoundType(parsed.frame.startType),
          std::move(startValue),
          toBoundType(parsed.frame.endType),
          std::move(endValue));
      break;
  }

  if (parsed.ignoreNulls) {
    spec.ignoreNulls();
  }

  return spec;
}

} // namespace

std::vector<ExprApi> PlanBuilder::parse(const std::vector<std::string>& exprs) {
  std::vector<ExprApi> untypedExprs;
  untypedExprs.reserve(exprs.size());
  for (const auto& sql : exprs) {
    auto result = sqlParser_->parseScalarOrWindowExpr(sql);
    if (auto* windowExpr = std::get_if<velox::parse::WindowExpr>(&result)) {
      auto callExpr = ExprApi(windowExpr->functionCall);
      auto spec = toWindowSpec(*windowExpr);
      untypedExprs.push_back(callExpr.over(spec));
    } else {
      untypedExprs.emplace_back(std::get<velox::core::ExprPtr>(result));
    }
  }

  return untypedExprs;
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

// Tracks duplicate names and adds mappings accordingly.
// When allowDuplicates is true, names that appear multiple times are skipped
// to avoid ambiguous lookups.
class NameTracker {
 public:
  NameTracker(bool allowDuplicates, NameMappings& mappings)
      : allowDuplicates_{allowDuplicates}, mappings_{mappings} {}

  void track(const std::vector<ExprApi>& expressions) {
    for (const auto& expr : expressions) {
      track(expr.name().value_or(""));
    }
  }

  void track(const std::vector<std::string>& names) {
    for (const auto& name : names) {
      track(name);
    }
  }

  // Tracks a single name for duplicate detection without adding mappings.
  void track(const std::string& name) {
    if (allowDuplicates_ && !name.empty() && !seen_.insert(name).second) {
      duplicates_.insert(name);
    }
  }

  // Adds a mapping from unqualified 'name' to 'id'. Skips duplicates if
  // allowDuplicates is set.
  void add(const std::string& name, const std::string& id) {
    if (isDuplicate(name)) {
      return;
    }
    mappings_.add(name, id);
  }

  // Adds a mapping from qualified 'name' (with optional alias) to 'id'. Skips
  // duplicates if allowDuplicates is set.
  void add(const NameMappings::QualifiedName& name, const std::string& id) {
    if (isDuplicate(name.name)) {
      return;
    }
    mappings_.add(name, id);
  }

  // Adds mappings from both unqualified 'name' and qualified 'alias.name' to
  // 'id'. Skips duplicates if allowDuplicates is set.
  void add(
      const std::string& name,
      const std::string& id,
      const std::optional<std::string>& alias) {
    if (isDuplicate(name)) {
      return;
    }
    addWithAlias(name, id, alias);
  }

  // Adds a mapping from 'alias' to 'id'. If the alias is a duplicate, skips
  // the mapping but stores a user name so the OutputNode can map the ID back
  // to the original alias. If the ID differs from the alias (e.g. due to
  // deduplication), also stores a user name.
  void addNamed(const std::string& alias, const std::string& id) {
    if (isDuplicate(alias)) {
      mappings_.addUserName(id, alias);
    } else {
      add(alias, id);
      if (id != alias) {
        mappings_.addUserName(id, alias);
      }
    }
  }

 private:
  // Returns true if 'name' is a duplicate and should be skipped.
  bool isDuplicate(const std::string& name) const {
    return allowDuplicates_ && duplicates_.contains(name);
  }

  // Adds mappings from both unqualified 'name' and qualified 'alias.name' to
  // 'id'. Does not check for duplicates.
  void addWithAlias(
      const std::string& name,
      const std::string& id,
      const std::optional<std::string>& alias) {
    mappings_.add(name, id);
    if (alias.has_value()) {
      mappings_.add(
          NameMappings::QualifiedName{.alias = alias, .name = name}, id);
    }
  }

  bool allowDuplicates_;
  NameMappings& mappings_;
  std::unordered_set<std::string> seen_;
  std::unordered_set<std::string> duplicates_;
};

} // namespace

void PlanBuilder::resolveProjections(
    const std::vector<ExprApi>& projections,
    std::vector<std::string>& outputNames,
    std::vector<ExprPtr>& exprs,
    NameMappings& mappings) {
  std::unordered_set<std::string> seenColumnIds;
  NameTracker nameTracker(allowAmbiguousOutputNames_, mappings);

  // Track all projection names for duplicate detection. For identity
  // projections without explicit aliases (e.g. Col("x") from SELECT *),
  // track the resolved column name so collisions with explicit aliases
  // (e.g. 'foo' as x) are detected.
  for (const auto& projection : projections) {
    const auto& alias = projection.name();
    if (alias.has_value()) {
      nameTracker.track(alias.value());
    } else if (
        allowAmbiguousOutputNames_ &&
        projection.expr()->is(velox::core::IExpr::Kind::kFieldAccess)) {
      const auto& fieldName =
          projection.expr()->as<velox::core::FieldAccessExpr>()->name();
      nameTracker.track(fieldName);
    }
  }

  // Default name hint for unnamed expressions.
  static const std::string kExprHint = "expr";

  for (const auto& projection : projections) {
    ExprPtr expr;
    if (projection.windowSpec() != nullptr) {
      expr = resolveWindowTypes(projection.expr(), *projection.windowSpec());
    } else {
      expr = resolveScalarTypes(projection.expr());
    }

    const auto& alias = projection.name();

    // Empty alias: generate a unique physical name using column name or "expr"
    // as hint.
    if (alias.has_value() && alias.value().empty()) {
      VELOX_USER_CHECK(
          allowAmbiguousOutputNames_, "Empty column alias is not allowed");
      auto hint = expr->isInputReference()
          ? expr->as<InputReferenceExpr>()->name()
          : kExprHint;
      outputNames.push_back(newName(hint));
      mappings.addUserName(outputNames.back(), "");
      exprs.push_back(std::move(expr));
      continue;
    }

    if (expr->isInputReference() &&
        (!alias.has_value() ||
         alias.value() == expr->as<InputReferenceExpr>()->name())) {
      // Identity projection without rename.
      const auto& id = expr->as<InputReferenceExpr>()->name();
      if (seenColumnIds.emplace(id).second) {
        outputNames.push_back(id);
        for (const auto& name : outputMapping_->reverseLookup(id)) {
          nameTracker.add(name, outputNames.back());
        }
        mappings.copyUserName(id, *outputMapping_);
      } else {
        outputNames.push_back(newName(id));
        // Store user name for the duplicate identity so the OutputNode
        // can map the disambiguated physical name back to the original.
        if (auto userName = outputMapping_->userName(id)) {
          mappings.addUserName(outputNames.back(), *userName);
        } else if (auto name = pickName(outputMapping_->reverseLookup(id))) {
          mappings.addUserName(outputNames.back(), *name);
        }
      }
    } else if (alias.has_value()) {
      outputNames.push_back(newName(alias.value()));
      nameTracker.addNamed(alias.value(), outputNames.back());
    } else {
      outputNames.push_back(newName(kExprHint));
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

    newOutputMapping->copyUserName(id, *outputMapping_);

    exprs.push_back(makeInputRef(inputType->childAt(i), id));
  }

  resolveProjections(projections, outputNames, exprs, *newOutputMapping);

  node_ = std::make_shared<ProjectNode>(
      nextId(), std::move(node_), std::move(outputNames), std::move(exprs));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

namespace {

// Parses SQL aggregate expressions into ExprApi objects and their options
// (FILTER, ORDER BY, DISTINCT).
void parseAggregates(
    velox::parse::SqlExpressionsParser& parser,
    const std::vector<std::string>& aggregates,
    std::vector<ExprApi>& parsedAggregates,
    std::vector<PlanBuilder::AggregateOptions>& options) {
  parsedAggregates.reserve(aggregates.size());
  options.reserve(aggregates.size());

  for (const auto& sql : aggregates) {
    auto parsed = parser.parseAggregateExpr(sql);

    parsedAggregates.emplace_back(parsed.expr);

    std::vector<SortKey> sortingKeys;
    sortingKeys.reserve(parsed.orderBy.size());
    for (const auto& orderBy : parsed.orderBy) {
      sortingKeys.emplace_back(
          SortKey{
              ExprApi(orderBy.expr), orderBy.ascending, orderBy.nullsFirst});
    }

    options.emplace_back(
        std::move(parsed.filter), std::move(sortingKeys), parsed.distinct);
  }
}

} // namespace

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates) {
  std::vector<ExprApi> parsedAggregates;
  std::vector<AggregateOptions> options;
  parseAggregates(*sqlParser_, aggregates, parsedAggregates, options);

  return aggregate(parse(groupingKeys), parsedAggregates, options);
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

  resolveAggregates(aggregates, options, outputNames, exprs, *newOutputMapping);

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

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::vector<ExprApi>>& groupingSets,
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options,
    const std::string& groupingSetIndexName) {
  // Extract unique grouping keys from all sets and build index mapping.
  std::vector<ExprApi> groupingKeys;
  velox::core::ExprMap<int32_t> exprToIndex;

  auto getOrAddKey = [&](const ExprApi& expr) -> int32_t {
    int32_t idx = static_cast<int32_t>(groupingKeys.size());
    auto [it, inserted] = exprToIndex.try_emplace(expr.expr(), idx);
    if (inserted) {
      groupingKeys.push_back(expr);
    }
    return it->second;
  };

  // Build index-based grouping sets.
  std::vector<std::vector<int32_t>> groupingSetsIndices;
  groupingSetsIndices.reserve(groupingSets.size());
  for (const auto& groupingSet : groupingSets) {
    std::vector<int32_t> indices;
    indices.reserve(groupingSet.size());
    for (const auto& expr : groupingSet) {
      indices.push_back(getOrAddKey(expr));
    }
    groupingSetsIndices.push_back(std::move(indices));
  }

  return aggregate(
      groupingKeys,
      groupingSetsIndices,
      aggregates,
      options,
      groupingSetIndexName);
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::vector<std::string>>& groupingSets,
    const std::vector<std::string>& aggregates,
    const std::string& groupingSetIndexName) {
  std::vector<std::vector<ExprApi>> exprGroupingSets;
  exprGroupingSets.reserve(groupingSets.size());
  for (const auto& groupingSet : groupingSets) {
    exprGroupingSets.push_back(parse(groupingSet));
  }

  std::vector<ExprApi> parsedAggregates;
  std::vector<AggregateOptions> options;
  parseAggregates(*sqlParser_, aggregates, parsedAggregates, options);

  return aggregate(
      exprGroupingSets, parsedAggregates, options, groupingSetIndexName);
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::vector<int32_t>>& groupingSets,
    const std::vector<std::string>& aggregates,
    const std::vector<AggregateOptions>& options,
    const std::string& groupingSetIndexName) {
  return aggregate(
      parse(groupingKeys),
      groupingSets,
      parse(aggregates),
      options,
      groupingSetIndexName);
}

PlanBuilder& PlanBuilder::aggregate(
    const std::vector<ExprApi>& groupingKeys,
    const std::vector<std::vector<int32_t>>& groupingSets,
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options,
    const std::string& groupingSetIndexName) {
  VELOX_USER_CHECK_NOT_NULL(node_, "Aggregate node cannot be a leaf node");

  const auto numKeys = static_cast<int32_t>(groupingKeys.size());
  for (const auto& groupingSet : groupingSets) {
    for (const auto index : groupingSet) {
      VELOX_USER_CHECK_LT(
          index, numKeys, "Grouping set index {} is out of bounds", index);
    }
  }

  velox::core::ExprMap<bool> seen;
  for (const auto& key : groupingKeys) {
    VELOX_USER_CHECK(
        seen.try_emplace(key.expr(), true).second, "Duplicate grouping key");
  }

  std::vector<std::string> outputNames;
  outputNames.reserve(groupingKeys.size() + aggregates.size() + 1);

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(groupingKeys.size());

  auto newOutputMapping = std::make_shared<NameMappings>();

  resolveProjections(groupingKeys, outputNames, keyExprs, *newOutputMapping);

  std::vector<AggregateExprPtr> exprs;
  exprs.reserve(aggregates.size());

  resolveAggregates(aggregates, options, outputNames, exprs, *newOutputMapping);

  outputNames.push_back(newName(groupingSetIndexName));
  newOutputMapping->add(groupingSetIndexName, outputNames.back());

  node_ = std::make_shared<AggregateNode>(
      nextId(),
      std::move(node_),
      std::move(keyExprs),
      groupingSets,
      std::move(exprs),
      std::move(outputNames));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);

  return *this;
}

void PlanBuilder::resolveAggregates(
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options,
    std::vector<std::string>& outputNames,
    std::vector<AggregateExprPtr>& exprs,
    NameMappings& mappings) {
  if (!options.empty()) {
    VELOX_USER_CHECK_EQ(options.size(), aggregates.size());
  }

  NameTracker tracker(allowAmbiguousOutputNames_, mappings);
  tracker.track(aggregates);

  for (size_t i = 0; i < aggregates.size(); ++i) {
    const auto& aggregate = aggregates[i];

    velox::core::ExprPtr filter;
    std::vector<SortKey> sortKeys;
    bool distinct = false;

    if (!options.empty()) {
      filter = options[i].filter;
      sortKeys = options[i].orderBy;
      distinct = options[i].distinct;
    }

    auto expr =
        resolveAggregateTypes(aggregate.expr(), filter, sortKeys, distinct);

    if (aggregate.name().has_value()) {
      const auto& alias = aggregate.name().value();
      outputNames.push_back(newName(alias));
      tracker.add(alias, outputNames.back());
    } else {
      outputNames.push_back(newName(expr->name()));
    }

    exprs.emplace_back(std::move(expr));
  }
}

PlanBuilder& PlanBuilder::rollup(
    const std::vector<ExprApi>& groupingKeys,
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options,
    const std::string& groupingSetIndexName) {
  // ROLLUP(a, b, c) -> [[0,1,2], [0,1], [0], []]
  std::vector<std::vector<int32_t>> groupingSets;
  groupingSets.reserve(groupingKeys.size() + 1);

  for (size_t i = groupingKeys.size(); i > 0; --i) {
    std::vector<int32_t> indices(i);
    std::iota(indices.begin(), indices.end(), 0);
    groupingSets.push_back(std::move(indices));
  }
  groupingSets.emplace_back();

  return aggregate(
      groupingKeys, groupingSets, aggregates, options, groupingSetIndexName);
}

PlanBuilder& PlanBuilder::rollup(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& groupingSetIndexName) {
  return rollup(
      parse(groupingKeys), parse(aggregates), {}, groupingSetIndexName);
}

PlanBuilder& PlanBuilder::cube(
    const std::vector<ExprApi>& groupingKeys,
    const std::vector<ExprApi>& aggregates,
    const std::vector<AggregateOptions>& options,
    const std::string& groupingSetIndexName) {
  // CUBE(a, b) -> [[0,1], [0], [1], []]
  const size_t n = groupingKeys.size();

  const size_t numSets = 1ULL << n;
  std::vector<std::vector<int32_t>> groupingSets;
  groupingSets.reserve(numSets);

  for (size_t mask = numSets; mask > 0; --mask) {
    size_t bits = mask - 1;
    std::vector<int32_t> indices;
    for (size_t i = 0; i < n; ++i) {
      if (bits & (1ULL << (n - 1 - i))) {
        indices.push_back(static_cast<int32_t>(i));
      }
    }
    groupingSets.push_back(std::move(indices));
  }

  return aggregate(
      groupingKeys, groupingSets, aggregates, options, groupingSetIndexName);
}

PlanBuilder& PlanBuilder::cube(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& groupingSetIndexName) {
  return cube(parse(groupingKeys), parse(aggregates), {}, groupingSetIndexName);
}

PlanBuilder& PlanBuilder::distinct() {
  VELOX_USER_CHECK_NOT_NULL(
      node_, "Distinct aggregation node cannot be a leaf node");

  const auto& inputType = node_->outputType();

  std::vector<ExprPtr> keyExprs;
  keyExprs.reserve(inputType->size());
  for (auto i = 0; i < inputType->size(); i++) {
    keyExprs.push_back(
        makeInputRef(inputType->childAt(i), inputType->nameOf(i)));
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
    const std::optional<ExprApi>& ordinality,
    const std::optional<std::string>& alias,
    const std::vector<std::string>& unnestAliases) {
  if (!node_) {
    values(velox::ROW({}), {velox::Variant::row({})});
  }

  static const std::string kElementHint = "e";
  static const std::string kKeyHint = "k";
  static const std::string kValueHint = "v";

  // Create new mappings for unnested columns, then merge with existing.
  NameMappings unnestMapping;
  NameTracker tracker(allowAmbiguousOutputNames_, unnestMapping);

  if (unnestAliases.empty()) {
    for (const auto& unnestExpr : unnestExprs) {
      tracker.track(unnestExpr.unnestedAliases());
    }
  } else {
    tracker.track(unnestAliases);
  }

  if (ordinality.has_value() && ordinality->alias().has_value()) {
    tracker.track(*ordinality->alias());
  }

  size_t index = 0;

  std::vector<ExprPtr> exprs;
  std::vector<std::vector<std::string>> outputNames;

  auto addUnnestOutput = [&](const std::string& name, const std::string& hint) {
    if (name.empty()) {
      VELOX_USER_CHECK(
          allowAmbiguousOutputNames_, "Empty column alias is not allowed");
      outputNames.back().emplace_back(newName(hint));
      unnestMapping.addUserName(outputNames.back().back(), "");
    } else {
      outputNames.back().emplace_back(newName(name));
      tracker.add(name, outputNames.back().back(), alias);
    }
    ++index;
  };

  for (const auto& unnestExpr : unnestExprs) {
    auto expr = resolveScalarTypes(unnestExpr.expr());
    exprs.push_back(expr);
    outputNames.emplace_back();

    // Per-expression aliases (unnestedAliases) take priority over per-relation
    // aliases (unnestAliases). When neither is provided, generate default
    // names.
    const auto& aliases = unnestExpr.unnestedAliases();

    switch (expr->type()->kind()) {
      case velox::TypeKind::ARRAY:
        if (!aliases.empty()) {
          VELOX_USER_CHECK_EQ(aliases.size(), 1);
          addUnnestOutput(aliases[0], kElementHint);
        } else if (!unnestAliases.empty()) {
          VELOX_USER_CHECK_LT(index, unnestAliases.size());
          addUnnestOutput(unnestAliases[index], kElementHint);
        } else {
          outputNames.back().emplace_back(newName(kElementHint));
        }
        break;

      case velox::TypeKind::MAP:
        if (!aliases.empty()) {
          VELOX_USER_CHECK_EQ(aliases.size(), 2);
          addUnnestOutput(aliases[0], kKeyHint);
          addUnnestOutput(aliases[1], kValueHint);
        } else if (!unnestAliases.empty()) {
          VELOX_USER_CHECK_LT(index, unnestAliases.size());
          addUnnestOutput(unnestAliases[index], kKeyHint);
          addUnnestOutput(unnestAliases[index], kValueHint);
        } else {
          outputNames.back().emplace_back(newName(kKeyHint));
          outputNames.back().emplace_back(newName(kValueHint));
        }
        break;

      default:
        VELOX_USER_FAIL(
            "Unsupported type to unnest: {}", expr->type()->toString());
    }
  }

  std::optional<std::string> ordinalityName;
  if (ordinality.has_value()) {
    const auto& ordinalityAlias = ordinality->alias();
    if (ordinalityAlias.has_value() && !ordinalityAlias->empty()) {
      ordinalityName = newName(*ordinalityAlias);
      tracker.add(*ordinalityAlias, *ordinalityName, alias);
    } else if (ordinalityAlias.has_value()) {
      VELOX_USER_CHECK(
          allowAmbiguousOutputNames_, "Empty column alias is not allowed");
      ordinalityName = newName("ordinality");
      unnestMapping.addUserName(*ordinalityName, "");
    } else {
      ordinalityName = newName("ordinality");
    }
  }

  node_ = std::make_shared<UnnestNode>(
      nextId(),
      std::move(node_),
      std::move(exprs),
      std::move(outputNames),
      std::move(ordinalityName),
      /*flattenArrayOfRows=*/false);

  outputMapping_->merge(unnestMapping);

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
      return makeInputRef(inputRowType->findChild(id.value()), id.value());
    }

    return nullptr;
  }

  if (auto id = mapping.lookup(name)) {
    return makeInputRef(inputRowType->findChild(id.value()), id.value());
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

PlanBuilder& PlanBuilder::joinUsing(
    const PlanBuilder& right,
    const std::vector<std::string>& columns,
    JoinType joinType) {
  VELOX_USER_CHECK(!columns.empty(), "USING columns cannot be empty");
  VELOX_USER_CHECK_NOT_NULL(node_, "Join node cannot be a leaf node");
  VELOX_USER_CHECK_NOT_NULL(right.node_);

  // Find internal IDs for each USING column from both sides before merge.
  std::vector<UsingColumn> usingColumns;
  usingColumns.reserve(columns.size());
  for (const auto& column : columns) {
    auto leftId = outputMapping_->lookup(column);
    VELOX_USER_CHECK(
        leftId.has_value(),
        "USING column not found on the left side of the join: {}",
        column);
    auto rightId = right.outputMapping_->lookup(column);
    VELOX_USER_CHECK(
        rightId.has_value(),
        "USING column not found on the right side of the join: {}",
        column);
    usingColumns.push_back({column, leftId.value(), rightId.value()});
  }

  // Build equi-join condition.
  auto leftType = node_->outputType();
  auto rightType = right.node_->outputType();

  std::vector<ExprPtr> eqExprs;
  eqExprs.reserve(usingColumns.size());
  for (const auto& column : usingColumns) {
    auto leftColumnType = leftType->findChild(column.leftId);
    auto rightColumnType = rightType->findChild(column.rightId);
    VELOX_USER_CHECK(
        leftColumnType->equivalent(*rightColumnType),
        "USING column has different types on left and right sides of the join: {} ({} vs {})",
        column.name,
        leftColumnType->toString(),
        rightColumnType->toString());
    auto leftRef = makeInputRef(leftColumnType, column.leftId);
    auto rightRef = makeInputRef(rightColumnType, column.rightId);
    eqExprs.push_back(
        std::make_shared<CallExpr>(
            velox::BOOLEAN(), "eq", std::vector<ExprPtr>{leftRef, rightRef}));
  }

  ExprPtr condition;
  if (eqExprs.size() == 1) {
    condition = std::move(eqExprs[0]);
  } else {
    condition = std::make_shared<SpecialFormExpr>(
        velox::BOOLEAN(), SpecialForm::kAnd, std::move(eqExprs));
  }

  // Merge mappings and create JoinNode.
  outputMapping_->merge(*right.outputMapping_);

  node_ = std::make_shared<JoinNode>(
      nextId(), std::move(node_), right.node_, joinType, std::move(condition));

  // Add projection to deduplicate USING columns.
  addJoinUsingProjection(usingColumns, joinType);

  return *this;
}

void PlanBuilder::addJoinUsingProjection(
    const std::vector<UsingColumn>& usingColumns,
    JoinType joinType) {
  auto joinOutputType = node_->outputType();

  std::vector<std::string> outputNames;
  std::vector<ExprPtr> exprs;
  auto newOutputMapping = std::make_shared<NameMappings>();

  // Emit USING columns first (one copy each).
  for (const auto& column : usingColumns) {
    const auto& type = joinOutputType->findChild(column.leftId);
    if (joinType == JoinType::kFull) {
      // Coalesce left and right for FULL OUTER joins.
      auto leftRef = makeInputRef(type, column.leftId);
      auto rightRef = makeInputRef(type, column.rightId);
      exprs.push_back(
          std::make_shared<SpecialFormExpr>(
              type,
              SpecialForm::kCoalesce,
              std::vector<ExprPtr>{leftRef, rightRef}));
      outputNames.push_back(newName(column.name));
    } else if (joinType == JoinType::kRight) {
      // Use the right side's column for RIGHT joins.
      exprs.push_back(makeInputRef(type, column.rightId));
      outputNames.push_back(column.rightId);
    } else {
      // Pass through the left side's column for INNER/LEFT joins.
      exprs.push_back(makeInputRef(type, column.leftId));
      outputNames.push_back(column.leftId);
    }

    newOutputMapping->add(column.name, outputNames.back());
  }

  // Emit all non-USING columns from both sides in order.
  for (size_t i = 0; i < joinOutputType->size(); i++) {
    const auto& id = joinOutputType->nameOf(i);

    if (UsingColumn::containsId(usingColumns, id)) {
      continue;
    }

    if (outputMapping_->isHidden(id)) {
      continue;
    }

    outputNames.push_back(id);
    exprs.push_back(makeInputRef(joinOutputType->childAt(i), id));

    for (const auto& qualifiedName : outputMapping_->reverseLookup(id)) {
      newOutputMapping->add(qualifiedName, id);
    }

    newOutputMapping->copyUserName(id, *outputMapping_);
  }

  node_ = std::make_shared<ProjectNode>(
      nextId(), std::move(node_), std::move(outputNames), std::move(exprs));

  newOutputMapping->enableUnqualifiedAccess();
  outputMapping_ = std::move(newOutputMapping);
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

          auto inputRef = makeInputRef(inputType, name);

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

bool PlanBuilder::hasColumn(const std::string& name) const {
  return outputMapping_ != nullptr && outputMapping_->lookup(name).has_value();
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
      return makeInputRef(
          node_->outputType()->findChild(id.value()), id.value());
    }

    if (outerScope_ != nullptr) {
      // TODO Figure out how to handle dereference.
      return outerScope_(alias, name);
    }

    return nullptr;
  }

  if (auto id = outputMapping_->lookup(name)) {
    return makeInputRef(node_->outputType()->findChild(id.value()), id.value());
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
    const velox::core::ExprPtr& filter,
    const std::vector<SortKey>& ordering,
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

WindowExprPtr PlanBuilder::resolveWindowTypes(
    const velox::core::ExprPtr& expr,
    const WindowSpec& windowSpec) const {
  return resolver_.resolveWindowTypes(
      expr, windowSpec, [&](const auto& alias, const auto& name) {
        return resolveInputName(alias, name);
      });
}

PlanBuilder& PlanBuilder::as(const std::string& alias) {
  outputMapping_->setAlias(alias);
  return *this;
}

PlanBuilder& PlanBuilder::setTableScanAlias(const std::string& alias) {
  VELOX_USER_CHECK_NOT_NULL(node_);

  if (node_->is(NodeKind::kTableScan)) {
    const auto* scan = node_->as<TableScanNode>();
    node_ = std::make_shared<TableScanNode>(
        scan->id(),
        scan->outputType(),
        scan->connectorId(),
        scan->tableName(),
        scan->columnNames(),
        alias);
  } else if (node_->is(NodeKind::kProject)) {
    // When the SQL has both column aliases and a table alias, e.g.
    // `FROM orders AS o (col1, col2)`, the parser first creates a ProjectNode
    // for the column renames, then calls setTableScanAlias for the table alias.
    // Look through the ProjectNode to set the alias on the underlying scan.
    const auto& input = node_->onlyInput();
    if (input->is(NodeKind::kTableScan)) {
      const auto* scan = input->as<TableScanNode>();
      auto newScan = std::make_shared<TableScanNode>(
          scan->id(),
          scan->outputType(),
          scan->connectorId(),
          scan->tableName(),
          scan->columnNames(),
          alias);

      const auto* project = node_->as<ProjectNode>();
      node_ = std::make_shared<ProjectNode>(
          project->id(),
          std::move(newScan),
          project->names(),
          project->expressions());
    }
  }

  return *this;
}

std::string PlanBuilder::newName(const std::string& hint) {
  return nameAllocator_->newName(hint);
}

size_t PlanBuilder::numOutput() const {
  VELOX_CHECK_NOT_NULL(node_);
  return node_->outputType()->size();
}

std::vector<std::optional<std::string>> PlanBuilder::outputNames(
    bool includeHiddenColumns) const {
  const auto size = numOutput();
  const auto& inputType = node_->outputType();

  std::vector<std::optional<std::string>> names;
  names.reserve(size);

  for (auto i = 0; i < size; i++) {
    const auto& id = inputType->nameOf(i);

    if (!includeHiddenColumns && outputMapping_->isHidden(id)) {
      continue;
    }

    if (auto userName = outputMapping_->userName(id)) {
      names.push_back(*userName);
    } else {
      names.push_back(pickName(outputMapping_->reverseLookup(id)));
    }
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

LogicalPlanNodePtr PlanBuilder::buildOutputNode() {
  auto defaultNames = findOrAssignOutputNames(/*includeHiddenColumns=*/true);
  const auto numColumns = defaultNames.size();

  std::vector<OutputNode::Entry> entries;
  entries.reserve(numColumns);

  for (size_t i = 0; i < numColumns; ++i) {
    std::string name;
    if (auto userName =
            outputMapping_->userName(node_->outputType()->nameOf(i))) {
      name = *userName;
    } else {
      name = std::move(defaultNames[i]);
    }
    entries.emplace_back(
        OutputNode::Entry{static_cast<int32_t>(i), std::move(name)});
  }

  return std::make_shared<OutputNode>(nextId(), node_, std::move(entries));
}

LogicalPlanNodePtr PlanBuilder::buildRenameProject() {
  const auto names = outputMapping_->uniqueNames();

  const auto& rowType = node_->outputType();

  std::vector<std::string> outputNames;
  outputNames.reserve(rowType->size());

  std::vector<ExprPtr> exprs;
  exprs.reserve(rowType->size());

  bool needRename = false;
  for (auto i = 0; i < rowType->size(); i++) {
    const auto& id = rowType->nameOf(i);
    const auto& type = rowType->childAt(i);

    auto it = names.find(id);
    if (it != names.end()) {
      outputNames.push_back(it->second);
    } else {
      outputNames.push_back(id);
    }

    if (id != outputNames.back()) {
      needRename = true;
    }

    exprs.push_back(makeInputRef(type, id));
  }

  if (needRename) {
    return std::make_shared<ProjectNode>(
        nextId(), node_, std::move(outputNames), std::move(exprs));
  }

  return node_;
}

LogicalPlanNodePtr PlanBuilder::build() {
  VELOX_USER_CHECK_NOT_NULL(node_);
  VELOX_CHECK_NOT_NULL(outputMapping_);

  if (allowAmbiguousOutputNames_ && !node_->is(NodeKind::kTableWrite)) {
    return buildOutputNode();
  }

  return buildRenameProject();
}

} // namespace facebook::axiom::logical_plan
