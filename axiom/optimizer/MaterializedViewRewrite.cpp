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

#include "axiom/optimizer/MaterializedViewRewrite.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/MaterializedViewDefinition.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/ToGraph.h"

namespace facebook::axiom::optimizer {

namespace {

Value constantValue(const velox::TypePtr& type) {
  return Value{toType(type), 1};
}

/// Recursively replaces column references in 'expr'. If a column matches one
/// in 'source', it is replaced with the corresponding entry in 'target'.
ExprCP remapColumns(
    ExprCP expr,
    const ColumnVector& source,
    const ColumnVector& target) {
  if (!expr) {
    return nullptr;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr:
      for (size_t i = 0; i < source.size(); ++i) {
        if (source[i] == expr) {
          return target[i];
        }
      }
      return expr;
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr: {
      auto children = expr->children();
      ExprVector newChildren(children.size());
      bool anyChange = false;
      FunctionSet functions;
      for (size_t i = 0; i < children.size(); ++i) {
        newChildren[i] = remapColumns(children[i]->as<Expr>(), source, target);
        anyChange |= newChildren[i] != children[i];
        if (newChildren[i]->isFunction()) {
          functions = functions | newChildren[i]->as<Call>()->functions();
        }
      }
      if (!anyChange) {
        return expr;
      }
      const auto* call = expr->as<Call>();
      return make<Call>(
          call->name(), call->value(), std::move(newChildren), functions);
    }
    case PlanType::kFieldExpr: {
      auto* field = expr->as<Field>();
      auto* newBase = remapColumns(field->base(), source, target);
      if (newBase != field->base()) {
        return make<Field>(field->value().type, newBase, field->field());
      }
      return expr;
    }
    default:
      return expr;
  }
}

/// Rewrites all expression vectors in 'parent' that may reference columns
/// from an old table (source) to point at the replacement table's columns
/// (target). This covers exprs, conjuncts, having, and orderKeys.
void remapParentReferences(
    DerivedTable& parent,
    const ColumnVector& source,
    const ColumnVector& target) {
  for (auto*& expr : parent.exprs) {
    expr = remapColumns(expr, source, target);
  }
  for (auto*& expr : parent.conjuncts) {
    expr = remapColumns(expr, source, target);
  }
  for (auto*& expr : parent.having) {
    expr = remapColumns(expr, source, target);
  }
  for (auto*& expr : parent.orderKeys) {
    expr = remapColumns(expr, source, target);
  }
}

} // namespace

MaterializedViewRewrite::MaterializedViewRewrite(ToGraph& toGraph)
    : toGraph_(toGraph) {}

bool MaterializedViewRewrite::rewrite(DerivedTable& root) {
  bool changed = false;

  // Collect MV candidates to avoid mutating while iterating.
  std::vector<BaseTable*> mvCandidates;
  for (auto* table : root.tables) {
    if (table->is(PlanType::kTableNode)) {
      auto* baseTable = const_cast<BaseTable*>(table->as<BaseTable>());
      if (baseTable->schemaTable &&
          baseTable->schemaTable->connectorTable->materializedViewDefinition()
              .has_value()) {
        mvCandidates.push_back(baseTable);
      }
    } else if (table->is(PlanType::kDerivedTableNode)) {
      auto* dt = const_cast<DerivedTable*>(table->as<DerivedTable>());
      if (rewrite(*dt)) {
        changed = true;
      }
    }
  }

  for (auto* mvTable : mvCandidates) {
    if (rewriteBaseTable(root, mvTable)) {
      changed = true;
    }
  }

  // Recurse into children for set operations (UNION ALL, etc.).
  for (auto* child : root.children) {
    if (rewrite(*child)) {
      changed = true;
    }
  }

  return changed;
}

std::optional<std::string> MaterializedViewRewrite::getConnectorId(
    const BaseTable& mvTable) {
  const auto& layouts = mvTable.schemaTable->connectorTable->layouts();
  if (layouts.empty()) {
    return std::nullopt;
  }
  return std::string(layouts[0]->connectorId());
}

std::pair<BaseTable*, ColumnVector> MaterializedViewRewrite::makeBaseTableNode(
    SchemaTableCP baseSchemaTable,
    const BaseTable& mvTable,
    const folly::F14FastMap<std::string, std::string>& columnNameMap) {
  auto* baseBt = make<BaseTable>();
  baseBt->cname = toGraph_.newCName("base_");
  baseBt->schemaTable = baseSchemaTable;

  ColumnVector baseColumns;
  for (auto* mvCol : mvTable.columns) {
    std::string mvColName = mvCol->name();
    auto it = columnNameMap.find(mvColName);
    std::string baseColName =
        (it != columnNameMap.end()) ? it->second : mvColName;

    auto baseSchemaCol = baseSchemaTable->findColumn(toName(baseColName));
    Value colValue = baseSchemaCol ? baseSchemaCol->value() : mvCol->value();

    auto* baseCol = make<Column>(
        toName(baseColName), baseBt, colValue, nullptr, toName(baseColName));
    baseBt->columns.push_back(baseCol);
    baseColumns.push_back(baseCol);
  }

  return {baseBt, std::move(baseColumns)};
}

void MaterializedViewRewrite::translateFilters(
    const BaseTable& mvTable,
    BaseTable* baseBt,
    const folly::F14FastMap<std::string, std::string>& columnNameMap) {
  // Build a column mapping from MV columns to base table columns by
  // matching position (both are parallel to each other via the same
  // columnNameMap used in makeBaseTableNode).
  ColumnVector source;
  ColumnVector target;
  for (size_t i = 0; i < mvTable.columns.size(); ++i) {
    source.push_back(mvTable.columns[i]);
    target.push_back(baseBt->columns[i]);
  }

  for (auto* filterExpr : mvTable.columnFilters) {
    auto* remapped = remapColumns(filterExpr, source, target);
    baseBt->addFilter(remapped);
  }

  for (auto* filterExpr : mvTable.filter) {
    auto* remapped = remapColumns(filterExpr, source, target);
    baseBt->addFilter(remapped);
  }
}

bool MaterializedViewRewrite::replaceWithBaseTable(
    DerivedTable& parent,
    BaseTable* mvTable) {
  const auto& mvDef =
      *mvTable->schemaTable->connectorTable->materializedViewDefinition();

  if (mvDef.baseTables().size() != 1) {
    return false;
  }
  const auto& baseTableName = mvDef.baseTables()[0];

  auto connectorId = getConnectorId(*mvTable);
  if (!connectorId) {
    return false;
  }

  auto baseSchemaTable =
      toGraph_.schema().findTable(*connectorId, baseTableName);
  if (!baseSchemaTable) {
    return false;
  }

  auto columnNameMap = buildColumnNameMapping(mvDef, baseTableName);
  auto [baseBt, baseColumns] =
      makeBaseTableNode(baseSchemaTable, *mvTable, columnNameMap);

  // No need to translate filters here. The MV rewrite runs before
  // initializePlans(), so user filters are still in parent.conjuncts.
  // distributeConjuncts() will push them into baseDt naturally.

  // Wrap base table in a DerivedTable that maps base columns back to MV
  // column names, so the parent DT sees the same output schema.
  auto* baseDt = make<DerivedTable>();
  baseDt->cname = toGraph_.newCName("base_dt_");
  baseDt->addTable(baseBt);

  for (size_t i = 0; i < mvTable->columns.size(); ++i) {
    auto* mvCol = mvTable->columns[i];
    auto* outCol =
        make<Column>(mvCol->name(), baseDt, mvCol->value(), mvCol->alias());
    baseDt->columns.push_back(outCol);
    baseDt->exprs.push_back(baseColumns[i]);
  }

  // Replace mvTable with baseDt in the parent.
  for (size_t i = 0; i < parent.tables.size(); ++i) {
    if (parent.tables[i] == mvTable) {
      parent.tables[i] = baseDt;
      parent.tableSet.erase(mvTable);
      parent.tableSet.add(baseDt);
      break;
    }
  }
  std::replace(
      parent.joinOrder.begin(),
      parent.joinOrder.end(),
      mvTable->id(),
      baseDt->id());

  // Remap parent expressions from old mvTable columns to baseDt columns.
  ColumnVector oldCols(mvTable->columns.begin(), mvTable->columns.end());
  remapParentReferences(parent, oldCols, baseDt->columns);

  return true;
}

bool MaterializedViewRewrite::rewriteBaseTable(
    DerivedTable& parent,
    BaseTable* mvTable) {
  const auto& mvDef =
      *mvTable->schemaTable->connectorTable->materializedViewDefinition();

  // Phase 1: Only support single base table.
  if (mvDef.baseTables().size() != 1) {
    return false;
  }
  const auto& baseTableName = mvDef.baseTables()[0];

  // Must have at least one refresh column for range-based stitching.
  if (!mvDef.validRefreshColumns().has_value() ||
      mvDef.validRefreshColumns()->empty()) {
    return false;
  }
  const auto& refreshColumnName = mvDef.validRefreshColumns()->at(0);

  // Get the connector ID from the MV table's layout.
  auto connectorId = getConnectorId(*mvTable);
  if (!connectorId) {
    return false;
  }

  auto* connectorMetadata =
      connector::ConnectorMetadata::tryMetadata(*connectorId);
  if (!connectorMetadata) {
    return false;
  }

  auto statusOpt = connectorMetadata->getMaterializedViewStatus(
      *mvTable->schemaTable->connectorTable);
  if (!statusOpt.has_value()) {
    return false;
  }
  const auto& status = *statusOpt;

  switch (status.state) {
    case connector::MaterializedViewState::kFullyMaterialized:
      return false;
    case connector::MaterializedViewState::kNotMaterialized:
    case connector::MaterializedViewState::kTooManyPartitionsMissing:
      // MV has no usable data — replace with full base table scan.
      return replaceWithBaseTable(parent, mvTable);
    case connector::MaterializedViewState::kPartiallyMaterialized:
      break;
  }

  // Need boundary values in pairs.
  if (status.boundaryValues.empty() || status.boundaryValues.size() % 2 != 0) {
    return false;
  }

  // Find the refresh column on the MV BaseTable.
  ColumnCP mvRefreshColumn = nullptr;
  for (auto* col : mvTable->columns) {
    if (col->name() == toName(refreshColumnName)) {
      mvRefreshColumn = col;
      break;
    }
  }
  if (!mvRefreshColumn) {
    return false;
  }

  // Resolve the base table's SchemaTable.
  auto baseSchemaTable =
      toGraph_.schema().findTable(*connectorId, baseTableName);
  if (!baseSchemaTable) {
    return false;
  }

  auto columnNameMap = buildColumnNameMapping(mvDef, baseTableName);

  // --- Construct the UNION ALL DerivedTable ---

  // 1. MV child DerivedTable: wraps original mvTable with fresh-range filter.
  auto* mvDt = make<DerivedTable>();
  mvDt->cname = toGraph_.newCName("mv_");
  mvDt->addTable(mvTable);

  // Build MV range filter from boundary pairs.
  ExprVector mvRangeFilters;
  for (size_t i = 0; i + 1 < status.boundaryValues.size(); i += 2) {
    mvRangeFilters.push_back(makeRangeFilter(
        mvRefreshColumn,
        status.boundaryValues[i],
        status.boundaryValues[i + 1]));
  }

  // Combine MV range filters with OR if multiple pairs.
  ExprCP mvFilter = mvRangeFilters[0];
  for (size_t i = 1; i < mvRangeFilters.size(); ++i) {
    mvFilter = make<Call>(
        toName(SpecialFormCallNames::kOr),
        constantValue(velox::BOOLEAN()),
        ExprVector{mvFilter, mvRangeFilters[i]},
        FunctionSet());
  }
  // 2. Base table child DerivedTable.
  auto [baseBt, baseColumns] =
      makeBaseTableNode(baseSchemaTable, *mvTable, columnNameMap);

  // Add the range filter to mvTable.
  mvTable->addFilter(mvFilter);

  // Find the refresh column on the base table.
  auto refreshIt = columnNameMap.find(refreshColumnName);
  std::string baseRefreshColName = (refreshIt != columnNameMap.end())
      ? refreshIt->second
      : refreshColumnName;

  ColumnCP baseRefreshColumn = nullptr;
  for (auto* col : baseBt->columns) {
    if (col->name() == toName(baseRefreshColName)) {
      baseRefreshColumn = col;
      break;
    }
  }
  if (!baseRefreshColumn) {
    return false;
  }

  // Build complement filter for base table: everything outside fresh ranges.
  ExprVector baseComplementFilters;
  for (size_t i = 0; i + 1 < status.boundaryValues.size(); i += 2) {
    baseComplementFilters.push_back(makeComplementFilter(
        baseRefreshColumn,
        status.boundaryValues[i],
        status.boundaryValues[i + 1]));
  }

  // For single range: complement directly.
  // For multiple ranges: complement1 AND complement2 AND ...
  ExprCP baseFilter = baseComplementFilters[0];
  for (size_t i = 1; i < baseComplementFilters.size(); ++i) {
    baseFilter = make<Call>(
        toName(SpecialFormCallNames::kAnd),
        constantValue(velox::BOOLEAN()),
        ExprVector{baseFilter, baseComplementFilters[i]},
        FunctionSet());
  }
  baseBt->addFilter(baseFilter);

  auto* baseDt = make<DerivedTable>();
  baseDt->cname = toGraph_.newCName("base_dt_");
  baseDt->addTable(baseBt);

  // 3. Parent UNION ALL DerivedTable.
  auto* unionDt = make<DerivedTable>();
  unionDt->cname = toGraph_.newCName("mv_union_");
  unionDt->setOp = logical_plan::SetOperation::kUnionAll;
  unionDt->children.push_back(mvDt);
  unionDt->children.push_back(baseDt);

  // Create shared output columns owned by unionDt. All children share these
  // same column objects — this is how ToVelox maps set-operation outputs.
  for (auto* mvCol : mvTable->columns) {
    auto* outCol =
        make<Column>(mvCol->name(), unionDt, mvCol->value(), mvCol->alias());
    unionDt->columns.push_back(outCol);
  }

  // Children share parent's columns and use exprs to map inner → outer.
  mvDt->columns = unionDt->columns;
  for (auto* col : mvTable->columns) {
    mvDt->exprs.push_back(col);
  }

  baseDt->columns = unionDt->columns;
  for (size_t i = 0; i < baseColumns.size(); ++i) {
    baseDt->exprs.push_back(baseColumns[i]);
  }

  // 4. Replace mvTable with unionDt in the parent DerivedTable.
  for (size_t i = 0; i < parent.tables.size(); ++i) {
    if (parent.tables[i] == mvTable) {
      parent.tables[i] = unionDt;
      parent.tableSet.erase(mvTable);
      parent.tableSet.add(unionDt);
      break;
    }
  }
  std::replace(
      parent.joinOrder.begin(),
      parent.joinOrder.end(),
      mvTable->id(),
      unionDt->id());

  // 5. Remap parent expressions from old mvTable columns to unionDt columns.
  // The parent's exprs/conjuncts reference columns with relation()==mvTable,
  // which is no longer in parent.tables. Rewrite them to point at unionDt's
  // columns (same names/types, but relation()==unionDt).
  ColumnVector oldCols(mvTable->columns.begin(), mvTable->columns.end());
  remapParentReferences(parent, oldCols, unionDt->columns);

  return true;
}

folly::F14FastMap<std::string, std::string>
MaterializedViewRewrite::buildColumnNameMapping(
    const connector::MaterializedViewDefinition& mvDef,
    const connector::SchemaTableName& baseTableName) {
  folly::F14FastMap<std::string, std::string> mapping;

  for (const auto& cm : mvDef.columnMappings()) {
    for (const auto& baseCol : cm.baseTableColumns) {
      if (baseCol.tableName == baseTableName) {
        mapping[cm.viewColumn.columnName] = baseCol.columnName;
        break;
      }
    }
  }

  return mapping;
}

ExprCP MaterializedViewRewrite::makeRangeFilter(
    ColumnCP refreshColumn,
    const std::string& lo,
    const std::string& hi) {
  const auto& funcs = toGraph_.functionNames();

  auto* loLit = makeLiteral(lo, refreshColumn->value().type);
  auto* hiLit = makeLiteral(hi, refreshColumn->value().type);

  // refreshCol >= lo
  auto* gteExpr = make<Call>(
      funcs.gte,
      constantValue(velox::BOOLEAN()),
      ExprVector{refreshColumn, loLit},
      FunctionSet());

  // refreshCol <= hi
  auto* lteExpr = make<Call>(
      funcs.lte,
      constantValue(velox::BOOLEAN()),
      ExprVector{refreshColumn, hiLit},
      FunctionSet());

  // refreshCol >= lo AND refreshCol <= hi
  return make<Call>(
      toName(SpecialFormCallNames::kAnd),
      constantValue(velox::BOOLEAN()),
      ExprVector{gteExpr, lteExpr},
      FunctionSet());
}

ExprCP MaterializedViewRewrite::makeComplementFilter(
    ColumnCP refreshColumn,
    const std::string& lo,
    const std::string& hi) {
  const auto& funcs = toGraph_.functionNames();

  auto* loLit = makeLiteral(lo, refreshColumn->value().type);
  auto* hiLit = makeLiteral(hi, refreshColumn->value().type);

  // refreshCol < lo
  auto* ltExpr = make<Call>(
      funcs.lt,
      constantValue(velox::BOOLEAN()),
      ExprVector{refreshColumn, loLit},
      FunctionSet());

  // refreshCol > hi
  auto* gtExpr = make<Call>(
      funcs.gt,
      constantValue(velox::BOOLEAN()),
      ExprVector{refreshColumn, hiLit},
      FunctionSet());

  // refreshCol < lo OR refreshCol > hi
  return make<Call>(
      toName(SpecialFormCallNames::kOr),
      constantValue(velox::BOOLEAN()),
      ExprVector{ltExpr, gtExpr},
      FunctionSet());
}

ExprCP MaterializedViewRewrite::makeLiteral(
    const std::string& value,
    const velox::Type* type) {
  auto* variant = registerVariant(std::string(value));
  return make<Literal>(Value(type, 1), variant);
}

} // namespace facebook::axiom::optimizer
