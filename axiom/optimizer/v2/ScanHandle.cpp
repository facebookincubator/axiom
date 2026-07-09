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

#include "axiom/optimizer/v2/ScanHandle.h"

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/ExprEmitter.h"

namespace facebook::axiom::optimizer::v2 {

ScanHandle ScanHandle::build(
    const Scan& scan,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  const auto* baseTable = scan.baseTable();
  const auto* layout = baseTable->schemaTable->columnGroups[0]->layout;
  auto connectorSession =
      session.toConnectorSession(layout->connector()->connectorId());

  // Read schema is the consumer output columns plus the columns referenced
  // only by filters; the latter keep their schema names.
  folly::F14FastSet<Name> seenSchemaNames;
  const ColumnVector& consumerColumns = scan.outputColumns();
  for (ColumnCP column : consumerColumns) {
    seenSchemaNames.insert(column->name());
  }
  ColumnVector filterOnlyColumns;
  PlanObjectSet filterColumns;
  filterColumns.unionColumns(scan.filters());
  filterColumns.forEach<Column>([&](ColumnCP column) {
    if (seenSchemaNames.insert(column->name()).second) {
      filterOnlyColumns.push_back(column);
    }
  });

  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  columnHandles.reserve(consumerColumns.size() + filterOnlyColumns.size());
  for (ColumnCP column : consumerColumns) {
    columnHandles.push_back(layout->createColumnHandle(
        connectorSession, column->name(), /*subfields=*/{}));
  }
  for (ColumnCP column : filterOnlyColumns) {
    columnHandles.push_back(layout->createColumnHandle(
        connectorSession, column->name(), /*subfields=*/{}));
  }

  ExprEmitter exprEmitter{evaluator.pool()};
  std::vector<velox::core::TypedExprPtr> filters;
  filters.reserve(scan.filters().size());
  for (ExprCP filter : scan.filters()) {
    filters.push_back(
        exprEmitter.toTypedExpr(filter, ColumnNaming::kSchemaName));
  }
  std::vector<velox::core::TypedExprPtr> filterConjuncts = filters;

  std::vector<velox::core::TypedExprPtr> rejectedFilters;
  velox::connector::ConnectorTableHandlePtr tableHandle =
      layout->createTableHandle(
          connectorSession,
          columnHandles,
          evaluator,
          std::move(filters),
          rejectedFilters);

  return ScanHandle{
      .tableHandle = std::move(tableHandle),
      .columnHandles = std::move(columnHandles),
      .filterOnlyColumns = std::move(filterOnlyColumns),
      .rejectedFilters = std::move(rejectedFilters),
      .filterConjuncts = std::move(filterConjuncts),
  };
}

const ScanHandle& ScanHandleCache::getOrBuild(
    const Scan& scan,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator) {
  const int32_t id = scan.baseTable()->id();
  auto it = byBaseTableId_.find(id);
  if (it != byBaseTableId_.end()) {
    return it->second;
  }
  return byBaseTableId_.emplace(id, ScanHandle::build(scan, session, evaluator))
      .first->second;
}

const ScanHandle* ScanHandleCache::find(const Scan& scan) const {
  auto it = byBaseTableId_.find(scan.baseTable()->id());
  return it != byBaseTableId_.end() ? &it->second : nullptr;
}

} // namespace facebook::axiom::optimizer::v2
