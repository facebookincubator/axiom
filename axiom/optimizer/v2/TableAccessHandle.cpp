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

#include "axiom/optimizer/v2/TableAccessHandle.h"

#include <stddef.h>
#include <stdint.h>
#include <optional>
#include <string>
#include <utility>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/lang/Assume.h>

#include "axiom/connectors/ConnectorContext.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorSession.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/ToSubfield.h"
#include "axiom/optimizer/v2/ExprEmitter.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/Connector.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

TableAccessHandle buildTableAccess(
    const BaseTable& baseTable,
    const ColumnVector& outputColumns,
    const ExprVector& filters,
    const SubfieldsOf& subfieldsOf,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    ExprVector& rejected,
    std::optional<connector::LookupKeys> lookupKeys) {
  const auto* layout = baseTable.schemaTable->columnGroups[0]->layout;
  auto connectorSession = session.context()->sessionFor(layout->connectorId());

  // Read schema is the consumer output columns plus the columns referenced
  // only by filters.
  folly::F14FastSet<Name> seenSchemaNames;
  for (ColumnCP column : outputColumns) {
    seenSchemaNames.insert(column->schemaName());
  }
  ColumnVector filterOnlyColumns;
  PlanObjectSet filterColumns;
  filterColumns.unionColumns(filters);
  filterColumns.forEach<Column>([&](ColumnCP column) {
    if (seenSchemaNames.insert(column->schemaName()).second) {
      filterOnlyColumns.push_back(column);
    }
  });

  const size_t numColumns = outputColumns.size() + filterOnlyColumns.size();
  std::vector<velox::connector::ColumnHandlePtr> readSchema;
  readSchema.reserve(numColumns);
  const auto addHandle = [&](ColumnCP column) {
    auto handle = layout->createColumnHandle(
        connectorSession, column->schemaName(), subfieldsOf(column));
    readSchema.push_back(handle);
    return handle;
  };

  folly::F14FastMap<ColumnCP, velox::connector::ColumnHandlePtr> columnHandles;
  columnHandles.reserve(outputColumns.size());
  for (ColumnCP column : outputColumns) {
    columnHandles.emplace(column, addHandle(column));
  }
  // Kept aside: a filter-only column belongs in the returned map only if the
  // connector rejects the conjunct that reads it, which puts the column in the
  // scan's output. That is not known until the handle is built.
  std::vector<std::pair<ColumnCP, velox::connector::ColumnHandlePtr>>
      filterOnlyHandles;
  filterOnlyHandles.reserve(filterOnlyColumns.size());
  for (ColumnCP column : filterOnlyColumns) {
    filterOnlyHandles.emplace_back(column, addHandle(column));
  }

  // createTableHandle reports the rejected filters by index into the vector it
  // is given, so the TypedExprs stay aligned with 'filters'.
  ExprEmitter exprEmitter{evaluator.pool()};
  std::vector<velox::core::TypedExprPtr> typedFilters;
  typedFilters.reserve(filters.size());
  for (ExprCP filter : filters) {
    typedFilters.push_back(
        exprEmitter.toTypedExpr(filter, ColumnNaming::kSchemaName));
  }
  std::vector<int32_t> rejectedFilterIndices;
  velox::connector::ConnectorTableHandlePtr tableHandle =
      layout->createTableHandle(
          connectorSession,
          std::move(readSchema),
          evaluator,
          std::move(typedFilters),
          rejectedFilterIndices,
          /*dataColumns=*/nullptr,
          std::move(lookupKeys));

  // Each rejected index selects a conjunct the caller must apply above the
  // scan; the rest are the connector's responsibility, inside 'tableHandle'.
  for (int32_t index : rejectedFilterIndices) {
    VELOX_CHECK_GE(
        index,
        0,
        "createTableHandle returned a negative rejected filter index");
    VELOX_CHECK_LT(
        index,
        static_cast<int32_t>(filters.size()),
        "createTableHandle returned an out-of-range rejected filter index");
    rejected.push_back(filters[index]);
  }

  PlanObjectSet rejectedColumns;
  rejectedColumns.unionColumns(rejected);
  for (auto& [column, handle] : filterOnlyHandles) {
    if (rejectedColumns.contains(column)) {
      columnHandles.emplace(column, std::move(handle));
    }
  }

  return TableAccessHandle{
      .tableHandle = std::move(tableHandle),
      .columnHandles = std::move(columnHandles),
  };
}

} // namespace

TableAccessHandle TableAccessHandle::buildScan(
    const BaseTable& baseTable,
    const ColumnVector& outputColumns,
    const ExprVector& filters,
    const SubfieldsOf& subfieldsOf,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    ExprVector& rejected) {
  return buildTableAccess(
      baseTable,
      outputColumns,
      filters,
      subfieldsOf,
      session,
      evaluator,
      rejected,
      std::nullopt);
}

TableAccessHandle TableAccessHandle::buildIndexLookup(
    const BaseTable& baseTable,
    const ColumnVector& outputColumns,
    const ExprVector& filters,
    const ColumnVector& lookupKeys,
    const SubfieldsOf& subfieldsOf,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    ExprVector& rejected) {
  connector::LookupKeys connectorKeys;
  connectorKeys.equalityColumns.reserve(lookupKeys.size());
  for (ColumnCP key : lookupKeys) {
    connectorKeys.equalityColumns.emplace_back(key->schemaName());
  }
  return buildTableAccess(
      baseTable,
      outputColumns,
      filters,
      subfieldsOf,
      session,
      evaluator,
      rejected,
      std::move(connectorKeys));
}

} // namespace facebook::axiom::optimizer::v2
