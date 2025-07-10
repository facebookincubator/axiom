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

#include "optimizer/connectors/hive/HiveConnectorMetadata.h" //@manual
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::connector::hive {

namespace {
HiveColumnHandle::ColumnType columnType(
    const HiveTableLayout& layout,
    const std::string& columnName) {
  auto& columns = layout.hivePartitionColumns();
  for (auto& c : columns) {
    if (c->name() == columnName) {
      return HiveColumnHandle::ColumnType::kPartitionKey;
    }
  }
  // TODO recognize special names like $path, $bucket etc.
  return HiveColumnHandle::ColumnType::kRegular;
}
} // namespace

ColumnHandlePtr HiveConnectorMetadata::createColumnHandle(
    const TableLayout& layout,
    const std::string& columnName,
    std::vector<common::Subfield> subfields,
    std::optional<TypePtr> castToType,
    SubfieldMapping subfieldMapping) {
  // castToType and subfieldMapping are not yet supported.
  VELOX_CHECK(subfieldMapping.empty());
  auto* hiveLayout = reinterpret_cast<const HiveTableLayout*>(&layout);
  auto* column = hiveLayout->findColumn(columnName);
  auto handle = std::make_shared<HiveColumnHandle>(
      columnName,
      columnType(*hiveLayout, columnName),
      column->type(),
      column->type(),
      std::move(subfields));
  return std::dynamic_pointer_cast<const ColumnHandle>(handle);
}

ConnectorTableHandlePtr HiveConnectorMetadata::createTableHandle(
    const TableLayout& layout,
    std::vector<ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<core::TypedExprPtr> filters,
    std::vector<core::TypedExprPtr>& rejectedFilters,
    RowTypePtr dataColumns,
    std::optional<LookupKeys> lookupKeys) {
  VELOX_CHECK(!lookupKeys.has_value(), "Hive does not support lookup keys");
  auto* hiveLayout = dynamic_cast<const HiveTableLayout*>(&layout);

  std::vector<core::TypedExprPtr> remainingConjuncts;
  common::SubfieldFilters subfieldFilters;
  for (auto& typedExpr : filters) {
    try {
      auto pair = velox::exec::toSubfieldFilter(typedExpr, &evaluator);
      if (!pair.second) {
        remainingConjuncts.push_back(std::move(typedExpr));
        continue;
      }
      auto it = subfieldFilters.find(pair.first);
      if (it != subfieldFilters.end()) {
        auto merged = it->second->mergeWith(pair.second.get());
        subfieldFilters[std::move(pair.first)] = std::move(merged);
      } else {
        subfieldFilters[std::move(pair.first)] = std::move(pair.second);
      }
    } catch (const std::exception&) {
      remainingConjuncts.push_back(std::move(typedExpr));
    }
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
  return std::dynamic_pointer_cast<const ConnectorTableHandle>(
      std::make_shared<HiveTableHandle>(
          hiveConnector_->connectorId(),
          hiveLayout->table()->name(),
          true,
          std::move(subfieldFilters),
          remainingFilter,
          dataColumns ? dataColumns : layout.rowType()));
}

} // namespace facebook::velox::connector::hive
