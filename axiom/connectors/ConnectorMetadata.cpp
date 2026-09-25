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

#include "axiom/connectors/ConnectorMetadata.h"

#include <cmath>

#include "axiom/connectors/ConnectorMetadataRegistry.h"

namespace facebook::axiom::connector {

void applyFilterEstimates(
    const folly::F14FastMap<std::string, const velox::common::Filter*>&
        commonFilters,
    const velox::core::TypedExprPtr& remainingFilter,
    const std::vector<const Column*>& columns,
    const FilterSelectivityEstimator& estimator,
    FilteredTableStats& stats) {
  if (stats.columnStats.empty()) {
    return;
  }

  folly::F14FastMap<std::string, const velox::Type*> typeByName;
  typeByName.reserve(columns.size());
  for (const auto* column : columns) {
    typeByName.emplace(column->name(), column->type().get());
  }

  folly::F14FastMap<std::string, TypedColumnStatistics> columnStatsByName;
  folly::F14FastMap<std::string, size_t> indexByName;
  for (size_t i = 0; i < stats.columnStats.size(); ++i) {
    const auto& columnStats = stats.columnStats[i];
    auto typeIt = typeByName.find(columnStats.name);
    VELOX_CHECK(
        typeIt != typeByName.end(),
        "Missing type for statistics column {}",
        columnStats.name);
    columnStatsByName.emplace(
        columnStats.name, TypedColumnStatistics{typeIt->second, columnStats});
    indexByName[columnStats.name] = i;
  }

  double selectivity = 1.0;
  const auto fold = [&](const FilterEstimate& estimate) {
    selectivity *= estimate.selectivity;
    for (const auto& [name, refined] : estimate.columnStats) {
      auto it = indexByName.find(name);
      if (it == indexByName.end()) {
        continue;
      }
      // Overwrite the refined fields, preserving name/numValues/etc.
      auto& existing = stats.columnStats[it->second];
      existing.numDistinct = refined.numDistinct;
      existing.min = refined.min;
      existing.max = refined.max;
      existing.nullPct = refined.nullPct;
      existing.nonNull = refined.nonNull;
    }
  };

  if (!commonFilters.empty()) {
    fold(estimator.estimate(commonFilters, columnStatsByName));
  }
  if (remainingFilter != nullptr) {
    std::vector<velox::core::TypedExprPtr> remaining{remainingFilter};
    fold(estimator.estimate(remaining, columnStatsByName));
  }

  stats.numRows =
      static_cast<uint64_t>(std::llround(stats.numRows * selectivity));
}

namespace {

const auto& writeKindNames() {
  static const folly::F14FastMap<WriteKind, std::string_view> kNames = {
      {WriteKind::kCreate, "CREATE"},
      {WriteKind::kInsert, "INSERT"},
      {WriteKind::kUpdate, "UPDATE"},
      {WriteKind::kDelete, "DELETE"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

DeleteInput::DeleteInput(
    std::vector<Column> shuffleKeys,
    std::vector<std::string> sortKeys,
    std::vector<SortOrder> sortOrders)
    : shuffleKeys_{std::move(shuffleKeys)},
      sortKeys_{std::move(sortKeys)},
      sortOrders_{std::move(sortOrders)} {
  VELOX_CHECK_EQ(
      sortKeys_.size(),
      sortOrders_.size(),
      "Delete sort keys and sort orders must be one-to-one");
  for (const auto& key : shuffleKeys_) {
    VELOX_CHECK(!key.name.empty(), "Delete shuffle key requires a name");
    VELOX_CHECK_NOT_NULL(
        key.expr, "Delete shuffle key requires an expression: {}", key.name);
  }
}

void MetadataCountGroup::checkConsistency() const {
  VELOX_CHECK_GE(numRows, 0, "Row count must be non-negative");
  for (const auto nulls : numNulls) {
    VELOX_CHECK_GE(nulls, 0, "Null count must be non-negative");
    VELOX_CHECK_LE(nulls, numRows, "Null count must not exceed row count");
  }
}

void Procedure::checkConsistency() const {
  folly::F14FastSet<std::string> names;
  bool seenOptional = false;
  for (const auto& parameter : parameters) {
    VELOX_CHECK(!parameter.name.empty(), "Procedure parameter name is empty");
    VELOX_CHECK(
        names.insert(parameter.name).second,
        "Duplicate procedure parameter name: {}",
        parameter.name);
    if (parameter.defaultValue.has_value()) {
      seenOptional = true;
    } else {
      VELOX_CHECK(
          !seenOptional,
          "Required procedure parameter must not follow an optional one: {}",
          parameter.name);
    }
  }
}

namespace {

// @return RowType that represents a subset of 'columns' which are not hidden.
velox::RowTypePtr toSchema(
    const std::vector<std::unique_ptr<const Column>>& columns) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (const auto& column : columns) {
    if (column->hidden()) {
      continue;
    }
    names.push_back(column->name());
    types.push_back(column->type());
  }
  return velox::ROW(std::move(names), std::move(types));
}

std::vector<const Column*> toPtrs(
    const std::vector<std::unique_ptr<const Column>>& columns) {
  std::vector<const Column*> ptrs;
  ptrs.reserve(columns.size());
  for (auto& column : columns) {
    ptrs.push_back(column.get());
  }
  return ptrs;
}

folly::F14FastMap<std::string, const Column*> toMap(
    const std::vector<std::unique_ptr<const Column>>& columns) {
  folly::F14FastMap<std::string, const Column*> map;
  for (auto& column : columns) {
    VELOX_CHECK(!column->name().empty(), "Column name cannot be empty");
    auto inserted = map.emplace(column->name(), column.get()).second;
    VELOX_CHECK(inserted, "Duplicate column: {}", column->name());
  }
  return map;
}
} // namespace

Table::Table(
    SchemaTableName name,
    std::vector<std::unique_ptr<const Column>> columns,
    folly::F14FastMap<std::string, velox::Variant> options)
    : name_(std::move(name)),
      type_(toSchema(columns)),
      columns_(std::move(columns)),
      columnPtrs_(toPtrs(columns_)),
      columnMap_(toMap(columns_)),
      options_(std::move(options)) {
  VELOX_CHECK(!name_.schema.empty());
  VELOX_CHECK(!name_.table.empty());
  VELOX_CHECK_NOT_NULL(type_);
}

// static
std::vector<std::unique_ptr<const Column>> Table::makeColumns(
    const velox::RowTypePtr& rowType) {
  std::vector<std::unique_ptr<const Column>> columns;
  columns.reserve(rowType->size());
  for (auto i = 0; i < rowType->size(); ++i) {
    columns.push_back(
        std::make_unique<const Column>(
            rowType->nameOf(i), rowType->childAt(i), /*hidden=*/false));
  }
  return columns;
}

namespace {
velox::RowTypePtr makeRowType(const std::vector<const Column*>& columns) {
  folly::F14FastSet<std::string> uniqueNames;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;

  names.reserve(columns.size());
  types.reserve(columns.size());

  for (auto* column : columns) {
    VELOX_CHECK_NOT_NULL(column);
    if (column->hidden()) {
      continue;
    }

    const auto& name = column->name();

    VELOX_CHECK(!name.empty());
    VELOX_CHECK(uniqueNames.insert(name).second);

    names.emplace_back(name);
    types.emplace_back(column->type());
  }

  return ROW(std::move(names), std::move(types));
}
} // namespace

TableLayout::TableLayout(
    std::string label,
    const Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::vector<const Column*> partitionColumns,
    std::vector<const Column*> orderColumns,
    std::vector<SortOrder> sortOrder,
    std::vector<const Column*> lookupKeys,
    bool supportsScan)
    : label_(std::move(label)),
      table_(table),
      connector_(connector),
      columns_(std::move(columns)),
      partitionColumns_(std::move(partitionColumns)),
      orderColumns_(std::move(orderColumns)),
      sortOrder_(std::move(sortOrder)),
      lookupKeys_(std::move(lookupKeys)),
      supportsScan_(supportsScan),
      rowType_{makeRowType(columns_)} {
  VELOX_CHECK_NOT_NULL(table);
  VELOX_CHECK_NOT_NULL(connector);

  for (auto column : partitionColumns_) {
    VELOX_CHECK_NOT_NULL(column);
  }

  for (auto column : orderColumns_) {
    VELOX_CHECK_NOT_NULL(column);
  }

  VELOX_CHECK_EQ(orderColumns_.size(), sortOrder_.size());

  for (auto column : lookupKeys_) {
    VELOX_CHECK_NOT_NULL(column);
  }
}

const Column* FOLLY_NULLABLE
TableLayout::findColumn(std::string_view name) const {
  for (const auto& column : columns_) {
    if (column->name() == name) {
      return column;
    }
  }
  return nullptr;
}

std::vector<std::string> Table::ioColumnPriority() const {
  std::vector<std::string> priority;
  for (const auto* column : allColumns()) {
    if (column->includeInExplainIo()) {
      priority.push_back(column->name());
    }
  }
  return priority;
}

// static
ConnectorMetadata* FOLLY_NULLABLE
ConnectorMetadata::tryMetadata(std::string_view connectorId) {
  auto metadata =
      ConnectorMetadataRegistry::global().find(std::string(connectorId));
  return metadata == nullptr ? nullptr : metadata.get();
}

// static
ConnectorMetadata* ConnectorMetadata::metadata(std::string_view connectorId) {
  auto* result = tryMetadata(connectorId);
  VELOX_CHECK_NOT_NULL(
      result, "Connector metadata is not registered: {}", connectorId);
  return result;
}

// static
void ConnectorMetadata::registerMetadata(
    std::string_view connectorId,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK_NOT_NULL(metadata);
  VELOX_CHECK(!connectorId.empty());
  if (!ConnectorMetadataRegistry::global().find(std::string(connectorId))) {
    ConnectorMetadataRegistry::global().insert(
        std::string(connectorId), std::move(metadata));
  }
}

// static
void ConnectorMetadata::unregisterMetadata(std::string_view connectorId) {
  ConnectorMetadataRegistry::global().erase(std::string(connectorId));
}

// static
void ConnectorMetadata::unregisterAllMetadata() {
  ConnectorMetadataRegistry::global().clear();
}

// static
std::vector<std::string> ConnectorMetadata::allMetadataIds() {
  auto entries = ConnectorMetadataRegistry::global().snapshot();
  std::vector<std::string> ids;
  ids.reserve(entries.size());
  for (auto&& [id, _] : entries) {
    ids.emplace_back(id);
  }
  return ids;
}

} // namespace facebook::axiom::connector
