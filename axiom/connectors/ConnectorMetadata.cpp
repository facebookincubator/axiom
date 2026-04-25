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

#include "axiom/connectors/ConnectorMetadataRegistry.h"

namespace facebook::axiom::connector {
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
