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

namespace facebook::axiom::connector {

namespace {
const auto& tableKindNames() {
  static const folly::F14FastMap<TableKind, std::string_view> kNames = {
      {TableKind::kTable, "TABLE"},
      {TableKind::kTempTable, "TEMP_TABLE"},
  };
  return kNames;
}

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

AXIOM_DEFINE_ENUM_NAME(TableKind, tableKindNames);

AXIOM_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

namespace {
velox::RowTypePtr makeRowType(const std::vector<const Column*>& columns) {
  folly::F14FastSet<std::string> uniqueNames;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;

  names.reserve(columns.size());
  types.reserve(columns.size());

  for (auto* column : columns) {
    VELOX_CHECK_NOT_NULL(column);

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
    std::string name,
    const Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::vector<const Column*> partitionColumns,
    std::vector<const Column*> orderColumns,
    std::vector<SortOrder> sortOrder,
    std::vector<const Column*> lookupKeys,
    bool supportsScan)
    : name_(std::move(name)),
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

const Column* TableLayout::findColumn(std::string_view name) const {
  for (const auto& column : columns_) {
    if (column->name() == name) {
      return column;
    }
  }
  return nullptr;
}

namespace {

folly::F14FastMap<std::string, std::shared_ptr<ConnectorMetadata>>&
metadataRegistry() {
  static folly::F14FastMap<std::string, std::shared_ptr<ConnectorMetadata>>
      kRegistry;
  return kRegistry;
}
} // namespace

// static
ConnectorMetadata* ConnectorMetadata::tryMetadata(
    std::string_view connectorId) {
  auto it = metadataRegistry().find(connectorId);
  if (it != metadataRegistry().end()) {
    return it->second.get();
  }

  return nullptr;
}

// static
ConnectorMetadata* ConnectorMetadata::metadata(std::string_view connectorId) {
  auto* metadata = tryMetadata(connectorId);
  VELOX_CHECK_NOT_NULL(
      metadata, "Connector metadata is not registered: {}", connectorId);
  return metadata;
}

// static
ConnectorMetadata* ConnectorMetadata::metadata(
    velox::connector::Connector* connector) {
  return ConnectorMetadata::metadata(connector->connectorId());
}

// static
void ConnectorMetadata::registerMetadata(
    std::string_view connectorId,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK_NOT_NULL(metadata);
  VELOX_CHECK(!connectorId.empty());
  metadataRegistry().emplace(connectorId, std::move(metadata));
}

// static
void ConnectorMetadata::unregisterMetadata(std::string_view connectorId) {
  metadataRegistry().erase(connectorId);
}

} // namespace facebook::axiom::connector
