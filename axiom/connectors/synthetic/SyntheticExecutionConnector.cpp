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

#include "axiom/connectors/synthetic/SyntheticExecutionConnector.h"

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom::connector::synthetic {

std::unique_ptr<velox::connector::DataSource>
SyntheticExecutionConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  const auto* handle =
      dynamic_cast<const SyntheticTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(
      handle,
      "Synthetic execution connector expects a SyntheticTableHandle, got: {}",
      tableHandle->name());

  auto metadata = ConnectorMetadataRegistry::get(handle->sourceCatalog());

  auto provider = metadata->findSyntheticProvider(handle->schemaTableName());
  VELOX_CHECK_NOT_NULL(
      provider, "No synthetic provider for routed table: {}", handle->name());

  return std::make_unique<SyntheticDataSource>(
      outputType,
      columnHandles,
      std::move(provider),
      handle->acceptedFilters(),
      connectorQueryCtx->memoryPool());
}

TablePtr findSyntheticTable(
    const SchemaTableName& name,
    ConnectorMetadata* metadata,
    std::string_view catalog) {
  auto provider = metadata->findSyntheticProvider(name);
  if (provider == nullptr) {
    return nullptr;
  }
  auto connector = velox::connector::ConnectorRegistry::tryGet(
      std::string(kSyntheticConnectorId));
  VELOX_CHECK_NOT_NULL(
      connector,
      "Synthetic execution connector is not registered; call "
      "registerSyntheticExecutionConnector() first");
  return std::make_shared<SyntheticTable>(
      name, std::move(provider), connector.get(), std::string(catalog));
}

namespace {
bool& registeredFlag() {
  static bool registered{false};
  return registered;
}
} // namespace

void registerSyntheticExecutionConnector() {
  if (registeredFlag()) {
    return;
  }
  registerSyntheticSerDe();

  const std::string id{kSyntheticConnectorId};
  velox::connector::ConnectorRegistry::global().insert(
      id, std::make_shared<SyntheticExecutionConnector>(id));
  ConnectorMetadataRegistry::global().insert(
      id, std::make_shared<SyntheticExecutionMetadata>());
  registeredFlag() = true;
}

void unregisterSyntheticExecutionConnector() {
  if (!registeredFlag()) {
    return;
  }
  const std::string id{kSyntheticConnectorId};
  ConnectorMetadataRegistry::global().erase(id);
  velox::connector::ConnectorRegistry::global().erase(id);
  registeredFlag() = false;
}

} // namespace facebook::axiom::connector::synthetic
