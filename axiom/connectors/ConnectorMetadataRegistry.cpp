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

#include "axiom/connectors/ConnectorMetadataRegistry.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/core/QueryCtx.h"

namespace facebook::axiom::connector {

using Registry = ConnectorMetadataRegistry::Registry;
using QueryCtx = velox::core::QueryCtx;

namespace {

Registry& registryFor(const QueryCtx& queryCtx) {
  auto registry =
      queryCtx.registry<Registry>(ConnectorMetadataRegistry::kRegistryKey);
  return registry ? *registry : ConnectorMetadataRegistry::global();
}

} // namespace

// static
Registry& ConnectorMetadataRegistry::global() {
  static Registry kMetadataRegistry;
  return kMetadataRegistry;
}

// static
std::shared_ptr<ConnectorMetadataRegistry::Registry>
ConnectorMetadataRegistry::create(const Registry* parent) {
  return std::make_shared<Registry>(parent);
}

// static
std::shared_ptr<ConnectorMetadata> FOLLY_NULLABLE
ConnectorMetadataRegistry::tryGet(
    const QueryCtx& queryCtx,
    const std::string& connectorId) {
  return registryFor(queryCtx).find(std::string(connectorId));
}

// static
std::shared_ptr<ConnectorMetadata> FOLLY_NULLABLE
ConnectorMetadataRegistry::tryGet(const std::string& connectorId) {
  return global().find(std::string(connectorId));
}

// static
std::shared_ptr<ConnectorMetadata> FOLLY_NULLABLE
ConnectorMetadataRegistry::get(
    const QueryCtx& queryCtx,
    const std::string& connectorId) {
  auto metadata = tryGet(queryCtx, connectorId);
  VELOX_CHECK_NOT_NULL(
      metadata, "ConnectorMetadata not registered: {}", connectorId);
  return metadata;
}

// static
std::shared_ptr<ConnectorMetadata> FOLLY_NULLABLE
ConnectorMetadataRegistry::get(const std::string& connectorId) {
  auto metadata = tryGet(connectorId);
  VELOX_CHECK_NOT_NULL(
      metadata, "ConnectorMetadata not registered: {}", connectorId);
  return metadata;
}

namespace {

std::vector<std::string> metadataIds(const Registry& registry) {
  auto entries = registry.snapshot();
  auto ids = std::vector<std::string>{};
  ids.reserve(entries.size());
  for (auto&& [id, _] : entries) {
    ids.emplace_back(id);
  }
  return ids;
}

} // namespace

// static
std::vector<std::string> ConnectorMetadataRegistry::allMetadataIds(
    const QueryCtx& queryCtx) {
  return metadataIds(registryFor(queryCtx));
}

// static
std::vector<std::string> ConnectorMetadataRegistry::allMetadataIds() {
  return metadataIds(global());
}

// static
void ConnectorMetadataRegistry::unregisterAll(const QueryCtx& queryCtx) {
  auto registry =
      queryCtx.registry<Registry>(ConnectorMetadataRegistry::kRegistryKey);
  if (registry) {
    registry->clear();
  }
}

// static
void ConnectorMetadataRegistry::unregisterAll() {
  global().clear();
}

// static
std::vector<std::pair<std::string, std::shared_ptr<ConnectorMetadata>>>
ConnectorMetadataRegistry::snapshot(const QueryCtx& queryCtx) {
  return registryFor(queryCtx).snapshot();
}

} // namespace facebook::axiom::connector
