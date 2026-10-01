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

#include "axiom/connectors/ConnectorEnvironment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/core/QueryCtx.h"

namespace facebook::axiom::connector {
namespace {

template <typename T>
std::shared_ptr<T> nonOwning(T& value) {
  return std::shared_ptr<T>{&value, [](T*) {}};
}

} // namespace

ConnectorEnvironment::ConnectorEnvironment(
    std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
        connectorRegistry,
    std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry,
    std::shared_ptr<const ConnectorEnvironment> parent,
    bool sealed,
    bool ownsRegistries)
    : parent_{std::move(parent)},
      connectorRegistry_{std::move(connectorRegistry)},
      metadataRegistry_{std::move(metadataRegistry)},
      sealed_{sealed},
      ownsRegistries_{ownsRegistries} {
  VELOX_CHECK_NOT_NULL(connectorRegistry_);
  VELOX_CHECK_NOT_NULL(metadataRegistry_);
}

ConnectorEnvironment::~ConnectorEnvironment() {
  if (ownsRegistries_) {
    metadataRegistry_->clear();
    connectorRegistry_->clear();
  }
}

std::shared_ptr<ConnectorEnvironment> ConnectorEnvironment::create() {
  return std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
      velox::connector::ConnectorRegistry::create(),
      ConnectorMetadataRegistry::create(),
      /*parent=*/nullptr,
      /*sealed=*/false,
      /*ownsRegistries=*/true}};
}

std::shared_ptr<ConnectorEnvironment> ConnectorEnvironment::createChild(
    std::shared_ptr<const ConnectorEnvironment> parent) {
  VELOX_CHECK_NOT_NULL(parent, "Connector environment parent is required");
  VELOX_CHECK(
      parent->ownsRegistries_,
      "Legacy global connector environment cannot be used as a parent");
  VELOX_CHECK(parent->sealed(), "Connector environment parent must be sealed");
  return std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
      velox::connector::ConnectorRegistry::create(
          parent->connectorRegistry_.get()),
      ConnectorMetadataRegistry::create(parent->metadataRegistry_.get()),
      std::move(parent),
      /*sealed=*/false,
      /*ownsRegistries=*/true}};
}

std::shared_ptr<ConnectorEnvironment> ConnectorEnvironment::global() {
  static const auto kGlobal =
      std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
          nonOwning(velox::connector::ConnectorRegistry::global()),
          nonOwning(ConnectorMetadataRegistry::global()),
          /*parent=*/nullptr,
          /*sealed=*/false,
          /*ownsRegistries=*/false}};
  return kGlobal;
}

void ConnectorEnvironment::registerConnector(
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK(!ownsRegistries_ || !sealed(), "Connector environment is sealed");
  VELOX_CHECK_NOT_NULL(connector);
  VELOX_CHECK_NOT_NULL(metadata);

  const auto connectorId = connector->connectorId();
  connectorRegistry_->insert(connectorId, connector);
  try {
    metadataRegistry_->insert(connectorId, std::move(metadata));
  } catch (...) {
    connectorRegistry_->erase(connectorId);
    throw;
  }
}

void ConnectorEnvironment::registerMetadata(
    std::string connectorId,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK(!ownsRegistries_ || !sealed(), "Connector environment is sealed");
  VELOX_CHECK_NOT_NULL(metadata);
  metadataRegistry_->insert(std::move(connectorId), std::move(metadata));
}

void ConnectorEnvironment::seal() {
  VELOX_CHECK(
      ownsRegistries_, "Legacy global connector environment is mutable");
  sealed_.store(true, std::memory_order_release);
}

std::shared_ptr<velox::connector::Connector> ConnectorEnvironment::connector(
    std::string_view connectorId) const {
  auto connector = connectorRegistry_->find(std::string{connectorId});
  VELOX_CHECK_NOT_NULL(
      connector, "Connector is not registered: {}", connectorId);
  return connector;
}

std::shared_ptr<ConnectorMetadata> ConnectorEnvironment::tryMetadata(
    std::string_view connectorId) const {
  return metadataRegistry_->find(std::string{connectorId});
}

std::shared_ptr<ConnectorMetadata> ConnectorEnvironment::metadata(
    std::string_view connectorId) const {
  auto metadata = tryMetadata(connectorId);
  VELOX_CHECK_NOT_NULL(
      metadata, "Connector metadata is not registered: {}", connectorId);
  return metadata;
}

std::vector<
    std::pair<std::string, std::shared_ptr<velox::connector::Connector>>>
ConnectorEnvironment::connectors() const {
  return connectorRegistry_->snapshot();
}

std::vector<std::string> ConnectorEnvironment::metadataIds() const {
  const auto entries = metadataRegistry_->snapshot();
  std::vector<std::string> ids;
  ids.reserve(entries.size());
  for (const auto& [id, _] : entries) {
    ids.push_back(id);
  }
  return ids;
}

void ConnectorEnvironment::attachTo(velox::core::QueryCtx& queryCtx) const {
  VELOX_CHECK(
      !ownsRegistries_ || sealed(),
      "Connector environment must be sealed before use");
  queryCtx.setRegistry(
      velox::connector::ConnectorRegistry::kRegistryKey, connectorRegistry_);
  queryCtx.setRegistry(
      ConnectorMetadataRegistry::kRegistryKey, metadataRegistry_);
  queryCtx.setRegistry(
      kRegistryKey,
      std::const_pointer_cast<ConnectorEnvironment>(shared_from_this()));
}

} // namespace facebook::axiom::connector
