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
    bool mutableEnvironment,
    bool ownsRegistries)
    : parent_{std::move(parent)},
      connectorRegistry_{std::move(connectorRegistry)},
      metadataRegistry_{std::move(metadataRegistry)},
      mutableEnvironment_{mutableEnvironment},
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

std::shared_ptr<ConnectorEnvironment::Builder>
ConnectorEnvironment::Builder::create() {
  auto environment =
      std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
          velox::connector::ConnectorRegistry::create(),
          ConnectorMetadataRegistry::create(),
          /*parent=*/nullptr,
          /*mutableEnvironment=*/true,
          /*ownsRegistries=*/true}};
  return std::shared_ptr<Builder>{new Builder{std::move(environment)}};
}

std::shared_ptr<ConnectorEnvironment::Builder>
ConnectorEnvironment::Builder::createChild(
    std::shared_ptr<const ConnectorEnvironment> parent) {
  VELOX_CHECK_NOT_NULL(parent, "Connector environment parent is required");
  VELOX_CHECK(
      parent->ownsRegistries_,
      "Process-wide global connector environment cannot be used as a parent");
  VELOX_CHECK(
      !parent->mutableEnvironment_,
      "Connector environment parent must be built");
  auto environment =
      std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
          velox::connector::ConnectorRegistry::create(
              parent->connectorRegistry_.get()),
          ConnectorMetadataRegistry::create(parent->metadataRegistry_.get()),
          std::move(parent),
          /*mutableEnvironment=*/true,
          /*ownsRegistries=*/true}};
  return std::shared_ptr<Builder>{new Builder{std::move(environment)}};
}

std::shared_ptr<ConnectorEnvironment> ConnectorEnvironment::global() {
  static const auto kGlobal =
      std::shared_ptr<ConnectorEnvironment>{new ConnectorEnvironment{
          nonOwning(velox::connector::ConnectorRegistry::global()),
          nonOwning(ConnectorMetadataRegistry::global()),
          /*parent=*/nullptr,
          /*mutableEnvironment=*/true,
          /*ownsRegistries=*/false}};
  return kGlobal;
}

void ConnectorEnvironment::registerProcessWideConnector(
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK(
      !ownsRegistries_,
      "Process-wide registration requires the global connector environment");
  registerConnector(std::move(connector), std::move(metadata));
}

void ConnectorEnvironment::registerProcessWideMetadata(
    std::string connectorId,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK(
      !ownsRegistries_,
      "Process-wide registration requires the global connector environment");
  registerMetadata(std::move(connectorId), std::move(metadata));
}

void ConnectorEnvironment::registerConnector(
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<ConnectorMetadata> metadata) {
  VELOX_CHECK_NOT_NULL(connector);
  VELOX_CHECK_NOT_NULL(metadata);

  VELOX_CHECK(
      mutableEnvironment_, "Connector environment registration is complete");
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
  VELOX_CHECK_NOT_NULL(metadata);
  VELOX_CHECK(
      mutableEnvironment_, "Connector environment registration is complete");
  metadataRegistry_->insert(std::move(connectorId), std::move(metadata));
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
      !ownsRegistries_ || !mutableEnvironment_,
      "Connector environment must be built before use");
  queryCtx.setRegistry(
      velox::connector::ConnectorRegistry::kRegistryKey, connectorRegistry_);
  queryCtx.setRegistry(
      ConnectorMetadataRegistry::kRegistryKey, metadataRegistry_);
  queryCtx.setRegistry(
      kRegistryKey,
      std::const_pointer_cast<ConnectorEnvironment>(shared_from_this()));
}

ConnectorEnvironment::Builder::Builder(
    std::shared_ptr<ConnectorEnvironment> environment)
    : environment_{std::move(environment)} {
  VELOX_CHECK_NOT_NULL(environment_);
  VELOX_CHECK(
      environment_->ownsRegistries_ && environment_->mutableEnvironment_,
      "Connector environment builder requires mutable owned state");
}

ConnectorEnvironment& ConnectorEnvironment::Builder::mutableEnvironment()
    const {
  VELOX_CHECK_NOT_NULL(
      environment_, "Connector environment builder has already completed");
  return *environment_;
}

void ConnectorEnvironment::Builder::registerConnector(
    std::shared_ptr<velox::connector::Connector> connector,
    std::shared_ptr<ConnectorMetadata> metadata) {
  mutableEnvironment().registerConnector(
      std::move(connector), std::move(metadata));
}

void ConnectorEnvironment::Builder::registerMetadata(
    std::string connectorId,
    std::shared_ptr<ConnectorMetadata> metadata) {
  mutableEnvironment().registerMetadata(
      std::move(connectorId), std::move(metadata));
}

std::vector<
    std::pair<std::string, std::shared_ptr<velox::connector::Connector>>>
ConnectorEnvironment::Builder::connectors() const {
  return mutableEnvironment().connectors();
}

std::shared_ptr<ConnectorMetadata> ConnectorEnvironment::Builder::tryMetadata(
    std::string_view connectorId) const {
  return mutableEnvironment().tryMetadata(connectorId);
}

const ConnectorMetadataRegistry::Registry&
ConnectorEnvironment::Builder::metadataRegistry() const {
  return mutableEnvironment().metadataRegistry();
}

std::shared_ptr<ConnectorEnvironment> ConnectorEnvironment::Builder::build() {
  auto environment = std::move(environment_);
  VELOX_CHECK_NOT_NULL(
      environment, "Connector environment builder has already completed");
  environment->mutableEnvironment_ = false;
  return environment;
}

} // namespace facebook::axiom::connector
