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

#include "axiom/connectors/ConnectorSession.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector {

ConnectorSessionPtr ConnectorSession::createProcessWide(
    std::string queryId,
    std::string user,
    Properties properties,
    std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter) {
  return std::make_shared<ConnectorSession>(
      std::move(queryId),
      std::move(user),
      std::move(properties),
      std::move(statsWriter),
      velox::connector::ConnectorRegistry::processWide(),
      ConnectorMetadataRegistry::processWide());
}

ConnectorSession::ConnectorSession(
    std::string queryId,
    std::string user,
    Properties properties,
    std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter,
    std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
        connectorRegistry,
    std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry)
    : queryId_{std::move(queryId)},
      user_{std::move(user)},
      properties_{std::move(properties)},
      statsWriter_{std::move(statsWriter)},
      connectorRegistry_{std::move(connectorRegistry)},
      metadataRegistry_{std::move(metadataRegistry)} {
  VELOX_CHECK_NOT_NULL(statsWriter_, "ConnectorSession requires a writer");
  VELOX_CHECK_NOT_NULL(
      connectorRegistry_, "ConnectorSession requires a connector registry");
  VELOX_CHECK_NOT_NULL(
      metadataRegistry_,
      "ConnectorSession requires a connector metadata registry");
}

std::optional<std::string_view> ConnectorSession::property(
    std::string_view name) const {
  auto it = properties_.find(name);
  if (it == properties_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::shared_ptr<velox::connector::Connector> ConnectorSession::connector(
    std::string_view connectorId) const {
  auto connector = connectorRegistry_->find(std::string{connectorId});
  VELOX_CHECK_NOT_NULL(
      connector, "Connector is not registered: {}", connectorId);
  return connector;
}

std::shared_ptr<ConnectorMetadata> ConnectorSession::metadata(
    std::string_view connectorId) const {
  auto metadata = metadataRegistry_->find(std::string{connectorId});
  VELOX_CHECK_NOT_NULL(
      metadata, "Connector metadata is not registered: {}", connectorId);
  return metadata;
}

} // namespace facebook::axiom::connector
