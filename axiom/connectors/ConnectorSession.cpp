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

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector {

ConnectorSessionPtr ConnectorSession::create(
    std::string queryId,
    std::string user,
    Properties properties,
    std::shared_ptr<ConnectorMetadata> metadata,
    std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter) {
  return std::shared_ptr<ConnectorSession>(new ConnectorSession(
      std::move(queryId),
      std::move(user),
      std::move(properties),
      std::move(metadata),
      std::move(statsWriter)));
}

ConnectorSessionPtr ConnectorSession::testingCreate(
    std::string queryId,
    std::string user,
    Properties properties,
    std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter) {
  return std::shared_ptr<ConnectorSession>(new ConnectorSession(
      std::move(queryId),
      std::move(user),
      std::move(properties),
      nullptr,
      std::move(statsWriter)));
}

ConnectorSession::ConnectorSession(
    std::string queryId,
    std::string user,
    Properties properties,
    std::shared_ptr<ConnectorMetadata> metadata,
    std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter)
    : queryId_{std::move(queryId)},
      user_{std::move(user)},
      properties_{std::move(properties)},
      metadata_{std::move(metadata)},
      statsWriter_{std::move(statsWriter)} {
  VELOX_CHECK_NOT_NULL(statsWriter_, "ConnectorSession requires a writer");
}

} // namespace facebook::axiom::connector
