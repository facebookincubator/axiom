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
#pragma once

#include <string>
#include <string_view>
#include <utility>

#include <folly/container/F14Map.h>

#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

/// Map of connector id to that connector's property bag.
using ConnectorProperties = folly::F14FastMap<std::string, Properties>;

/// Base class for component sessions. Holds queryId, user, the per-connector
/// property map, and an optional query-scoped runtime stats sink; spawns
/// ConnectorSessions on demand.
///
/// Example:
///   class OptimizerSession : public BaseSession { ... };
///   auto cs = optimizerSession.toConnectorSession("hive");
class BaseSession {
 public:
  BaseSession(
      std::string queryId,
      std::string user,
      ConnectorProperties connectorProperties,
      std::shared_ptr<QueryRuntimeStats> runtimeStats = nullptr)
      : queryId_{std::move(queryId)},
        user_{std::move(user)},
        connectorProperties_{std::move(connectorProperties)},
        runtimeStats_{std::move(runtimeStats)} {}

  virtual ~BaseSession() = default;

  const std::string& queryId() const {
    return queryId_;
  }

  const std::string& user() const {
    return user_;
  }

  /// Returns the query-scoped runtime stats sink, or null when the session has
  /// none. Handed to every ConnectorSession this session mints.
  const std::shared_ptr<QueryRuntimeStats>& runtimeStats() const {
    return runtimeStats_;
  }

  /// Spawns a ConnectorSession for 'connectorId' carrying queryId, user,
  /// that connector's property slice (empty when no properties were set for
  /// it), and this session's runtime stats sink.
  ConnectorSessionPtr toConnectorSession(std::string_view connectorId) const {
    return std::make_shared<ConnectorSession>(
        queryId_, user_, propertiesForConnector(connectorId), runtimeStats_);
  }

 private:
  Properties propertiesForConnector(std::string_view connectorId) const {
    auto it = connectorProperties_.find(connectorId);
    if (it == connectorProperties_.end()) {
      return {};
    }
    return it->second;
  }

  const std::string queryId_;
  const std::string user_;
  const ConnectorProperties connectorProperties_;
  const std::shared_ptr<QueryRuntimeStats> runtimeStats_;
};

} // namespace facebook::axiom::connector
