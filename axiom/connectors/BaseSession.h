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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <folly/CPortability.h>
#include <folly/container/F14Map.h>

#include "axiom/connectors/ConnectorSession.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::axiom::connector {

/// Map of connector id to that connector's property bag.
using ConnectorProperties = folly::F14FastMap<std::string, Properties>;

/// Resolves a connector id to the stat writer that connector records into. An
/// application backs this with its own aggregate; a session that should not
/// record connector stats is given noopConnectorWriterProvider().
using ConnectorWriterProvider =
    std::function<velox::BaseRuntimeStatWriter&(std::string_view connectorId)>;

/// Discards every metric written to it.
class NoopStatWriter : public velox::BaseRuntimeStatWriter {};

/// Returns a process-wide NoopStatWriter. Handed to a session (or connector)
/// that should not record; it holds no state, so one shared instance is a safe
/// concurrent sink.
FOLLY_EXPORT inline velox::BaseRuntimeStatWriter& noopStatWriter() {
  static NoopStatWriter instance;
  return instance;
}

/// Returns a ConnectorWriterProvider that routes every connector to
/// noopStatWriter().
inline ConnectorWriterProvider noopConnectorWriterProvider() {
  return [](std::string_view) -> velox::BaseRuntimeStatWriter& {
    return noopStatWriter();
  };
}

/// Base class for component sessions. Holds queryId, user, the per-connector
/// property map, this component's stat writer, and a provider that hands out a
/// connector's stat writer; spawns ConnectorSessions on demand.
///
/// Example:
///   class OptimizerSession : public BaseSession { ... };
///   auto cs = optimizerSession.toConnectorSession("hive");
class BaseSession {
 public:
  /// 'statsWriter' is this component's write handle; it must outlive the
  /// session. 'connectorWriterProvider' resolves a connector id to that
  /// connector's write handle when toConnectorSession() spawns a session.
  BaseSession(
      std::string queryId,
      std::string user,
      ConnectorProperties connectorProperties,
      velox::BaseRuntimeStatWriter& statsWriter,
      ConnectorWriterProvider connectorWriterProvider)
      : queryId_{std::move(queryId)},
        user_{std::move(user)},
        connectorProperties_{std::move(connectorProperties)},
        statsWriter_{statsWriter},
        connectorWriterProvider_{std::move(connectorWriterProvider)} {
    VELOX_CHECK(
        connectorWriterProvider_,
        "BaseSession requires a connectorWriterProvider; use "
        "noopConnectorWriterProvider() for a session that should not record "
        "connector stats.");
  }

  virtual ~BaseSession() = default;

  const std::string& queryId() const {
    return queryId_;
  }

  const std::string& user() const {
    return user_;
  }

  /// Returns the per-connector property slices, for spawning a sibling session
  /// that shares this one's identity but records elsewhere.
  const ConnectorProperties& connectorProperties() const {
    return connectorProperties_;
  }

  /// Returns this component's write handle into the query-wide stats.
  velox::BaseRuntimeStatWriter& statsWriter() const {
    return statsWriter_;
  }

  /// Spawns a ConnectorSession for 'connectorId' carrying queryId, user,
  /// that connector's property slice (empty when no properties were set for
  /// it), and its stat-writer handle from the provider. The session borrows the
  /// handle, so the provider's backing store must outlive it.
  ConnectorSessionPtr toConnectorSession(std::string_view connectorId) const {
    return std::make_shared<ConnectorSession>(
        queryId_,
        user_,
        propertiesForConnector(connectorId),
        connectorWriterProvider_(connectorId));
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
  velox::BaseRuntimeStatWriter& statsWriter_;
  const ConnectorWriterProvider connectorWriterProvider_;
};

} // namespace facebook::axiom::connector
