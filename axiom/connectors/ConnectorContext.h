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

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/CallOnce.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

/// Asks the application for the writer a component or connector records into.
using StatWriterProvider =
    std::function<std::shared_ptr<velox::BaseRuntimeStatWriter>(
        std::string_view id)>;

class ConnectorContext;
using ConnectorContextPtr = std::shared_ptr<ConnectorContext>;

/// Query-lifetime factory and cache for connector sessions. Builds each
/// connector's session on first use from this query's properties, writer
/// provider, and metadata registry.
///
/// Invariants:
///   - `statWriterProvider` is non-empty.
///   - `metadataRegistry` is non-null and lives as long as this context.
///   - One session per connector id, and the same one for the life of this
///     context.
///
/// Example:
///   auto session = context->sessionFor(connectorId);
///   context->metadataFor(connectorId)->beginWrite(session, ...);
class ConnectorContext {
 public:
  /// Creates a query scope over the global metadata registry.
  ConnectorContext(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      StatWriterProvider statWriterProvider);

  /// Uses 'metadataRegistry' for connector-specific query state.
  ConnectorContext(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      StatWriterProvider statWriterProvider,
      std::shared_ptr<const ConnectorMetadataRegistry::Registry>
          metadataRegistry);

  ConnectorContext(const ConnectorContext&) = delete;
  ConnectorContext& operator=(const ConnectorContext&) = delete;
  ConnectorContext(ConnectorContext&&) = delete;
  ConnectorContext& operator=(ConnectorContext&&) = delete;

  ~ConnectorContext() = default;

  /// Returns the query identifier.
  const std::string& queryId() const {
    return queryId_;
  }

  /// Returns the identity of the user who submitted the query.
  const std::string& user() const {
    return user_;
  }

  /// Returns 'connectorId's session, building it on first use. Concurrent
  /// callers share one session; a failed build may be retried. An unregistered
  /// connector receives no query state.
  ConnectorSessionPtr sessionFor(std::string_view connectorId);

  /// Returns the connector metadata visible to this query, or nullptr if the
  /// connector id is not registered.
  std::shared_ptr<ConnectorMetadata> metadataFor(
      std::string_view connectorId) const;

  /// Returns a provider that records nothing.
  static StatWriterProvider noopStatWriterProvider();

 private:
  // One connector's session and the state of making it. Held by shared_ptr so
  // the session can be built after the map lock is released.
  struct Entry {
    // Guards building 'session'.
    folly::once_flag once;
    ConnectorSessionPtr session;
  };

  const std::string queryId_;
  const std::string user_;
  const ConnectorProperties properties_;
  const StatWriterProvider statWriterProvider_;
  const std::shared_ptr<const ConnectorMetadataRegistry::Registry>
      metadataRegistry_;
  // The lock guards the map alone; a session is built under its entry's flag.
  folly::Synchronized<folly::F14FastMap<std::string, std::shared_ptr<Entry>>>
      sessions_;
};

} // namespace facebook::axiom::connector
