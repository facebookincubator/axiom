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
#include <vector>

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

/// Resolves one engine's connector metadata and caches connector sessions for
/// one query. `sessionFor()` slices the requested connector's properties, asks
/// for its writer, and builds one session. Every stage of the query shares the
/// same context.
///
/// A component is handed its property slice and writer directly
/// (`BaseSession(context, stats.writerFor(kOptimizer), properties)`) while a
/// connector gets both indirectly, because the application knows its components
/// up front and cannot know which connectors a query will touch.
///
/// Example:
///   auto context = std::make_shared<ConnectorContext>(
///       queryId,
///       user,
///       connectorProperties,
///       statWriterProvider,
///       metadataRegistry);
///   metadata->beginWrite(context->sessionFor(connectorId), ...);
///
/// Invariants:
///   - `statWriterProvider` is non-empty.
///   - The engine's metadata registry is non-null.
///   - One session per connector id, and the same one for the life of this
///     context.
class ConnectorContext {
 public:
  /// Creates a context backed by process-wide registrations for tests and
  /// hosts that have not migrated to engine-owned registries.
  static ConnectorContextPtr testingCreate(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      StatWriterProvider statWriterProvider);

  /// Creates a context from an engine-owned metadata registry.
  ConnectorContext(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      StatWriterProvider statWriterProvider,
      std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry);

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

  /// Returns metadata registered under `connectorId`, or nullptr if absent.
  std::shared_ptr<ConnectorMetadata> tryMetadata(
      std::string_view connectorId) const;

  /// Returns metadata registered under `connectorId`.
  std::shared_ptr<ConnectorMetadata> metadata(
      std::string_view connectorId) const;

  /// Returns all metadata IDs registered for this engine.
  std::vector<std::string> metadataIds() const;

  /// Returns 'connectorId's session, made on first use. Concurrent callers get
  /// the same one, and it is the same one for the life of this context. The
  /// writer provider is called at most once per successful build; a throw from
  /// the build runs it again, and the writer it returns is shared by every
  /// thread that connector runs on.
  ConnectorSessionPtr sessionFor(std::string_view connectorId);

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
  const std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry_;
  // The lock guards the map alone; a session is built under its entry's flag.
  folly::Synchronized<folly::F14FastMap<std::string, std::shared_ptr<Entry>>>
      sessions_;
};

} // namespace facebook::axiom::connector
