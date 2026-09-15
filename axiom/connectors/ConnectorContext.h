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

#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

/// Asks the application for the writer a component or connector records into.
using StatWriterProvider =
    std::function<std::shared_ptr<velox::BaseRuntimeStatWriter>(
        std::string_view id)>;

class ConnectorContext;
using ConnectorContextPtr = std::shared_ptr<ConnectorContext>;

/// A per-query factory and cache for `ConnectorSession`, parameterized by a
/// properties map and a writer provider. `sessionFor()` slices the properties
/// by connector id, asks the provider for that id's writer, and builds the
/// session once. The application makes one when a query begins, drops it when
/// the query is finished with, and gives every component session of that query
/// the same one.
///
/// A component is handed its property slice and writer directly
/// (`BaseSession(context, stats.writerFor(kOptimizer), properties)`) while a
/// connector gets both indirectly, because the application knows its components
/// up front and cannot know which connectors a query will touch.
///
/// Example:
///   auto context = std::make_shared<ConnectorContext>(
///       queryId, user, connectorProperties, statWriterProvider);
///   metadata->beginWrite(context->sessionFor(connectorId), ...);
///
/// Invariants:
///   - `statWriterProvider` is non-empty.
///   - One session per connector id, and the same one for the life of this
///     context.
class ConnectorContext {
 public:
  ConnectorContext(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      StatWriterProvider statWriterProvider);

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
  // The lock guards the map alone; a session is built under its entry's flag.
  folly::Synchronized<folly::F14FastMap<std::string, std::shared_ptr<Entry>>>
      sessions_;
};

} // namespace facebook::axiom::connector
