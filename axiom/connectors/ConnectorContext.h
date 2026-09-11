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

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/CallOnce.h>

#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

class ConnectorMetadata;

/// Asks the application for the writer a connector records into. Called when
/// that connector's session is made, at most once per successful build: a
/// throw from the build runs it again. The writer it returns is shared by
/// every thread that connector runs on.
using ConnectorStatWriterProvider =
    std::function<std::shared_ptr<velox::BaseRuntimeStatWriter>(
        std::string_view connectorId)>;

/// Returns a provider that gives every connector a discarding writer, for an
/// application that records nothing.
inline ConnectorStatWriterProvider discardingStatWriters() {
  return [](std::string_view) {
    return std::make_shared<velox::NoopRuntimeStatWriter>();
  };
}

class ConnectorContext;
using ConnectorContextPtr = std::shared_ptr<ConnectorContext>;

/// Holds one query's identity and everything its connectors are given. The
/// application makes one when a query begins and drops it when the query is
/// finished with, and gives every component session of that query the same
/// one.
///
/// Example:
///   auto context = std::make_shared<ConnectorContext>(
///       queryId, user, connectorProperties, statWriterProvider);
///   metadata->beginWrite(context->sessionFor(*metadata), ...);
///
/// Invariants:
///   - `statWriterProvider` is non-empty.
///   - A connector's session is made once and is the same one for the life of
///     this context.
///   - Neither a connector making its state nor `statWriterProvider` asks the
///     context for a session.
///   - `statWriterProvider` is safe to call from several threads at once, as
///     two connectors' sessions are made independently.
class ConnectorContext {
 public:
  ConnectorContext(
      std::string queryId,
      std::string user,
      ConnectorProperties properties,
      ConnectorStatWriterProvider statWriterProvider);

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

  /// Returns 'connector's session, made on first use: this builds it, asks
  /// 'connector' for its state, and only then returns it. Concurrent callers
  /// get the same one. Throws if the thread building a session asks for one
  /// again, from either makeQueryState or the writer provider. Two threads
  /// building each other's connectors instead wait on each other.
  ConnectorSessionPtr sessionFor(const ConnectorMetadata& connector);

 private:
  // One connector's session and the state of making it. Held by shared_ptr so
  // the session can be built after the map lock is released.
  struct Entry {
    // Guards building 'session', so concurrent callers get the same one.
    folly::once_flag once;
    // The thread inside makeQueryState, so that thread asking for this
    // connector again fails instead of waiting on 'once' forever. A cycle
    // across threads is not detected.
    std::atomic<std::thread::id> builder{};
    // Set once 'once' has run.
    ConnectorSessionPtr session;
    // The metadata that built 'session'. A second metadata reporting the same
    // id would otherwise be handed the first one's state and property slice.
    // Not owned, and only ever compared: a connector unregistered mid-query
    // leaves an address that is stale but never dereferenced.
    const ConnectorMetadata* metadata{nullptr};
  };

  const std::string queryId_;
  const std::string user_;
  const ConnectorProperties properties_;
  const ConnectorStatWriterProvider statWriterProvider_;
  // The lock guards the map alone; a session is built under its entry's flag,
  // so a connector doing remote work does not hold up the query's others.
  folly::Synchronized<folly::F14FastMap<std::string, std::shared_ptr<Entry>>>
      sessions_;
};

} // namespace facebook::axiom::connector
