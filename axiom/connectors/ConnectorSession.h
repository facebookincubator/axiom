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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <folly/container/F14Map.h>

#include "axiom/common/Enums.h"
#include "velox/common/Casts.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::axiom::connector {

/// Property bag for a single component or connector.
using Properties = folly::F14FastMap<std::string, std::string>;

/// Map of connector id to that connector's property bag.
using ConnectorProperties = folly::F14FastMap<std::string, Properties>;

class ConnectorSession;
using ConnectorSessionPtr = std::shared_ptr<ConnectorSession>;

/// Whether a query changes data or metadata.
enum class QueryKind {
  /// Reads data and metadata only.
  kRead,
  /// Changes data or metadata, e.g. adds rows or creates a table.
  kWrite,
};

AXIOM_DECLARE_ENUM_NAME(QueryKind);

/// The kind of a query, set once the engine has parsed the query, std::nullopt
/// before that.
using QueryKindState = std::atomic<std::optional<QueryKind>>;

/// Holds connector-defined state for one query. Connector APIs may access the
/// state concurrently, so implementations provide any required synchronization.
///
/// Example:
///   if (auto* cache = session->queryStateAs<CacheState>()) {
///     cache->record(...);
///   }
class ConnectorQueryState {
 public:
  virtual ~ConnectorQueryState() = default;
};

/// Holds what one connector is given for one query: the query's identity, this
/// connector's property slice, and the writer it records into. Every connector
/// API takes one.
///
/// ConnectorContext makes one on first use. Connector-defined query state lives
/// as long as this session.
///
/// Example:
///   auto session = context->sessionFor(connectorId);
///   metadata->beginWrite(session, ...);
///
/// Invariants:
///   - `statsWriter` is non-null.
class ConnectorSession final {
 public:
  /// Creates a session whose connector query state is uninitialized.
  /// `queryStateAs` fails on it until `setQueryState` runs.
  ConnectorSession(
      std::string queryId,
      std::string user,
      Properties properties,
      std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter,
      std::shared_ptr<const QueryKindState> queryKind = nullptr)
      : queryId_{std::move(queryId)},
        user_{std::move(user)},
        properties_{std::move(properties)},
        statsWriter_{std::move(statsWriter)},
        queryKind_{std::move(queryKind)} {
    VELOX_CHECK_NOT_NULL(statsWriter_, "ConnectorSession requires a writer");
  }

  /// Returns the query identifier.
  const std::string& queryId() const {
    return queryId_;
  }

  /// Returns the identity of the user who submitted the query.
  const std::string& user() const {
    return user_;
  }

  /// Returns the value of session property 'name' if set on this session,
  /// or std::nullopt otherwise. The returned view is valid for the lifetime
  /// of this ConnectorSession.
  std::optional<std::string_view> property(std::string_view name) const {
    auto it = properties_.find(name);
    if (it == properties_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  const Properties& properties() const {
    return properties_;
  }

  /// Returns this connector's write handle into the query's stats.
  velox::BaseRuntimeStatWriter& statsWriter() const {
    return *statsWriter_;
  }

  /// Returns whether the query writes, or std::nullopt until the engine sets it
  /// (see ConnectorContext::setQueryKind()) and for a session made outside a
  /// ConnectorContext.
  std::optional<QueryKind> queryKind() const {
    return queryKind_ ? queryKind_->load() : std::nullopt;
  }

  /// Sets optional connector-defined state during engine initialization. Fails
  /// if called more than once.
  void setQueryState(std::unique_ptr<ConnectorQueryState> queryState) {
    VELOX_CHECK(
        !queryState_.has_value(),
        "Connector query state is already initialized");
    queryState_.emplace(std::move(queryState));
  }

  /// Returns this connector's query state as type 'T', or nullptr if the
  /// connector keeps no state. Fails if state initialization has not run or if
  /// 'T' does not match the state's dynamic type.
  template <typename T>
  T* queryStateAs() const {
    VELOX_CHECK(
        queryState_.has_value(), "Connector query state is not initialized");
    if (queryState_.value() == nullptr) {
      return nullptr;
    }
    return velox::checkedPointerCast<T>(queryState_->get());
  }

 private:
  const std::string queryId_;
  const std::string user_;
  const Properties properties_;
  const std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter_;
  // Shared with the ConnectorContext that sets it. Null for a session made
  // outside a context.
  const std::shared_ptr<const QueryKindState> queryKind_;
  // Tracks whether initialization has run, including when the connector keeps
  // no query state.
  std::optional<std::unique_ptr<ConnectorQueryState>> queryState_;
};

} // namespace facebook::axiom::connector
