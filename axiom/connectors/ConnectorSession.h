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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <folly/container/F14Map.h>

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

/// Holds the state a connector keeps for one query. A connector defines a
/// concrete type and reaches it with asChecked, as it does for table and
/// column handles.
/// Nothing outside the connector reads it.
///
/// Reached as const from a session several threads share, so a connector that
/// mutates it synchronizes it.
class ConnectorQueryState {
 public:
  ConnectorQueryState() = default;
  ConnectorQueryState(const ConnectorQueryState&) = delete;
  ConnectorQueryState& operator=(const ConnectorQueryState&) = delete;
  ConnectorQueryState(ConnectorQueryState&&) = delete;
  ConnectorQueryState& operator=(ConnectorQueryState&&) = delete;

  virtual ~ConnectorQueryState() = default;

  /// Returns this object as type 'T'. Throws if it is not of that type.
  template <typename T>
  const T* asChecked() const {
    return velox::checkedPointerCast<const T>(this);
  }
};

/// Holds what one connector is given for one query: the query's identity, this
/// connector's property slice, the writer it records into, and whatever it
/// keeps for the query. Every connector API takes one.
///
/// Made by ConnectorContext on first use and released with it; a connector
/// retains nothing beyond the call it was passed to.
///
/// Invariants:
///   - `statsWriter` is non-null.
///   - State is attached once, before the session is handed out.
class ConnectorSession final {
 public:
  ConnectorSession(
      std::string queryId,
      std::string user,
      Properties properties,
      std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter)
      : queryId_{std::move(queryId)},
        user_{std::move(user)},
        properties_{std::move(properties)},
        statsWriter_{std::move(statsWriter)} {
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

  /// Returns this connector's write handle into the query's stats.
  velox::BaseRuntimeStatWriter& statsWriter() const {
    return *statsWriter_;
  }

  /// Returns what this connector keeps for the query, or null if it keeps
  /// nothing.
  const ConnectorQueryState* queryState() const {
    return queryState_.get();
  }

  /// Returns this connector's state as type 'T'. Fails if the connector kept
  /// nothing for the query or kept something else, so a connector reading its
  /// own state never dereferences null.
  template <typename T>
  const T& queryStateAs() const {
    VELOX_CHECK_NOT_NULL(
        queryState_, "Connector kept no state for query {}", queryId_);
    return *queryState_->asChecked<T>();
  }

  /// Completes this session with whatever makeQueryState returned, before it
  /// is given to anyone. Fails if called again, so state cannot be attached to
  /// a session already in use.
  void initializeQueryState(std::unique_ptr<ConnectorQueryState> state);

 private:
  const std::string queryId_;
  const std::string user_;
  const Properties properties_;
  const std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter_;
  std::unique_ptr<ConnectorQueryState> queryState_;
};

} // namespace facebook::axiom::connector
