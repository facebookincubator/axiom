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

/// State a connector keeps for one query. A connector defines a concrete type
/// and reaches it with asChecked, as it does for table and column handles.
/// Nothing outside the connector reads it.
///
/// A session may be shared by several threads. A connector that mutates its
/// state must synchronize access.
class ConnectorQueryState {
 public:
  virtual ~ConnectorQueryState() = default;

  /// Returns this state as type 'T'. Throws if it is not of that type.
  template <typename T>
  T* asChecked() {
    return velox::checkedPointerCast<T>(this);
  }
};

/// Holds what one connector is given for one query: the query's identity, this
/// connector's property slice, and the writer it records into. Every connector
/// API takes one.
///
/// Made by ConnectorContext on first use and released with it. Connector-owned
/// query-lifetime state is stored here instead of retained by metadata.
///
/// Invariants:
///   - `statsWriter` is non-null.
///   - Query state is set at most once, before the session is given to anyone.
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

  /// Returns what this connector keeps for the query, or nullptr if it keeps
  /// nothing.
  ConnectorQueryState* queryState() const {
    return queryState_.get();
  }

  /// Returns what this connector keeps for the query as type 'T'. Throws if
  /// this connector kept nothing, or kept a different type.
  template <typename T>
  T& queryStateAs() const {
    VELOX_CHECK_NOT_NULL(queryState_, "Connector kept no state for the query");
    return *queryState_->asChecked<T>();
  }

  /// Completes this session with what the connector kept, before the session is
  /// given to anyone.
  void initQueryState(std::unique_ptr<ConnectorQueryState> queryState) {
    VELOX_CHECK_NULL(queryState_, "ConnectorSession already has query state");
    queryState_ = std::move(queryState);
  }

 private:
  const std::string queryId_;
  const std::string user_;
  const Properties properties_;
  const std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter_;
  std::unique_ptr<ConnectorQueryState> queryState_;
};

} // namespace facebook::axiom::connector
