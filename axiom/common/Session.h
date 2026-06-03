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
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <folly/container/F14Map.h>
#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom {

/// Read-only query-specific information.
class Session final {
 public:
  /// Session properties grouped by connector id; the inner map holds one
  /// connector's properties keyed by name.
  using ConnectorProperties = folly::F14FastMap<
      std::string,
      connector::ConnectorSession::ConnectorProperties,
      folly::transparent<std::hash<std::string_view>>,
      folly::transparent<std::equal_to<>>>;

  Session(
      std::string queryId,
      std::optional<std::string> user,
      ConnectorProperties connectorProperties)
      : queryId_{std::move(queryId)},
        user_{std::move(user)},
        connectorProperties_{std::move(connectorProperties)} {}

  /// Returns the query identifier.
  const std::string& queryId() const {
    return queryId_;
  }

  /// Returns the user who submitted the query, or `std::nullopt` if
  /// unavailable.
  const std::optional<std::string>& user() const {
    return user_;
  }

  /// Returns a connector-scoped session for `connectorId` carrying that
  /// connector's properties, or an empty set if none.
  connector::ConnectorSessionPtr toConnectorSession(
      std::string_view connectorId) const;

 private:
  const std::string queryId_;
  const std::optional<std::string> user_;
  const ConnectorProperties connectorProperties_;
};

using SessionPtr = std::shared_ptr<Session>;

} // namespace facebook::axiom
