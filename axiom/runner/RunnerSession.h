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

#include "axiom/connectors/BaseSession.h"

namespace facebook::axiom::runner {

/// Runner-scoped property bag.
using Properties = folly::F14FastMap<std::string, std::string>;

/// Runner-scoped session view. Carries the shared identity (queryId, user,
/// connector-session factory) plus the runner's own property slice.
class RunnerSession final : public connector::BaseSession {
 public:
  RunnerSession(
      std::string queryId,
      std::string user,
      Properties properties,
      connector::ConnectorProperties connectorProperties,
      std::shared_ptr<QueryRuntimeStats> runtimeStats = nullptr)
      : BaseSession(
            std::move(queryId),
            std::move(user),
            std::move(connectorProperties),
            std::move(runtimeStats)),
        properties_{std::move(properties)} {}

  /// Returns the value of runner-scoped property 'name' if set.
  std::optional<std::string_view> property(std::string_view name) const {
    auto it = properties_.find(name);
    if (it == properties_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

 private:
  const Properties properties_;
};

using RunnerSessionPtr = std::shared_ptr<RunnerSession>;

} // namespace facebook::axiom::runner
