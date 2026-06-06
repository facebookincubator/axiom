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
#include <string>
#include <utility>

#include "axiom/connectors/BaseSession.h"
#include "axiom/optimizer/OptimizerOptions.h"

namespace facebook::axiom::optimizer {

/// Optimizer-scoped session view. Carries the shared identity (queryId,
/// user, connector-session factory) plus the typed OptimizerOptions for
/// this query.
class OptimizerSession final : public connector::BaseSession {
 public:
  /// Constructs an optimizer session with the given identity, typed
  /// `options`, and the per-connector property map used to spawn
  /// ConnectorSessions for plan-time metadata calls.
  OptimizerSession(
      std::string queryId,
      std::string user,
      OptimizerOptions options,
      connector::ConnectorProperties connectorProperties)
      : BaseSession(
            std::move(queryId),
            std::move(user),
            std::move(connectorProperties)),
        options_{std::move(options)} {}

  const OptimizerOptions& options() const {
    return options_;
  }

 private:
  const OptimizerOptions options_;
};

using OptimizerSessionPtr = std::shared_ptr<OptimizerSession>;

} // namespace facebook::axiom::optimizer
