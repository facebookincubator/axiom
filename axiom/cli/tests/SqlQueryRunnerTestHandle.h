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

#include <folly/executors/FunctionScheduler.h>
#include <memory>
#include <string>

#include "axiom/cli/Connectors.h"
#include "axiom/cli/SqlQueryRunner.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace axiom::sql {

/// Keeps a test runner and its catalog state in one lifetime.
///
/// Register catalogs through `connectors()` before running queries:
/// @code
/// SqlQueryRunnerTestHandle runner{"test_user"};
/// auto& connectors = runner.connectors();
/// runner->initialize([&] {
///   auto connector = connectors.registerTestConnector();
///   return std::make_pair(connector->connectorId(), std::string{"default"});
/// });
/// @endcode
///
/// `RunnerT` must derive from SqlQueryRunner and accept its registry
/// constructor arguments.
template <typename RunnerT = SqlQueryRunner>
class SqlQueryRunnerTestHandle {
 public:
  /// Creates an empty handle for fixture members. Assign a runner before use.
  SqlQueryRunnerTestHandle() = default;

  explicit SqlQueryRunnerTestHandle(
      std::string user,
      folly::FunctionScheduler* progressScheduler = nullptr,
      bool useOptimizerV2 = true)
      : state_{std::make_unique<State>(
            std::move(user),
            progressScheduler,
            useOptimizerV2)} {}

  SqlQueryRunnerTestHandle(const SqlQueryRunnerTestHandle&) = delete;
  SqlQueryRunnerTestHandle& operator=(const SqlQueryRunnerTestHandle&) = delete;
  SqlQueryRunnerTestHandle(SqlQueryRunnerTestHandle&&) = default;
  SqlQueryRunnerTestHandle& operator=(SqlQueryRunnerTestHandle&&) = default;

  RunnerT* operator->() const {
    return state_->runner.get();
  }

  RunnerT& operator*() const {
    return *state_->runner;
  }

  void reset() {
    state_.reset();
  }

  /// Returns the registrar bound to this runner's catalog registries.
  facebook::axiom::Connectors& connectors() {
    return state_->connectors;
  }

 private:
  struct State {
    State(
        std::string user,
        folly::FunctionScheduler* progressScheduler,
        bool useOptimizerV2)
        : connectorRegistry{facebook::velox::connector::ConnectorRegistry::
                                create()},
          metadataRegistry{
              facebook::axiom::connector::ConnectorMetadataRegistry::create()},
          connectors{*connectorRegistry, *metadataRegistry},
          runner{std::make_unique<RunnerT>(
              std::move(user),
              *connectorRegistry,
              *metadataRegistry,
              progressScheduler,
              useOptimizerV2)} {}

    // Supplies execution connectors to the runner.
    std::shared_ptr<facebook::velox::connector::ConnectorRegistry::Registry>
        connectorRegistry;

    // Supplies catalog metadata to the runner.
    std::shared_ptr<
        facebook::axiom::connector::ConnectorMetadataRegistry::Registry>
        metadataRegistry;

    // Registers catalogs into this runner's registries.
    facebook::axiom::Connectors connectors;

    // Declared last so the runner is destroyed before the state it references.
    std::unique_ptr<RunnerT> runner;
  };

  std::unique_ptr<State> state_;
};

} // namespace axiom::sql
