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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::velox::core {
class QueryCtx;
}

namespace facebook::axiom::connector {

/// Owns the Velox connectors and Axiom metadata visible to one engine.
///
/// Build an isolated environment, register connector and metadata pairs, seal
/// it before serving queries, and attach it to every execution QueryCtx:
///
///   auto environment = ConnectorEnvironment::create();
///   environment->registerConnector(connector, metadata);
///   environment->seal();
///   environment->attachTo(*queryCtx);
///
/// A child may inherit an immutable parent environment. The child retains the
/// parent because ScopedRegistry stores a non-owning parent pointer.
///
/// Invariants:
///   - Owned environments accept registration only before seal().
///   - The legacy global environment remains mutable for application startup.
///   - Connector metadata is destroyed before its matching connector.
///   - A child environment retains its parent for its complete lifetime.
class ConnectorEnvironment final
    : public std::enable_shared_from_this<ConnectorEnvironment> {
 public:
  /// Key used to retain the complete environment on a QueryCtx.
  static constexpr std::string_view kRegistryKey = "connectorEnvironment";

  /// Creates an isolated environment with no inherited catalogs.
  static std::shared_ptr<ConnectorEnvironment> create();

  /// Creates an environment whose missing catalogs resolve through `parent`.
  /// The parent must own its registries and already be sealed. The mutable
  /// legacy global environment cannot be a parent.
  static std::shared_ptr<ConnectorEnvironment> createChild(
      std::shared_ptr<const ConnectorEnvironment> parent);

  /// Returns the legacy process-global environment. Its registries remain
  /// mutable for application-root startup registration.
  static std::shared_ptr<ConnectorEnvironment> global();

  ConnectorEnvironment(const ConnectorEnvironment&) = delete;
  ConnectorEnvironment& operator=(const ConnectorEnvironment&) = delete;
  ConnectorEnvironment(ConnectorEnvironment&&) = delete;
  ConnectorEnvironment& operator=(ConnectorEnvironment&&) = delete;

  ~ConnectorEnvironment();

  /// Registers one execution connector and its matching metadata atomically.
  void registerConnector(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Registers metadata for a catalog that has no execution connector.
  void registerMetadata(
      std::string connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Prevents further registration and makes the environment query-safe.
  void seal();

  /// Returns true after seal() has completed.
  bool sealed() const {
    return sealed_.load(std::memory_order_acquire);
  }

  /// Returns the execution connector registered under `connectorId`.
  std::shared_ptr<velox::connector::Connector> connector(
      std::string_view connectorId) const;

  /// Returns metadata registered under `connectorId`, or nullptr if absent.
  std::shared_ptr<ConnectorMetadata> tryMetadata(
      std::string_view connectorId) const;

  /// Returns metadata registered under `connectorId`.
  std::shared_ptr<ConnectorMetadata> metadata(
      std::string_view connectorId) const;

  /// Returns all connector registrations visible in this environment.
  std::vector<
      std::pair<std::string, std::shared_ptr<velox::connector::Connector>>>
  connectors() const;

  /// Returns all metadata IDs visible in this environment.
  std::vector<std::string> metadataIds() const;

  /// Returns the metadata registry for APIs that accept an explicit registry.
  const ConnectorMetadataRegistry::Registry& metadataRegistry() const {
    return *metadataRegistry_;
  }

  /// Installs this environment on `queryCtx` for execution-time lookup.
  void attachTo(velox::core::QueryCtx& queryCtx) const;

 private:
  ConnectorEnvironment(
      std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
          connectorRegistry,
      std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry,
      std::shared_ptr<const ConnectorEnvironment> parent,
      bool sealed,
      bool ownsRegistries);

  // Keeps a scoped registry's non-owning parent pointers valid.
  const std::shared_ptr<const ConnectorEnvironment> parent_;

  // Declared before metadataRegistry_ so metadata is destroyed first.
  const std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_;
  const std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry_;
  std::atomic<bool> sealed_{false};
  const bool ownsRegistries_{true};
};

} // namespace facebook::axiom::connector
