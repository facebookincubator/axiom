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
/// Build an isolated environment, register connector and metadata pairs, and
/// attach the completed environment to every execution QueryCtx:
///
///   auto builder = ConnectorEnvironment::Builder::create();
///   builder->registerConnector(connector, metadata);
///   auto environment = builder->build();
///   environment->attachTo(*queryCtx);
///
/// A child may inherit an immutable parent environment. The child retains the
/// parent because ScopedRegistry stores a non-owning parent pointer.
///
/// Invariants:
///   - Owned environments are immutable after Builder::build().
///   - The process-wide global environment remains mutable for application
///     startup.
///   - The environment releases metadata registrations before connector
///     registrations.
///   - A child environment retains its parent for its complete lifetime.
class ConnectorEnvironment final
    : public std::enable_shared_from_this<ConnectorEnvironment> {
 public:
  class Builder;

  /// Key used to retain the complete environment on a QueryCtx.
  static constexpr std::string_view kRegistryKey = "connectorEnvironment";

  /// Returns the process-wide global environment. Its registries remain
  /// mutable for application-root startup registration.
  static std::shared_ptr<ConnectorEnvironment> global();

  ConnectorEnvironment(const ConnectorEnvironment&) = delete;
  ConnectorEnvironment& operator=(const ConnectorEnvironment&) = delete;
  ConnectorEnvironment(ConnectorEnvironment&&) = delete;
  ConnectorEnvironment& operator=(ConnectorEnvironment&&) = delete;

  ~ConnectorEnvironment();

  /// Registers one execution connector and its matching metadata in the
  /// process-wide global environment.
  void registerProcessWideConnector(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Registers metadata for a catalog without an execution connector in the
  /// process-wide global environment.
  void registerProcessWideMetadata(
      std::string connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);

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
      bool mutableEnvironment,
      bool ownsRegistries);

  void registerConnector(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<ConnectorMetadata> metadata);

  void registerMetadata(
      std::string connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);

  // Keeps a scoped registry's non-owning parent pointers valid.
  const std::shared_ptr<const ConnectorEnvironment> parent_;

  // Declared before metadataRegistry_ so metadata is destroyed first.
  const std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_;
  const std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry_;
  bool mutableEnvironment_{false};
  const bool ownsRegistries_{true};
};

/// Accumulates catalog registrations for one owned ConnectorEnvironment.
///
///   auto builder = ConnectorEnvironment::Builder::create();
///   builder->registerConnector(connector, metadata);
///   auto environment = builder->build();
///
/// Invariants:
///   - Registration and inspection occur before build().
///   - build() succeeds exactly once.
///   - The returned environment is immutable.
class ConnectorEnvironment::Builder final {
 public:
  /// Creates a builder for an isolated environment with no inherited catalogs.
  static std::shared_ptr<Builder> create();

  /// Creates a builder whose missing catalogs resolve through `parent`.
  /// The parent must be an owned environment returned by build(). The mutable
  /// process-wide global environment cannot be a parent.
  static std::shared_ptr<Builder> createChild(
      std::shared_ptr<const ConnectorEnvironment> parent);

  Builder(const Builder&) = delete;
  Builder& operator=(const Builder&) = delete;
  Builder(Builder&&) = delete;
  Builder& operator=(Builder&&) = delete;

  /// Registers one execution connector and its matching metadata atomically.
  void registerConnector(
      std::shared_ptr<velox::connector::Connector> connector,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Registers metadata for a catalog that has no execution connector.
  void registerMetadata(
      std::string connectorId,
      std::shared_ptr<ConnectorMetadata> metadata);

  /// Returns the execution connectors registered so far.
  std::vector<
      std::pair<std::string, std::shared_ptr<velox::connector::Connector>>>
  connectors() const;

  /// Returns metadata registered under `connectorId`, or nullptr if absent.
  std::shared_ptr<ConnectorMetadata> tryMetadata(
      std::string_view connectorId) const;

  /// Returns the metadata registry for registration-time dependencies.
  const ConnectorMetadataRegistry::Registry& metadataRegistry() const;

  /// Completes registration and returns the immutable environment.
  std::shared_ptr<ConnectorEnvironment> build();

 private:
  explicit Builder(std::shared_ptr<ConnectorEnvironment> environment);

  ConnectorEnvironment& mutableEnvironment() const;

  std::shared_ptr<ConnectorEnvironment> environment_;
};

} // namespace facebook::axiom::connector
