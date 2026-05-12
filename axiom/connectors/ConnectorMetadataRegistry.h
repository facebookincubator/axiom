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

#include <string>
#include <vector>

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/common/ScopedRegistry.h"

namespace facebook::velox::core {
class QueryCtx;
} // namespace facebook::velox::core

namespace facebook::axiom::connector {

/// Manages connector metadata registration and lookup. All methods are
/// thread-safe.
///
/// Two groups of APIs:
///
/// - Query-scoped APIs take a QueryCtx& and check for per-query registry
///   overrides before falling back to the global registry. Use these in
///   operator and expression evaluation code where a QueryCtx is available.
///
/// - Global APIs operate directly on the global registry. Use these for
///   process-level operations: startup registration, shutdown cleanup, and
///   process-wide lookups (e.g., periodic stats reporting).
class ConnectorMetadataRegistry {
 public:
  /// Type alias for the scoped metadata registry.
  using Registry = velox::ScopedRegistry<std::string, ConnectorMetadata>;

  /// Registry key for per-query metadata overrides on QueryCtx.
  static constexpr std::string_view kRegistryKey = "connectorMetadata";

  /// Return the global registry (root scope).
  static Registry& global();

  /// Create a per-query registry. If 'parent' is provided, lookups fall back
  /// to it. Pass nullptr for isolation mode (no fallback).
  static std::shared_ptr<Registry> create(const Registry* parent = nullptr);

  /// Return the metadata connector with the specified ID, or nullptr if
  /// not registered.  Checks per-query override on QueryCtx first, falls back
  /// to the global registry if no override is set.
  static std::shared_ptr<ConnectorMetadata> tryGet(
      const velox::core::QueryCtx& queryCtx,
      const std::string& connectorId);

  /// Return the metadata connector with the specified ID from the global
  /// registry, or nullptr if not registered.
  static std::shared_ptr<ConnectorMetadata> tryGet(
      const std::string& connectorId);

  /// Return the metadata connector with the specified ID, or an exception if
  /// not registered.  Checks per-query override on QueryCtx first, falls back
  /// to the global registry if no override is set.
  /// @throws VeloxError if the shared_ptr is null.
  static std::shared_ptr<ConnectorMetadata> get(
      const velox::core::QueryCtx& queryCtx,
      const std::string& connectorId);

  /// Return the metadata connector with the specified ID from the global
  /// registry, or an exception if not registered.
  /// @throws VeloxError if the shared_ptr is null.
  static std::shared_ptr<ConnectorMetadata> get(const std::string& connectorId);

  /// Return all metadata connectors whose implementation is of type T. Checks
  /// per-query override on QueryCtx first, falls back to the global registry if
  /// no override is set.
  template <typename T>
  static std::vector<std::shared_ptr<T>> findAll(
      const velox::core::QueryCtx& queryCtx) {
    std::vector<std::shared_ptr<T>> result;
    for (auto& [_, connectorMetadata] : snapshot(queryCtx)) {
      if (auto casted = std::dynamic_pointer_cast<T>(connectorMetadata)) {
        result.push_back(std::move(casted));
      }
    }
    return result;
  }

  /// Return all metadata connectors from the global registry whose
  /// implementation is of type T.
  template <typename T>
  static std::vector<std::shared_ptr<T>> findAll() {
    std::vector<std::shared_ptr<T>> result;
    for (auto& [_, connectorMetadata] : global().snapshot()) {
      if (auto casted = std::dynamic_pointer_cast<T>(connectorMetadata)) {
        result.push_back(std::move(casted));
      }
    }
    return result;
  }

  /// Return all registered ids from the query. Locally registered ids
  /// override parent or global registrations: each return id will be
  /// unique.
  static std::vector<std::string> allMetadataIds(
      const velox::core::QueryCtx& queryCtx);

  /// Return all registered ids from the global registry.
  static std::vector<std::string> allMetadataIds();

  /// Unregister all metadata connectors from the registry visible to the given
  /// query.
  static void unregisterAll(const velox::core::QueryCtx& queryCtx);

  /// Unregister all metadata connectors from the global registry.
  static void unregisterAll();

 private:
  // Return a snapshot of all metadata connectors visible to the given query.
  // Uses per-query registry if set, otherwise the global registry.
  static std::vector<std::pair<std::string, std::shared_ptr<ConnectorMetadata>>>
  snapshot(const velox::core::QueryCtx& queryCtx);
};

} // namespace facebook::axiom::connector
