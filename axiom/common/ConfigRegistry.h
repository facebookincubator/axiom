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
#include <vector>

#include <folly/container/F14Map.h>
#include "velox/common/config/ConfigProvider.h"

namespace facebook::axiom {

/// Maps namespace prefixes to ConfigProviders. Built once at startup and
/// shared across sessions. Immutable after assembly.
class ConfigRegistry {
 public:
  /// Registers a provider under a namespace prefix. Takes a snapshot
  /// of the provider's properties at registration time; later changes
  /// to the provider's property list are not reflected. Throws if the
  /// prefix is already registered. Not thread-safe — call only during
  /// single-threaded startup before sharing the registry.
  ///
  /// If 'configFileOverrides' is provided, its values override the code
  /// defaults in ConfigProperty::defaultValue for matching property names.
  /// This enables a three-layer config cascade: code default → config file
  /// → session override.
  void add(
      std::string_view prefix,
      std::shared_ptr<velox::config::ConfigProvider> provider,
      const std::unordered_map<std::string, std::string>& configFileOverrides =
          {});

  /// Result of resolving a prefix-qualified property name.
  struct Resolution {
    /// Provider that owns the property (for validation).
    const velox::config::ConfigProvider* provider;

    /// Property descriptor.
    velox::config::ConfigProperty property;
  };

  /// Looks up provider and property for a prefix-qualified name
  /// (e.g., "optimizer.sample_joins"). Throws if not found.
  Resolution resolve(std::string_view qualifiedName) const;

  /// A property with its namespace prefix.
  struct Entry {
    std::string prefix;
    velox::config::ConfigProperty property;
  };

  /// Returns all properties across all providers.
  std::vector<Entry> all() const;

 private:
  struct ProviderEntry {
    std::shared_ptr<velox::config::ConfigProvider> provider;
    folly::F14FastMap<std::string, velox::config::ConfigProperty> properties;
  };

  folly::F14FastMap<std::string, ProviderEntry> providers_;
};

} // namespace facebook::axiom
