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

#include <folly/container/F14Map.h>
#include "axiom/common/ConfigRegistry.h"

namespace facebook::axiom {

/// Per-session mutable state. Holds a reference to the shared
/// ConfigRegistry and a map of property overrides. Reads check the
/// override map first, then fall back to the provider's default.
class SessionConfig {
 public:
  explicit SessionConfig(std::shared_ptr<const ConfigRegistry> registry);

  /// Sets a property by prefix-qualified name. Validates type and
  /// delegates to provider for domain validation. Returns true if
  /// the value changed.
  bool set(std::string_view qualifiedName, std::string_view value);

  /// Convenience overload: set("prefix", "name", "value") is equivalent to
  /// set("prefix.name", "value").
  bool
  set(std::string_view prefix, std::string_view name, std::string_view value);

  /// Resets a property to its default. Returns true if the value
  /// changed.
  bool reset(std::string_view qualifiedName);

  /// Validates that 'qualifiedName' names a known property and that 'value' is
  /// a valid value for it (type and domain validation). Throws on failure.
  /// Does not modify the session.
  void validate(std::string_view qualifiedName, std::string_view value) const;

  /// Validates that 'qualifiedName' names a known property. Throws on failure.
  /// Does not modify the session.
  void validate(std::string_view qualifiedName) const;

  /// Returns effective value (override or default) for a single
  /// property. Returns nullopt if no default and not set.
  std::optional<std::string> effectiveValue(
      std::string_view qualifiedName) const;

  /// Returns effective values (override or default) for all properties
  /// under a component prefix as a flat map.
  folly::F14FastMap<std::string, std::string> effectiveValues(
      std::string_view prefix) const;

  /// A property with its current effective value for SHOW SESSION output.
  struct Entry {
    std::string prefix;

    velox::config::ConfigProperty property;

    /// Override or default value. Nullopt if no default and not set.
    std::optional<std::string> currentValue;

    /// True if the value was explicitly set to a non-default value.
    bool isOverridden;
  };

  /// Returns all properties with current values.
  std::vector<Entry> all() const;

 private:
  std::shared_ptr<const ConfigRegistry> registry_;
  folly::F14FastMap<std::string, std::string> overrides_;
};

} // namespace facebook::axiom
