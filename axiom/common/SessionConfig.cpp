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
#include "axiom/common/SessionConfig.h"

#include <fmt/format.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom {

using velox::config::ConfigPropertyType;

namespace {
// Returns true if 'value' is a valid integer string (optional leading '-'
// followed by one or more digits).
bool isInteger(std::string_view value) {
  auto start = !value.empty() && value[0] == '-' ? 1u : 0u;
  return value.size() > start &&
      std::all_of(value.begin() + start, value.end(), [](char c) {
           return std::isdigit(c);
         });
}

// Returns true if 'value' is a valid double string.
bool isDouble(std::string_view value) {
  if (value.empty()) {
    return false;
  }
  auto str = std::string(value);
  char* end = nullptr;
  std::strtod(str.c_str(), &end);
  return end != nullptr && *end == '\0';
}

// Validates and normalizes 'value' for 'type'. Lowercases booleans,
// passes through integers, doubles, and strings. Throws on invalid values.
std::string normalizeType(
    std::string_view qualifiedName,
    ConfigPropertyType type,
    std::string_view value) {
  switch (type) {
    case ConfigPropertyType::kBoolean: {
      auto lower = std::string(value);
      std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
      VELOX_USER_CHECK(
          lower == "true" || lower == "false",
          "Expected boolean value for session property: {} = {}",
          qualifiedName,
          value);
      return lower;
    }
    case ConfigPropertyType::kInteger:
      VELOX_USER_CHECK(
          isInteger(value),
          "Expected integer value for session property: {} = {}",
          qualifiedName,
          value);
      return std::string(value);
    case ConfigPropertyType::kDouble:
      VELOX_USER_CHECK(
          isDouble(value),
          "Expected double value for session property: {} = {}",
          qualifiedName,
          value);
      return std::string(value);
    case ConfigPropertyType::kString:
      return std::string(value);
  }
  VELOX_UNREACHABLE();
}

struct NormalizedAndDefault {
  std::string normalizedValue;
  std::optional<std::string> defaultValue;

  bool normalizedEqualsDefault() const {
    return defaultValue.has_value() && normalizedValue == defaultValue.value();
  }
};

// Resolves 'qualifiedName' and normalizes 'value'.
// Throws if 'qualifiedName' is not found or 'value' is invalid.
NormalizedAndDefault resolveAndNormalize(
    const ConfigRegistry& registry,
    std::string_view qualifiedName,
    std::string_view value) {
  auto resolved = registry.resolve(qualifiedName);
  auto normalized = normalizeType(qualifiedName, resolved.property.type, value);
  normalized = resolved.provider->normalize(resolved.property.name, normalized);
  return {std::move(normalized), std::move(resolved.property.defaultValue)};
}
} // namespace

SessionConfig::SessionConfig(std::shared_ptr<const ConfigRegistry> registry)
    : registry_(std::move(registry)) {}

bool SessionConfig::set(
    std::string_view qualifiedName,
    std::string_view value) {
  auto key = std::string(qualifiedName);
  auto normalizedAndDefault =
      resolveAndNormalize(*registry_, qualifiedName, value);

  // If the normalized value equals the default, clear any override.
  if (normalizedAndDefault.normalizedEqualsDefault()) {
    return overrides_.erase(key) > 0;
  }

  // Check if value is same as current override.
  auto it = overrides_.find(key);
  if (it != overrides_.end() &&
      it->second == normalizedAndDefault.normalizedValue) {
    return false;
  }

  overrides_[std::move(key)] = std::move(normalizedAndDefault.normalizedValue);
  return true;
}

bool SessionConfig::set(
    std::string_view prefix,
    std::string_view name,
    std::string_view value) {
  return set(fmt::format("{}.{}", prefix, name), value);
}

bool SessionConfig::reset(std::string_view qualifiedName) {
  // Verify property exists.
  registry_->resolve(qualifiedName);
  return overrides_.erase(qualifiedName) > 0;
}

void SessionConfig::validate(
    std::string_view qualifiedName,
    std::string_view value) const {
  resolveAndNormalize(*registry_, qualifiedName, value);
}

void SessionConfig::validate(std::string_view qualifiedName) const {
  registry_->resolve(qualifiedName);
}

std::optional<std::string> SessionConfig::effectiveValue(
    std::string_view qualifiedName) const {
  auto it = overrides_.find(qualifiedName);
  if (it != overrides_.end()) {
    return it->second;
  }
  auto resolved = registry_->resolve(qualifiedName);
  return resolved.property.defaultValue;
}

folly::F14FastMap<std::string, std::string> SessionConfig::effectiveValues(
    std::string_view prefix) const {
  folly::F14FastMap<std::string, std::string> result;
  for (const auto& entry : registry_->all()) {
    if (entry.prefix != prefix) {
      continue;
    }
    auto qualifiedName = entry.prefix + "." + entry.property.name;
    auto it = overrides_.find(qualifiedName);
    if (it != overrides_.end()) {
      result[entry.property.name] = it->second;
    } else if (entry.property.defaultValue.has_value()) {
      result[entry.property.name] = entry.property.defaultValue.value();
    }
  }
  return result;
}

std::vector<SessionConfig::Entry> SessionConfig::all() const {
  std::vector<Entry> result;
  for (const auto& registryEntry : registry_->all()) {
    auto qualifiedName =
        registryEntry.prefix + "." + registryEntry.property.name;
    auto it = overrides_.find(qualifiedName);
    std::optional<std::string> currentValue;
    if (it != overrides_.end()) {
      currentValue = it->second;
    } else {
      currentValue = registryEntry.property.defaultValue;
    }
    result.push_back(
        {registryEntry.prefix,
         registryEntry.property,
         currentValue,
         it != overrides_.end()});
  }
  return result;
}

} // namespace facebook::axiom
