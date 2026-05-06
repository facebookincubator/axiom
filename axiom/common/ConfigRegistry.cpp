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
#include "axiom/common/ConfigRegistry.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom {

using velox::config::ConfigProperty;
using velox::config::ConfigProvider;

void ConfigRegistry::add(
    std::string_view prefix,
    std::shared_ptr<ConfigProvider> provider,
    const std::unordered_map<std::string, std::string>& configFileOverrides) {
  const auto key = std::string(prefix);
  VELOX_USER_CHECK(
      providers_.find(key) == providers_.end(),
      "Config prefix already registered: {}",
      prefix);

  folly::F14FastMap<std::string, ConfigProperty> propertyMap;
  for (auto& prop : provider->properties()) {
    auto name = prop.name;
    // Override code default with config-file value if available.
    auto fileIt = configFileOverrides.find(name);
    if (fileIt != configFileOverrides.end()) {
      prop.defaultValue = fileIt->second;
    }
    VELOX_CHECK(
        propertyMap.emplace(name, std::move(prop)).second,
        "Duplicate config property: {}.{}",
        prefix,
        name);
  }
  providers_[key] = ProviderEntry{std::move(provider), std::move(propertyMap)};
}

ConfigRegistry::Resolution ConfigRegistry::resolve(
    std::string_view qualifiedName) const {
  auto dot = qualifiedName.find('.');
  VELOX_USER_CHECK(
      dot != std::string_view::npos,
      "Session property must be prefix-qualified (e.g., "
      "'optimizer.sample_joins'): {}",
      qualifiedName);

  const auto prefix = std::string(qualifiedName.substr(0, dot));
  auto it = providers_.find(prefix);
  VELOX_USER_CHECK(
      it != providers_.end(), "Unknown session property prefix: {}", prefix);

  const auto name = std::string(qualifiedName.substr(dot + 1));
  auto propIt = it->second.properties.find(name);
  VELOX_USER_CHECK(
      propIt != it->second.properties.end(),
      "Unknown session property: {}.{}",
      prefix,
      name);
  return {it->second.provider.get(), propIt->second};
}

std::vector<ConfigRegistry::Entry> ConfigRegistry::all() const {
  std::vector<Entry> result;
  for (const auto& [prefix, entry] : providers_) {
    for (const auto& [name, prop] : entry.properties) {
      result.push_back({prefix, prop});
    }
  }
  return result;
}

} // namespace facebook::axiom
