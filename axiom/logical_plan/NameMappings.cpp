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
#include "axiom/logical_plan/NameMappings.h"
#include <velox/common/base/Exceptions.h>

namespace facebook::axiom::logical_plan {

std::string NameMappings::QualifiedName::toString() const {
  if (alias.has_value()) {
    return fmt::format("{}.{}", alias.value(), name);
  }

  return name;
}

void NameMappings::add(const QualifiedName& name, const std::string& id) {
  bool ok = mappings_.emplace(name, id).second;
  VELOX_CHECK(ok, "Duplicate name: {}", name.toString());
}

void NameMappings::add(const std::string& name, const std::string& id) {
  bool ok =
      mappings_.emplace(QualifiedName{.alias = {}, .name = name}, id).second;
  VELOX_CHECK(ok, "Duplicate name: {}", name);
}

std::optional<std::string> NameMappings::lookup(const std::string& name) const {
  auto it = mappings_.find(QualifiedName{.alias = {}, .name = name});
  if (it != mappings_.end()) {
    return it->second;
  }

  return std::nullopt;
}

std::optional<std::string> NameMappings::lookup(
    const std::string& alias,
    const std::string& name) const {
  auto it = mappings_.find(QualifiedName{.alias = alias, .name = name});
  if (it != mappings_.end()) {
    return it->second;
  }

  return std::nullopt;
}

std::vector<NameMappings::QualifiedName> NameMappings::reverseLookup(
    const std::string& id) const {
  std::vector<QualifiedName> names;
  for (const auto& [key, value] : mappings_) {
    if (value == id) {
      names.push_back(key);
    }
  }

  VELOX_CHECK_LE(names.size(), 2);
  if (names.size() == 2) {
    VELOX_CHECK_EQ(names[0].name, names[1].name);
    VELOX_CHECK_NE(names[0].alias.has_value(), names[1].alias.has_value());
  }

  return names;
}

void NameMappings::setAlias(const std::string& alias) {
  std::vector<std::pair<std::string, std::string>> names;
  for (auto it = mappings_.begin(); it != mappings_.end();) {
    if (it->first.alias.has_value()) {
      it = mappings_.erase(it);
    } else {
      names.emplace_back(it->first.name, it->second);
      ++it;
    }
  }

  for (auto& [name, id] : names) {
    mappings_.emplace(
        QualifiedName{.alias = alias, .name = std::move(name)}, std::move(id));
  }
}

void NameMappings::merge(const NameMappings& other) {
  for (const auto& [name, id] : other.mappings_) {
    if (mappings_.contains(name)) {
      VELOX_CHECK(!name.alias.has_value());
      mappings_.erase(name);
    } else {
      mappings_.emplace(name, id);
    }
  }
}

std::unordered_map<std::string, std::string> NameMappings::uniqueNames() const {
  std::unordered_map<std::string, std::string> names;
  for (const auto& [name, id] : mappings_) {
    if (!name.alias.has_value()) {
      names.emplace(id, name.name);
    }
  }
  return names;
}

std::string NameMappings::toString() const {
  bool first = true;
  std::stringstream out;
  for (const auto& [name, id] : mappings_) {
    if (!first) {
      out << ", ";
    } else {
      first = false;
    }
    out << name.toString() << " -> " << id;
  }
  return out.str();
}

size_t NameMappings::QualifiedNameHasher::operator()(
    const QualifiedName& value) const {
  size_t h1 = 0;
  if (value.alias.has_value()) {
    h1 = std::hash<std::string>()(value.alias.value());
  }

  size_t h2 = std::hash<std::string>()(value.name);

  return h1 ^ (h2 << 1U);
}
} // namespace facebook::axiom::logical_plan
