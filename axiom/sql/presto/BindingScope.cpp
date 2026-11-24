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
#include "axiom/sql/presto/BindingScope.h"

namespace axiom::sql::presto {

// static
std::optional<std::string> BindingScope::lookup(
    const facebook::axiom::logical_plan::NameMappings& names,
    const std::optional<std::string>& qualifier,
    const std::string& name) {
  return qualifier.has_value() ? names.lookup(*qualifier, name)
                               : names.lookup(name);
}

std::vector<std::string> BindingScope::lookup(
    const std::optional<std::string>& qualifier,
    const std::string& name) const {
  std::vector<std::string> ids;
  for (const auto& relation : relations) {
    auto id = lookup(relation.names, qualifier, name);
    if (id.has_value()) {
      ids.push_back(std::move(id.value()));
      if (qualifier.has_value()) {
        break;
      }
    }
  }
  return ids;
}

bool BindingScope::isAmbiguous(
    const std::optional<std::string>& qualifier,
    const std::string& name) const {
  return std::ranges::any_of(relations, [&](const auto& relation) {
    return relation.names.isAmbiguous({qualifier, name});
  });
}

facebook::velox::TypePtr BindingScope::typeOf(const std::string& id) const {
  for (const auto& relation : relations) {
    if (auto index = relation.type->getChildIdxIfExists(id)) {
      return relation.type->childAt(*index);
    }
  }
  return nullptr;
}

} // namespace axiom::sql::presto
