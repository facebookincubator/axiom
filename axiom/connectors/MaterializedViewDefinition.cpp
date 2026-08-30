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

#include "axiom/connectors/MaterializedViewDefinition.h"

#include <sstream>

namespace facebook::axiom::connector {

std::string MaterializedViewDefinition::toString() const {
  std::stringstream ss;
  ss << "MaterializedViewDefinition{";
  ss << "originalSql=" << originalSql_;
  ss << ", schema=" << schema_;
  ss << ", table=" << table_;
  ss << ", baseTables=[";
  for (size_t i = 0; i < baseTables_.size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << baseTables_[i].toString();
  }
  ss << "]";
  if (owner_.has_value()) {
    ss << ", owner=" << owner_.value();
  }
  if (securityMode_.has_value()) {
    ss << ", securityMode="
       << (securityMode_.value() == ViewSecurity::kInvoker ? "INVOKER"
                                                           : "DEFINER");
  }
  ss << ", columnMappings=[" << columnMappings_.size() << " mappings]";
  ss << ", baseTablesOnOuterJoinSide=[";
  for (size_t i = 0; i < baseTablesOnOuterJoinSide_.size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << baseTablesOnOuterJoinSide_[i].toString();
  }
  ss << "]";
  if (validRefreshColumns_.has_value()) {
    ss << ", validRefreshColumns=[";
    const auto& cols = validRefreshColumns_.value();
    for (size_t i = 0; i < cols.size(); ++i) {
      if (i > 0) {
        ss << ", ";
      }
      ss << cols[i];
    }
    ss << "]";
  }
  ss << "}";
  return ss.str();
}

} // namespace facebook::axiom::connector
