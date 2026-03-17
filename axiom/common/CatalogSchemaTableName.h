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

#include "axiom/common/SchemaTableName.h"

namespace facebook::axiom {

/// Structured representation of a fully-qualified table name, where catalog,
/// schema, and table are always set. When the catalog of a table is unknown,
/// prefer SchemaTableName.
struct CatalogSchemaTableName {
  std::string catalogName;
  SchemaTableName schemaTableName;

  /// Returns "catalog.schema.table" for display/logging only.
  std::string toString() const;

  bool operator==(const CatalogSchemaTableName&) const = default;
};

inline std::ostream& operator<<(
    std::ostream& os,
    const CatalogSchemaTableName& name) {
  return os << name.toString();
}

} // namespace facebook::axiom

template <>
struct std::hash<facebook::axiom::CatalogSchemaTableName> {
  size_t operator()(const facebook::axiom::CatalogSchemaTableName& name) const;
};

template <>
struct fmt::formatter<facebook::axiom::CatalogSchemaTableName>
    : fmt::formatter<std::string_view> {
  auto format(
      const facebook::axiom::CatalogSchemaTableName& name,
      format_context& ctx) const {
    return fmt::format_to(
        ctx.out(),
        "{}.{}.{}",
        name.catalogName,
        name.schemaTableName.schema,
        name.schemaTableName.table);
  }
};
