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

#include <fmt/format.h>
#include <ostream>

namespace facebook::axiom {

/// Structured representation of a schema-qualified table name.
/// Both schema and table are always set. Catalog/connectorId is kept separate.
/// Avoids ambiguity when identifiers contain dots (e.g., a quoted identifier
/// "a.b" is stored as-is in the schema field, not split on dots).
struct SchemaTableName {
  std::string schema;
  std::string table;

  /// Returns "schema"."table" for display/logging only.
  std::string toString() const;

  bool operator==(const SchemaTableName&) const = default;
};

inline std::ostream& operator<<(std::ostream& os, const SchemaTableName& name) {
  return os << name.toString();
}

} // namespace facebook::axiom

template <>
struct std::hash<facebook::axiom::SchemaTableName> {
  size_t operator()(const facebook::axiom::SchemaTableName& name) const;
};

template <>
struct fmt::formatter<facebook::axiom::SchemaTableName>
    : fmt::formatter<std::string_view> {
  auto format(const facebook::axiom::SchemaTableName& name, format_context& ctx)
      const {
    return fmt::format_to(ctx.out(), R"d("{}"."{}")d", name.schema, name.table);
  }
};
