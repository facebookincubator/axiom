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

/// Structured representation of a schema-qualified type name.
/// Parallels SchemaTableName for user-defined types (e.g., enums).
struct SchemaTypeName {
  std::string schema;
  std::string type;

  /// Returns "schema"."type" for display/logging only.
  std::string toString() const;

  bool operator==(const SchemaTypeName&) const = default;
};

// Used by gtest to print values in assertion failures.
inline std::ostream& operator<<(std::ostream& os, const SchemaTypeName& name) {
  return os << name.toString();
}

} // namespace facebook::axiom

template <>
struct std::hash<facebook::axiom::SchemaTypeName> {
  size_t operator()(const facebook::axiom::SchemaTypeName& name) const;
};

template <>
struct fmt::formatter<facebook::axiom::SchemaTypeName>
    : fmt::formatter<std::string_view> {
  auto format(const facebook::axiom::SchemaTypeName& name, format_context& ctx)
      const {
    return fmt::format_to(ctx.out(), R"d("{}"."{}")d", name.schema, name.type);
  }
};
