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

#include <cstdint>
#include <optional>
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

  /// Snapshot the reference is pinned to, when the query asked for a specific
  /// table version (Iceberg time travel, `FOR VERSION AS OF <id>`). Empty for
  /// an ordinary reference, which reads the current version. Part of the
  /// identity: two references differing only by snapshot are different tables,
  /// so a version-aware connector serves each its own state and the optimizer
  /// caches them apart.
  std::optional<int64_t> snapshotId;

  /// Returns "schema"."table" for display/logging only, with an `@<id>` suffix
  /// when pinned to a snapshot.
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
    if (name.snapshotId.has_value()) {
      return fmt::format_to(
          ctx.out(),
          R"d("{}"."{}"@{})d",
          name.schema,
          name.table,
          *name.snapshotId);
    }
    return fmt::format_to(ctx.out(), R"d("{}"."{}")d", name.schema, name.table);
  }
};
