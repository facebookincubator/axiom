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
#include <optional>
#include <string>
#include <vector>

#include "axiom/logical_plan/NameMappings.h"
#include "velox/type/Type.h"

namespace axiom::sql::presto {

/// The columns a query block's expressions bind names against, beyond the
/// block's own input: the names of each relation in scope, captured when the
/// scope is entered, and the scope enclosing this one.
///
/// A subquery binds against the FROM clause of the block it appears in, as
/// that block's FROM produced its columns, not against what the block builds
/// on top of them later, such as an aggregation. A JOIN ON condition binds
/// against both sides; an unqualified name on both is ambiguous.
///
///   BindingScope scope{.relations = {{names, type}}, .parent = outer};
///   auto ids = scope.lookup(std::nullopt, "a");
struct BindingScope {
  /// A relation in scope: the names and the types of its columns.
  struct Relation {
    facebook::axiom::logical_plan::NameMappings names;
    facebook::velox::RowTypePtr type;
  };

  /// The relations in scope, one per JOIN side, else one.
  std::vector<Relation> relations;

  /// The scope enclosing this one, or null.
  std::shared_ptr<const BindingScope> parent;

  /// Returns the ID 'qualifier.name', or 'name' if there is no qualifier,
  /// names in 'names', or nullopt.
  static std::optional<std::string> lookup(
      const facebook::axiom::logical_plan::NameMappings& names,
      const std::optional<std::string>& qualifier,
      const std::string& name);

  /// Returns the IDs 'qualifier.name', or 'name' if there is no qualifier,
  /// names in this scope alone: none, one, or several when an unqualified name
  /// names a column of more than one relation.
  std::vector<std::string> lookup(
      const std::optional<std::string>& qualifier,
      const std::string& name) const;

  /// Returns true if 'qualifier.name', or 'name', names more than one column
  /// of a relation in this scope alone.
  bool isAmbiguous(
      const std::optional<std::string>& qualifier,
      const std::string& name) const;

  /// Returns the type of the column 'id' of a relation in this scope alone,
  /// or nullptr.
  facebook::velox::TypePtr typeOf(const std::string& id) const;
};

} // namespace axiom::sql::presto
