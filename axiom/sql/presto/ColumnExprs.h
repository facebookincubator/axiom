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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "axiom/logical_plan/ExprApi.h"

namespace facebook::axiom::logical_plan {
class PlanBuilder;
} // namespace facebook::axiom::logical_plan

namespace axiom::sql::presto {

/// Lists of column references, for plans built without SQL text. The
/// overloads that take a plan refer to its columns by ID, for a plan that
/// resolves columns by ID (see PlanBuilder::Context::columnsById). The others
/// refer to columns by name.
class ColumnExprs {
 public:
  /// Returns the ID of the output column of 'plan' named 'name'. Throws if
  /// there is none.
  static std::string id(
      const facebook::axiom::logical_plan::PlanBuilder& plan,
      std::string_view name);

  /// Returns a reference to the output column of 'plan' named 'name', by ID.
  static facebook::axiom::logical_plan::ExprApi column(
      const facebook::axiom::logical_plan::PlanBuilder& plan,
      std::string_view name) {
    return facebook::axiom::logical_plan::Col(id(plan, name)).as(name);
  }

  /// Returns a reference to each column of 'plan' named in 'names', by ID,
  /// named 'prefix' + its name.
  static std::vector<facebook::axiom::logical_plan::ExprApi> renamed(
      const facebook::axiom::logical_plan::PlanBuilder& plan,
      const std::vector<std::string>& names,
      std::string_view prefix);

  /// Returns a reference to each column of 'plan' named in 'names', by ID.
  static std::vector<facebook::axiom::logical_plan::ExprApi> of(
      const facebook::axiom::logical_plan::PlanBuilder& plan,
      const std::vector<std::string>& names) {
    return renamed(plan, names, "");
  }

  /// Returns the conjunction of each column of 'left' named in 'names' = the
  /// column of 'right' named 'prefix' + its name, for a join of the two;
  /// nullopt when 'names' is empty.
  static std::optional<facebook::axiom::logical_plan::ExprApi> equal(
      const facebook::axiom::logical_plan::PlanBuilder& left,
      const facebook::axiom::logical_plan::PlanBuilder& right,
      const std::vector<std::string>& names,
      std::string_view prefix);

  /// Returns message(prefix, names) over the columns of 'plan'.
  static facebook::axiom::logical_plan::ExprApi message(
      const facebook::axiom::logical_plan::PlanBuilder& plan,
      std::string_view prefix,
      const std::vector<std::string>& names);

  /// Returns a reference to each column of 'names', named 'prefix' + its name.
  static std::vector<facebook::axiom::logical_plan::ExprApi> renamed(
      const std::vector<std::string>& names,
      std::string_view prefix);

  /// Returns a reference to each column of 'names'.
  static std::vector<facebook::axiom::logical_plan::ExprApi> of(
      const std::vector<std::string>& names) {
    return renamed(names, "");
  }

  /// Returns 'exprs' followed by 'more'.
  static std::vector<facebook::axiom::logical_plan::ExprApi> concat(
      std::vector<facebook::axiom::logical_plan::ExprApi> exprs,
      const std::vector<facebook::axiom::logical_plan::ExprApi>& more);

  /// Returns 'exprs' followed by 'expr'.
  static std::vector<facebook::axiom::logical_plan::ExprApi> concat(
      std::vector<facebook::axiom::logical_plan::ExprApi> exprs,
      const facebook::axiom::logical_plan::ExprApi& expr);

  /// Returns the conjunction of each column of 'names' = the column named
  /// 'prefix' + its name, or nullopt when 'names' is empty.
  static std::optional<facebook::axiom::logical_plan::ExprApi> equal(
      const std::vector<std::string>& names,
      std::string_view prefix);

  /// Returns 'prefix' followed by the values of the columns 'names', for an
  /// error message that names a row:
  ///   concat(prefix, CAST(c0 AS VARCHAR), ', ', CAST(c1 AS VARCHAR), ...)
  static facebook::axiom::logical_plan::ExprApi message(
      std::string_view prefix,
      const std::vector<std::string>& names);
};

} // namespace axiom::sql::presto
