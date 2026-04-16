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
#include <vector>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstNode.h"

namespace axiom::sql::presto {

/// Expands COLUMNS('regex') pseudo-function calls in expressions and matches
/// output columns by regex pattern.
class ColumnsExpansion {
 public:
  /// Filters output columns by regex pattern. Returns matching columns.
  /// Optionally restricts to columns from a specific table prefix.
  static std::vector<
      facebook::axiom::logical_plan::PlanBuilder::OutputColumnName>
  matchByRegex(
      const facebook::axiom::logical_plan::PlanBuilder& builder,
      const std::string& pattern,
      const std::optional<std::string>& prefix,
      NodeLocation location);

  /// Expands COLUMNS('regex') pseudo-function calls in an expression. Each
  /// COLUMNS() call is matched against the builder's visible output columns.
  /// When multiple COLUMNS() calls appear in the same expression, they are
  /// expanded pairwise (zip): all calls must match the same number of columns,
  /// and the i-th output replaces every COLUMNS() call with its i-th match.
  ///
  /// Returns one expression per matched column position. Returns empty if the
  /// expression contains no COLUMNS() calls.
  static std::vector<facebook::axiom::logical_plan::ExprApi> expand(
      const facebook::axiom::logical_plan::ExprApi& expr,
      const facebook::axiom::logical_plan::PlanBuilder& builder,
      NodeLocation location);
};

} // namespace axiom::sql::presto
