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

namespace axiom::sql::presto {

/// Lists of column references, for the plans the parser builds without SQL
/// text.
class ColumnExprs {
 public:
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
};

} // namespace axiom::sql::presto
