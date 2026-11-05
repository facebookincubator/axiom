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
#include "axiom/sql/presto/ColumnExprs.h"

#include <fmt/format.h>

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

// static
std::vector<lp::ExprApi> ColumnExprs::renamed(
    const std::vector<std::string>& names,
    std::string_view prefix) {
  std::vector<lp::ExprApi> exprs;
  exprs.reserve(names.size());
  for (const auto& name : names) {
    exprs.push_back(lp::Col(name).as(fmt::format("{}{}", prefix, name)));
  }
  return exprs;
}

// static
std::vector<lp::ExprApi> ColumnExprs::concat(
    std::vector<lp::ExprApi> exprs,
    const std::vector<lp::ExprApi>& more) {
  exprs.insert(exprs.end(), more.begin(), more.end());
  return exprs;
}

// static
std::optional<lp::ExprApi> ColumnExprs::equal(
    const std::vector<std::string>& names,
    std::string_view prefix) {
  std::optional<lp::ExprApi> condition;
  for (const auto& name : names) {
    auto columnsEqual =
        lp::Col(name) == lp::Col(fmt::format("{}{}", prefix, name));
    condition =
        condition.has_value() ? *condition && columnsEqual : columnsEqual;
  }
  return condition;
}

} // namespace axiom::sql::presto
