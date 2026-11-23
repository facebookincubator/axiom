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
#include <functional>

#include "axiom/logical_plan/NameMappings.h"
#include "axiom/logical_plan/PlanBuilder.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

namespace {

// Makes a reference to the column named 'name'.
using ColumnRef = std::function<lp::ExprApi(std::string_view name)>;

lp::ExprApi byName(std::string_view name) {
  return lp::Col(name);
}

ColumnRef byId(const lp::PlanBuilder& plan) {
  return [&plan](std::string_view name) {
    return ColumnExprs::column(plan, name);
  };
}

std::vector<lp::ExprApi> renamedColumns(
    const ColumnRef& column,
    const std::vector<std::string>& names,
    std::string_view prefix) {
  std::vector<lp::ExprApi> exprs;
  exprs.reserve(names.size());
  for (const auto& name : names) {
    exprs.push_back(column(name).as(fmt::format("{}{}", prefix, name)));
  }
  return exprs;
}

std::optional<lp::ExprApi> equalColumns(
    const ColumnRef& left,
    const ColumnRef& right,
    const std::vector<std::string>& names,
    std::string_view prefix) {
  std::optional<lp::ExprApi> condition;
  for (const auto& name : names) {
    auto columnsEqual = left(name) == right(fmt::format("{}{}", prefix, name));
    condition =
        condition.has_value() ? *condition && columnsEqual : columnsEqual;
  }
  return condition;
}

lp::ExprApi columnsMessage(
    const ColumnRef& column,
    std::string_view prefix,
    const std::vector<std::string>& names) {
  std::vector<lp::ExprApi> parts{lp::Lit(prefix)};
  for (const auto& name : names) {
    if (parts.size() > 1) {
      parts.push_back(lp::Lit(", "));
    }
    parts.push_back(lp::Cast(facebook::velox::VARCHAR(), column(name)));
  }
  return lp::Call("concat", parts);
}

} // namespace

// static
std::string ColumnExprs::id(
    const lp::PlanBuilder& plan,
    std::string_view name) {
  const auto* names = plan.outputMapping();
  auto id = names != nullptr ? names->lookup(std::string(name)) : std::nullopt;
  VELOX_CHECK(id.has_value(), "Column not found: {}", name);
  return std::move(id.value());
}

// static
std::vector<lp::ExprApi> ColumnExprs::renamed(
    const lp::PlanBuilder& plan,
    const std::vector<std::string>& names,
    std::string_view prefix) {
  return renamedColumns(byId(plan), names, prefix);
}

// static
std::optional<lp::ExprApi> ColumnExprs::equal(
    const lp::PlanBuilder& left,
    const lp::PlanBuilder& right,
    const std::vector<std::string>& names,
    std::string_view prefix) {
  return equalColumns(byId(left), byId(right), names, prefix);
}

// static
lp::ExprApi ColumnExprs::message(
    const lp::PlanBuilder& plan,
    std::string_view prefix,
    const std::vector<std::string>& names) {
  return columnsMessage(byId(plan), prefix, names);
}

// static
std::vector<lp::ExprApi> ColumnExprs::renamed(
    const std::vector<std::string>& names,
    std::string_view prefix) {
  return renamedColumns(byName, names, prefix);
}

// static
std::vector<lp::ExprApi> ColumnExprs::concat(
    std::vector<lp::ExprApi> exprs,
    const std::vector<lp::ExprApi>& more) {
  exprs.insert(exprs.end(), more.begin(), more.end());
  return exprs;
}

// static
std::vector<lp::ExprApi> ColumnExprs::concat(
    std::vector<lp::ExprApi> exprs,
    const lp::ExprApi& expr) {
  exprs.push_back(expr);
  return exprs;
}

// static
std::optional<lp::ExprApi> ColumnExprs::equal(
    const std::vector<std::string>& names,
    std::string_view prefix) {
  return equalColumns(byName, byName, names, prefix);
}

// static
lp::ExprApi ColumnExprs::message(
    std::string_view prefix,
    const std::vector<std::string>& names) {
  return columnsMessage(byName, prefix, names);
}

} // namespace axiom::sql::presto
