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

#include "axiom/sql/presto/DisplayNames.h"

#include <memory>

#include <folly/container/F14Map.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstNode.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

// static
std::optional<std::string> DisplayNames::displayName(
    const SingleColumn& singleColumn) {
  if (singleColumn.alias() != nullptr) {
    return singleColumn.alias()->value();
  }
  if (singleColumn.expression()->is(NodeType::kIdentifier)) {
    return singleColumn.expression()->as<Identifier>()->value();
  }
  if (singleColumn.expression()->is(NodeType::kDereferenceExpression)) {
    // For chained dereferences (a.b.c), field() is the rightmost.
    return singleColumn.expression()
        ->as<DereferenceExpression>()
        ->field()
        ->value();
  }
  return std::nullopt;
}

std::optional<std::string> DisplayNames::displayName(
    const std::optional<std::string>& alias,
    const std::string& name) const {
  if (auto it = accumulatedNames.find({alias, name});
      it != accumulatedNames.end()) {
    return it->second;
  }
  if (alias.has_value()) {
    if (auto it = accumulatedNames.find({std::nullopt, name});
        it != accumulatedNames.end()) {
      return it->second;
    }
  }
  return std::nullopt;
}

void DisplayNames::captureLastNames(const lp::PlanBuilder& builder) {
  std::vector<std::optional<std::string>> names =
      builder.outputNames(/*includeHiddenColumns=*/false);
  lastNames.clear();
  lastNames.reserve(names.size());
  for (const auto& name : names) {
    lastNames.push_back(
        name.has_value() ? displayName(std::nullopt, *name) : std::nullopt);
  }
}

void DisplayNames::captureLastNames(const std::vector<IdentifierPtr>& aliases) {
  lastNames.clear();
  lastNames.reserve(aliases.size());
  for (const auto& id : aliases) {
    lastNames.emplace_back(id->value());
  }
}

void DisplayNames::captureLastNames(const std::vector<SelectItemPtr>& items) {
  lastNames.clear();
  lastNames.reserve(items.size());
  for (const auto& item : items) {
    lastNames.push_back(displayName(*item->as<SingleColumn>()));
  }
}

void DisplayNames::accumulate(
    const lp::PlanBuilder& builder,
    const std::optional<std::string>& relationAlias) {
  accumulateFrom(builder, relationAlias, 0);
}

void DisplayNames::accumulateFrom(
    const lp::PlanBuilder& builder,
    const std::optional<std::string>& relationAlias,
    size_t firstColumn) {
  if (lastNames.empty()) {
    return;
  }

  std::vector<std::optional<std::string>> names =
      builder.outputNames(/*includeHiddenColumns=*/false);
  VELOX_CHECK_EQ(names.size(), lastNames.size());
  for (size_t i = firstColumn; i < names.size(); ++i) {
    const auto& name = names[i];
    const auto& display = lastNames[i];
    if (!display.has_value() || !name.has_value()) {
      continue;
    }

    accumulatedNames[{relationAlias, *name}] = *display;
    if (relationAlias.has_value()) {
      // Also key by nullopt so unprefixed lookups find the same entry.
      accumulatedNames[{std::nullopt, *name}] = *display;
    }
  }
}

} // namespace axiom::sql::presto
