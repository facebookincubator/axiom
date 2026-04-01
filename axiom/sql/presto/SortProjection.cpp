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
#include "axiom/sql/presto/SortProjection.h"

#include "folly/container/F14Map.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/IExpr.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

namespace {
namespace core = facebook::velox::core;

// Recursively replaces root FieldAccessExpr nodes that match an output alias
// with the alias's underlying expression. Throws if the alias is ambiguous.
core::ExprPtr replaceAliases(
    const core::ExprPtr& expr,
    const folly::F14FastMap<std::string, size_t>& aliasMap,
    const std::vector<lp::ExprApi>& projections) {
  if (expr->is(core::IExpr::Kind::kFieldAccess)) {
    auto* field = expr->as<core::FieldAccessExpr>();
    if (field->isRootColumn()) {
      auto alias = aliasMap.find(field->name());
      if (alias != aliasMap.end()) {
        VELOX_USER_CHECK_NE(
            alias->second, 0, "Column is ambiguous: {}", field->name());
        return projections[alias->second - 1].expr();
      }
      return expr;
    }
  }

  const auto& inputs = expr->inputs();
  if (inputs.empty()) {
    return expr;
  }

  std::vector<core::ExprPtr> newInputs;
  bool changed = false;
  for (const auto& input : inputs) {
    auto newInput = replaceAliases(input, aliasMap, projections);
    if (newInput.get() != input.get()) {
      changed = true;
    }
    newInputs.push_back(std::move(newInput));
  }

  return changed ? expr->replaceInputs(std::move(newInputs)) : expr;
}
} // namespace

std::vector<size_t> SortProjection::widenProjections(
    const std::vector<lp::ExprApi>& sortKeyExprs,
    const std::vector<size_t>& preResolvedOrdinals,
    std::vector<lp::ExprApi>& projections) {
  VELOX_CHECK_EQ(
      preResolvedOrdinals.size(),
      sortKeyExprs.size(),
      "Must be the same size as sortKeyExprs.");
  std::vector<size_t> ordinals;
  ordinals.reserve(sortKeyExprs.size());

  facebook::velox::core::ExprMap<size_t> projectionMap;
  folly::F14FastMap<std::string, size_t> aliasMap;
  // Iterate through projections matching ordinals to projections and aliases.
  for (size_t i = 0; i < projections.size(); ++i) {
    projectionMap.emplace(projections[i].expr(), i + 1);
    if (projections[i].alias().has_value()) {
      auto [alias, inserted] =
          aliasMap.emplace(projections[i].alias().value(), i + 1);
      // We throw for ambiguous aliases only if they are referenced by a
      // sorting key. Let's mark it as ambiguous here for now denoted by the
      // '0' ordinal and throw later when we process the sorting keys.
      if (!inserted) {
        alias->second = 0;
      }
    }
  }

  // Iterate through sort keys matching them to ordinals.
  for (size_t i = 0; i < sortKeyExprs.size(); ++i) {
    // Use pre-resolved ordinal if available.
    if (preResolvedOrdinals[i] != 0) {
      ordinals.push_back(preResolvedOrdinals[i]);
    } else {
      // Match alias if one is used.
      const auto aliasMapValue = sortKeyExprs[i].alias().has_value()
          ? aliasMap.find(sortKeyExprs[i].alias().value())
          : aliasMap.end();
      if (aliasMapValue != aliasMap.end()) {
        auto ordinal = aliasMapValue->second;
        VELOX_USER_CHECK_NE(
            ordinal, 0, "Column is ambiguous: {}", aliasMapValue->first);
        ordinals.push_back(ordinal);
      }
      // Match expression directly if no alias is used, expanding out
      // projections list if sorts keys don't match our SELECT list.
      else {
        auto resolved =
            replaceAliases(sortKeyExprs[i].expr(), aliasMap, projections);
        auto [projectionIt, inserted] =
            projectionMap.emplace(resolved, projections.size() + 1);
        if (inserted) {
          ordinals.push_back(projections.size() + 1);
          lp::ExprApi newProjection(resolved, sortKeyExprs[i].alias());
          if (sortKeyExprs[i].windowSpec()) {
            newProjection = newProjection.over(*sortKeyExprs[i].windowSpec());
          }
          projections.push_back(std::move(newProjection));
        } else {
          ordinals.push_back(projectionIt->second);
        }
      }
    }
  }

  return ordinals;
}

void SortProjection::sortAndTrim(
    lp::PlanBuilder& builder,
    const std::vector<std::shared_ptr<SortItem>>& sortItems,
    const std::vector<size_t>& sortKeyOrdinals,
    size_t numOutputColumns) {
  VELOX_CHECK(!sortItems.empty());

  // Resolve sort key ordinals to output column names.
  std::vector<lp::SortKey> resolvedKeys;
  resolvedKeys.reserve(sortItems.size());
  for (size_t i = 0; i < sortItems.size(); ++i) {
    const auto name = builder.findOrAssignOutputNameAt(sortKeyOrdinals[i] - 1);
    const auto& item = sortItems[i];
    resolvedKeys.emplace_back(
        lp::Col(name), item->isAscending(), item->isNullsFirst());
  }

  builder.sort(resolvedKeys);

  // Only trim if extra columns were added for sorting.
  if (numOutputColumns < builder.numOutput()) {
    std::vector<lp::ExprApi> finalProjections;
    finalProjections.reserve(numOutputColumns);
    for (size_t i = 0; i < numOutputColumns; ++i) {
      finalProjections.emplace_back(
          lp::Col(builder.findOrAssignOutputNameAt(i)));
    }
    builder.project(finalProjections);
  }
}

} // namespace axiom::sql::presto
