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
#include "axiom/sql/presto/ParserUtils.h"

#include "velox/parse/IExpr.h"

namespace axiom::sql::presto {

std::vector<size_t> widenProjectionsForSort(
    std::vector<lp::ExprApi>& projections,
    const std::vector<lp::SortKey>& sortKeys,
    const std::vector<size_t>& preResolvedOrdinals) {
  std::vector<size_t> ordinals;
  ordinals.reserve(sortKeys.size());

  facebook::velox::core::ExprMap<size_t> projectionMap;
  for (size_t i = 0; i < projections.size(); ++i) {
    projectionMap.emplace(projections[i].expr(), i + 1);
  }

  for (size_t i = 0; i < sortKeys.size(); ++i) {
    // Use pre-resolved ordinal if available.
    if (i < preResolvedOrdinals.size() && preResolvedOrdinals[i] != 0) {
      ordinals.push_back(preResolvedOrdinals[i]);
      continue;
    }

    auto [projectionIt, inserted] =
        projectionMap.emplace(sortKeys[i].expr.expr(), projections.size() + 1);
    if (inserted) {
      // Pointer-based lookup failed. Try name-based matching for aliased
      // expressions. This handles the case where expressions were rewritten
      // (e.g., after GROUP BY) and pointer identity is lost, but the name
      // (alias) is preserved.
      std::optional<size_t> matchedOrdinal;
      const auto& sortKeyName = sortKeys[i].expr.name();
      if (sortKeyName.has_value() && !sortKeyName.value().empty()) {
        for (size_t j = 0; j < projections.size(); ++j) {
          if (projections[j].name() == sortKeyName) {
            if (matchedOrdinal.has_value()) {
              VELOX_USER_FAIL("Column is ambiguous: {}", sortKeyName.value());
            }
            matchedOrdinal = j + 1;
          }
        }
      }

      if (matchedOrdinal.has_value()) {
        // Found by name - use existing projection.
        ordinals.push_back(matchedOrdinal.value());
        // Remove the spurious entry we just added to the map.
        projectionMap.erase(sortKeys[i].expr.expr());
      } else {
        // Not found by pointer or name - append to projections.
        ordinals.push_back(projections.size() + 1);
        projections.push_back(sortKeys[i].expr);
      }
    } else {
      // Pointer-based lookup succeeded. Check for name ambiguity.
      const auto& sortKeyName = sortKeys[i].expr.name();
      if (sortKeyName.has_value() && !sortKeyName.value().empty()) {
        size_t matchCount = 0;
        for (size_t j = 0; j < projections.size(); ++j) {
          if (projections[j].name() == sortKeyName) {
            ++matchCount;
            if (matchCount > 1) {
              VELOX_USER_FAIL("Column is ambiguous: {}", sortKeyName.value());
            }
          }
        }
      }
      ordinals.push_back(projectionIt->second);
    }
  }

  return ordinals;
}

void sortAndTrimProjections(
    lp::PlanBuilder& builder,
    const std::vector<lp::SortKey>& sortKeys,
    const std::vector<size_t>& sortKeyOrdinals,
    size_t numOutputColumns) {
  if (sortKeys.empty()) {
    return;
  }

  // Resolve sort key ordinals to output column names.
  std::vector<lp::SortKey> resolvedKeys;
  resolvedKeys.reserve(sortKeys.size());
  for (size_t i = 0; i < sortKeys.size(); ++i) {
    const auto name = builder.findOrAssignOutputNameAt(sortKeyOrdinals[i] - 1);
    resolvedKeys.emplace_back(
        lp::Col(name), sortKeys[i].ascending, sortKeys[i].nullsFirst);
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
