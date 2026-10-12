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

#include <optional>
#include <string>
#include <utility>

#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstSupport.h"
#include "folly/container/F14Set.h"
#include "folly/container/HeterogeneousAccess.h"
#include "folly/lang/Assume.h"
#include "folly/lang/Hint.h"
#include "velox/common/base/Exceptions.h"
#include "velox/parse/IExpr.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

namespace {
namespace core = facebook::velox::core;

// Indexes the SELECT-list projections by expression for matching ORDER BY
// sort keys. A sort key naming an output alias had it substituted during
// translation, so it arrives as that item's expression and matches here.
struct ProjectionIndex {
  core::ExprMap<size_t> byExpr;
  folly::F14FastSet<std::string> aliases;

  explicit ProjectionIndex(const std::vector<lp::ExprApi>& projections) {
    for (size_t i = 0; i < projections.size(); ++i) {
      byExpr.emplace(projections[i].expr(), i + 1);
      if (projections[i].alias().has_value()) {
        aliases.insert(projections[i].alias().value());
      }
    }
  }
};

// Looks up a single sort key in 'index'. Returns the matching 1-based
// ordinal, or 0 if not present (caller decides whether to widen or fail).
// 'preResolvedOrdinal' short-circuits the lookup when the sort key was
// already resolved (e.g., from a positional ORDER BY ordinal).
size_t lookupSortKey(
    const lp::ExprApi& sortKey,
    size_t preResolvedOrdinal,
    const ProjectionIndex& index) {
  if (preResolvedOrdinal != 0) {
    return preResolvedOrdinal;
  }

  auto it = index.byExpr.find(sortKey.expr());
  return it != index.byExpr.end() ? it->second : 0;
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

  ProjectionIndex index{projections};
  std::vector<size_t> ordinals;
  ordinals.reserve(sortKeyExprs.size());

  // Output names already claimed, by a SELECT item or by a key widened below.
  folly::F14FastSet<std::string> claimedNames;
  for (const auto& name : index.aliases) {
    claimedNames.insert(name);
  }

  for (size_t i = 0; i < sortKeyExprs.size(); ++i) {
    auto ordinal =
        lookupSortKey(sortKeyExprs[i], preResolvedOrdinals[i], index);
    if (ordinal != 0) {
      ordinals.push_back(ordinal);
      continue;
    }
    // Sort key not present in the SELECT list; widen the projection.
    ordinal = projections.size() + 1;
    index.byExpr.emplace(sortKeyExprs[i].expr(), ordinal);

    // A name another output column already carries would leave both
    // unresolvable, so the column is projected unnamed and trimmed after the
    // sort. Empty, not nullopt: nullopt makes a column reference an identity
    // projection, which keeps the very name being avoided here.
    std::optional<std::string> alias{sortKeyExprs[i].alias()};
    if (alias.has_value() && !claimedNames.insert(alias.value()).second) {
      alias = "";
    }

    projections.emplace_back(sortKeyExprs[i].expr(), std::move(alias));
    ordinals.push_back(ordinal);
  }

  return ordinals;
}

std::vector<size_t> SortProjection::resolveSortKeys(
    const std::vector<lp::ExprApi>& sortKeyExprs,
    const std::vector<size_t>& preResolvedOrdinals,
    const std::vector<lp::ExprApi>& projections,
    const std::function<void(size_t)>& onUnresolved) {
  VELOX_CHECK_EQ(
      preResolvedOrdinals.size(),
      sortKeyExprs.size(),
      "Must be the same size as sortKeyExprs.");

  ProjectionIndex index{projections};
  std::vector<size_t> ordinals;
  ordinals.reserve(sortKeyExprs.size());

  for (size_t i = 0; i < sortKeyExprs.size(); ++i) {
    auto ordinal =
        lookupSortKey(sortKeyExprs[i], preResolvedOrdinals[i], index);
    if (ordinal == 0) {
      onUnresolved(i);
      VELOX_FAIL("onUnresolved callback must throw");
    }
    ordinals.push_back(ordinal);
  }

  return ordinals;
}

void SortProjection::sortAndTrim(
    lp::PlanBuilder& builder,
    const std::vector<std::shared_ptr<SortItem>>& sortItems,
    const std::vector<size_t>& sortKeyOrdinals,
    size_t numOutputColumns) {
  VELOX_CHECK(!sortItems.empty());
  VELOX_CHECK_EQ(sortItems.size(), sortKeyOrdinals.size());

  std::vector<bool> ascending;
  std::vector<bool> nullsFirst;
  ascending.reserve(sortItems.size());
  nullsFirst.reserve(sortItems.size());
  for (const auto& item : sortItems) {
    ascending.push_back(item->isAscending());
    nullsFirst.push_back(item->isNullsFirst());
  }

  sortAndTrim(
      builder, sortKeyOrdinals, ascending, nullsFirst, numOutputColumns);
}

void SortProjection::sortAndTrim(
    lp::PlanBuilder& builder,
    const std::vector<size_t>& sortKeyOrdinals,
    const std::vector<bool>& ascending,
    const std::vector<bool>& nullsFirst,
    size_t numOutputColumns) {
  VELOX_CHECK(!sortKeyOrdinals.empty());
  VELOX_CHECK_EQ(ascending.size(), sortKeyOrdinals.size());
  VELOX_CHECK_EQ(nullsFirst.size(), sortKeyOrdinals.size());

  // Resolve sort key ordinals to output column names.
  std::vector<lp::SortKey> resolvedKeys;
  resolvedKeys.reserve(sortKeyOrdinals.size());
  for (size_t i = 0; i < sortKeyOrdinals.size(); ++i) {
    const auto column =
        builder.findOrAssignOutputNameAt(sortKeyOrdinals[i] - 1);
    resolvedKeys.emplace_back(column.toCol(), ascending[i], nullsFirst[i]);
  }

  builder.sort(resolvedKeys);

  // Only trim if extra columns were added for sorting.
  if (numOutputColumns < builder.numOutput()) {
    std::vector<lp::ExprApi> finalProjections;
    finalProjections.reserve(numOutputColumns);
    for (size_t i = 0; i < numOutputColumns; ++i) {
      finalProjections.emplace_back(
          builder.findOrAssignOutputNameAt(i).toCol());
    }
    builder.project(finalProjections);
  }
}

} // namespace axiom::sql::presto
