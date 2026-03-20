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

#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstSupport.h"

namespace axiom::sql::presto {

class SortProjection {
 public:
  /// Matches sort key expressions against items in the SELECT list. For
  /// unmatched expressions, appends them to `projections`, widening the
  /// projection list. Returns a 1-based ordinal for each sort key in the
  /// widened projection list.
  ///
  /// @param sortKeyExprs List of expressions from the ORDER BY list. Keys not
  /// in the projection list (above) are considered unmatched and used to widen
  /// the projection list.
  /// @param preResolvedOrdinals 1-based ordinals for sort keys that are already
  /// resolved. Must be the same size as sortKeyExprs. A value of 0 means
  /// unresolved. All non-zero values must map to valid positions in projections
  /// (1-based).
  /// @param projections Mutable list of expressions from the SELECT list. This
  /// is where unmatched sort key expressions are appended. All pre-resolved
  /// ordinals should match to a projection here.
  static std::vector<size_t> widenProjections(
      const std::vector<facebook::axiom::logical_plan::ExprApi>& sortKeyExprs,
      const std::vector<size_t>& preResolvedOrdinals,
      std::vector<facebook::axiom::logical_plan::ExprApi>& projections);

  /// Adds a SortNode using sort keys resolved by ordinals, then drops any extra
  /// columns that were added for sorting.
  ///
  /// @param builder The plan builder to add a sort to.
  /// @param sortItems List of sort items from the ORDER BY clause. This should
  /// be non-empty.
  /// @param sortKeyOrdinals 1-based ordinals from the widened projection list,
  /// used to match the sort key with its corresponding ordinal.
  /// @param numOutputColumns Number of columns in the final output. Extra
  /// columns that were appended to the projection list for sorting can be
  /// trimmed from the end, keeping only numOutputColumns in the final
  /// projection.
  static void sortAndTrim(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      const std::vector<std::shared_ptr<SortItem>>& sortItems,
      const std::vector<size_t>& sortKeyOrdinals,
      size_t numOutputColumns);
};

} // namespace axiom::sql::presto
