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

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Matches sort key expressions against projection expressions by identity.
/// For unmatched sort keys, appends their expressions to `projections`.
/// Returns a 1-based ordinal for each sort key in the (possibly widened)
/// projection list.
std::vector<size_t> widenProjectionsForSort(
    std::vector<lp::ExprApi>& projections,
    const std::vector<lp::SortKey>& sortKeys,
    const std::vector<size_t>& preResolvedOrdinals = {});

/// Adds a SortNode using sort keys resolved by ordinal position in the
/// builder's current output, then drops any extra columns beyond
/// `numOutputColumns` with a final ProjectNode if needed.
void sortAndTrimProjections(
    lp::PlanBuilder& builder,
    const std::vector<lp::SortKey>& sortKeys,
    const std::vector<size_t>& sortKeyOrdinals,
    size_t numOutputColumns);

} // namespace axiom::sql::presto
