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
#include <utility>
#include <vector>

#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/optimizer/DerivedTable.h"

namespace facebook::axiom::optimizer {

/// Generates EXPLAIN IO JSON from a set of input tables and their scan filters.
///
/// For each (BaseTable, filters) pair, extracts column constraints from
/// 'filters' for columns marked with includeInExplainIo (e.g., partition
/// columns), grouping and merging by connector table. If 'outputTable' is
/// provided (INSERT/CTAS), includes it in the JSON output. This is the shared
/// core used by both the v1 (DerivedTable) and v2 (Scan tree) optimizers.
std::string explainIo(
    const std::vector<std::pair<BaseTableCP, ExprVector>>& tableFilters,
    std::optional<CatalogSchemaTableName> outputTable = std::nullopt);

/// Generates EXPLAIN IO JSON from the v1 query graph by walking the
/// DerivedTable tree to find input tables and reading each table's
/// columnFilters.
std::string explainIo(
    DerivedTableCP rootDt,
    std::optional<CatalogSchemaTableName> outputTable = std::nullopt);

} // namespace facebook::axiom::optimizer
