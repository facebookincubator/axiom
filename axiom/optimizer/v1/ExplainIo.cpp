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

#include "axiom/optimizer/v1/ExplainIo.h"

#include "axiom/optimizer/ExplainIo.h"

namespace facebook::axiom::optimizer::v1 {

namespace {
// Recursively collects all BaseTables from a DerivedTable tree.
void collectBaseTables(
    DerivedTableCP dt,
    std::vector<BaseTableCP>& baseTables) {
  for (auto* table : dt->tables) {
    if (table->is(PlanType::kTableNode)) {
      baseTables.push_back(table->as<BaseTable>());
    } else if (table->is(PlanType::kDerivedTableNode)) {
      collectBaseTables(table->as<DerivedTable>(), baseTables);
    }
  }
  for (auto* child : dt->unionInputs) {
    collectBaseTables(child, baseTables);
  }
}
} // namespace

std::string explainIo(
    DerivedTableCP rootDt,
    std::optional<CatalogSchemaTableName> outputTable) {
  std::vector<BaseTableCP> baseTables;
  collectBaseTables(rootDt, baseTables);

  std::vector<std::pair<BaseTableCP, ExprVector>> tableFilters;
  tableFilters.reserve(baseTables.size());
  for (auto* baseTable : baseTables) {
    // baseTable->filter holds the multi-column residual conjuncts (cross-column
    // disjunctions); combine them with the single-column columnFilters.
    ExprVector filters = baseTable->columnFilters;
    for (auto* conjunct : baseTable->filter) {
      filters.push_back(conjunct);
    }
    tableFilters.emplace_back(baseTable, std::move(filters));
  }
  return optimizer::explainIo(tableFilters, std::move(outputTable));
}

} // namespace facebook::axiom::optimizer::v1
