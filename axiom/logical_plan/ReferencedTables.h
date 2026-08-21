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
#include <unordered_set>
#include "axiom/common/CatalogSchemaTableName.h"

namespace facebook::axiom::logical_plan {

/// Tables referenced by a query, extracted from its SQL text or its logical
/// plan.
///
/// Each table is a CatalogSchemaTableName with three parts:
///   - catalogName: the connector ID (e.g., "prism", "impulse", "tpch").
///   - schemaTableName.schema: the schema or namespace (e.g., "di", "default").
///   - schemaTableName.table: the table name (e.g., "orders", "lineitem").
///
/// Both fields are complete: empty means the query touches nothing, never that
/// the producer could not tell. A producer that cannot name everything a query
/// reads or writes must fail instead of reporting less.
struct ReferencedTables {
  /// Every table the query reads.
  std::unordered_set<CatalogSchemaTableName> inputTables;

  /// The table the query writes.
  std::optional<CatalogSchemaTableName> outputTable;
};

} // namespace facebook::axiom::logical_plan
