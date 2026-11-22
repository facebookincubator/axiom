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
#include "axiom/sql/presto/SqlStatement.h"

namespace axiom::sql::presto {

/// Table names read from SQL text without looking any table up.
///
/// Each name is (connector, schema, table): a missing connector or
/// schema takes the parser's default, the connector normalizes the
/// letter case, and information_schema names resolve to the
/// information_schema connector. No table is looked up, so a name
/// can refer to a view, to a table that does not exist, to a
/// synthetic table such as t$partitions, or to a table only an
/// unread CTE reads.
struct SyntacticTableNames {
  /// Tables whose rows the statement reads.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName> rowInputTables;

  /// Table whose rows the statement writes, if any.
  std::optional<facebook::axiom::CatalogSchemaTableName> rowOutputTable;

  /// Tables whose definition alone the statement reads, such as a
  /// LIKE source or a SHOW target.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName>
      definitionInputTables;

  /// Table whose definition alone the statement changes, such as a
  /// CREATE TABLE, DROP TABLE, or ADD COLUMN target, if any.
  std::optional<facebook::axiom::CatalogSchemaTableName> definitionOutputTable;

  /// Kind of the statement, for routing decisions. Absent when the
  /// statement form has no planner kind.
  std::optional<SqlStatementKind> kind;
};

} // namespace axiom::sql::presto
