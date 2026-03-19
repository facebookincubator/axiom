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

#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::connector {
class SchemaResolver;
} // namespace facebook::axiom::connector

namespace facebook::axiom::optimizer {

class ToGraph;

/// Rewrites materialized view (MV) table scans into UNION ALL of fresh MV
/// partitions and missing base table partitions. Traverses the query graph
/// looking for BaseTable nodes backed by an MV with a
/// MaterializedViewDefinition. For each partially materialized MV, constructs:
///
///   UNION ALL
///   ├── MV scan with filter: refreshCol >= lo AND refreshCol <= hi
///   └── base table scan with filter: refreshCol < lo OR refreshCol > hi
///
/// Column mappings from the MV definition are applied to the base table side
/// so that both children produce columns with the same output names.
class MaterializedViewRewrite {
 public:
  MaterializedViewRewrite(ToGraph& toGraph);

  /// Traverses the DerivedTable tree and rewrites any MV BaseTable into a
  /// UNION ALL of MV partitions + missing base table partitions. Returns true
  /// if any rewrite was performed.
  bool rewrite(DerivedTable& root);

 private:
  /// Attempts to rewrite a single MV BaseTable into a UNION ALL. Returns true
  /// if the rewrite was performed.
  bool rewriteBaseTable(DerivedTable& parent, BaseTable* mvTable);

  /// Replaces an MV BaseTable with a full base table scan. Used when the MV
  /// has no data or too many partitions are missing. Returns true on success.
  bool replaceWithBaseTable(DerivedTable& parent, BaseTable* mvTable);

  /// Gets the connector ID from the MV table's layout.
  std::optional<std::string> getConnectorId(const BaseTable& mvTable);

  /// Creates a new BaseTable for the base table, with columns mapped from the
  /// MV columns. Returns the BaseTable and the parallel column vector, or
  /// nullptr if resolution fails.
  std::pair<BaseTable*, ColumnVector> makeBaseTableNode(
      SchemaTableCP baseSchemaTable,
      const BaseTable& mvTable,
      const folly::F14FastMap<std::string, std::string>& columnNameMap);

  /// Translates filters from the MV BaseTable to the base table, remapping
  /// column references using the column name mapping.
  void translateFilters(
      const BaseTable& mvTable,
      BaseTable* baseBt,
      const folly::F14FastMap<std::string, std::string>& columnNameMap);

  /// Builds a mapping from MV column name to base table column name using
  /// the MaterializedViewDefinition's columnMappings.
  folly::F14FastMap<std::string, std::string> buildColumnNameMapping(
      const connector::MaterializedViewDefinition& mvDef,
      const connector::SchemaTableName& baseTableName);

  /// Creates a range filter expression: refreshCol >= lo AND refreshCol <= hi.
  ExprCP makeRangeFilter(
      ColumnCP refreshColumn,
      const std::string& lo,
      const std::string& hi);

  /// Creates the complement filter: refreshCol < lo OR refreshCol > hi.
  ExprCP makeComplementFilter(
      ColumnCP refreshColumn,
      const std::string& lo,
      const std::string& hi);

  /// Creates a literal expression from a string value with the given type.
  ExprCP makeLiteral(const std::string& value, const velox::Type* type);

  ToGraph& toGraph_;
};

} // namespace facebook::axiom::optimizer
