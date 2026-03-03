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

#include "axiom/sql/presto/TableVisitor.h"

#include <fmt/format.h>

#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

TableVisitor::TableVisitor(
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema)
    : defaultConnectorId_(defaultConnectorId), defaultSchema_(defaultSchema) {}

void TableVisitor::visitWithQuery(WithQuery* node) {
  // To cover the case where a CTE aliases an underlying
  // table, e.g. 'WITH t AS (SELECT * FROM t)', we need to
  // traverse the inner query before tracking the CTE alias.
  DefaultTraversalVisitor::visitWithQuery(node);
  ctes_.insert(node->name()->value());
}

void TableVisitor::visitTable(Table* node) {
  const auto& parts = node->name()->parts();
  if (parts.size() == 1 && ctes_.count(parts[0]) > 0) {
    return;
  }
  inputTables_.insert(constructTableName(*node->name()));
  DefaultTraversalVisitor::visitTable(node);
}

void TableVisitor::visitInsert(Insert* node) {
  setOutputTable(*node->target());
  DefaultTraversalVisitor::visitInsert(node);
}

void TableVisitor::visitCreateTableAsSelect(CreateTableAsSelect* node) {
  setOutputTable(*node->name());
  DefaultTraversalVisitor::visitCreateTableAsSelect(node);
}

void TableVisitor::visitUpdate(Update* node) {
  setOutputTable(*node->table());
  DefaultTraversalVisitor::visitUpdate(node);
}

void TableVisitor::visitDelete(Delete* node) {
  setOutputTable(*node->table());
  DefaultTraversalVisitor::visitDelete(node);
}

void TableVisitor::visitCreateTable(CreateTable* node) {
  setOutputTable(*node->name());
  DefaultTraversalVisitor::visitCreateTable(node);
}

void TableVisitor::visitCreateView(CreateView* node) {
  setOutputTable(*node->name());
  DefaultTraversalVisitor::visitCreateView(node);
}

void TableVisitor::visitCreateMaterializedView(CreateMaterializedView* node) {
  setOutputTable(*node->name());
  DefaultTraversalVisitor::visitCreateMaterializedView(node);
}

void TableVisitor::visitDropTable(DropTable* node) {
  setOutputTable(*node->tableName());
  DefaultTraversalVisitor::visitDropTable(node);
}

void TableVisitor::visitDropView(DropView* node) {
  setOutputTable(*node->viewName());
  DefaultTraversalVisitor::visitDropView(node);
}

void TableVisitor::visitDropMaterializedView(DropMaterializedView* node) {
  setOutputTable(*node->viewName());
  DefaultTraversalVisitor::visitDropMaterializedView(node);
}

std::string TableVisitor::constructTableName(const QualifiedName& name) const {
  const auto& parts = name.parts();
  VELOX_CHECK(!parts.empty(), "Table name cannot be empty");
  // TODO: This limits table names to at most 3 dot-separated components
  // (catalog.schema.table). Connectors with multi-dot paths (e.g., XDB tier
  // paths like "ephemeralxdb.on_demand.ftw.784.tasks") require double-quoting
  // at the SQL layer to collapse them into a single identifier. However, the
  // resulting dot-joined string loses identifier boundaries downstream. To
  // fully support multi-dot paths in SQL, this function and toConnectorTable()
  // in PrestoParser.cpp need to propagate QualifiedName parts instead of a
  // flat dot-joined string.
  VELOX_CHECK_LE(
      parts.size(),
      3,
      "Table name must have 1-3 components, '{}'",
      name.fullyQualifiedName());
  switch (parts.size()) {
    case 1:
      if (defaultSchema_.has_value()) {
        return fmt::format(
            "{}.{}.{}", defaultConnectorId_, defaultSchema_.value(), parts[0]);
      }
      return fmt::format("{}.{}", defaultConnectorId_, parts[0]);
    case 2:
      return fmt::format("{}.{}.{}", defaultConnectorId_, parts[0], parts[1]);
    case 3:
      return fmt::format("{}.{}.{}", parts[0], parts[1], parts[2]);
    default:
      VELOX_UNREACHABLE();
  }
}

void TableVisitor::setOutputTable(const QualifiedName& name) {
  VELOX_CHECK(!outputTable_.has_value());
  outputTable_ = constructTableName(name);
}

} // namespace axiom::sql::presto
