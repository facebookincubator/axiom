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
#include <vector>

#include "axiom/common/SchemaTableName.h"

namespace facebook::axiom::connector {

using SchemaTableName = ::facebook::axiom::SchemaTableName;

/// Security mode for views and materialized views.
enum class ViewSecurity {
  kInvoker, // Execute with the permissions of the current user.
  kDefiner, // Execute with the permissions of the view owner.
};

struct TableColumn {
  SchemaTableName tableName;
  std::string columnName;
  // False for columns mapped through outer joins where the mapping is indirect.
  std::optional<bool> isDirectMapped;

  bool operator==(const TableColumn& other) const {
    return tableName == other.tableName && columnName == other.columnName &&
        isDirectMapped == other.isDirectMapped;
  }
};

struct ColumnMapping {
  TableColumn viewColumn;
  std::vector<TableColumn> baseTableColumns;

  bool operator==(const ColumnMapping& other) const {
    return viewColumn == other.viewColumn &&
        baseTableColumns == other.baseTableColumns;
  }
};

/// Metadata for a materialized view.
class MaterializedViewDefinition {
 public:
  MaterializedViewDefinition(
      std::string originalSql,
      std::string schema,
      std::string table,
      std::vector<SchemaTableName> baseTables,
      std::optional<std::string> owner,
      std::optional<ViewSecurity> securityMode,
      /// columnMappings: Maps each MV column to its source column(s) in base
      /// tables. Used for determining MV partition status, query rewriting,
      /// and refresh.
      std::vector<ColumnMapping> columnMappings,
      /// baseTablesOnOuterJoinSide : Tables on the outer side of an outer join
      /// in the MV definition. Columns from these tables may be NULL due to
      /// outer join semantics, which affects incremental refresh
      /// correctness and query rewriting rules.
      std::vector<SchemaTableName> baseTablesOnOuterJoinSide,
      /// validRefreshColumns: Columns (typically partition columns like 'ds')
      /// that can be used for partition-based incremental refresh. When base
      /// table partitions change, only MV partitions matching these column
      /// values need to be refreshed.
      std::optional<std::vector<std::string>> validRefreshColumns)
      : originalSql_{std::move(originalSql)},
        schema_{std::move(schema)},
        table_{std::move(table)},
        baseTables_{std::move(baseTables)},
        owner_{std::move(owner)},
        securityMode_{std::move(securityMode)},
        columnMappings_{std::move(columnMappings)},
        baseTablesOnOuterJoinSide_{std::move(baseTablesOnOuterJoinSide)},
        validRefreshColumns_{std::move(validRefreshColumns)} {}

  const std::string& originalSql() const {
    return originalSql_;
  }

  const std::string& schema() const {
    return schema_;
  }

  const std::string& table() const {
    return table_;
  }

  SchemaTableName schemaTableName() const {
    return SchemaTableName{schema_, table_};
  }

  const std::vector<SchemaTableName>& baseTables() const {
    return baseTables_;
  }

  const std::optional<std::string>& owner() const {
    return owner_;
  }

  const std::optional<ViewSecurity>& securityMode() const {
    return securityMode_;
  }

  const std::vector<ColumnMapping>& columnMappings() const {
    return columnMappings_;
  }

  const std::vector<SchemaTableName>& baseTablesOnOuterJoinSide() const {
    return baseTablesOnOuterJoinSide_;
  }

  const std::optional<std::vector<std::string>>& validRefreshColumns() const {
    return validRefreshColumns_;
  }

  std::string toString() const;

 private:
  const std::string originalSql_;
  const std::string schema_;
  const std::string table_;
  const std::vector<SchemaTableName> baseTables_;
  const std::optional<std::string> owner_;
  const std::optional<ViewSecurity> securityMode_;
  const std::vector<ColumnMapping> columnMappings_;
  const std::vector<SchemaTableName> baseTablesOnOuterJoinSide_;
  const std::optional<std::vector<std::string>> validRefreshColumns_;
};

} // namespace facebook::axiom::connector
