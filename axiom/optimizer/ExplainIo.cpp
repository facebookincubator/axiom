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

#include "axiom/optimizer/ExplainIo.h"

#include <algorithm>

#include <folly/dynamic.h>
#include <folly/json.h>

#include "axiom/optimizer/Domain.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

namespace {

// Serializes a table reference (catalog + schema + table) to JSON.
folly::dynamic tableToJson(
    const std::string& connectorId,
    const SchemaTableName& tableName) {
  return folly::dynamic::object("catalog", connectorId)(
      "schemaTable",
      folly::dynamic::object("schema", tableName.schema)(
          "table", tableName.table));
}

// Serializes a Range to JSON.
folly::dynamic rangeToJson(const Range& range) {
  folly::dynamic result = folly::dynamic::object;
  if (range.isSingleValue()) {
    folly::dynamic bound = folly::dynamic::object(
        "value", range.low()->value.serialize()["value"])("bound", "EXACTLY");
    result["low"] = bound;
    result["high"] = bound;
  } else {
    if (range.low()) {
      result["low"] = folly::dynamic::object(
          "value", range.low()->value.serialize()["value"])(
          "bound", range.low()->inclusive ? "EXACTLY" : "ABOVE");
    }
    if (range.high()) {
      result["high"] = folly::dynamic::object(
          "value", range.high()->value.serialize()["value"])(
          "bound", range.high()->inclusive ? "EXACTLY" : "BELOW");
    }
  }
  return result;
}

// Serializes a Domain to JSON.
folly::dynamic domainToJson(const Domain& domain) {
  folly::dynamic result = folly::dynamic::object;
  result["nullsAllowed"] = domain.nullsAllowed();
  folly::dynamic ranges = folly::dynamic::array;
  for (const auto& range : domain.ranges()) {
    ranges.push_back(rangeToJson(range));
  }
  result["ranges"] = std::move(ranges);
  return result;
}

// Computes the combined Domain for a column from the table's columnFilters.
// columnFilters are conjuncts (ANDed), so unconvertible filters are skipped
// (making the result broader, which is safe).
Domain columnDomain(ColumnCP column, const ExprVector& columnFilters) {
  Domain domain = Domain::all();
  for (auto* filterExpr : columnFilters) {
    if (!filterExpr->is(PlanType::kCallExpr)) {
      continue;
    }

    // columnFilters are single-column predicates. Check if this filter
    // references the target column.
    if (!filterExpr->columns().contains(column)) {
      continue;
    }

    auto filterDomain = exprToDomain(filterExpr);
    if (!filterDomain) {
      // Cannot convert; skip this conjunct (broader is safe).
      continue;
    }
    domain = domain.intersect(*filterDomain);
  }
  return domain;
}

// Per-column domains for a single table. Maps connector column name to Domain.
using ColumnDomainMap = folly::F14FastMap<std::string, Domain>;

// Computes column domains for a single BaseTable from 'filters' (the scan's
// conjuncts). Returns nullopt if any explain_io column has an empty domain
// (table should be excluded).
std::optional<ColumnDomainMap> computeColumnDomains(
    BaseTableCP baseTable,
    const ExprVector& filters) {
  const auto* connectorTable = baseTable->schemaTable->connectorTable;
  ColumnDomainMap domains;

  for (auto* column : baseTable->columns) {
    auto* schemaColumn = column->schemaColumn();
    if (!schemaColumn) {
      continue;
    }

    auto* connectorColumn = connectorTable->findColumn(schemaColumn->name());
    VELOX_CHECK_NOT_NULL(connectorColumn);
    if (!connectorColumn->includeInExplainIo()) {
      continue;
    }

    const auto typeKind = connectorColumn->type()->kind();
    VELOX_USER_CHECK(
        typeKind == velox::TypeKind::VARCHAR ||
            typeKind == velox::TypeKind::TINYINT ||
            typeKind == velox::TypeKind::SMALLINT ||
            typeKind == velox::TypeKind::INTEGER ||
            typeKind == velox::TypeKind::BIGINT,
        "Unsupported type for EXPLAIN IO column. "
        "Only VARCHAR and integer types are supported: {}.{} {}",
        baseTable->schemaTable->name(),
        connectorColumn->name(),
        connectorColumn->type()->toString());

    auto domain = columnDomain(column, filters);
    if (domain.isNone()) {
      return std::nullopt;
    }

    domains.emplace(connectorColumn->name(), std::move(domain));
  }

  return domains;
}

// Merges column domains from two scans of the same table by uniting domains
// for each column.
void mergeColumnDomains(ColumnDomainMap& target, const ColumnDomainMap& other) {
  for (const auto& [columnName, domain] : other) {
    auto it = target.find(columnName);
    if (it == target.end()) {
      target.emplace(columnName, domain);
    } else {
      it->second = it->second.unite(domain);
    }
  }
}

// Serializes column domains to JSON, omitting unconstrained columns.
// Sorts by column name for deterministic output.
folly::dynamic columnDomainsToJson(
    const ColumnDomainMap& domains,
    const connector::Table& connectorTable) {
  std::vector<std::string> columnNames;
  for (const auto& [columnName, domain] : domains) {
    if (!domain.isAll()) {
      columnNames.push_back(columnName);
    }
  }
  std::sort(columnNames.begin(), columnNames.end());

  folly::dynamic constraints = folly::dynamic::array;
  for (const auto& columnName : columnNames) {
    auto* connectorColumn = connectorTable.findColumn(columnName);
    const auto& domain = domains.at(columnName);
    folly::dynamic constraint = folly::dynamic::object;
    constraint["columnName"] = columnName;
    constraint["typeSignature"] = connectorColumn->type()->name();
    constraint["domain"] = domainToJson(domain);
    constraints.push_back(std::move(constraint));
  }
  return constraints;
}

} // namespace

std::string explainIo(
    const std::vector<std::pair<BaseTableCP, ExprVector>>& tableFilters,
    std::optional<CatalogSchemaTableName> outputTable) {
  // Group input tables by connector table and merge column domains. Multiple
  // scans of the same table (e.g., UNION ALL branches) are combined by
  // uniting their per-column domains.
  folly::F14FastMap<const connector::Table*, ColumnDomainMap> mergedDomains;
  std::vector<const connector::Table*> tableOrder;

  for (const auto& [baseTable, filters] : tableFilters) {
    auto domains = computeColumnDomains(baseTable, filters);
    if (!domains) {
      continue;
    }

    auto* connectorTable = baseTable->schemaTable->connectorTable;
    auto it = mergedDomains.find(connectorTable);
    if (it == mergedDomains.end()) {
      tableOrder.push_back(connectorTable);
      mergedDomains.emplace(connectorTable, std::move(*domains));
    } else {
      mergeColumnDomains(it->second, *domains);
    }
  }

  // Sort by connector ID, schema, table for deterministic output.
  std::sort(
      tableOrder.begin(),
      tableOrder.end(),
      [](const auto* lhs, const auto* rhs) {
        // Use pointer identity for same-table dedup, but sort by name.
        if (lhs == rhs) {
          return false;
        }
        // Connector ID comes from the table's layouts.
        const auto& lhsId = lhs->layouts()[0]->connectorId();
        const auto& rhsId = rhs->layouts()[0]->connectorId();
        if (lhsId != rhsId) {
          return lhsId < rhsId;
        }
        if (lhs->name().schema != rhs->name().schema) {
          return lhs->name().schema < rhs->name().schema;
        }
        return lhs->name().table < rhs->name().table;
      });

  folly::dynamic tableInfos = folly::dynamic::array;

  for (auto* connectorTable : tableOrder) {
    folly::dynamic tableInfo = folly::dynamic::object;
    tableInfo["table"] = tableToJson(
        connectorTable->layouts()[0]->connectorId(), connectorTable->name());
    tableInfo["columnConstraints"] =
        columnDomainsToJson(mergedDomains[connectorTable], *connectorTable);
    tableInfos.push_back(std::move(tableInfo));
  }

  folly::dynamic root = folly::dynamic::object;
  root["inputTableColumnInfos"] = std::move(tableInfos);

  if (outputTable) {
    root["outputTable"] =
        tableToJson(outputTable->catalogName, outputTable->schemaTableName);
  }

  folly::json::serialization_opts opts;
  opts.pretty_formatting = true;
  opts.sort_keys = false;
  return folly::json::serialize(root, opts);
}

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
    tableFilters.emplace_back(baseTable, baseTable->columnFilters);
  }
  return explainIo(tableFilters, std::move(outputTable));
}

} // namespace facebook::axiom::optimizer
