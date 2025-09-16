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

#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/PlanUtils.h"

#include <numbers>

namespace facebook::axiom::optimizer {

float Value::byteSize() const {
  if (type->isFixedWidth()) {
    return static_cast<float>(type->cppSizeInBytes());
  }
  switch (type->kind()) {
      // Add complex types here.
    default:
      return 16;
  }
}

std::vector<ColumnCP> SchemaTable::toColumns(
    const std::vector<std::string>& names) const {
  std::vector<ColumnCP> columns(names.size());
  VELOX_DCHECK(!columns.empty());
  for (size_t i = 0; i < names.size(); ++i) {
    columns[i] = findColumn(name);
  }

  return columns;
}

ColumnGroupCP SchemaTable::addIndex(
    const char* name,
    int32_t numKeysUnique,
    int32_t numOrdering,
    const ColumnVector& keys,
    DistributionType distributionType,
    const ColumnVector& partition,
    ColumnVector columnsVector,
    const connector::TableLayout* layout) {
  VELOX_CHECK_LE(numKeysUnique, keys.size());

  Distribution distribution;
  distribution.orderTypes.reserve(numOrdering);
  for (auto i = 0; i < numOrdering; ++i) {
    distribution.orderTypes.push_back(OrderType::kAscNullsFirst);
  }
  distribution.numKeysUnique = numKeysUnique;
  appendToVector(distribution.orderKeys, keys);
  distribution.distributionType = distributionType;
  appendToVector(distribution.partition, partition);
  VELOX_DCHECK_EQ(
      distribution.orderKeys.size(), distribution.orderTypes.size());
  columnGroups.push_back(make<ColumnGroup>(
      name, this, std::move(distribution), std::move(columnsVector), layout));
  return columnGroups.back();
}

ColumnCP SchemaTable::column(std::string_view name, const Value& value) {
  auto it = columns.find(toName(name));
  if (it != columns.end()) {
    return it->second;
  }
  auto* column = make<Column>(toName(name), nullptr, value);
  columns[toName(name)] = column;
  return column;
}

ColumnCP SchemaTable::findColumn(std::string_view name) const {
  auto it = columns.find(toName(name));
  VELOX_CHECK(it != columns.end(), "Column not found: {}", name);
  return it->second;
}

Schema::Schema(
    const char* name,
    const std::vector<SchemaTableCP>& tables,
    LocusCP locus)
    : name_{name}, defaultLocus_{locus} {
  for (auto& table : tables) {
    tables_[table->name] = table;
  }
}

Schema::Schema(const char* name, SchemaResolver* source, LocusCP locus)
    : name_{name}, source_{source}, defaultLocus_{locus} {}

SchemaTableCP Schema::findTable(
    std::string_view connectorId,
    std::string_view name) const {
  auto internedName = toName(name);
  auto it = tables_.find(internedName);
  if (it != tables_.end()) {
    return it->second;
  }

  VELOX_CHECK_NOT_NULL(source_);
  auto connectorTable =
      source_->findTable(std::string(connectorId), std::string(name));
  if (!connectorTable) {
    return nullptr;
  }

  auto* schemaTable = make<SchemaTable>(
      internedName, connectorTable->type(), connectorTable->numRows());
  schemaTable->connectorTable = connectorTable.get();

  ColumnVector columns;
  for (const auto& [columnName, tableColumn] : connectorTable->columnMap()) {
    const auto cardinality = static_cast<float>(tableColumn->approxNumDistinct(
        static_cast<int64_t>(connectorTable->numRows())));
    Value value(toType(tableColumn->type()), cardinality);
    auto* column = make<Column>(toName(columnName), nullptr, value);
    schemaTable->columns[column->name()] = column;
    columns.push_back(column);
  }
  DistributionType defaultDistributionType;
  defaultDistributionType.locus = defaultLocus_;
  schemaTable->addIndex(
      toName("pk"),
      0,
      0,
      {},
      defaultDistributionType,
      {},
      std::move(columns),
      connectorTable->layouts()[0]);
  addTable(schemaTable);
  queryCtx()->optimization()->retainConnectorTable(std::move(connectorTable));
  return schemaTable;
}

void Schema::addTable(SchemaTableCP table) const {
  tables_[table->name] = table;
}

float tableCardinality(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    return table->as<BaseTable>()
        ->schemaTable->columnGroups[0]
        ->table->cardinality;
  }
  if (table->is(PlanType::kValuesTableNode)) {
    return table->as<ValuesTable>()->cardinality();
  }
  VELOX_CHECK(table->is(PlanType::kDerivedTableNode));
  return table->as<DerivedTable>()->cardinality;
}

// The fraction of rows of a base table selected by non-join filters. 0.2
// means 1 in 5 are selected.
float baseSelectivity(PlanObjectCP object) {
  if (object->is(PlanType::kTableNode)) {
    return object->as<BaseTable>()->filterSelectivity;
  }
  return 1;
}

namespace {
template <typename T>
ColumnCP findColumnByName(const T& columns, Name name) {
  for (auto column : columns) {
    if (column->is(PlanType::kColumnExpr) &&
        column->template as<Column>()->name() == name) {
      return column->template as<Column>();
    }
  }
  return nullptr;
}
} // namespace

bool SchemaTable::isUnique(CPSpan<Column> columns) const {
  for (auto& column : columns) {
    if (column->isNot(PlanType::kColumnExpr)) {
      return false;
    }
  }
  for (auto index : columnGroups) {
    auto nUnique = index->distribution.numKeysUnique;
    if (!nUnique) {
      continue;
    }
    bool unique = true;
    for (auto i = 0; i < nUnique; ++i) {
      auto part = findColumnByName(columns, index->columns[i]->name());
      if (!part) {
        unique = false;
        break;
      }
    }
    if (unique) {
      return true;
    }
  }
  return false;
}

namespace {

float combine(float card, size_t ith, float otherCard) {
  if (ith == 0) {
    return card / otherCard;
  }
  if (otherCard > card) {
    return 1;
  }
  return card / otherCard;
}
} // namespace

IndexInfo SchemaTable::indexInfo(
    ColumnGroupCP index,
    CPSpan<Column> columnsSpan) const {
  IndexInfo info;
  info.index = index;
  info.scanCardinality = index->table->cardinality;
  info.joinCardinality = index->table->cardinality;

  const auto& distribution = index->distribution;

  const auto numSorting = distribution.orderTypes.size();
  const auto numUnique = distribution.numKeysUnique;

  PlanObjectSet covered;
  for (auto i = 0; i < numSorting || i < numUnique; ++i) {
    auto orderKey = distribution.orderKeys[i];
    auto part = findColumnByName(columnsSpan, orderKey->as<Column>()->name());
    if (!part) {
      break;
    }

    covered.add(part);
    if (i < numSorting) {
      info.scanCardinality =
          combine(info.scanCardinality, i, orderKey->value().cardinality);
      info.lookupKeys.push_back(part);
      info.joinCardinality = info.scanCardinality;
    } else {
      info.joinCardinality =
          combine(info.joinCardinality, i, orderKey->value().cardinality);
    }
    if (i == numUnique - 1) {
      info.unique = true;
    }
  }

  for (auto column : columnsSpan) {
    if (column->isNot(PlanType::kColumnExpr)) {
      // Join key is an expression dependent on the table.
      covered.unionColumns(column->as<Expr>());
      info.joinCardinality = combine(
          info.joinCardinality, covered.size(), column->value().cardinality);
      continue;
    }
    if (covered.contains(column)) {
      continue;
    }
    auto part = findColumnByName(index->columns, column->name());
    if (!part) {
      continue;
    }
    covered.add(column);
    info.joinCardinality = combine(
        info.joinCardinality, covered.size(), column->value().cardinality);
  }
  info.coveredColumns = std::move(covered);
  return info;
}

IndexInfo SchemaTable::indexByColumns(CPSpan<Column> columns) const {
  // Match 'columns' against all indices. Pick the one that has the
  // longest prefix intersection with 'columns'. If 'columns' are a
  // unique combination on any index, then unique is true of the
  // result.
  IndexInfo pkInfo;
  IndexInfo best;
  bool unique = isUnique(columns);
  float bestPrediction = 0;
  for (auto iIndex = 0; iIndex < columnGroups.size(); ++iIndex) {
    auto index = columnGroups[iIndex];
    auto candidate = indexInfo(index, columns);
    if (iIndex == 0) {
      pkInfo = candidate;
      best = candidate;
      bestPrediction = best.joinCardinality;
      continue;
    }
    if (candidate.lookupKeys.empty()) {
      // No prefix match for secondary index.
      continue;
    }
    // The join cardinality estimate from the longest prefix is preferred for
    // the estimate. The index with the least scan cardinality is preferred
    if (candidate.lookupKeys.size() > best.lookupKeys.size()) {
      bestPrediction = candidate.joinCardinality;
    }
    if (candidate.scanCardinality < best.scanCardinality) {
      best = candidate;
    }
  }
  best.joinCardinality = bestPrediction;
  best.unique = unique;
  return best;
}

IndexInfo joinCardinality(PlanObjectCP table, CPSpan<Column> keys) {
  if (table->is(PlanType::kTableNode)) {
    auto schemaTable = table->as<BaseTable>()->schemaTable;
    return schemaTable->indexByColumns(keys);
  }
  IndexInfo result;
  auto computeCardinalities = [&](float scanCardinality) {
    result.scanCardinality = scanCardinality;
    result.joinCardinality = scanCardinality;
    for (size_t i = 0; i < keys.size(); ++i) {
      result.joinCardinality =
          combine(result.joinCardinality, i, keys[i]->value().cardinality);
    }
  };

  if (table->is(PlanType::kValuesTableNode)) {
    const auto* valuesTable = table->as<ValuesTable>();
    computeCardinalities(valuesTable->cardinality());
    return result;
  }
  VELOX_CHECK(table->is(PlanType::kDerivedTableNode));
  const auto* dt = table->as<DerivedTable>();
  computeCardinalities(dt->cardinality);
  result.unique =
      dt->aggregation && keys.size() >= dt->aggregation->groupingKeys().size();
  return result;
}

ColumnCP IndexInfo::schemaColumn(ColumnCP keyValue) const {
  for (auto& column : index->columns) {
    if (column->name() == keyValue->name()) {
      return column;
    }
  }
  return nullptr;
}

bool Distribution::isSamePartition(const Distribution& other) const {
  if (distributionType != other.distributionType) {
    return false;
  }
  if (isBroadcast || other.isBroadcast) {
    return true;
  }
  if (partition.size() != other.partition.size()) {
    return false;
  }
  if (partition.size() == 0) {
    // If the partitioning columns are not in the columns or if there
    // are no partitioning columns, there can be  no copartitioning.
    return false;
  }
  for (auto i = 0; i < partition.size(); ++i) {
    if (!partition[i]->sameOrEqual(*other.partition[i])) {
      return false;
    }
  }
  return true;
}

bool Distribution::isSameOrder(const Distribution& other) const {
  if (orderKeys.size() != other.orderKeys.size()) {
    return false;
  }
  for (size_t i = 0; i < orderKeys.size(); ++i) {
    if (!orderKeys[i]->sameOrEqual(*other.orderKeys[i]) ||
        orderTypes[i] != other.orderTypes[i]) {
      return false;
    }
  }
  return true;
}

Distribution Distribution::rename(
    const ExprVector& exprs,
    const ColumnVector& names) const {
  Distribution result = *this;
  // Partitioning survives projection if all partitioning columns are projected
  // out.
  if (!replace(result.partition, exprs, names)) {
    result.partition.clear();
  }
  // Ordering survives if a prefix of the previous order continues to be
  // projected out.
  const auto newOrderSize = prefixSize(result.orderKeys, exprs);
  result.orderKeys.resize(newOrderSize);
  result.orderTypes.resize(newOrderSize);
  replace(result.orderKeys, exprs, names);
  VELOX_DCHECK_EQ(result.orderKeys.size(), result.orderTypes.size());
  return result;
}

namespace {

void exprsToString(const ExprVector& exprs, std::stringstream& out) {
  for (size_t i = 0, size = exprs.size(); i < size; ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << exprs[i]->toString();
  }
}

} // namespace

std::string Distribution::toString() const {
  if (isBroadcast) {
    return "broadcast";
  }

  if (distributionType.isGather) {
    return "gather";
  }

  std::stringstream out;
  if (!partition.empty()) {
    out << "P ";
    exprsToString(partition, out);
    out << " " << distributionType.numPartitions << " ways";
  }
  if (!orderKeys.empty()) {
    out << " O ";
    exprsToString(orderKeys, out);
  }
  if (numKeysUnique && numKeysUnique >= orderKeys.size()) {
    out << " first " << numKeysUnique << " unique";
  }
  return out.str();
}

float ColumnGroup::lookupCost(float range) const {
  // Add 2 because it takes a compare and access also if hitting the
  // same row. log(1) == 0, so this would other wise be zero cost.
  return Costs::kKeyCompareCost * std::log(range + 2) /
      std::numbers::ln2_v<float>;
}

} // namespace facebook::axiom::optimizer
