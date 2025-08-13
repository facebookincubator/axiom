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
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/RelationOp.h"

namespace facebook::velox::optimizer {

float Value::byteSize() const {
  if (type->isFixedWidth()) {
    return type->cppSizeInBytes();
  }
  switch (type->kind()) {
      // Add complex types here.
    default:
      return 16;
  }
}

std::vector<ColumnCP> SchemaTable::toColumns(
    const std::vector<std::string>& names) {
  std::vector<ColumnCP> columns(names.size());
  VELOX_DCHECK(!columns.empty());
  for (auto i = 0; i < names.size(); ++i) {
    columns[i] = findColumn(name);
  }

  return columns;
}

ColumnGroupP SchemaTable::addIndex(
    const char* name,
    float cardinality,
    int32_t numKeysUnique,
    int32_t numOrdering,
    const ColumnVector& keys,
    DistributionType distType,
    const ColumnVector& partition,
    const ColumnVector& columns) {
  Distribution distribution;
  distribution.cardinality = cardinality;
  for (auto i = 0; i < numOrdering; ++i) {
    distribution.orderType.push_back(OrderType::kAscNullsFirst);
  }
  distribution.numKeysUnique = numKeysUnique;
  appendToVector(distribution.order, keys);
  distribution.distributionType = distType;
  appendToVector(distribution.partition, partition);
  columnGroups.push_back(make<ColumnGroup>(name, this, distribution, columns));
  return columnGroups.back();
}

ColumnCP SchemaTable::column(const std::string& name, const Value& value) {
  auto it = columns.find(toName(name));
  if (it != columns.end()) {
    return it->second;
  }
  auto* column = make<Column>(toName(name), nullptr, value);
  columns[toName(name)] = column;
  return column;
}

ColumnCP SchemaTable::findColumn(const std::string& name) const {
  auto it = columns.find(toName(name));
  VELOX_CHECK(it != columns.end(), "Column not found: {}", name);
  return it->second;
}

Schema::Schema(
    const char* _name,
    const std::vector<SchemaTableCP>& tables,
    LocusCP locus)
    : name_(_name), defaultLocus_(locus) {
  for (auto& table : tables) {
    tables_[table->name] = table;
  }
}

Schema::Schema(const char* _name, SchemaResolver* source, LocusCP locus)
    : name_(_name), source_(source), defaultLocus_(locus) {}

SchemaTableCP Schema::findTable(std::string_view name) const {
  auto internedName = toName(name);
  auto it = tables_.find(internedName);
  if (it != tables_.end()) {
    return it->second;
  }
  VELOX_CHECK_NOT_NULL(source_);
  auto* table = source_->findTable(std::string(name));
  if (!table) {
    return nullptr;
  }
  auto* schemaTable = make<SchemaTable>(internedName, table->rowType());
  schemaTable->connectorTable = table;
  ColumnVector columns;
  for (auto& pair : table->columnMap()) {
    auto& tableColumn = *pair.second;
    float cardinality = tableColumn.approxNumDistinct(table->numRows());
    Value value(toType(tableColumn.type()), cardinality);
    auto columnName = toName(pair.first);
    auto* column = make<Column>(columnName, nullptr, value);
    schemaTable->columns[columnName] = column;
    columns.push_back(column);
  }
  DistributionType defaultDist;
  defaultDist.locus = defaultLocus_;
  auto* pk = schemaTable->addIndex(
      toName("pk"), table->numRows(), 0, 0, {}, defaultDist, {}, columns);
  addTable(schemaTable);
  pk->layout = table->layouts()[0];
  return schemaTable;
}

void Schema::addTable(SchemaTableCP table) const {
  tables_[table->name] = table;
}

float tableCardinality(PlanObjectCP table) {
  if (table->type() == PlanType::kTableNode) {
    return table->as<BaseTable>()
        ->schemaTable->columnGroups[0]
        ->distribution()
        .cardinality;
  } else if (table->type() == PlanType::kValuesTableNode) {
    return table->as<ValuesTable>()->cardinality();
  }
  VELOX_CHECK(table->type() == PlanType::kDerivedTableNode);
  return table->as<DerivedTable>()->distribution->cardinality;
}

// The fraction of rows of a base table selected by non-join filters. 0.2
// means 1 in 5 are selected.
float baseSelectivity(PlanObjectCP object) {
  if (object->type() == PlanType::kTableNode) {
    return object->as<BaseTable>()->filterSelectivity;
  }
  return 1;
}

namespace {
template <typename T>
ColumnCP findColumnByName(const T& columns, Name name) {
  for (auto column : columns) {
    if (column->type() == PlanType::kColumnExpr &&
        column->template as<Column>()->name() == name) {
      return column->template as<Column>();
    }
  }
  return nullptr;
}
} // namespace

bool SchemaTable::isUnique(CPSpan<Column> columns) const {
  for (auto& column : columns) {
    if (column->type() != PlanType::kColumnExpr) {
      return false;
    }
  }
  for (auto index : columnGroups) {
    auto nUnique = index->distribution().numKeysUnique;
    if (!nUnique) {
      continue;
    }
    bool unique = true;
    for (auto i = 0; i < nUnique; ++i) {
      auto part = findColumnByName(columns, index->columns()[i]->name());
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

float combine(float card, int32_t ith, float otherCard) {
  if (ith == 0) {
    return card / otherCard;
  }
  if (otherCard > card) {
    return 1;
  }
  return card / otherCard;
}
} // namespace

IndexInfo SchemaTable::indexInfo(ColumnGroupP index, CPSpan<Column> columns)
    const {
  IndexInfo info;
  info.index = index;
  info.scanCardinality = index->distribution().cardinality;
  info.joinCardinality = index->distribution().cardinality;
  PlanObjectSet covered;
  int32_t numCovered = 0;
  int32_t numSorting = index->distribution().orderType.size();
  int32_t numUnique = index->distribution().numKeysUnique;
  for (auto i = 0; i < numSorting || i < numUnique; ++i) {
    auto part = findColumnByName(
        columns, index->distribution().order[i]->as<Column>()->name());
    if (!part) {
      break;
    }
    ++numCovered;
    covered.add(part);
    if (i < numSorting) {
      info.scanCardinality = combine(
          info.scanCardinality,
          i,
          index->distribution().order[i]->value().cardinality);
      info.lookupKeys.push_back(part);
      info.joinCardinality = info.scanCardinality;
    } else {
      info.joinCardinality = combine(
          info.joinCardinality,
          i,
          index->distribution().order[i]->value().cardinality);
    }
    if (i == numUnique - 1) {
      info.unique = true;
    }
  }

  for (auto i = 0; i < columns.size(); ++i) {
    auto column = columns[i];
    if (column->type() != PlanType::kColumnExpr) {
      // Join key is an expression dependent on the table.
      covered.unionColumns(column->as<Expr>());
      info.joinCardinality = combine(
          info.joinCardinality, numCovered, column->value().cardinality);
      continue;
    }
    if (covered.contains(column)) {
      continue;
    }
    auto part = findColumnByName(index->columns(), column->name());
    if (!part) {
      continue;
    }
    covered.add(column);
    ++numCovered;
    info.joinCardinality =
        combine(info.joinCardinality, numCovered, column->value().cardinality);
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
  if (table->type() == PlanType::kTableNode) {
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

  if (table->type() == PlanType::kValuesTableNode) {
    const auto* valuesTable = table->as<ValuesTable>();
    computeCardinalities(valuesTable->cardinality());
    return result;
  }
  VELOX_CHECK(table->type() == PlanType::kDerivedTableNode);
  const auto* dt = table->as<DerivedTable>();
  const auto* distribution = dt->distribution;
  VELOX_CHECK_NOT_NULL(distribution);
  computeCardinalities(distribution->cardinality);
  result.unique =
      dt->aggregation && keys.size() >= dt->aggregation->groupingKeys().size();
  return result;
}

ColumnCP IndexInfo::schemaColumn(ColumnCP keyValue) const {
  for (auto& column : index->columns()) {
    if (column->name() == keyValue->name()) {
      return column;
    }
  }
  return nullptr;
}

bool Distribution::isSamePartition(const Distribution& other) const {
  if (!(distributionType == other.distributionType)) {
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
  if (order.size() != other.order.size()) {
    return false;
  }
  for (auto i = 0; i < order.size(); ++i) {
    if (!order[i]->sameOrEqual(*other.order[i]) ||
        orderType[i] != other.orderType[i]) {
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
  result.order.resize(prefixSize(result.order, exprs));
  replace(result.order, exprs, names);
  return result;
}

namespace {

void exprsToString(const ExprVector& exprs, std::stringstream& out) {
  int32_t size = exprs.size();
  for (auto i = 0; i < size; ++i) {
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
  if (!order.empty()) {
    out << " O ";
    exprsToString(order, out);
  }
  if (numKeysUnique && numKeysUnique >= order.size()) {
    out << " first " << numKeysUnique << " unique";
  }
  return out.str();
}

float ColumnGroup::lookupCost(float range) const {
  // Add 2 because it takes a compare and access also if hitting the
  // same row. log(1) == 0, so this would other wise be zero cost.
  return Costs::kKeyCompareCost * log(range + 2) / log(2);
}

} // namespace facebook::velox::optimizer
