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

#include <numbers>

namespace facebook::axiom::optimizer {

namespace {
const auto& orderTypeNames() {
  static const folly::F14FastMap<OrderType, std::string_view> kNames = {
      {OrderType::kAscNullsFirst, "ASC NULLS FIRST"},
      {OrderType::kAscNullsLast, "ASC NULLS LAST"},
      {OrderType::kDescNullsFirst, "DESC NULLS FIRST"},
      {OrderType::kDescNullsLast, "DESC NULLS LAST"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(OrderType, orderTypeNames);

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

ColumnGroupCP SchemaTable::addIndex(
    const connector::TableLayout& layout,
    Distribution distribution,
    ColumnVector columns) {
  return columnGroups.emplace_back(
      make<ColumnGroup>(
          *this, layout, std::move(distribution), std::move(columns)));
}

ColumnCP SchemaTable::findColumn(Name name) const {
  auto it = columns.find(name);
  VELOX_CHECK(it != columns.end(), "Column not found: {}", name);
  return it->second;
}

const SchemaTable& Schema::getTable(
    const connector::Table& connectorTable) const {
  auto*& table = tables_.try_emplace(&connectorTable).first->second;
  if (table) {
    return *table;
  }

  auto* schemaTable = make<SchemaTable>(connectorTable);
  auto& schemaColumns = schemaTable->columns;

  auto& tableColumns = connectorTable.columnMap();
  schemaColumns.reserve(tableColumns.size());
  for (const auto& [columnName, tableColumn] : tableColumns) {
    const auto cardinality = std::max<float>(
        1,
        tableColumn->approxNumDistinct(
            static_cast<int64_t>(connectorTable.numRows())));
    Value value(toType(tableColumn->type()), cardinality);
    auto* column = make<Column>(toName(columnName), nullptr, value);
    schemaColumns[column->name()] = column;
  }

  auto appendColumns = [&](const auto& from, auto& to) {
    VELOX_DCHECK(to.empty());
    to.reserve(from.size());
    for (const auto* column : from) {
      VELOX_CHECK_NOT_NULL(column);
      const auto& name = column->name();
      auto it = schemaColumns.find(toName(name));
      VELOX_CHECK(it != schemaColumns.end(), "Column not found: {}", name);
      to.push_back(it->second);
    }
  };

  for (const auto* layout : connectorTable.layouts()) {
    VELOX_CHECK_NOT_NULL(layout);
    ExprVector partition;
    appendColumns(layout->partitionColumns(), partition);

    ExprVector orderKeys;
    appendColumns(layout->orderColumns(), orderKeys);

    OrderTypeVector orderTypes;
    orderTypes.reserve(orderKeys.size());
    for (auto orderType : layout->sortOrder()) {
      orderTypes.push_back(
          orderType.isAscending
              ? (orderType.isNullsFirst ? OrderType::kAscNullsFirst
                                        : OrderType::kAscNullsLast)
              : (orderType.isNullsFirst ? OrderType::kDescNullsFirst
                                        : OrderType::kDescNullsLast));
    }
    VELOX_CHECK_EQ(orderKeys.size(), orderTypes.size());

    Distribution distribution(
        DistributionType{.partitionType = layout->partitionType()},
        std::move(partition),
        std::move(orderKeys),
        std::move(orderTypes));

    ColumnVector columns;
    appendColumns(layout->columns(), columns);
    schemaTable->addIndex(*layout, std::move(distribution), std::move(columns));
  }
  table = schemaTable;
  return *table;
}

float tableCardinality(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    return table->as<BaseTable>()->schemaTable->cardinality;
  }
  if (table->is(PlanType::kValuesTableNode)) {
    return table->as<ValuesTable>()->cardinality();
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    return table->as<UnnestTable>()->cardinality();
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
    auto nUnique = index->distribution.numKeysUnique();
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

  const auto numSorting = distribution.orderTypes().size();
  const auto numUnique = distribution.numKeysUnique();

  PlanObjectSet covered;
  for (auto i = 0; i < numSorting || i < numUnique; ++i) {
    auto orderKey = distribution.orderKeys()[i];
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
  VELOX_DCHECK(table);
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
  if (table->is(PlanType::kUnnestTableNode)) {
    const auto* unnestTable = table->as<UnnestTable>();
    computeCardinalities(unnestTable->cardinality());
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

Distribution::NeedsShuffle Distribution::maybeNeedsShuffle(
    const Distribution& desired) const {
  if (isBroadcast()) {
    // If 'this' is broadcast, no repartitioning is needed.
    return NeedsShuffle::kNo;
  }
  if (desired.isBroadcast()) {
    // If 'desired' is broadcast, repartitioning is needed.
    return NeedsShuffle::kYes;
  }
  if (isGather() && desired.isGather()) {
    // Both are gather, no repartitioning needed.
    return NeedsShuffle::kNo;
  }
  if (isGather() || desired.isGather()) {
    // One is gather, the other is not, repartitioning needed.
    return NeedsShuffle::kYes;
  }
  return NeedsShuffle::kMaybe;
}

bool Distribution::needsShuffle(const Distribution& desired) const {
  const auto needsShuffle = maybeNeedsShuffle(desired);
  if (needsShuffle != NeedsShuffle::kMaybe) {
    return needsShuffle == NeedsShuffle::kYes;
  }
  if (!hasCopartition(partitionType(), desired.partitionType())) {
    // Different partition types, repartitioning needed.
    return true;
  }
  // TODO: Probably we want copartition type decide this.
  // For an example range partitioning may not need shuffle if
  // the ranges are compatible, e.g. "a, b, c" and "a, b".

  if (partitionKeys().size() != desired.partitionKeys().size()) {
    // Different number of partition keys, repartitioning needed.
    return true;
  }
  for (size_t i = 0; i < partitionKeys().size(); ++i) {
    if (!partitionKeys()[i]->sameOrEqual(*desired.partitionKeys()[i])) {
      // Different partition key, repartitioning needed.
      return true;
    }
  }
  return false;
}

bool Distribution::needsSort(const Distribution& desired) const {
  if (orderKeys().size() < desired.orderKeys().size()) {
    // Not enough ordering keys, needs sort.
    return true;
  }
  for (size_t i = 0; i < desired.orderKeys().size(); ++i) {
    if (!orderKeys()[i]->sameOrEqual(*desired.orderKeys()[i]) ||
        orderTypes()[i] != desired.orderTypes()[i]) {
      // Different ordering key or order type, needs sort.
      return true;
    }
  }
  return false;
}

Distribution Distribution::rename(
    const ExprVector& exprs,
    const ColumnVector& names) const {
  // Partitioning survives projection if all partitioning columns are projected
  // out.
  ExprVector partitionKeys = partitionKeys_;
  if (!replace(partitionKeys, exprs, names)) {
    partitionKeys.clear();
  }

  // Ordering survives if a prefix of the previous order continues to be
  // projected out.
  ExprVector orderKeys = orderKeys_;
  OrderTypeVector orderTypes = orderTypes_;

  const auto newOrderSize = prefixSize(orderKeys_, exprs);
  orderKeys.resize(newOrderSize);
  orderTypes.resize(newOrderSize);
  replace(orderKeys, exprs, names);
  VELOX_DCHECK_EQ(orderKeys.size(), orderTypes.size());
  return {
      distributionType_,
      std::move(partitionKeys),
      std::move(orderKeys),
      std::move(orderTypes),
      numKeysUnique_};
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
  if (isBroadcast()) {
    return "broadcast";
  }

  if (isGather()) {
    return "gather";
  }

  std::stringstream out;
  if (!partitionKeys().empty()) {
    out << "P ";
    exprsToString(partitionKeys(), out);
    if (partitionType() != nullptr) {
      out << " " << partitionType()->toString();
    } else {
      out << " Velox hash";
    }
  }
  if (!orderKeys().empty()) {
    out << " O ";
    exprsToString(orderKeys(), out);
  }
  if (numKeysUnique() && numKeysUnique() >= orderKeys().size()) {
    out << " first " << numKeysUnique() << " unique";
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
