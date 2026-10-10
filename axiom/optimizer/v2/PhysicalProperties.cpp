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

#include "axiom/optimizer/v2/PhysicalProperties.h"

#include <algorithm>

#include <folly/container/F14Map.h>

#include "axiom/connectors/ConnectorMetadata.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

const folly::F14FastMap<PropertyScope, std::string_view>& propertyScopeNames() {
  static const folly::F14FastMap<PropertyScope, std::string_view> kNames = {
      {PropertyScope::kGlobal, "Global"},
      {PropertyScope::kDriver, "Driver"},
  };
  return kNames;
}

const folly::F14FastMap<PartitionKind, std::string_view>& partitionKindNames() {
  static const folly::F14FastMap<PartitionKind, std::string_view> kNames = {
      {PartitionKind::kUnspecified, "Unspecified"},
      {PartitionKind::kPartitioned, "Partitioned"},
      {PartitionKind::kGather, "Gather"},
      {PartitionKind::kBroadcast, "Broadcast"},
      {PartitionKind::kArbitrary, "Arbitrary"},
  };
  return kNames;
}

const folly::F14FastMap<LocalPropertyKind, std::string_view>&
localPropertyKindNames() {
  static const folly::F14FastMap<LocalPropertyKind, std::string_view> kNames = {
      {LocalPropertyKind::kGrouped, "Grouped"},
      {LocalPropertyKind::kSorted, "Sorted"},
  };
  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(PropertyScope, propertyScopeNames);
AXIOM_DEFINE_ENUM_NAME(PartitionKind, partitionKindNames);
AXIOM_DEFINE_ENUM_NAME(LocalPropertyKind, localPropertyKindNames);

Partitioning Partitioning::hash(
    PropertyScope scope,
    const ExprVector& keys,
    bool replicateNullsAndAny) {
  VELOX_CHECK(
      !replicateNullsAndAny || scope == PropertyScope::kGlobal,
      "Replicating nulls and any row requires global partitioning");
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .keys = keys,
      .scope = scope,
      .replicateNullsAndAny = replicateNullsAndAny};
}

Partitioning Partitioning::connectorHash(
    PropertyScope scope,
    const ExprVector& keys,
    const connector::PartitionType* partitionType) {
  VELOX_DCHECK_NOT_NULL(partitionType);
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .partitionType = partitionType,
      .keys = keys,
      .scope = scope,
  };
}

std::optional<Partitioning::CommonType> Partitioning::commonPartitionType(
    const connector::PartitionType* left,
    const connector::PartitionType* right) {
  if (left == nullptr || right == nullptr) {
    return left == right ? std::optional<CommonType>{{nullptr}} : std::nullopt;
  }
  const auto* common = queryCtx()->copartitionedType(left, right);
  return common == nullptr ? std::nullopt : std::optional<CommonType>{{common}};
}

std::optional<Partitioning::CommonType> Partitioning::commonUnionPartitionType(
    const connector::PartitionType* left,
    const connector::PartitionType* right) {
  return commonPartitionType(left, right);
}

std::optional<Partitioning::CommonType> Partitioning::commonJoinPartitionType(
    const connector::PartitionType* left,
    const connector::PartitionType* right) {
  const auto common = commonPartitionType(left, right);
  if (!common.has_value() || common->connectorType == nullptr) {
    return common;
  }
  return common->connectorType->numPartitions() == left->numPartitions() &&
          common->connectorType->numPartitions() == right->numPartitions()
      ? common
      : std::nullopt;
}

Partitioning Partitioning::gather(PropertyScope scope) {
  return Partitioning{.kind = PartitionKind::kGather, .scope = scope};
}

Partitioning Partitioning::gatherMerge(
    PropertyScope scope,
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes) {
  return Partitioning{
      .kind = PartitionKind::kGather,
      .orderKeys = orderKeys,
      .orderTypes = orderTypes,
      .scope = scope};
}

Partitioning Partitioning::globalBroadcast() {
  return Partitioning{
      .kind = PartitionKind::kBroadcast, .scope = PropertyScope::kGlobal};
}

Partitioning Partitioning::globalReplicatedTo(const Partitioning& consumer) {
  return consumer.is(PartitionKind::kGather) ? gather(PropertyScope::kGlobal)
                                             : globalBroadcast();
}

Partitioning Partitioning::globalArbitrary() {
  return Partitioning{
      .kind = PartitionKind::kArbitrary, .scope = PropertyScope::kGlobal};
}

bool Partitioning::coLocates(const ExprVector& columns) const {
  if (kind != PartitionKind::kPartitioned || keys.empty()) {
    return false;
  }
  const auto columnSet = PlanObjectSet::fromObjects(columns);
  for (ExprCP key : keys) {
    if (!columnSet.contains(key)) {
      return false;
    }
  }
  return true;
}

bool Partitioning::isBucketedOn(const ExprVector& otherKeys) const {
  if (otherKeys.empty() || kind != PartitionKind::kPartitioned ||
      partitionType == nullptr || keys.size() != otherKeys.size()) {
    return false;
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!keys[i]->sameOrEqual(*otherKeys[i])) {
      return false;
    }
  }
  return true;
}

bool Partitioning::satisfiesWritePartitioning(
    const ExprVector& otherKeys,
    const connector::PartitionType& targetType) const {
  if (!isBucketedOn(otherKeys)) {
    return false;
  }
  const auto common = commonPartitionType(partitionType, &targetType);
  return common.has_value() &&
      common->connectorType->numPartitions() == partitionType->numPartitions();
}

bool Partitioning::sameClassAs(const Partitioning& other) const {
  if (kind != other.kind || partitionType != other.partitionType ||
      replicateNullsAndAny != other.replicateNullsAndAny ||
      keys.size() != other.keys.size()) {
    return false;
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] != other.keys[i]) {
      return false;
    }
  }
  return true;
}

bool PhysicalProperties::driverCoLocates(const ExprVector& keys) const {
  if (driverPartition.is(PartitionKind::kGather)) {
    return true;
  }
  if (!keys.empty() && driverPartition.coLocates(keys)) {
    return true;
  }

  const auto keySet = PlanObjectSet::fromObjects(keys);
  if (std::ranges::any_of(unique, [&](const UniqueKeySet& key) {
        return key.scope == PropertyScope::kGlobal &&
            key.columns.isSubset(keySet);
      })) {
    return true;
  }
  if (keys.empty()) {
    return false;
  }
  const auto isKey = [&](ColumnCP column) {
    return std::ranges::any_of(
        keys, [&](ExprCP key) { return column->sameOrEqual(*key); });
  };
  return std::ranges::any_of(local, [&](const LocalProperty& property) {
    return property.kind == LocalPropertyKind::kGrouped &&
        std::ranges::all_of(property.columns, isKey);
  });
}

void PhysicalProperties::checkReferencesColumns(
    const PlanObjectSet& outputColumns) const {
  VELOX_CHECK_EQ(
      globalPartition.scope,
      PropertyScope::kGlobal,
      "globalPartition must have global scope");
  VELOX_CHECK_EQ(
      driverPartition.scope,
      PropertyScope::kDriver,
      "driverPartition must have driver scope");
  // A partitioning may be on computed expressions (the partition function's
  // input, e.g. a cast); the requirement is that it is evaluable here — every
  // column its keys and merge order depend on is an output column.
  const auto checkPartition = [&](const Partitioning& partition) {
    VELOX_CHECK(
        outputColumns.containsColumns(partition.keys) &&
            outputColumns.containsColumns(partition.orderKeys),
        "Partitioning depends on a column that is not in the node's output");
  };
  checkPartition(globalPartition);
  checkPartition(driverPartition);
  // Local orderings / groupings and unique key-sets are columns, not
  // expressions.
  for (const LocalProperty& property : local) {
    VELOX_CHECK(
        outputColumns.containsAll(property.columns),
        "Local property references a column that is not in the node's output");
  }
  for (const UniqueKeySet& keySet : unique) {
    VELOX_CHECK(
        keySet.columns.isSubset(outputColumns),
        "Unique key-set references a column that is not in the node's output");
  }
}

} // namespace facebook::axiom::optimizer::v2
