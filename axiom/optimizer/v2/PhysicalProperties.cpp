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

Partitioning Partitioning::globalHash(
    const ExprVector& keys,
    bool replicateNullsAndAny) {
  return Partitioning{
      .kind = PartitionKind::kPartitioned,
      .keys = keys,
      .scope = PropertyScope::kGlobal,
      .replicateNullsAndAny = replicateNullsAndAny};
}

Partitioning Partitioning::globalGather() {
  return Partitioning{
      .kind = PartitionKind::kGather, .scope = PropertyScope::kGlobal};
}

Partitioning Partitioning::globalGatherMerge(
    const ExprVector& orderKeys,
    const OrderTypeVector& orderTypes) {
  return Partitioning{
      .kind = PartitionKind::kGather,
      .orderKeys = orderKeys,
      .orderTypes = orderTypes,
      .scope = PropertyScope::kGlobal};
}

Partitioning Partitioning::globalBroadcast() {
  return Partitioning{
      .kind = PartitionKind::kBroadcast, .scope = PropertyScope::kGlobal};
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

bool Partitioning::isBucketedCompatibleWith(
    const ExprVector& otherKeys,
    const connector::PartitionType& targetType) const {
  if (!isBucketedOn(otherKeys)) {
    return false;
  }
  const auto compatible = partitionType->copartition(targetType);
  return compatible != nullptr &&
      compatible->numPartitions() == partitionType->numPartitions();
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

void PhysicalProperties::checkReferencesColumns(
    const PlanObjectSet& outputColumns) const {
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
