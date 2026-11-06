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

#include "axiom/connectors/VectorIndex.h"

#include <fmt/format.h>

#include <vector>

#include "velox/common/vectorindex/LocalVectorIndexOptions.h"

namespace facebook::axiom::connector {

namespace vectorindex = velox::common::vectorindex;

namespace {

const auto& vectorIndexCodecPolicyNames() {
  static const folly::F14FastMap<VectorIndexCodecPolicy, std::string_view>
      kNames = {
          {VectorIndexCodecPolicy::kPerPartition, "per_partition"},
          {VectorIndexCodecPolicy::kShared, "shared"},
      };
  return kNames;
}

// Removes 'name' from 'properties' and returns its value, or std::nullopt if
// it is not set.
std::optional<velox::Variant> take(
    folly::F14FastMap<std::string, velox::Variant>& properties,
    std::string_view name) {
  auto it = properties.find(name);
  if (it == properties.end()) {
    return std::nullopt;
  }
  auto value = std::move(it->second);
  properties.erase(it);
  VELOX_USER_CHECK(
      !value.isNull(), "Vector index property must not be null: {}", name);
  return value;
}

velox::Variant takeRequired(
    folly::F14FastMap<std::string, velox::Variant>& properties,
    std::string_view name) {
  auto value = take(properties, name);
  VELOX_USER_CHECK(
      value.has_value(), "Missing required vector index property: {}", name);
  return std::move(*value);
}

std::string toString(const velox::Variant& value, std::string_view name) {
  VELOX_USER_CHECK_EQ(
      value.kind(),
      velox::TypeKind::VARCHAR,
      "Vector index property must be a string: {}",
      name);
  VELOX_USER_CHECK(
      !value.isNull(), "Vector index property must not be null: {}", name);
  return value.value<std::string>();
}

int64_t toInteger(const velox::Variant& value, std::string_view name) {
  switch (value.kind()) {
    case velox::TypeKind::TINYINT:
      return value.value<int8_t>();
    case velox::TypeKind::SMALLINT:
      return value.value<int16_t>();
    case velox::TypeKind::INTEGER:
      return value.value<int32_t>();
    case velox::TypeKind::BIGINT:
      return value.value<int64_t>();
    default:
      VELOX_USER_FAIL("Vector index property must be an integer: {}", name);
  }
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(VectorIndexCodecPolicy, vectorIndexCodecPolicyNames);

VectorIndexProperties VectorIndexProperties::extract(
    folly::F14FastMap<std::string, velox::Variant>& properties) {
  const auto metricName =
      toString(takeRequired(properties, kDistanceMetric), kDistanceMetric);
  const auto metric =
      vectorindex::VectorIndexMetricName::tryToVectorIndexMetric(metricName);
  VELOX_USER_CHECK(
      metric.has_value(), "Unsupported distance metric: {}", metricName);

  const auto typeName =
      toString(takeRequired(properties, kIndexType), kIndexType);
  const auto indexKind =
      vectorindex::VectorIndexKindName::tryToVectorIndexKind(typeName);
  VELOX_USER_CHECK(
      indexKind.has_value(), "Unsupported index type: {}", typeName);

  const auto numClusters =
      toInteger(takeRequired(properties, kNumClusters), kNumClusters);
  VELOX_USER_CHECK_GT(
      numClusters,
      0,
      "Vector index property must be positive: {}",
      kNumClusters);

  const auto dimension =
      toInteger(takeRequired(properties, kDimension), kDimension);
  VELOX_USER_CHECK_GT(
      dimension, 0, "Vector index property must be positive: {}", kDimension);

  auto codecPolicy = VectorIndexCodecPolicy::kPerPartition;
  if (auto value = take(properties, kCodecPolicy)) {
    const auto policyName = toString(*value, kCodecPolicy);
    const auto policy =
        VectorIndexCodecPolicyName::tryToVectorIndexCodecPolicy(policyName);
    VELOX_USER_CHECK(
        policy.has_value(),
        "Unsupported codec policy: {}. Supported: {}",
        policyName,
        VectorIndexCodecPolicyName::allNames());
    codecPolicy = *policy;
  }

  std::map<std::string, std::string> indexOptions;
  if (auto value = take(properties, kIndexOptions)) {
    VELOX_USER_CHECK_EQ(
        value->kind(),
        velox::TypeKind::MAP,
        "Vector index property must be a map of strings: {}",
        kIndexOptions);
    for (const auto& [key, option] : value->map()) {
      indexOptions.emplace(
          toString(key, kIndexOptions), toString(option, kIndexOptions));
    }
  }

  std::vector<std::pair<std::string_view, std::string_view>> optionViews(
      indexOptions.begin(), indexOptions.end());
  vectorindex::LocalVectorIndexOptions::parse(*indexKind, optionViews);

  return {
      .metric = *metric,
      .indexKind = *indexKind,
      .numClusters = numClusters,
      .dimension = dimension,
      .codecPolicy = codecPolicy,
      .indexOptions = std::move(indexOptions),
  };
}

folly::F14FastMap<std::string, velox::Variant>
VectorIndexProperties::toProperties() const {
  folly::F14FastMap<std::string, velox::Variant> properties{
      {std::string(kDistanceMetric),
       velox::Variant(vectorindex::VectorIndexMetricName::toName(metric))},
      {std::string(kIndexType),
       velox::Variant(vectorindex::VectorIndexKindName::toName(indexKind))},
      {std::string(kNumClusters), velox::Variant(numClusters)},
      {std::string(kDimension), velox::Variant(dimension)},
      {std::string(kCodecPolicy),
       velox::Variant(VectorIndexCodecPolicyName::toName(codecPolicy))},
  };
  if (!indexOptions.empty()) {
    std::map<velox::Variant, velox::Variant> options;
    for (const auto& [key, value] : indexOptions) {
      options.emplace(velox::Variant(key), velox::Variant(value));
    }
    properties.emplace(
        std::string(kIndexOptions), velox::Variant::map(std::move(options)));
  }
  return properties;
}

namespace {

// Checks that no partition column takes the name of a column the codec,
// index or centroids table adds.
void checkPartitionColumnNames(const velox::RowTypePtr& partitionColumns) {
  for (const auto& name : partitionColumns->names()) {
    for (const auto reserved :
         {VectorIndex::kCodec,
          VectorIndex::kCodecId,
          VectorIndex::kClusterId,
          VectorIndex::kGroupId,
          VectorIndex::kLocalIndex,
          VectorIndex::kCentroid}) {
      VELOX_USER_CHECK(
          name != reserved,
          "Vector index partition column name is reserved: {}",
          name);
    }
  }
}

} // namespace

// static
velox::RowTypePtr VectorIndex::codecTableType(
    const velox::RowTypePtr& partitionColumns) {
  checkPartitionColumnNames(partitionColumns);
  return velox::ROW(std::string(kCodec), velox::VECTOR_INDEX())
      ->unionWith(partitionColumns)
      ->unionWith(velox::ROW(std::string(kCodecId), velox::BIGINT()));
}

// static
velox::RowTypePtr VectorIndex::indexTableType(
    const velox::RowTypePtr& partitionColumns) {
  checkPartitionColumnNames(partitionColumns);
  return velox::ROW({{std::string(kCodecId), velox::BIGINT()},
                     {std::string(kClusterId), velox::BIGINT()},
                     {std::string(kGroupId), velox::BIGINT()},
                     {std::string(kLocalIndex), velox::VECTOR_INDEX()}})
      ->unionWith(partitionColumns);
}

// static
SchemaTableName VectorIndex::centroidsTableName(
    const SchemaTableName& table,
    std::string_view name) {
  return {
      table.schema,
      fmt::format("{}${}{}", table.table, name, kCentroidsSuffix)};
}

// static
std::optional<std::pair<SchemaTableName, std::string>>
VectorIndex::parseCentroidsTableName(const SchemaTableName& name) {
  if (!name.table.ends_with(kCentroidsSuffix)) {
    return std::nullopt;
  }
  const std::string_view tableAndIndex{
      name.table.data(), name.table.size() - kCentroidsSuffix.size()};
  const auto separator = tableAndIndex.rfind('$');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 == tableAndIndex.size()) {
    return std::nullopt;
  }
  return std::make_pair(
      SchemaTableName{
          name.schema, std::string(tableAndIndex.substr(0, separator))},
      std::string(tableAndIndex.substr(separator + 1)));
}

velox::RowTypePtr VectorIndex::centroidColumns() const {
  return partitionColumns->unionWith(
      velox::ROW(
          {{std::string(kCodecId), velox::BIGINT()},
           {std::string(kClusterId), velox::BIGINT()},
           {std::string(kCentroid), velox::ARRAY(velox::REAL())}}));
}

} // namespace facebook::axiom::connector
