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

#include "axiom/connectors/hive/HiveVectorIndex.h"

#include <fmt/format.h>

#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "velox/common/vectorindex/VectorIndexKind.h"
#include "velox/common/vectorindex/VectorIndexMetric.h"

namespace facebook::axiom::connector::hive {

folly::dynamic HiveVectorIndex::toJson(const VectorIndex& index) {
  folly::dynamic indexOptions = folly::dynamic::object;
  for (const auto& [key, value] : index.properties.indexOptions) {
    indexOptions[key] = value;
  }
  return folly::dynamic::object("name", index.name)(
      "keyColumn", index.keyColumn)("embeddingColumn", index.embeddingColumn)(
      "distanceMetric",
      velox::common::vectorindex::VectorIndexMetricName::toName(
          index.properties.metric))(
      "indexType",
      velox::common::vectorindex::VectorIndexKindName::toName(
          index.properties.indexKind))(
      "numClusters", index.properties.numClusters)(
      "dimension", index.properties.dimension)(
      "codecPolicy",
      VectorIndexCodecPolicyName::toName(index.properties.codecPolicy))(
      "indexOptions", std::move(indexOptions));
}

VectorIndexPtr HiveVectorIndex::fromJson(
    const SchemaTableName& table,
    const velox::RowTypePtr& partitionColumns,
    const folly::dynamic& json) {
  std::map<std::string, std::string> indexOptions;
  for (const auto& [key, value] : json["indexOptions"].items()) {
    indexOptions.emplace(key.asString(), value.asString());
  }
  auto name = json["name"].asString();
  auto [codecTable, indexTable] = storageTableNames(table, name);
  return std::make_shared<const VectorIndex>(VectorIndex{
      .name = std::move(name),
      .keyColumn = json["keyColumn"].asString(),
      .embeddingColumn = json["embeddingColumn"].asString(),
      .partitionColumns = partitionColumns,
      .properties =
          {
              .metric = velox::common::vectorindex::VectorIndexMetricName::
                  toVectorIndexMetric(json["distanceMetric"].asString()),
              .indexKind = velox::common::vectorindex::VectorIndexKindName::
                  toVectorIndexKind(json["indexType"].asString()),
              .numClusters = json["numClusters"].asInt(),
              .dimension = json["dimension"].asInt(),
              .codecPolicy =
                  VectorIndexCodecPolicyName::toVectorIndexCodecPolicy(
                      json["codecPolicy"].asString()),
              .indexOptions = std::move(indexOptions),
          },
      .codecTable = std::move(codecTable),
      .indexTable = std::move(indexTable),
  });
}

std::pair<SchemaTableName, SchemaTableName> HiveVectorIndex::storageTableNames(
    const SchemaTableName& table,
    std::string_view name) {
  return {
      {table.schema, fmt::format("{}__{}__codecs", table.table, name)},
      {table.schema, fmt::format("{}__{}__index", table.table, name)},
  };
}

std::pair<VectorIndexPtr, folly::F14FastMap<std::string, velox::Variant>>
HiveVectorIndex::make(
    const SchemaTableName& table,
    const velox::RowTypePtr& partitionColumns,
    ResolvedVectorIndex resolved) {
  auto& storageProperties = resolved.storageProperties;
  for (const auto* property : {
           HiveWriteOptions::kPartitionedBy,
           HiveWriteOptions::kBucketedBy,
           HiveWriteOptions::kBucketCount,
           HiveWriteOptions::kSortedBy,
       }) {
    VELOX_USER_CHECK(
        !storageProperties.contains(property),
        "Unsupported property for a vector index: {}",
        property);
  }
  auto [codecTable, indexTable] = storageTableNames(table, resolved.name);
  return {
      std::make_shared<const VectorIndex>(VectorIndex{
          .name = std::move(resolved.name),
          .keyColumn = std::move(resolved.keyColumn),
          .embeddingColumn = std::move(resolved.embeddingColumn),
          .partitionColumns = partitionColumns,
          .properties = std::move(resolved.properties),
          .codecTable = std::move(codecTable),
          .indexTable = std::move(indexTable),
      }),
      std::move(storageProperties)};
}

std::array<std::pair<SchemaTableName, TableDefinition>, 2>
HiveVectorIndex::storageTableDefinitions(
    const VectorIndex& index,
    const folly::F14FastMap<std::string, velox::Variant>& storageProperties) {
  const auto partitionedBy = [](std::vector<std::string> names) {
    return velox::Variant::array(
        std::vector<velox::Variant>(names.begin(), names.end()));
  };
  auto codecDefinition = VectorIndex::codecTableDefinition(
      index.partitionColumns, storageProperties);
  codecDefinition.properties[HiveWriteOptions::kPartitionedBy] =
      partitionedBy(codecDefinition.primaryKey);
  auto indexProperties = storageProperties;
  if (index.partitionColumns->size() > 0) {
    indexProperties[HiveWriteOptions::kPartitionedBy] =
        partitionedBy(index.partitionColumns->names());
  }
  return {{
      {
          index.codecTable,
          std::move(codecDefinition),
      },
      {
          index.indexTable,
          TableDefinition::create(
              VectorIndex::indexTableType(index.partitionColumns),
              std::move(indexProperties)),
      },
  }};
}

} // namespace facebook::axiom::connector::hive
