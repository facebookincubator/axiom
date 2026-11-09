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
#include "axiom/sql/presto/VectorIndexRefresh.h"

#include <fmt/format.h>
#include <map>

#include "axiom/sql/presto/ColumnExprs.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
using facebook::axiom::connector::VectorIndex;
using facebook::velox::BIGINT;
using facebook::velox::MAP;
using facebook::velox::TypePtr;
using facebook::velox::VARCHAR;
using facebook::velox::Variant;
using facebook::velox::VECTOR_INDEX;
using facebook::velox::common::vectorindex::VectorIndexKindName;
using facebook::velox::common::vectorindex::VectorIndexMetricName;

namespace {

// The points each partition's codec is trained on, per cluster.
constexpr int64_t kSamplesPerCluster{256};
// The most rows a local index holds.
constexpr int64_t kRowsPerLocalIndex{50'000};

// The plans name the query's columns by these names and partitionNames(), so
// they never collide with the plans' own columns.
constexpr std::string_view kKey{"key"};
constexpr std::string_view kEmbedding{"embedding"};

// Returns the names the plans give the partition columns: p0, p1, ... The
// writes map them back to the index's partition columns.
std::vector<std::string> partitionNames(size_t numPartitionColumns) {
  std::vector<std::string> names;
  names.reserve(numPartitionColumns);
  for (size_t i = 0; i < numPartitionColumns; ++i) {
    names.push_back(fmt::format("p{}", i));
  }
  return names;
}

// Returns 'value', or fails the refresh with 'message' when 'condition' holds:
//   if(condition, CAST(fail(message) AS type), value)
lp::ExprApi failIf(
    const lp::ExprApi& condition,
    const lp::ExprApi& message,
    const TypePtr& type,
    const lp::ExprApi& value) {
  return lp::Call(
      "if", condition, lp::Cast(type, lp::Call("fail", message)), value);
}

// Returns 'prefix' followed by the values of 'partitions':
//   concat(prefix, CAST(p0 AS VARCHAR), ', ', CAST(p1 AS VARCHAR), ...)
lp::ExprApi partitionMessage(
    std::string_view prefix,
    const std::vector<std::string>& partitions) {
  std::vector<lp::ExprApi> parts{lp::Lit(std::string(prefix))};
  for (const auto& partition : partitions) {
    if (parts.size() > 1) {
      parts.push_back(lp::Lit(", "));
    }
    parts.push_back(lp::Cast(VARCHAR(), lp::Col(partition)));
  }
  return lp::Call("concat", parts);
}

// Returns 'query' (see VectorIndexRefresh::plan) with its columns named kKey,
// kEmbedding, then 'partitions', by position.
lp::PlanBuilder namedRows(
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& query) {
  const auto columns = query.findOrAssignOutputNames();
  std::vector<lp::ExprApi> projections{
      columns[0].toCol().as(kKey),
      columns[1].toCol().as(kEmbedding),
  };
  for (size_t i = 0; i < partitions.size(); ++i) {
    projections.push_back(columns[i + 2].toCol().as(partitions[i]));
  }
  return lp::PlanBuilder(query).project(projections);
}

// Returns whether a row of namedRows() is valid for 'index':
//   key IS NOT NULL
//   AND cardinality(embedding) = <dimension>
//   AND none_match(embedding, x -> x IS NULL OR NOT is_finite(x))
//   AND l2_norm(embedding) > 0
lp::ExprApi isValid(const VectorIndex& index) {
  const auto embedding = lp::Col(kEmbedding);
  const auto element = lp::Col("x");
  return lp::Col(kKey).isNotNull() &&
      lp::Call("cardinality", embedding) ==
      lp::Lit(index.properties.dimension) &&
      lp::Call(
             "none_match",
             embedding,
             lp::Lambda(
                 {"x"},
                 element.isNull() ||
                     lp::Call("not", lp::Call("is_finite", element)))) &&
      lp::Call("l2_norm", embedding) > lp::Lit(0.0);
}

// Returns the valid rows of 'query' (see namedRows() and isValid()).
lp::PlanBuilder validRows(
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& query) {
  return namedRows(partitions, query).filter(isValid(index));
}

// Returns the distinct partitions of 'query' with no NULL value, with their
// columns named 'partitions'. The refresh writes a codec and local indexes for
// each of them, NULL for one with no valid rows, so the index covers every
// partition it refreshed. A valid row with a NULL partition value fails the
// refresh; an invalid one is skipped.
lp::PlanBuilder selectedPartitions(
    const facebook::axiom::SchemaTableName& table,
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& query) {
  const auto types = query.outputTypes();
  const auto valid = isValid(index);
  std::vector<lp::ExprApi> checked;
  std::optional<lp::ExprApi> notNull;
  for (size_t i = 0; i < partitions.size(); ++i) {
    const auto value = lp::Col(partitions[i]);
    const auto isNull = value.isNull();
    checked.push_back(
        failIf(
            isNull && valid,
            lp::Lit(
                fmt::format(
                    "Vector index row has a NULL partition value: {} of {}",
                    index.name,
                    table.toString())),
            types[i + 2],
            value)
            .as(partitions[i]));
    const auto condition = value.isNotNull();
    notNull = notNull.has_value() ? *notNull && condition : condition;
  }
  return namedRows(partitions, query)
      .project(checked)
      .filter(*notNull)
      .aggregate(ColumnExprs::of(partitions), {});
}

// Returns a stratified sample of 'rows': up to kSamplesPerCluster times the
// number of clusters points of each partition, chosen by a hash of their keys.
lp::PlanBuilder sample(
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& rows) {
  const auto hash =
      lp::Call("xxhash64", lp::Call("to_big_endian_64", lp::Col(kKey)));
  return lp::PlanBuilder(rows)
      .with({lp::Call("row_number")
                 .over(
                     lp::WindowSpec()
                         .partitionBy(ColumnExprs::of(partitions))
                         .orderBy({lp::SortKey(hash)}))
                 .as("sample_row")})
      .filter(
          lp::Col("sample_row") <=
          lp::Lit(kSamplesPerCluster * index.properties.numClusters));
}

// Returns the plan that trains a codec for each partition of 'rows' and writes
// it to the codec table with the id 'codecId', with a NULL codec for each of
// 'selected' that has no rows.
lp::LogicalPlanNodePtr planTrain(
    const VectorIndex& index,
    const std::string& connectorId,
    const std::vector<std::string>& partitions,
    const lp::ExprApi& codecId,
    const std::string& metric,
    const lp::PlanBuilder& rows,
    const lp::PlanBuilder& selected) {
  const auto columnNames =
      VectorIndex::codecTableType(index.partitionColumns)->names();

  auto codecs =
      sample(index, partitions, rows)
          .aggregate(
              ColumnExprs::of(partitions),
              {lp::Call(
                   "vector_index_train",
                   lp::Col(kEmbedding),
                   lp::Col(kKey),
                   lp::Lit(index.properties.numClusters),
                   lp::Lit(metric),
                   lp::Lit(
                       Variant::map(std::map<Variant, Variant>{}),
                       MAP(VARCHAR(), VARCHAR())))
                   .as("codec")})
          .project(
              ColumnExprs::concat(
                  ColumnExprs::renamed(partitions, "t_"), lp::Col("codec")));

  return lp::PlanBuilder(selected)
      .join(codecs, *ColumnExprs::equal(partitions, "t_"), lp::JoinType::kLeft)
      .tableWrite(
          connectorId,
          index.codecTable.schema,
          index.codecTable.table,
          lp::WriteKind::kInsert,
          columnNames,
          ColumnExprs::concat(
              ColumnExprs::concat(
                  {lp::Col("codec")}, ColumnExprs::of(partitions)),
              codecId))
      .build();
}

// Returns the plan that assigns each row of 'rows' to the nearest cluster of
// its partition's codec 'codecId', builds the local indexes of each cluster and
// writes them to the index table, with a row of NULL local index for each of
// 'selected' that has no rows.
lp::LogicalPlanNodePtr planBuild(
    const facebook::axiom::SchemaTableName& table,
    const VectorIndex& index,
    const std::string& connectorId,
    const std::vector<std::string>& partitionColumns,
    const std::vector<std::string>& partitions,
    const lp::ExprApi& codecId,
    const std::string& metric,
    const lp::PlanBuilder& rows,
    const lp::PlanBuilder& selected) {
  const auto indexType =
      std::string(VectorIndexKindName::toName(index.properties.indexKind));
  const std::string codecIdColumn{VectorIndex::kCodecId};

  // The centroids of this refresh's codecs, with their partition columns named
  // "c_" + the partition's name in 'rows'.
  std::vector<lp::ExprApi> centroidColumns;
  for (size_t i = 0; i < partitions.size(); ++i) {
    centroidColumns.push_back(
        lp::Col(partitionColumns[i]).as("c_" + partitions[i]));
  }
  const auto condition = ColumnExprs::equal(partitions, "c_");
  centroidColumns.push_back(lp::Col(VectorIndex::kClusterId));
  centroidColumns.push_back(lp::Col(VectorIndex::kCentroid));
  const auto centroidsName = VectorIndex::centroidsTableName(table, index.name);
  auto centroids =
      rows.newPlan()
          .tableScan(connectorId, centroidsName.schema, centroidsName.table)
          .filter(lp::Col(codecIdColumn) == codecId)
          .project(centroidColumns);

  // The build reads the query again, so it can see a partition the train did
  // not, e.g. one a concurrent write added to the query's input. Such a row
  // has no codec and fails the refresh.
  const auto clusterId = failIf(
      lp::Col("cluster_id").isNull(),
      partitionMessage("No codec for partition: ", partitions),
      BIGINT(),
      lp::Col("cluster_id"));

  const auto partitionKeys = ColumnExprs::of(partitions);

  std::map<Variant, Variant> options;
  for (const auto& [key, value] : index.properties.indexOptions) {
    options.emplace(Variant(key), Variant(value));
  }

  const auto columnNames =
      VectorIndex::indexTableType(index.partitionColumns)->names();

  auto localIndexes =
      lp::PlanBuilder(rows)
          // Each row's nearest centroid of its partition's codec.
          .vectorSearch(
              centroids,
              condition,
              lp::JoinType::kLeft,
              {
                  .probeEmbedding = std::string(kEmbedding),
                  .buildEmbedding = std::string(VectorIndex::kCentroid),
                  .k = 1,
              })
          .project(
              ColumnExprs::concat(
                  partitionKeys,
                  {
                      clusterId.as("cluster_id"),
                      lp::Col(kKey),
                      lp::Col(kEmbedding),
                  }))
          // row_number() OVER (PARTITION BY <partition columns>, cluster_id)
          //     AS cluster_row
          .with({lp::Call("row_number")
                     .over(
                         lp::WindowSpec().partitionBy(
                             ColumnExprs::concat(
                                 partitionKeys, lp::Col("cluster_id"))))
                     .as("cluster_row")})
          .with({((lp::Col("cluster_row") - lp::Lit(int64_t{1})) /
                  lp::Lit(kRowsPerLocalIndex))
                     .as("group_id")})
          .aggregate(
              ColumnExprs::concat(
                  partitionKeys, {lp::Col("cluster_id"), lp::Col("group_id")}),
              {lp::Call(
                   "vector_index_build",
                   lp::Col(kEmbedding),
                   lp::Col(kKey),
                   lp::Lit(indexType),
                   lp::Lit(metric),
                   lp::Lit(Variant::map(options), MAP(VARCHAR(), VARCHAR())))
                   .as("local_index")})
          .project(
              ColumnExprs::concat(
                  ColumnExprs::renamed(partitions, "b_"),
                  {lp::Col("cluster_id"),
                   lp::Col("group_id"),
                   lp::Col("local_index")}));

  return lp::PlanBuilder(selected)
      .join(
          localIndexes,
          *ColumnExprs::equal(partitions, "b_"),
          lp::JoinType::kLeft)
      .tableWrite(
          connectorId,
          index.indexTable.schema,
          index.indexTable.table,
          lp::WriteKind::kInsert,
          columnNames,
          ColumnExprs::concat(
              {
                  codecId,
                  lp::Col("cluster_id"),
                  lp::Col("group_id"),
                  lp::Col("local_index"),
              },
              partitionKeys))
      .build();
}

} // namespace

// static
VectorIndexRefresh::Plans VectorIndexRefresh::plan(
    const facebook::axiom::SchemaTableName& table,
    const VectorIndex& index,
    const std::string& connectorId,
    const lp::PlanBuilder& query) {
  const auto& partitionColumns = index.partitionColumns->names();
  VELOX_CHECK(!partitionColumns.empty());
  const auto codecId = lp::Placeholder(BIGINT(), 1);
  const auto partitions = partitionNames(partitionColumns.size());
  const auto rows = validRows(index, partitions, query);
  const auto selected = selectedPartitions(table, index, partitions, query);
  const auto metric =
      std::string(VectorIndexMetricName::toName(index.properties.metric));
  return {
      .trainPlan = planTrain(
          index, connectorId, partitions, codecId, metric, rows, selected),
      .buildPlan = planBuild(
          table,
          index,
          connectorId,
          partitionColumns,
          partitions,
          codecId,
          metric,
          rows,
          selected),
  };
}

} // namespace axiom::sql::presto
