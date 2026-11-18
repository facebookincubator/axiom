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
#include "axiom/sql/presto/VectorIndexCodecTrainer.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
using facebook::axiom::connector::VectorIndex;
using facebook::velox::ARRAY;
using facebook::velox::BIGINT;
using facebook::velox::DOUBLE;
using facebook::velox::MAP;
using facebook::velox::REAL;
using facebook::velox::TypePtr;
using facebook::velox::VARCHAR;
using facebook::velox::Variant;
using facebook::velox::VECTOR_INDEX;
using facebook::velox::common::vectorindex::VectorIndexKindName;
using facebook::velox::common::vectorindex::VectorIndexMetricName;

namespace {

// The points per cluster vector_index_train trains each partition's codec on.
constexpr int64_t kSamplesPerCluster{256};

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

// Returns whether a row has no NULL partition value.
lp::ExprApi hasPartition(const std::vector<std::string>& partitions) {
  std::optional<lp::ExprApi> notNull;
  for (const auto& partition : partitions) {
    const auto condition = lp::Col(partition).isNotNull();
    notNull = notNull.has_value() ? *notNull && condition : condition;
  }
  return *notNull;
}

// Returns the valid rows of 'rows', a namedRows() (see isValid()), skipping a
// row with a NULL partition value.
lp::PlanBuilder validRows(
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& rows) {
  return lp::PlanBuilder(rows).filter(
      isValid(index) && hasPartition(partitions));
}

// Returns the number of rows of each partition of 'query', valid or not, in
// 'num_rows', with the partition columns named 'prefix' + 'partitions'. The
// optimizer reads the counts from the connector's metadata, and fails the
// refresh when the metadata can't answer them: counting the rows would read
// the whole input once more than the sample does.
lp::PlanBuilder partitionSizes(
    const std::vector<std::string>& partitions,
    std::string_view prefix,
    const lp::PlanBuilder& query) {
  return namedRows(partitions, query)
      .aggregate(
          ColumnExprs::of(partitions), {lp::MetadataRowCount().as("num_rows")})
      .project(
          ColumnExprs::concat(
              ColumnExprs::renamed(partitions, prefix), lp::Col("num_rows")));
}

// Returns the partitions of 'query' with no NULL value, with their columns
// named 'partitions'. The refresh writes a codec and local indexes for each of
// them, NULL for one with no valid rows, so the index covers every partition
// it refreshed. The optimizer answers the list from the connector's metadata.
lp::PlanBuilder selectedPartitions(
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& query) {
  return namedRows(partitions, query)
      .filter(hasPartition(partitions))
      .aggregate(ColumnExprs::of(partitions), {});
}

// Returns the hash of the key that samples the rows, uniform over BIGINT.
lp::ExprApi sampleHash() {
  return lp::Call(
      "from_big_endian_64",
      lp::Call("xxhash64", lp::Call("to_big_endian_64", lp::Col(kKey))));
}

// Returns a stratified sample of 'rows': the valid rows among about 'numRows'
// rows of each partition, chosen by a hash of their keys, so the sample is the
// same on every run. 'sizes' is partitionSizes(). The partition's row count
// includes invalid rows, so a partition with many gets a smaller sample. The
// sample has the columns VectorIndexSample::makeSample describes: the
// partition columns, key, embedding, sample_hash and num_rows.
lp::PlanBuilder sample(
    const std::vector<std::string>& partitions,
    const lp::PlanBuilder& sizes,
    const lp::PlanBuilder& rows,
    int64_t numRows) {
  // The hashes are uniform over [-2^63, 2^63). The cutoff passes a share of
  // numRows / num_rows of them.
  const auto cutoff = lp::Lit(static_cast<double>(numRows) * 0x1p64) /
          lp::Cast(DOUBLE(), lp::Col("num_rows")) +
      lp::Lit(-0x1p63);
  return lp::PlanBuilder(rows)
      .join(sizes, *ColumnExprs::equal(partitions, "n_"), lp::JoinType::kInner)
      .with({sampleHash().as("sample_hash")})
      .filter(lp::Cast(DOUBLE(), lp::Col("sample_hash")) < cutoff)
      .project(
          ColumnExprs::concat(
              ColumnExprs::of(partitions),
              {lp::Col(kKey),
               lp::Col(kEmbedding),
               lp::Col("sample_hash"),
               lp::Col("num_rows")}));
}

// Returns the centroids 'trainer' plans for each partition of 'sample', with
// their columns named 'partitions', cluster_id and centroid.
lp::PlanBuilder planCentroids(
    const VectorIndexCodecTrainer& trainer,
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const VectorIndexSample& sample) {
  auto centroids = trainer.plan(sample);

  const auto types = centroids.outputTypes();
  VELOX_CHECK_EQ(
      types.size(),
      partitions.size() + 2,
      "Vector index codec trainer must return the partition columns, a cluster ID and a centroid");
  for (uint32_t i = 0; i < partitions.size(); ++i) {
    VELOX_CHECK(
        types[i]->equivalent(*index.partitionColumns->childAt(i)),
        "Vector index codec trainer changed a partition column's type");
  }
  VELOX_CHECK(
      types[partitions.size()]->equivalent(*BIGINT()),
      "Vector index codec trainer must return BIGINT cluster IDs");
  VELOX_CHECK(
      types.back()->equivalent(*ARRAY(REAL())),
      "Vector index codec trainer must return ARRAY(REAL) centroids");

  const auto columns = centroids.findOrAssignOutputNames();
  std::vector<lp::ExprApi> named;
  named.reserve(columns.size());
  for (size_t i = 0; i < partitions.size(); ++i) {
    named.push_back(columns[i].toCol().as(partitions[i]));
  }
  named.push_back(columns[partitions.size()].toCol().as("cluster_id"));
  named.push_back(columns.back().toCol().as("centroid"));
  return centroids.project(named);
}

// Returns one codec per sampled partition: trained with vector_index_train,
// or packed with vector_index_pack from the centroids of 'trainer' if set.
// 'makeSample' plans sample() for a given number of rows.
lp::PlanBuilder trainCodecs(
    const VectorIndex& index,
    const std::vector<std::string>& partitions,
    const std::function<lp::PlanBuilder(int64_t)>& makeSample,
    const VectorIndexCodecTrainer* trainer) {
  const auto metric =
      std::string(VectorIndexMetricName::toName(index.properties.metric));
  if (trainer == nullptr) {
    return makeSample(kSamplesPerCluster * index.properties.numClusters)
        .aggregate(
            ColumnExprs::of(partitions),
            {lp::Call(
                 "vector_index_train",
                 lp::Col(kEmbedding),
                 lp::Col(kKey),
                 lp::Lit(index.properties.numClusters),
                 lp::Lit(metric),
                 lp::Lit(Variant::map({}), MAP(VARCHAR(), VARCHAR())))
                 .as("codec")});
  }

  return planCentroids(
             *trainer,
             index,
             partitions,
             {.properties = index.properties,
              .partitionColumns = index.partitionColumns,
              .makeSample = makeSample})
      .aggregate(
          ColumnExprs::of(partitions),
          {lp::Call(
               "vector_index_pack",
               lp::Col("cluster_id"),
               lp::Col("centroid"),
               lp::Lit(metric),
               lp::Lit(index.properties.numClusters),
               lp::Lit(index.properties.dimension))
               .as("codec")});
}

// Returns the plan that trains a codec for each sampled partition and writes
// it to the codec table with the id 'codecId', with a NULL codec for each of
// 'selected' that has no rows.
lp::LogicalPlanNodePtr planTrain(
    const VectorIndex& index,
    const std::string& connectorId,
    const std::vector<std::string>& partitions,
    const lp::ExprApi& codecId,
    const std::function<lp::PlanBuilder(int64_t)>& makeSample,
    const lp::PlanBuilder& selected,
    const VectorIndexCodecTrainer* trainer) {
  auto codecs =
      trainCodecs(index, partitions, makeSample, trainer)
          .project(
              ColumnExprs::concat(
                  ColumnExprs::renamed(partitions, "t_"), lp::Col("codec")));

  return lp::PlanBuilder(selected)
      .join(codecs, *ColumnExprs::equal(partitions, "t_"), lp::JoinType::kLeft)
      .tableWrite(
          connectorId,
          index.codecTable.schema,
          index.codecTable.table,
          lp::WriteKind::kVectorIndexRefresh,
          VectorIndex::codecTableType(index.partitionColumns)->names(),
          ColumnExprs::concat(
              ColumnExprs::concat(
                  {lp::Col("codec")}, ColumnExprs::of(partitions)),
              codecId))
      .build();
}

// Returns the plan that assigns each row of 'rows' to the nearest cluster of
// its partition's codec 'codecId', builds the local indexes of each cluster and
// writes them to the index table, with a row of NULL local index for each
// partition the train plan gave a NULL codec (see VectorIndex).
lp::LogicalPlanNodePtr planBuild(
    const facebook::axiom::SchemaTableName& table,
    const VectorIndex& index,
    const std::string& connectorId,
    const std::vector<std::string>& partitionColumns,
    const std::vector<std::string>& partitions,
    const lp::ExprApi& codecId,
    const lp::PlanBuilder& rows,
    const std::function<lp::PlanBuilder(lp::PlanBuilder)>& addCondition) {
  const std::string codecIdColumn{VectorIndex::kCodecId};

  // The rows of the storage table 'name' in the refreshed partitions.
  const auto scanPartitions =
      [&](const facebook::axiom::SchemaTableName& name) {
        return addCondition(
            rows.newPlan().tableScan(connectorId, name.schema, name.table));
      };

  // The partition columns of a storage table, each named 'prefix' + the
  // partition's name in 'rows'.
  const auto partitionsAs = [&](std::string_view prefix) {
    std::vector<lp::ExprApi> columns;
    columns.reserve(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
      columns.push_back(
          lp::Col(partitionColumns[i])
              .as(fmt::format("{}{}", prefix, partitions[i])));
    }
    return columns;
  };

  // The centroids of this refresh's codecs.
  auto centroids =
      scanPartitions(VectorIndex::centroidsTableName(table, index.name))
          .filter(lp::Col(codecIdColumn) == codecId)
          .project(
              ColumnExprs::concat(
                  partitionsAs("c_"),
                  {lp::Col(VectorIndex::kClusterId),
                   lp::Col(VectorIndex::kCentroid)}));

  // The build reads the query again, so it can see a partition the train did
  // not, e.g. one a concurrent write added to the query's input. Such a row
  // has no codec and fails the refresh.
  const auto clusterId = failIf(
      lp::Col("cluster_id").isNull(),
      ColumnExprs::message("No codec for partition: ", partitions),
      BIGINT(),
      lp::Col("cluster_id"));

  const auto partitionKeys = ColumnExprs::of(partitions);

  std::map<Variant, Variant> options;
  for (const auto& [key, value] : index.properties.indexOptions) {
    options.emplace(Variant(key), Variant(value));
  }

  const auto columnNames =
      VectorIndex::indexTableType(index.partitionColumns)->names();

  // The most rows a local index holds.
  constexpr int64_t kRowsPerLocalIndex{50'000};
  auto localIndexes =
      lp::PlanBuilder(rows)
          // Each row's nearest centroid of its partition's codec.
          .vectorSearch(
              centroids,
              ColumnExprs::equal(partitions, "c_"),
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
              {
                  lp::Call(
                      "vector_index_build",
                      lp::Col(kEmbedding),
                      lp::Col(kKey),
                      lp::Lit(
                          VectorIndexKindName::toName(
                              index.properties.indexKind)),
                      lp::Lit(
                          VectorIndexMetricName::toName(
                              index.properties.metric)),
                      lp::Lit(Variant::map(options), MAP(VARCHAR(), VARCHAR())))
                      .as("local_index"),
              })
          .project(
              ColumnExprs::concat(
                  partitionKeys,
                  {
                      lp::Col("cluster_id"),
                      lp::Col("group_id"),
                      lp::Col("local_index"),
                  }));

  // The partitions with no valid rows: those the train plan wrote a NULL codec
  // for.
  auto emptyPartitions =
      scanPartitions(index.codecTable)
          .filter(
              lp::Col(codecIdColumn) == codecId &&
              lp::Col(VectorIndex::kCodec).isNull())
          .project(
              ColumnExprs::concat(
                  partitionsAs(""),
                  {lp::NullLit(BIGINT()).as("cluster_id"),
                   lp::NullLit(BIGINT()).as("group_id"),
                   lp::NullLit(VECTOR_INDEX()).as("local_index")}));

  return localIndexes.unionAll(emptyPartitions)
      .tableWrite(
          connectorId,
          index.indexTable.schema,
          index.indexTable.table,
          lp::WriteKind::kVectorIndexRefresh,
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
    const std::function<lp::PlanBuilder()>& makeQuery,
    const std::function<lp::PlanBuilder(lp::PlanBuilder)>& addCondition,
    const VectorIndexCodecTrainer* trainer) {
  const auto& partitionColumns = index.partitionColumns->names();
  VELOX_CHECK(!partitionColumns.empty());
  const auto codecId = lp::Placeholder(BIGINT(), 1);
  const auto partitions = partitionNames(partitionColumns.size());
  const auto makeRows = [&]() {
    return validRows(index, partitions, namedRows(partitions, makeQuery()));
  };
  const auto makeSample = [&](int64_t numRows) {
    return sample(
        partitions,
        partitionSizes(partitions, "n_", makeQuery()),
        makeRows(),
        numRows);
  };
  return {
      .trainPlan = planTrain(
          index,
          connectorId,
          partitions,
          codecId,
          makeSample,
          selectedPartitions(partitions, makeQuery()),
          trainer),
      .buildPlan = planBuild(
          table,
          index,
          connectorId,
          partitionColumns,
          partitions,
          codecId,
          makeRows(),
          addCondition),
  };
}

} // namespace axiom::sql::presto
