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
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <folly/container/F14Map.h>

#include "axiom/common/Enums.h"
#include "axiom/common/SchemaTableName.h"
#include "velox/common/vectorindex/VectorIndexKind.h"
#include "velox/common/vectorindex/VectorIndexMetric.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::connector {

/// How often a vector index trains a codec.
enum class VectorIndexCodecPolicy {
  /// Each insert trains a codec on a sample of its input.
  kPerPartition,
  /// The first insert trains a codec; later inserts build with the latest
  /// partition's codec.
  kShared,
};

AXIOM_DECLARE_ENUM_NAME(VectorIndexCodecPolicy);

/// Settings that describe a whole vector index: how its codecs are trained and
/// how its local indexes are built. Adding the index sets them once.
///
/// Example:
///
///   auto properties = VectorIndexProperties::extract(options);
///   // 'options' now holds only the properties of the index's tables.
struct VectorIndexProperties {
  static constexpr std::string_view kDistanceMetric{"distance_metric"};
  static constexpr std::string_view kIndexType{"index_type"};
  static constexpr std::string_view kNumClusters{"num_clusters"};
  static constexpr std::string_view kCodecPolicy{"codec_policy"};
  static constexpr std::string_view kIndexOptions{"index_options"};

  /// How the index compares embeddings.
  velox::common::vectorindex::VectorIndexMetric metric{};

  /// The kind of each local index.
  velox::common::vectorindex::VectorIndexKind indexKind{};

  /// The number of clusters of each codec. Positive.
  int64_t numClusters{0};

  /// How often the index trains a codec.
  VectorIndexCodecPolicy codecPolicy{VectorIndexCodecPolicy::kPerPartition};

  /// Settings of each local index, such as 'ivf.nlist', by key.
  /// Valid for 'indexKind' (see velox::common::vectorindex::
  /// LocalVectorIndexOptions).
  std::map<std::string, std::string> indexOptions;

  bool operator==(const VectorIndexProperties&) const = default;

  /// Removes the index's own properties from 'properties' and returns them.
  /// What remains are the properties of the index's tables. Throws a user
  /// error for a missing required property, a value of the wrong type, or a
  /// value Velox's vector index library rejects.
  static VectorIndexProperties extract(
      folly::F14FastMap<std::string, velox::Variant>& properties);
};

/// A vector index the engine resolved against its table: what a connector
/// stores when it adds the index (see ConnectorMetadata::co_doAddVectorIndex).
/// The connector adds its own parts, the partition columns and the storage
/// tables, to make the VectorIndex.
struct ResolvedVectorIndex {
  /// The index's name, unique within its table.
  std::string name;

  /// The ARRAY(REAL) column the index indexes.
  std::string embeddingColumn;

  /// The BIGINT column with each row's key.
  std::string keyColumn;

  /// The index's own properties.
  VectorIndexProperties properties;

  /// The rest of the index's WITH clause: the properties of its storage
  /// tables.
  folly::F14FastMap<std::string, velox::Variant> storageProperties;
};

/// An index over an embedding column of a table. Given a query embedding, it
/// finds the k nearest embeddings efficiently.
///
/// The engine defines the index's format, builds it and searches it. A
/// connector stores it as two regular tables and records it as metadata of its
/// table (see Table::vectorIndexes()).
///
/// The index belongs to its table, and its name is unique within the table. It
/// holds every row of the table whose key and embedding are valid for it. A
/// vector search of the table that uses the index reads the index's storage
/// tables and returns each match's key and partition columns. The key is the
/// table's primary key, or, for a table that declares none, columns whose
/// values identify each row: a match's key finds the row it came from. For
/// now, the key is one BIGINT column.
///
/// The index has the table's partitions. The partition columns identify a
/// partition, so a search can read only the partitions a condition on them
/// selects.
///
/// The index does not follow its table's data. The engine maintains it
/// explicitly: REFRESH VECTOR INDEX builds the selected partitions, writing
/// each partition's codec, then its local indexes. The index covers a
/// partition once a refresh has written it, and a write that changes the
/// partition's rows removes it from the index. A search fails when a partition
/// it selects is not covered, since it would miss that partition's rows (see
/// TableLayout::co_checkVectorIndexCoverage()).
///
/// Each partition is stored in two levels:
/// 1. Clusters. The partition's embeddings are grouped into
///    'properties.numClusters' clusters, each identified by its centroid. Each
///    embedding belongs to the cluster whose centroid is nearest. The centroids
///    are trained on a sample of embeddings and stored together as a codec
///    (see VectorIndexCodecPolicy).
/// 2. Local indexes. The embeddings of each cluster are in local indexes of
///    'properties.indexKind' (see
///    velox::common::vectorindex::LocalVectorIndex). A large cluster is split
///    across several local indexes, numbered by 'group_id', so that each is
///    small enough to build, store and load as one value. The writer decides
///    how many embeddings each holds.
///
/// A search has three phases:
/// 1. In each partition it reads, find the clusters nearest to the query, as
///    many as the num_probes hint asks (see logical_plan::VectorSearchNode).
/// 2. Search each of these clusters' local indexes for its k nearest
///    embeddings.
/// 3. Keep the k nearest of all these.
///
/// Two tables of the index's connector store the levels:
/// - The codec table holds the clusters: (codec, partition columns, codec_id).
///   Each row is one codec, a single value with the centroids of all its
///   clusters, identified by the partition columns and 'codec_id'. Each
///   partition has one codec; 'codec_id' identifies the write that trained it.
/// - The index table holds the local indexes: (codec_id, cluster_id, group_id,
///   local_index, partition columns). Each row is one local index of the
///   cluster 'cluster_id' of the codec its partition columns and 'codec_id'
///   identify.
struct VectorIndex {
  // The columns of the codec and index tables, which store the index.

  /// VECTOR_INDEX. A column of the codec table: a codec, with the centroids of
  /// all its clusters.
  static constexpr std::string_view kCodec{"codec"};

  /// BIGINT. Identifies a codec within a partition. A column of the codec and
  /// index tables.
  static constexpr std::string_view kCodecId{"codec_id"};

  /// BIGINT. Identifies a cluster within a codec. A column of the index table.
  static constexpr std::string_view kClusterId{"cluster_id"};

  /// BIGINT. Numbers the local indexes of one cluster. A column of the index
  /// table.
  static constexpr std::string_view kGroupId{"group_id"};

  /// VECTOR_INDEX. A column of the index table: a local index.
  static constexpr std::string_view kLocalIndex{"local_index"};

  /// Returns the type of the stored codec table: 'kCodec', the columns of
  /// 'partitionColumns', then 'kCodecId'. Throws a user error when a partition
  /// column takes the name of a column the codec or index table adds.
  static velox::RowTypePtr codecTableType(
      const velox::RowTypePtr& partitionColumns);

  /// Returns the type of the stored index table, a row per local index:
  /// 'kCodecId', 'kClusterId', 'kGroupId', 'kLocalIndex', then the columns of
  /// 'partitionColumns'. Throws a user error as codecTableType() does.
  static velox::RowTypePtr indexTableType(
      const velox::RowTypePtr& partitionColumns);

  /// The index's name, unique within 'table'.
  std::string name;

  /// The column with each row's key, a BIGINT. The key is one column for now.
  std::string keyColumn;

  /// The column with each row's embedding, an ARRAY(REAL).
  std::string embeddingColumn;

  /// The table's partition columns (see above).
  velox::RowTypePtr partitionColumns;

  /// How the index trains its codecs and builds its local indexes.
  VectorIndexProperties properties;

  /// The codec table, a table of the index's connector, which names it.
  SchemaTableName codecTable;

  /// The index table, a table of the index's connector, which names it.
  SchemaTableName indexTable;
};

using VectorIndexPtr = std::shared_ptr<const VectorIndex>;

} // namespace facebook::axiom::connector
