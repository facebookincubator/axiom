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
/// how its local indexes are built. CREATE VECTOR INDEX sets them once.
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

/// A vector index: its settings, the names of its id and embedding columns,
/// and the two tables that hold it. Each index row names the codec it was built
/// with by the partition columns and 'codec_id'.
///
/// - The codec table holds a codec per partition and 'codec_id':
///   (codec, partition columns, codec_id). It is partitioned by the partition
///   columns and 'codec_id'.
/// - The index table holds the local indexes: (codec_id, cluster_id, group_id,
///   local_index, partition columns). It is partitioned by the partition
///   columns.
struct VectorIndex {
  /// VARBINARY. The codec table's packed codec.
  static constexpr std::string_view kCodec{"codec"};

  /// BIGINT. Identifies a codec within a partition. A column of both tables.
  static constexpr std::string_view kCodecId{"codec_id"};

  /// BIGINT. The codec cluster that a local index covers.
  static constexpr std::string_view kClusterId{"cluster_id"};

  /// BIGINT. Numbers the local indexes of one cluster.
  static constexpr std::string_view kGroupId{"group_id"};

  /// VARBINARY. The index table's packed local index.
  static constexpr std::string_view kLocalIndex{"local_index"};

  /// Returns the type of the codec table: 'kCodec', the columns of
  /// 'partitionColumns', then 'kCodecId'.
  static velox::RowTypePtr codecTableType(
      const velox::RowTypePtr& partitionColumns);

  /// Returns the type of the index table: 'kCodecId', 'kClusterId',
  /// 'kGroupId', 'kLocalIndex', then the columns of 'partitionColumns'.
  static velox::RowTypePtr indexTableType(
      const velox::RowTypePtr& partitionColumns);

  /// The index's name.
  SchemaTableName name;

  /// The column with each row's id, a BIGINT.
  std::string idColumn;

  /// The column with each row's embedding, an ARRAY(REAL).
  std::string embeddingColumn;

  /// How the index trains its codecs and builds its local indexes.
  VectorIndexProperties properties;

  /// The table that holds the codecs.
  SchemaTableName codecTable;

  /// The table that holds the local indexes.
  SchemaTableName indexTable;
};

using VectorIndexPtr = std::shared_ptr<const VectorIndex>;

} // namespace facebook::axiom::connector
