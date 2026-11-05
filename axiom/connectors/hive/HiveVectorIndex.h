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

#include <array>
#include <string_view>
#include <utility>

#include <folly/json/dynamic.h>

#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/TableDefinition.h"
#include "axiom/connectors/VectorIndex.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::hive {

/// Describes the vector indexes of a Hive table (see VectorIndex) the way Hive
/// connectors store them: as JSON records kept with the table's metadata, and
/// as two storage tables per index, named and partitioned after the table.
///
///   auto record = HiveVectorIndex::toJson(*index);
///   auto index = HiveVectorIndex::fromJson(table, partitionColumns, record);
class HiveVectorIndex {
 public:
  /// Returns the record of 'index': what the table's columns do not
  /// determine.
  static folly::dynamic toJson(const VectorIndex& index);

  /// Returns the index of 'table', with the partition columns
  /// 'partitionColumns', that 'json' records (see toJson()).
  static VectorIndexPtr fromJson(
      const SchemaTableName& table,
      const velox::RowTypePtr& partitionColumns,
      const folly::dynamic& json);

  /// Returns the names of the codec table and the index table of the index
  /// 'name' of 'table': the table's name, the index's name and "codecs" or
  /// "index", joined by "__", e.g. products__product_index__codecs.
  static std::pair<SchemaTableName, SchemaTableName> storageTableNames(
      const SchemaTableName& table,
      std::string_view name);

  /// Returns the index 'resolved' of 'table', with the partition columns
  /// 'partitionColumns', and the properties of its storage tables. Throws a
  /// user error for a partitioning, bucketing or sorting property: the storage
  /// tables take the table's partitions.
  static std::
      pair<VectorIndexPtr, folly::F14FastMap<std::string, velox::Variant>>
      make(
          const SchemaTableName& table,
          const velox::RowTypePtr& partitionColumns,
          ResolvedVectorIndex resolved);

  /// Returns the definitions of the codec table and the index table of
  /// 'index', with the properties 'storageProperties'. The codec table is
  /// partitioned by the index's partition columns and VectorIndex::kCodecId,
  /// the index table by the index's partition columns.
  static std::array<std::pair<SchemaTableName, TableDefinition>, 2>
  storageTableDefinitions(
      const VectorIndex& index,
      const folly::F14FastMap<std::string, velox::Variant>& storageProperties);
};

} // namespace facebook::axiom::connector::hive
