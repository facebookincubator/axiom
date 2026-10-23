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

#include <folly/container/F14Map.h>
#include "axiom/common/Enums.h"
#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/ColumnStatistics.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::connector {

/// The statistics a connector can ask for. Each one fills a field of
/// ColumnStatistics, or adds a column to StatisticsResults::sketches:
/// - kNonNullCount: numValues, the number of non-null values. Also sets
///   nullPct.
/// - kTrueCount: numTrue, the number of true values in a boolean column.
///   Together with kNonNullCount, it also sets numDistinct.
/// - kMin, kMax: min and max.
/// - kDistinctCount: numDistinct, an estimate.
/// - kTotalSize: avgSizeInBytes, the total size divided by the number of rows,
///   nulls included.
/// - kDistinctSketch: a sketch of the distinct values, in sketches. Sketches
///   from different groups can be merged.
/// - kAggregate: the result of an aggregate the connector names, in sketches.
enum class ColumnStatisticKind {
  kNonNullCount,
  kTrueCount,
  kMin,
  kMax,
  kDistinctCount,
  kTotalSize,
  kDistinctSketch,
  kAggregate,
};
AXIOM_DECLARE_ENUM_NAME(ColumnStatisticKind);

/// One statistic to compute for one column.
struct ColumnStatisticRequest {
  /// The table column's name.
  std::string column;

  ColumnStatisticKind kind;

  /// Constant values passed to the aggregate after the column, such as the
  /// maximum error for approx_distinct.
  std::vector<velox::Variant> arguments;

  /// For kAggregate only: the name of the aggregate function to call.
  std::string aggregate;
};

/// What the user asked for in the ANALYZE statement.
struct AnalyzeOptions {
  /// The columns listed in WITH (columns = ARRAY[...]), or std::nullopt when
  /// the user listed none and wants all of them. The connector asks only for
  /// statistics on these columns.
  std::optional<std::vector<std::string>> columns;

  /// The other WITH properties, already evaluated. The connector decides what
  /// they mean and rejects the ones it does not know.
  folly::F14FastMap<std::string, velox::Variant> properties;
};

/// Says which statistics to collect for a table, and for which groups of rows.
/// For ANALYZE, the connector returns one from
/// ConnectorMetadata::getStatisticsRequest. For a write, the write handle
/// returns one from statisticsRequest. The engine computes everything in one
/// aggregation over the table and returns the values as StatisticsResults.
///
///   auto request = std::make_shared<StatisticsRequest>(
///       StatisticsRequest::standard(table->type()));
///   request->table = table->name();
///   request->groupingColumns = {"ds"};
///   request->groups = {{velox::Variant("2026-10-23")}};
///
/// The engine passes the same object back to co_finishAnalyze, so a connector
/// can subclass it to keep anything it needs there.
///
/// ANALYZE checks that:
/// - 'table' is the table being analyzed.
/// - Every statistic is on a column the user chose.
/// - The grouping columns exist and are all different.
/// - Each group has one value for each grouping column.
class StatisticsRequest {
 public:
  virtual ~StatisticsRequest() = default;

  /// The table to read.
  SchemaTableName table;

  /// Compute the statistics separately for each value of these columns.
  /// Empty means one group for the whole table.
  std::vector<std::string> groupingColumns;

  /// If set, compute only these groups. Each group lists one value per
  /// grouping column. A null value matches rows where the column is null, as
  /// in GROUP BY.
  std::optional<std::vector<std::vector<velox::Variant>>> groups;

  /// The statistics to compute. StatisticsResults::sketches follows this
  /// order.
  std::vector<ColumnStatisticRequest> statistics;

  /// Returns the statistics that writes collect by default for the columns of
  /// 'type': a non-null count for every column, a true count for booleans,
  /// min, max and distinct count for numbers, timestamps, strings and binary
  /// values, and total size for variable-width types. Connectors can use it
  /// for ANALYZE too.
  static StatisticsRequest standard(const velox::RowTypePtr& type);

  /// Adds the non-null counts that boolean distinct counts need. Calling it
  /// again adds nothing.
  void addDependencies();
};

/// The statistics computed for a StatisticsRequest, with one row per group.
struct StatisticsResults {
  /// The grouping columns, then $row_count, the number of rows in the group.
  /// An ungrouped request has one row.
  velox::RowVectorPtr groups;

  /// For each group, in the same order as the rows of 'groups': one
  /// ColumnStatistics for each column that got at least one statistic. Its
  /// name is the column name.
  std::vector<std::vector<ColumnStatistics>> columnStats;

  /// One binary column per requested sketch, in request order, with one row
  /// per group.
  velox::RowVectorPtr sketches;

  /// Builds the results for 'request' from the output of the statistics
  /// aggregation. Starting at 'firstChannel', each batch has the grouping
  /// columns, the row count, and then one column for each statistic that
  /// 'collected' marks. The batches may use any vector encoding.
  ///
  /// The engine skips a statistic when its aggregate can't take the column's
  /// type. A skipped statistic sets no field. A skipped sketch becomes a null
  /// column, so the sketch columns still match the request.
  static StatisticsResults fromAggregates(
      const StatisticsRequest& request,
      const std::vector<bool>& collected,
      const std::vector<velox::RowVectorPtr>& batches,
      velox::memory::MemoryPool* pool,
      velox::column_index_t firstChannel);
};

} // namespace facebook::axiom::connector

AXIOM_ENUM_FORMATTER(facebook::axiom::connector::ColumnStatisticKind);
