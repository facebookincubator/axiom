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
#include "axiom/connectors/StatisticsRequest.h"
#include <cmath>
#include <optional>
#include <unordered_set>

namespace facebook::axiom::connector {
namespace {
const auto& statisticNames() {
  static const folly::F14FastMap<ColumnStatisticKind, std::string_view> names =
      {
          {ColumnStatisticKind::kNonNullCount, "COUNT"},
          {ColumnStatisticKind::kTrueCount, "COUNT_IF"},
          {ColumnStatisticKind::kMin, "MIN"},
          {ColumnStatisticKind::kMax, "MAX"},
          {ColumnStatisticKind::kDistinctCount, "APPROX_DISTINCT"},
          {ColumnStatisticKind::kTotalSize, "SUM_DATA_SIZE"},
          {ColumnStatisticKind::kDistinctSketch, "DISTINCT_SKETCH"},
          {ColumnStatisticKind::kAggregate, "AGGREGATE"},
      };
  return names;
}
} // namespace
AXIOM_DEFINE_ENUM_NAME(ColumnStatisticKind, statisticNames);

StatisticsRequest StatisticsRequest::standard(const velox::RowTypePtr& type) {
  StatisticsRequest request;
  for (auto i = 0; i < type->size(); ++i) {
    const auto& name = type->nameOf(i);
    const auto& t = type->childAt(i);
    auto add = [&](ColumnStatisticKind kind) {
      request.statistics.push_back({name, kind, {}, {}});
    };
    add(ColumnStatisticKind::kNonNullCount);
    if (t->isBoolean()) {
      add(ColumnStatisticKind::kTrueCount);
    } else if (
        t->isTinyint() || t->isSmallint() || t->isInteger() || t->isBigint() ||
        t->isReal() || t->isDouble() || t->isTimestamp() || t->isVarchar() ||
        t->isVarbinary()) {
      add(ColumnStatisticKind::kMin);
      add(ColumnStatisticKind::kMax);
      add(ColumnStatisticKind::kDistinctCount);
    }
    if (!t->isFixedWidth()) {
      add(ColumnStatisticKind::kTotalSize);
    }
  }
  return request;
}

void StatisticsRequest::addDependencies() {
  std::unordered_set<std::string> counts;
  for (const auto& stat : statistics) {
    if (stat.kind == ColumnStatisticKind::kNonNullCount) {
      counts.insert(stat.column);
    }
  }
  const auto size = statistics.size();
  for (size_t i = 0; i < size; ++i) {
    if (statistics[i].kind == ColumnStatisticKind::kTrueCount &&
        counts.insert(statistics[i].column).second) {
      statistics.push_back(
          {statistics[i].column, ColumnStatisticKind::kNonNullCount, {}, {}});
    }
  }
}

StatisticsResults StatisticsResults::fromAggregates(
    const StatisticsRequest& request,
    const std::vector<bool>& collected,
    const std::vector<velox::RowVectorPtr>& batches,
    velox::memory::MemoryPool* pool,
    velox::column_index_t firstChannel) {
  VELOX_CHECK(!batches.empty());
  VELOX_CHECK_EQ(collected.size(), request.statistics.size());
  const auto numKeys = request.groupingColumns.size();
  VELOX_CHECK_GE(batches[0]->childrenSize(), firstChannel + numKeys + 1);
  // The collected statistics use the channels after the row count, in order.
  std::vector<std::optional<velox::column_index_t>> channels;
  velox::column_index_t nextChannel = firstChannel + numKeys + 1;
  for (const bool isCollected : collected) {
    channels.push_back(
        isCollected ? std::optional{nextChannel++} : std::nullopt);
  }
  std::vector<std::string> names = request.groupingColumns;
  names.push_back("$row_count");
  std::vector<velox::TypePtr> types;
  for (size_t i = 0; i <= numKeys; ++i) {
    types.push_back(batches[0]->childAt(firstChannel + i)->type());
  }
  velox::vector_size_t size = 0;
  for (const auto& batch : batches) {
    VELOX_CHECK_EQ(batch->childrenSize(), nextChannel);
    size += batch->size();
  }
  StatisticsResults result;
  result.groups = velox::BaseVector::create<velox::RowVector>(
      velox::ROW(names, types), size, pool);
  result.columnStats.resize(size);
  // Only columns with at least one collected statistic get an entry.
  std::vector<std::string> columns;
  std::vector<size_t> positions(request.statistics.size());
  std::vector<std::optional<velox::column_index_t>> sketchChannels;
  std::vector<velox::TypePtr> sketchTypes;
  std::vector<std::string> sketchNames;
  for (size_t i = 0; i < request.statistics.size(); ++i) {
    const auto& stat = request.statistics[i];
    if (stat.kind == ColumnStatisticKind::kDistinctSketch ||
        stat.kind == ColumnStatisticKind::kAggregate) {
      sketchChannels.push_back(channels[i]);
      sketchTypes.push_back(
          channels[i] ? batches[0]->childAt(*channels[i])->type()
                      : velox::VARBINARY());
      sketchNames.push_back(fmt::format("sketch_{}", i));
    }
    if (!channels[i]) {
      continue;
    }
    const auto it = std::find(columns.begin(), columns.end(), stat.column);
    positions[i] = std::distance(columns.begin(), it);
    if (it == columns.end()) {
      columns.push_back(stat.column);
    }
  }
  for (auto& stats : result.columnStats) {
    for (const auto& name : columns) {
      stats.emplace_back().name = name;
    }
  }
  result.sketches = velox::BaseVector::create<velox::RowVector>(
      velox::ROW(sketchNames, sketchTypes), size, pool);
  for (size_t i = 0; i < sketchChannels.size(); ++i) {
    if (!sketchChannels[i]) {
      result.sketches->childAt(i) =
          velox::BaseVector::createNullConstant(velox::VARBINARY(), size, pool);
    }
  }
  velox::vector_size_t offset = 0;
  for (const auto& batch : batches) {
    for (size_t i = 0; i <= numKeys; ++i) {
      result.groups->childAt(i)->copy(
          batch->childAt(firstChannel + i).get(), offset, 0, batch->size());
    }
    for (size_t i = 0; i < sketchChannels.size(); ++i) {
      if (sketchChannels[i]) {
        result.sketches->childAt(i)->copy(
            batch->childAt(*sketchChannels[i]).get(), offset, 0, batch->size());
      }
    }
    for (auto row = 0; row < batch->size(); ++row) {
      const auto rows = batch->childAt(firstChannel + numKeys)
                            ->variantAt(row)
                            .value<int64_t>();
      for (size_t i = 0; i < request.statistics.size(); ++i) {
        if (!channels[i]) {
          continue;
        }
        const auto& vector = batch->childAt(*channels[i]);
        auto& stats = result.columnStats[offset + row][positions[i]];
        if (vector->isNullAt(row)) {
          if (request.statistics[i].kind == ColumnStatisticKind::kTotalSize) {
            stats.avgSizeInBytes = 0;
          }
          continue;
        }
        const auto kind = request.statistics[i].kind;
        if (kind == ColumnStatisticKind::kDistinctSketch ||
            kind == ColumnStatisticKind::kAggregate) {
          continue; // Sketch columns were copied above.
        }
        auto value = vector->variantAt(row);
        switch (request.statistics[i].kind) {
          case ColumnStatisticKind::kNonNullCount:
            stats.numValues = value.value<int64_t>();
            stats.nullPct =
                rows == 0 ? 0 : 100.0 * (1.0 - double(stats.numValues) / rows);
            break;
          case ColumnStatisticKind::kTrueCount:
            stats.numTrue = value.value<int64_t>();
            break;
          case ColumnStatisticKind::kMin:
            stats.min = value;
            break;
          case ColumnStatisticKind::kMax:
            stats.max = value;
            break;
          case ColumnStatisticKind::kDistinctCount:
            stats.numDistinct = value.value<int64_t>();
            break;
          case ColumnStatisticKind::kTotalSize:
            stats.avgSizeInBytes = rows == 0
                ? 0
                : std::llround(double(value.value<int64_t>()) / rows);
            break;
          case ColumnStatisticKind::kDistinctSketch:
          case ColumnStatisticKind::kAggregate:
            break;
        }
      }
      for (auto& stats : result.columnStats[offset + row]) {
        if (stats.numTrue.has_value()) {
          stats.numDistinct =
              (*stats.numTrue > 0) + (stats.numValues > *stats.numTrue);
        }
      }
    }
    offset += batch->size();
  }
  return result;
}
} // namespace facebook::axiom::connector
