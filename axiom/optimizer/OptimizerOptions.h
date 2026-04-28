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
#include <algorithm>
#include <cstdint>

#include "axiom/common/SchemaTableName.h"
#include "velox/common/config/ConfigProvider.h"
#include "velox/exec/Task.h"

namespace facebook::axiom::optimizer {

struct OptimizerOptions : public velox::config::ConfigProvider {
  /// Bit masks for use in 'traceFlags'.
  static constexpr uint32_t kRetained = 1;
  static constexpr uint32_t kExceededBest = 2;
  static constexpr uint32_t kSample = 4;
  static constexpr uint32_t kPreprocess = 8;

  static constexpr std::string_view kSampleJoins = "sample_joins";
  static constexpr std::string_view kSampleFilters = "sample_filters";
  static constexpr std::string_view kUseFilteredTableStats =
      "use_filtered_table_stats";
  static constexpr std::string_view kPushdownSubfields = "pushdown_subfields";
  static constexpr std::string_view kAllMapsAsStruct = "all_maps_as_struct";
  static constexpr std::string_view kSyntacticJoinOrder =
      "syntactic_join_order";
  static constexpr std::string_view kAlwaysPlanPartialAggregation =
      "always_plan_partial_aggregation";
  static constexpr std::string_view kEnableReducingExistences =
      "enable_reducing_existences";
  static constexpr std::string_view kParallelProjectWidth =
      "parallel_project_width";
  static constexpr std::string_view kTraceFlags = "trace_flags";

  /// Parallelizes independent projections over this many threads. 1 means no
  /// parallel projection.
  int32_t parallelProjectWidth{1};

  /// Produces skyline subfield sets of complex type columns as top level
  /// columns in table scan.
  bool pushdownSubfields{false};

  /// Makes all maps for which a known subset of keys is accessed to
  /// be projected out as structs.
  bool allMapsAsStruct{false};

  /// Map from table name to list of map columns to be read as structs unless
  /// the whole map is accessed as a map.
  folly::F14FastMap<std::string, std::vector<std::string>> mapAsStruct;

  /// Enable join order sampling during optimization. If this flag is set, joins
  /// are sampled to determine the optimal join order. If join sampling is
  /// disabled, the optimizer will fall back on cardinality estimation.
  bool sampleJoins{true};

  /// Enable filter selectivity sampling during optimization. If this flag is
  /// set, filters will be evaluated against a sample of source data to
  /// determine the estimated cardinality of the scan. If filter sampling is
  /// disabled, a default selectivity will be used.
  bool sampleFilters{true};

  /// Enable using connector-provided table statistics (co_estimateStats) for
  /// cardinality estimation. When disabled, the optimizer falls back to
  /// sampling or constraint-based estimation.
  bool useFilteredTableStats{true};

  /// Enable reducing semi joins.
  bool enableReducingExistences{true};

  /// Produce trace of plan candidates.
  uint32_t traceFlags{0};

  /// Disable cost-based join order selection. Perform the joins in the exact
  /// sequence specified in the query.
  /// TODO: Make this work for non-inner joins.
  bool syntacticJoinOrder{false};

  /// Disable cost-based decision re: whether to split an aggregation into
  /// partial + final or not.
  bool alwaysPlanPartialAggregation{false};

  /// When true, connectors skip side effects in createTable() and
  /// beginWrite(). Used for EXPLAIN queries.
  bool explain{false};

  /// Execution mode for runners spawned during optimization (e.g. constant
  /// folding). Should match the outer query's mode. Defaults to kParallel.
  velox::exec::Task::ExecutionMode mode{
      velox::exec::Task::ExecutionMode::kParallel};

  /// Constructs options from session property name-value pairs.
  /// Keys are unqualified property names (e.g., "sample_joins").
  /// Missing keys use struct defaults.
  static OptimizerOptions from(
      const folly::F14FastMap<std::string, std::string>& properties);

  // ConfigProvider implementation.
  std::vector<velox::config::ConfigProperty> properties() const override;
  std::string normalize(std::string_view name, std::string_view value)
      const override;

  bool isMapAsStruct(const SchemaTableName& tableName, std::string_view column)
      const {
    if (allMapsAsStruct) {
      return true;
    }
    // TODO: Handle tables from different schemas properly. Currently, the
    // mapAsStruct config is keyed by bare table name which is ambiguous across
    // schemas.
    auto it = mapAsStruct.find(tableName.table);
    if (it == mapAsStruct.end()) {
      return false;
    }
    return std::find(it->second.begin(), it->second.end(), column) !=
        it->second.end();
  }
};

} // namespace facebook::axiom::optimizer
