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
#include "axiom/optimizer/OptimizerOptions.h"

#include <fmt/format.h>
#include <folly/Conv.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

namespace facebook::axiom::optimizer {

using velox::config::ConfigProperty;
using velox::config::ConfigPropertyType;

namespace {

int32_t parsePositiveInt(std::string_view name, std::string_view value) {
  int32_t result;
  try {
    result = folly::to<int32_t>(value);
  } catch (const folly::ConversionError&) {
    VELOX_USER_FAIL("{} must be a valid 32-bit integer: {}", name, value);
  }
  VELOX_USER_CHECK_GE(result, 1, "{} must be >= 1: {}", name, value);
  return result;
}

std::vector<ConfigProperty> buildProperties(
    const std::unordered_map<std::string, std::string>& configOverrides) {
  std::vector<ConfigProperty> props = {
      {
          std::string(OptimizerOptions::kSampleJoins),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kSampleJoinsDefault),
          "Sample joins to determine optimal join order.",
      },
      {
          std::string(OptimizerOptions::kSampleFilters),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kSampleFiltersDefault),
          "Sample filters to estimate scan selectivity.",
      },
      {
          std::string(OptimizerOptions::kUseFilteredTableStats),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kUseFilteredTableStatsDefault),
          "Use connector-provided table statistics for cardinality estimation.",
      },
      {
          std::string(OptimizerOptions::kPushdownSubfields),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kPushdownSubfieldsDefault),
          "Extract accessed subfields of complex columns in table scan.",
      },
      {
          std::string(OptimizerOptions::kAllMapsAsStruct),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kAllMapsAsStructDefault),
          "Project maps with known key subsets as structs.",
      },
      {
          std::string(OptimizerOptions::kSyntacticJoinOrder),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kSyntacticJoinOrderDefault),
          "Disable cost-based join ordering; use query order.",
      },
      {
          std::string(OptimizerOptions::kAlwaysPlanPartialAggregation),
          ConfigPropertyType::kBoolean,
          fmt::to_string(
              OptimizerOptions::kAlwaysPlanPartialAggregationDefault),
          "Always split aggregation into partial + final.",
      },
      {
          std::string(OptimizerOptions::kEnableReducingExistences),
          ConfigPropertyType::kBoolean,
          fmt::to_string(OptimizerOptions::kEnableReducingExistencesDefault),
          "Enable reducing semi joins.",
      },
      {
          std::string(OptimizerOptions::kSmallQueryMaxScanRows),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kSmallQueryMaxScanRowsDefault),
          "A SELECT query whose scans are estimated to read at most this many "
          "rows in total is small and runs on 'small_query_num_workers' "
          "workers. A query that reads more, or whose scans report no "
          "estimate, runs on all available workers. 0 disables.",
      },
      {
          std::string(OptimizerOptions::kSmallQueryNumWorkers),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kSmallQueryNumWorkersDefault),
          "Workers a small query runs on, capped by the number available.",
      },
      {
          std::string(OptimizerOptions::kSmallQueryMaxSplits),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kSmallQueryMaxSplitsDefault),
          "A small query's scans must also produce at most this many splits "
          "in total. A query whose scans report no split estimate is then not "
          "small. Applies only when small_query_max_scan_rows is set. 0 "
          "disables the limit.",
      },
      {
          std::string(OptimizerOptions::kSmallQueryMaxScanBytes),
          ConfigPropertyType::kString,
          std::string(OptimizerOptions::kSmallQueryMaxScanBytesDefault),
          "A small query's scans must also read at most this many bytes in "
          "total, as a capacity string (e.g. \"10GB\"). A scan reads its rows "
          "times its bytes per row. A query whose scans report no bytes-per-row "
          "estimate is then not small. Applies only when "
          "small_query_max_scan_rows is set. \"0B\" disables the limit.",
      },
      {
          std::string(OptimizerOptions::kHashPartitionCount),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kHashPartitionCountDefault),
          "Tasks a hash-partitioned stage runs, capped by the number of "
          "workers available. 0 uses all available workers.",
      },
      {
          std::string(OptimizerOptions::kMinColumnarChannelsForCompactRow),
          ConfigPropertyType::kInteger,
          std::to_string(
              OptimizerOptions::kMinColumnarChannelsForCompactRowDefault),
          "Minimum number of Presto columnar encoding channels at which "
          "internal exchanges use CompactRow serialization. Must be >= 1.",
      },
      {
          std::string(OptimizerOptions::kMaxDuplicatedLiteralBytes),
          ConfigPropertyType::kString,
          std::string(OptimizerOptions::kMaxDuplicatedLiteralBytesDefault),
          "Maximum estimated size of redundant literal copies in a Project. "
          "When this limit is exceeded, v2 extracts the literal into a lower "
          "Project and reuses the resulting column. This avoids duplicating "
          "constants in serialized plans and during expression evaluation. "
          "Specify as a capacity string.",
      },
      {
          std::string(OptimizerOptions::kParallelProjectWidth),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kParallelProjectWidthDefault),
          "Number of threads for parallel projection. 1 disables.",
      },
      {
          std::string(OptimizerOptions::kGreedyJoinThreshold),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kGreedyJoinThresholdDefault),
          "Use a greedy join-order search instead of exhaustive enumeration "
          "when a single query block contains at least this many joined "
          "tables. Greedy is approximate but bounded in planning time.",
      },
      {
          std::string(OptimizerOptions::kBroadcastSizeLimit),
          ConfigPropertyType::kString,
          std::string(OptimizerOptions::kBroadcastSizeLimitDefault),
          "Maximum estimated build size eligible for broadcast in a keyed "
          "join, as a capacity string (e.g. \"100MB\", \"1GB\"). A broadcast "
          "copy must fit in each worker's memory. \"0B\" disables broadcast "
          "for keyed joins. Joins with no partitioned distributed shape may "
          "still broadcast: keyless joins, and IN / NOT IN whose correlation "
          "has no equality. The limit also bounds the single row that an "
          "inner join without keys takes as its build side; at \"0B\" that "
          "choice is off.",
      },
      {
          std::string(OptimizerOptions::kDphypEnumerationBudget),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kDphypEnumerationBudgetDefault),
          "Max connected-subgraph/complement pairs the DPhyp join enumerator "
          "evaluates before falling back to greedy ordering. Bounds planning "
          "time on dense join graphs. 0 means unlimited.",
      },
      {
          std::string(OptimizerOptions::kRecursionLimit),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kRecursionLimitDefault),
          "Maximum iterations for a recursion that has no bound of its own. Must be >= 1.",
      },
      {
          std::string(OptimizerOptions::kMaxPlanObjects),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kMaxPlanObjectsDefault),
          "Fails a query that creates more than this many plan objects. "
          "Bounds planning memory, which grows with the square of this count. "
          "A query that exceeds it is expanding rather than merely large, "
          "typically a CTE re-planned at each reference. Must be >= 1.",
      },
      {
          std::string(OptimizerOptions::kTraceFlags),
          ConfigPropertyType::kInteger,
          std::to_string(OptimizerOptions::kTraceFlagsDefault),
          "Bit mask for optimizer trace output: 1=retained, 2=exceeded best, 4=sample, 8=preprocess.",
      },
  };

  if (configOverrides.empty()) {
    return props;
  }

  std::unordered_map<std::string, size_t> propIndex;
  for (size_t i = 0; i < props.size(); ++i) {
    propIndex[props[i].name] = i;
  }
  for (const auto& [key, value] : configOverrides) {
    auto it = propIndex.find(key);
    if (it != propIndex.end()) {
      props[it->second].defaultValue = value;
    } else {
      LOG(WARNING) << "Unregistered session property in optimizer config: "
                   << key;
    }
  }

  return props;
}

} // namespace

OptimizerOptions::OptimizerOptions() : properties_(buildProperties({})) {}

OptimizerOptions::OptimizerOptions(
    std::unordered_map<std::string, std::string> configOverrides)
    : properties_(buildProperties(configOverrides)) {}

std::string OptimizerOptions::normalize(
    std::string_view name,
    std::string_view value) const {
  if (name == kSmallQueryMaxScanRows) {
    auto rows = std::stoll(std::string(value));
    VELOX_USER_CHECK_GE(
        rows, 0, "small_query_max_scan_rows must be >= 0: {}", value);
  } else if (name == kSmallQueryMaxSplits) {
    auto splits = std::stoll(std::string(value));
    VELOX_USER_CHECK_GE(
        splits, 0, "small_query_max_splits must be >= 0: {}", value);
  } else if (name == kSmallQueryNumWorkers) {
    auto workers = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        workers, 1, "small_query_num_workers must be >= 1: {}", value);
  } else if (name == kHashPartitionCount) {
    auto count = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        count, 0, "hash_partition_count must be >= 0: {}", value);
  } else if (name == kParallelProjectWidth) {
    auto width = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        width, 1, "parallel_project_width must be >= 1: {}", value);
  } else if (name == kGreedyJoinThreshold) {
    auto threshold = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        threshold, 1, "greedy_join_threshold must be >= 1: {}", value);
  } else if (
      name == kBroadcastSizeLimit || name == kMaxDuplicatedLiteralBytes ||
      name == kSmallQueryMaxScanBytes) {
    // Throws if 'value' is not a valid capacity string (e.g. "100MB").
    velox::config::toCapacity(
        std::string(value), velox::config::CapacityUnit::BYTE);
  } else if (
      name == kRecursionLimit || name == kMaxPlanObjects ||
      name == kMinColumnarChannelsForCompactRow) {
    parsePositiveInt(name, value);
  }
  return std::string(value);
}

OptimizerOptions OptimizerOptions::from(
    const folly::F14FastMap<std::string, std::string>& properties) {
  auto setBool = [&](std::string_view key, bool& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = it->second == "true";
    }
  };

  auto setInt = [&](std::string_view key, int32_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = std::stoi(it->second);
    }
  };

  auto setPositiveInt = [&](std::string_view key, int32_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = parsePositiveInt(key, it->second);
    }
  };

  auto setLong = [&](std::string_view key, int64_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = std::stoll(it->second);
    }
  };

  auto setCapacity = [&](std::string_view key, int64_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = static_cast<int64_t>(velox::config::toCapacity(
          it->second, velox::config::CapacityUnit::BYTE));
    }
  };

  OptimizerOptions options;
  setBool(kSampleJoins, options.sampleJoins);
  setBool(kSampleFilters, options.sampleFilters);
  setBool(kUseFilteredTableStats, options.useFilteredTableStats);
  setBool(kPushdownSubfields, options.pushdownSubfields);
  setBool(kAllMapsAsStruct, options.allMapsAsStruct);
  setBool(kSyntacticJoinOrder, options.syntacticJoinOrder);
  setBool(kAlwaysPlanPartialAggregation, options.alwaysPlanPartialAggregation);
  setBool(kEnableReducingExistences, options.enableReducingExistences);
  setLong(kSmallQueryMaxScanRows, options.smallQueryMaxScanRows);
  setInt(kSmallQueryNumWorkers, options.smallQueryNumWorkers);
  setLong(kSmallQueryMaxSplits, options.smallQueryMaxSplits);
  setCapacity(kSmallQueryMaxScanBytes, options.smallQueryMaxScanBytes);
  setInt(kHashPartitionCount, options.hashPartitionCount);
  setPositiveInt(
      kMinColumnarChannelsForCompactRow,
      options.minColumnarChannelsForCompactRow);
  setCapacity(kMaxDuplicatedLiteralBytes, options.maxDuplicatedLiteralBytes);
  setInt(kParallelProjectWidth, options.parallelProjectWidth);
  setPositiveInt(kMaxPlanObjects, options.maxPlanObjects);
  setInt(kGreedyJoinThreshold, options.greedyJoinThreshold);
  setCapacity(kBroadcastSizeLimit, options.broadcastSizeLimit);
  setInt(kDphypEnumerationBudget, options.dphypEnumerationBudget);
  setPositiveInt(kRecursionLimit, options.recursionLimit);

  auto setUint = [&](std::string_view key, uint32_t& field) {
    auto it = properties.find(key);
    if (it != properties.end()) {
      field = static_cast<uint32_t>(std::stoul(it->second));
    }
  };
  setUint(kTraceFlags, options.traceFlags);
  return options;
}

} // namespace facebook::axiom::optimizer
