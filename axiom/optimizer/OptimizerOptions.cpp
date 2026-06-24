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
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

namespace facebook::axiom::optimizer {

using velox::config::ConfigProperty;
using velox::config::ConfigPropertyType;

namespace {

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
          "Maximum estimated build size eligible for broadcast, as a capacity "
          "string (e.g. \"100MB\", \"1GB\"). A broadcast copy must fit in each "
          "worker's memory. \"0B\" disables broadcast.",
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
  if (name == kParallelProjectWidth) {
    auto width = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        width, 1, "parallel_project_width must be >= 1: {}", value);
  } else if (name == kGreedyJoinThreshold) {
    auto threshold = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        threshold, 1, "greedy_join_threshold must be >= 1: {}", value);
  } else if (name == kBroadcastSizeLimit) {
    // Throws if 'value' is not a valid capacity string (e.g. "100MB").
    velox::config::toCapacity(
        std::string(value), velox::config::CapacityUnit::BYTE);
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
  setInt(kParallelProjectWidth, options.parallelProjectWidth);
  setInt(kGreedyJoinThreshold, options.greedyJoinThreshold);
  setCapacity(kBroadcastSizeLimit, options.broadcastSizeLimit);

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
