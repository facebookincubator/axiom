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

#include "velox/common/base/Exceptions.h"
#include "velox/common/config/ConfigProperty.h"

namespace facebook::axiom::optimizer {

using velox::config::ConfigProperty;
using velox::config::ConfigPropertyType;

std::vector<ConfigProperty> OptimizerOptions::properties() const {
  // Defaults come from field initializers on a default-constructed instance.
  OptimizerOptions defaults;
  return {
      {
          std::string(kSampleJoins),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.sampleJoins),
          "Sample joins to determine optimal join order.",
      },
      {
          std::string(kSampleFilters),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.sampleFilters),
          "Sample filters to estimate scan selectivity.",
      },
      {
          std::string(kUseFilteredTableStats),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.useFilteredTableStats),
          "Use connector-provided table statistics for cardinality estimation.",
      },
      {
          std::string(kPushdownSubfields),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.pushdownSubfields),
          "Extract accessed subfields of complex columns in table scan.",
      },
      {
          std::string(kAllMapsAsStruct),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.allMapsAsStruct),
          "Project maps with known key subsets as structs.",
      },
      {
          std::string(kSyntacticJoinOrder),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.syntacticJoinOrder),
          "Disable cost-based join ordering; use query order.",
      },
      {
          std::string(kAlwaysPlanPartialAggregation),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.alwaysPlanPartialAggregation),
          "Always split aggregation into partial + final.",
      },
      {
          std::string(kEnableReducingExistences),
          ConfigPropertyType::kBoolean,
          fmt::to_string(defaults.enableReducingExistences),
          "Enable reducing semi joins.",
      },
      {
          std::string(kParallelProjectWidth),
          ConfigPropertyType::kInteger,
          std::to_string(defaults.parallelProjectWidth),
          "Number of threads for parallel projection. 1 disables.",
      },
      {
          std::string(kTraceFlags),
          ConfigPropertyType::kInteger,
          std::to_string(defaults.traceFlags),
          "Bit mask for optimizer trace output: 1=retained, 2=exceeded best, 4=sample, 8=preprocess.",
      },
  };
}

std::string OptimizerOptions::normalize(
    std::string_view name,
    std::string_view value) const {
  if (name == kParallelProjectWidth) {
    auto width = std::stoi(std::string(value));
    VELOX_USER_CHECK_GE(
        width, 1, "parallel_project_width must be >= 1: {}", value);
  }
  return std::string(value);
}

OptimizerOptions OptimizerOptions::from(
    const folly::F14FastMap<std::string, std::string>& properties) {
  // Sets 'field' from the property map if present, otherwise keeps default.
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
