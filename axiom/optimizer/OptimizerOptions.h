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

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace facebook::axiom::optimizer {

struct OptimizerOptions {
  /// Bit masks for use in 'traceFlags'.
  static constexpr uint32_t kRetained = 1;
  static constexpr uint32_t kExceededBest = 2;
  static constexpr uint32_t kSample = 4;
  static constexpr uint32_t kPreprocess = 8;

  /// Parallelizes independent projections over this many threads. 1 means no
  /// parallel projection.
  int32_t parallelProjectWidth = 1;

  /// Produces skyline subfield sets of complex type columns as top level
  /// columns in table scan.
  bool pushdownSubfields{false};

  /// Makes all maps for which a known subset of keys is accessed to
  /// be projected out as structs.
  bool allMapsAsStruct{false};

  /// Map from table name to  list of map columns to be read as structs unless
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

  /// Produce trace of plan candidates.
  uint32_t traceFlags{0};

  bool isMapAsStruct(const char* table, const char* column) const {
    if (allMapsAsStruct) {
      return true;
    }
    auto it = mapAsStruct.find(table);
    if (it == mapAsStruct.end()) {
      return false;
    }
    return std::find(it->second.begin(), it->second.end(), column) !=
        it->second.end();
  }
};

} // namespace facebook::axiom::optimizer
