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

#include <folly/Random.h>
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/core/Expressions.h"
#include "velox/vector/ComplexVector.h"

#pragma once

namespace facebook::axiom::optimizer::test {

struct FeatureOptions {
  int32_t numFloat{10};
  int32_t numInt{10};
  int32_t numIdList{10};
  int32_t idListMaxCard{1000};
  int32_t idListMinCard{10};
  int32_t idListMaxDistinct{1000};
  int32_t numIdScoreList{5};

  /// If true, idListSizes uses an exponential distribution with alternating
  /// small and large cardinalities (0, max-1, 2, max-3, ...).
  bool expCardinalities{false};

  /// Structs for use in reading the features. One field for each
  /// key. Filled in by makeFeatures().
  velox::RowTypePtr floatStruct;
  velox::RowTypePtr idListStruct;
  velox::RowTypePtr idScoreListStruct;

  // Parameters for generating test exprs.
  /// Number of projections for float one feature.
  int32_t floatExprsPct{130};
  int32_t idListExprPct{0};
  int32_t idScoreListExprPct{0};

  /// Percentage of projections that depend on multiple features.
  float multiColumnPct{20};

  /// Percentage of exprs with a rand.
  int32_t randomPct{20};

  /// Percentage of uid dependent exprs.
  int32_t uidPct{20};

  /// percentage of extra  + 1's.
  int32_t plusOnePct{20};

  /// Percentage of exprs wrapped in bucketize.
  int32_t bucketizePct{30};

  mutable folly::Random::DefaultGenerator rng;

  bool coinToss(int32_t pct) const {
    return folly::Random::rand32(rng) % 100u < pct;
  }
};

std::vector<velox::RowVectorPtr> makeFeatures(
    int32_t numBatches,
    int32_t batchSize,
    FeatureOptions& opts,
    velox::memory::MemoryPool* pool);

void makeExprs(
    const FeatureOptions& opts,
    std::vector<std::string>& names,
    std::vector<velox::core::TypedExprPtr>& exprs);

void makeLogicalExprs(
    const FeatureOptions& opts,
    std::vector<std::string>& names,
    std::vector<logical_plan::ExprPtr>& exprs);

/// Generates a multi-stage projection pipeline for processing id_score_list
/// features. The pipeline splits features into two paths:
/// - First half: coalesce -> row_constructor -> convert_format -> fillna
/// - Second half: coalesce -> map_keys -> first_x -> sigrid_hash_into_i32
///
/// @param opts Feature generation options
/// @param source Input plan node
/// @param featureKeys List of feature keys to process (e.g., {200000, 200200,
/// 200400})
/// @param passthroughColumns List of column names to project through in each
/// projection node
/// @return Plan node with 4 projection layers applied
logical_plan::LogicalPlanNodePtr makeIdScoreListPipeline(
    const FeatureOptions& opts,
    logical_plan::LogicalPlanNodePtr source,
    const std::vector<int32_t>& featureKeys,
    const std::vector<std::string>& passthroughColumns = {});

/// Generates a complete feature processing pipeline
/// @param opts Feature generation options (must have floatStruct, idListStruct,
/// idScoreListStruct initialized)
/// @param connectorId Connector ID to use for the table scan (defaults to
/// "test-hive")
/// @return Logical plan with table scan and multi-stage feature processing
logical_plan::LogicalPlanNodePtr makeFeaturePipeline(
    const FeatureOptions& opts,
    std::string_view connectorId = "test-hive");

} // namespace facebook::axiom::optimizer::test
