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

#include "axiom/optimizer/v2/JoinFanout.h"

#include <algorithm>
#include <cstddef>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer::v2 {

std::optional<float> JoinFanout::estimateJoinCardinality(
    std::optional<float> leftCardinality,
    std::optional<float> rightCardinality,
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    const ConstraintMap& constraints) {
  const auto ndv = [&](ExprCP key) {
    return value(constraints, key).cardinality;
  };

  if (!leftCardinality.has_value() || !rightCardinality.has_value()) {
    return std::nullopt;
  }

  float jointNdvLeft{1};
  float jointNdvRight{1};
  for (size_t i = 0; i < leftKeys.size(); ++i) {
    const auto leftNdv = ndv(leftKeys[i]);
    const auto rightNdv = ndv(rightKeys[i]);
    // An unknown join-key NDV makes the cardinality unknown.
    if (!leftNdv.has_value() || !rightNdv.has_value()) {
      return std::nullopt;
    }
    // A zero-NDV key means an empty side, hence an empty join.
    if (*leftNdv == 0 || *rightNdv == 0) {
      return 0;
    }
    jointNdvLeft *= *leftNdv;
    jointNdvRight *= *rightNdv;
  }
  jointNdvLeft = std::min(jointNdvLeft, *leftCardinality);
  jointNdvRight = std::min(jointNdvRight, *rightCardinality);
  return *leftCardinality * *rightCardinality /
      std::max(jointNdvLeft, jointNdvRight);
}

std::optional<float> JoinFanout::outputCardinality(
    velox::core::JoinType joinType,
    std::optional<float> leftCardinality,
    std::optional<float> rightCardinality,
    std::optional<float> matched) {
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return maxOf(matched, leftCardinality);
    case velox::core::JoinType::kRight:
      return maxOf(matched, rightCardinality);
    case velox::core::JoinType::kFull:
      return subtract(add(leftCardinality, rightCardinality), matched);
    case velox::core::JoinType::kLeftSemiFilter:
      return minOf(leftCardinality, matched);
    case velox::core::JoinType::kRightSemiFilter:
      return minOf(rightCardinality, matched);
    case velox::core::JoinType::kLeftSemiProject:
      // Mark-preserving: every left row is output with a boolean mark.
      return leftCardinality;
    case velox::core::JoinType::kRightSemiProject:
      return rightCardinality;
    case velox::core::JoinType::kAnti:
      return maxOf(
          0.0f, subtract(leftCardinality, minOf(leftCardinality, matched)));
    default:
      return matched;
  }
}

} // namespace facebook::axiom::optimizer::v2
