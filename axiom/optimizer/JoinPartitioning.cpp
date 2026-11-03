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

#include "axiom/optimizer/JoinPartitioning.h"

#include <algorithm>

namespace facebook::axiom::optimizer {

std::optional<JoinPartitioning> JoinPartitioning::from(
    const Partitioning& partitioning,
    const ExprVector& joinKeys) {
  if (partitioning.kind != PartitionKind::kPartitioned ||
      partitioning.keys.empty() || partitioning.replicateNullsAndAny) {
    return std::nullopt;
  }
  std::vector<size_t> positions;
  positions.reserve(partitioning.keys.size());
  for (ExprCP partitionKey : partitioning.keys) {
    const auto it =
        std::find_if(joinKeys.begin(), joinKeys.end(), [&](ExprCP joinKey) {
          return joinKey->sameOrEqual(*partitionKey);
        });
    if (it == joinKeys.end()) {
      return std::nullopt;
    }
    positions.push_back(it - joinKeys.begin());
  }
  return JoinPartitioning{std::move(positions), partitioning.partitionType};
}

bool JoinPartitioning::coPartitionsWith(const JoinPartitioning& other) const {
  return keyPositions == other.keyPositions &&
      Partitioning::commonJoinPartitionType(partitionType, other.partitionType)
          .has_value();
}

ExprVector JoinPartitioning::correspondingKeys(
    const ExprVector& joinKeys) const {
  ExprVector result;
  result.reserve(keyPositions.size());
  for (const size_t position : keyPositions) {
    result.push_back(joinKeys[position]);
  }
  return result;
}

} // namespace facebook::axiom::optimizer
