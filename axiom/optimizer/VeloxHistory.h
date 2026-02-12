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

#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/ToVelox.h"
#include "velox/exec/TaskStats.h"

namespace facebook::axiom::optimizer {

/// Records and retrieves estimated and actual cardinalities based on Velox
/// handles and execution stats.
class VeloxHistory : public History {
 public:
  void recordJoinSample(std::string_view key, float lr, float rl) override;

  /// Samples actual data from both sides of the join to estimate fanout.
  /// Returns {0, 0} if either side is not a BaseTable. Results are cached by a
  /// canonical key derived from the join's table names and key columns.
  std::pair<float, float> sampleJoin(JoinEdge* edge) override;

  std::optional<Cost> findCost(RelationOp& op) override {
    return std::nullopt;
  }

  void recordCost(const RelationOp& op, Cost cost) override {}

  /// Estimates filter selectivity for a table scan and updates column value
  /// constraints (cardinality, min, max, trueFraction, nullFraction, nullable)
  /// based on the table's filters. If sampling is enabled and a physical table
  /// is available, samples the table to refine the selectivity estimate.
  /// Returns true if the estimate is backed by sampling, false if it relies
  /// solely on statistics-based estimation.
  bool estimateLeafSelectivity(
      BaseTable& table,
      const velox::RowTypePtr& scanType) override;

  /// Stores observed costs and cardinalities from a query execution. If 'op' is
  /// non-null, non-leaf costs from non-leaf levels are recorded. Otherwise only
  /// leaf scan selectivities  are recorded.
  virtual void recordVeloxExecution(
      const PlanAndStats& plan,
      const std::vector<velox::exec::TaskStats>& stats);

  folly::dynamic serialize() override;

  void update(folly::dynamic& serialized) override;

 private:
  folly::F14FastMap<std::string, std::pair<float, float>> joinSamples_;
  folly::F14FastMap<std::string, NodePrediction> planHistory_;
};

} // namespace facebook::axiom::optimizer
