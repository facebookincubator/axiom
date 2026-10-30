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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer {

/// Builds the column statistics aggregation of a write or ANALYZE as
/// ColumnStatsSpecs. A write computes it in its TableWrite and
/// TableWriteMerge nodes. ANALYZE has no writer and computes the same
/// statistics in Aggregation nodes over the table scan, in the same steps.
///
///   WriteStatsBuilder builder(
///       table, columnNames, inputType, kind, numDrivers, numWorkers);
///   if (builder.hasStats()) {
///     auto spec = builder.writeSpec(); // The first step, over the input.
///   }
///   FinishWrite finishWrite{..., builder.statsMapping()};
///
/// Determines the topology (1, 2, or 3 specs) based on driver/worker counts:
///   - Single driver, single worker: writeSpec (kSingle)
///   - Multi driver, single worker: writeSpec (kPartial) + localMergeSpec
///     (kFinal)
///   - Multi worker: writeSpec (kPartial) + localMergeSpec (kIntermediate)
///     + finalMergeSpec (kFinal)
class WriteStatsBuilder {
 public:
  /// 'columnNames' are the table columns that the columns of 'inputType'
  /// hold, by position: every column of the table for a write, the analyzed
  /// ones for ANALYZE. They must include 'table.statsGroupingKeys()'. Throws
  /// for ANALYZE if the statistics aggregates are not registered.
  WriteStatsBuilder(
      const connector::Table& table,
      const std::vector<std::string>& columnNames,
      const velox::RowTypePtr& inputType,
      connector::WriteKind kind,
      int32_t numDrivers,
      int32_t numWorkers);

  /// True when the plan computes statistics. Always true for ANALYZE, which
  /// counts the rows even with no columns to analyze.
  bool hasStats() const {
    return hasStats_;
  }

  bool needsMerge() const {
    return hasStats_ && needsMerge_;
  }

  bool needsFinalMerge() const {
    return hasStats_ && needsFinalMerge_;
  }

  /// Returns how to read the statistics from the plan's output, or
  /// std::nullopt when there are none.
  std::optional<WriteStatsMapping> statsMapping() const;

  /// Spec for the TableWriteNode, or for ANALYZE the first Aggregation.
  /// kSingle when no merge needed, kPartial with intermediate output types
  /// otherwise.
  velox::core::ColumnStatsSpec writeSpec() const;

  /// Spec for the local TableWriteMergeNode, or Aggregation for ANALYZE.
  /// kFinal (single worker) or kIntermediate (multi worker). Reads
  /// intermediate state from sourceType.
  velox::core::ColumnStatsSpec localMergeSpec(
      const velox::RowTypePtr& sourceType) const;

  /// Spec for the final TableWriteMergeNode, or Aggregation for ANALYZE, in
  /// multi-worker plans. Always kFinal. Reads intermediate state from
  /// sourceType.
  velox::core::ColumnStatsSpec finalMergeSpec(
      const velox::RowTypePtr& sourceType) const;

 private:
  velox::core::ColumnStatsSpec buildMergeSpec(
      velox::core::AggregationNode::Step step,
      const velox::RowTypePtr& sourceType,
      const std::vector<velox::core::AggregationNode::Aggregate>&
          outputAggregates) const;

  bool hasStats_{false};
  bool needsMerge_{false};
  bool needsFinalMerge_{false};
  std::vector<velox::core::FieldAccessTypedExprPtr> groupingKeys_;
  std::vector<std::string> aggregateNames_;
  std::vector<velox::core::AggregationNode::Aggregate> aggregates_;
  WriteStatsMapping statsMapping_;
};

} // namespace facebook::axiom::optimizer
