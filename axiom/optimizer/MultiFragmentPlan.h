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

#include <optional>
#include <variant>

#include <folly/container/F14Map.h>
#include "axiom/common/Enums.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/core/MultiStagePlan.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::axiom::optimizer {

/// Identifies which field of ColumnStatistics an aggregate populates.
enum class ColumnStatField {
  kCount,
  kCountIf,
  kMin,
  kMax,
  kApproxDistinct,
  kSumDataSize,
  kVectorCount,
};

AXIOM_DECLARE_ENUM_NAME(ColumnStatField);

/// Maps stats aggregates for one table column to the ColumnStatistics fields
/// they populate.
struct ColumnStatMapping {
  /// Name of the column in the table schema.
  std::string columnName;

  /// Ordered list of stat fields collected for this column. Each field
  /// corresponds to one consecutive stats channel in the Velox write output.
  std::vector<ColumnStatField> fields;
};

/// Describes how to extract column statistics from the results of a write or
/// ANALYZE. Built by the optimizer alongside ColumnStatsSpec and stored on
/// FinishWrite.
struct WriteStatsMapping {
  /// Per-column stat mappings. The aggregate channels appear in this order
  /// in the stats output, after any grouping key columns.
  std::vector<ColumnStatMapping> columns;

  /// Number of grouping key columns in the stats aggregation. These appear
  /// as the first stats columns: after row_count, fragment and context in a
  /// write, and first in ANALYZE's output. Zero when there are no grouping
  /// keys.
  uint32_t numGroupingKeys{0};

  /// Table-level names for the grouping key columns, in the same order as
  /// they appear in the stats output. The stats output may use write-input
  /// column names (e.g. "expr_0"), but the grouping keys RowVector passed to
  /// finishWrite must use table column names (e.g. "pk"). Empty when there
  /// are no grouping keys.
  std::vector<std::string> groupingKeyNames;

  /// True when a "$row_count" column with each group's row count follows the
  /// grouping keys. Set when there are grouping keys, and always for ANALYZE,
  /// which has no writer to count the rows.
  bool hasRowCount{false};

  /// True for ANALYZE. Its statistics replace the stored ones, so each column
  /// carries its null percentage, computed from the group's row count.
  bool setsNullPct{false};
};

/// Callbacks to finalize writing to a connector after the query completes.
/// On success, the runner calls 'commit'. On failure, 'abort'. Only one of the
/// 'commit' or 'abort' is called.
class FinishWrite {
 public:
  FinishWrite() = default;
  FinishWrite(const FinishWrite&) = delete;
  FinishWrite(FinishWrite&& other) noexcept;
  FinishWrite& operator=(const FinishWrite&) = delete;
  FinishWrite& operator=(FinishWrite&& other) noexcept;

  FinishWrite(
      std::shared_ptr<connector::ConnectorMetadata> metadata,
      std::string connectorId,
      connector::ConnectorSessionPtr session,
      connector::ConnectorWriteHandlePtr handle,
      std::optional<WriteStatsMapping> statsMapping = std::nullopt);

  /// Commits a DELETE through finishDelete and aborts it through abortDelete.
  FinishWrite(
      std::shared_ptr<connector::ConnectorMetadata> metadata,
      std::string connectorId,
      connector::ConnectorSessionPtr session,
      connector::ConnectorDeleteHandlePtr handle);

  /// Best-effort abort if not already committed or aborted.
  ~FinishWrite();

  explicit operator bool() const {
    return operation_.has_value();
  }

  /// Commits a CREATE, INSERT or ANALYZE through finishWrite, extracting the
  /// statistics from the results, or a DELETE through finishDelete. Returns
  /// the number of rows written or removed, or for ANALYZE the number of rows
  /// analyzed.
  [[nodiscard]] connector::RowsFuture commit(
      const std::vector<velox::RowVectorPtr>& writeResults) &&;

  /// Aborts the write. Must be called if commit is not called.
  [[nodiscard]] velox::ContinueFuture abort() && noexcept;

  /// Returns a description of the write for EXPLAIN, naming the connector and
  /// leaving the detail to the handle. Empty if there is no write.
  std::string toString() const;

 private:
  struct WriteOperation {
    connector::ConnectorWriteHandlePtr handle;
    // Set when the write collects statistics.
    std::optional<WriteStatsMapping> statsMapping;
  };

  struct DeleteOperation {
    connector::ConnectorDeleteHandlePtr handle;
  };

  using Operation = std::variant<WriteOperation, DeleteOperation>;

  std::shared_ptr<connector::ConnectorMetadata> metadata_;
  std::string connectorId_;
  connector::ConnectorSessionPtr session_;
  std::optional<Operation> operation_;
};

/// Describes what a runner needs to read the scans of one stage of a
/// MultiFragmentPlan: which leaves run one bucket group at a time, and which
/// scans sample. Empty for most stages.
struct FragmentScans {
  /// The leaf nodes of a stage that runs one bucket group at a time. A scan
  /// read that way maps to the partitioning it is read by, already coarsened to
  /// the worker count, and the connector tags each emitted Split with a
  /// remotePartition the runtime routes by. An exchange feeding the same stage
  /// maps to null: its partitions line up with those groups by construction.
  /// All non-null entries share numPartitions(), which is the stage's
  /// numRemotePartitions. Empty when the stage does not run grouped.
  folly::F14FastMap<
      velox::core::PlanNodeId,
      std::shared_ptr<connector::PartitionType>>
      groupedNodes;

  /// Sample percentage per scan node for TABLESAMPLE SYSTEM. The split source
  /// for the scan emits each split with this probability, in the open interval
  /// (0, 100). Empty when no scan in this stage is sampled.
  folly::F14FastMap<velox::core::PlanNodeId, double> sampledScans;
};

/// Describes a distributed plan handed to a Runner for parallel/distributed
/// execution: a velox::core::MultiStagePlan, what Axiom adds about reading the
/// scans of its stages, and the options the plan was made for. A write the
/// connector performs through metadata alone runs no stage, so its plan is
/// empty.
///
///   MultiFragmentPlan plan(std::move(stages), std::move(scans), options);
///   for (const auto& stage : plan.stages()) {
///     const auto& scans = plan.scans(stage.id);
///     ...
///   }
///   plan.stagePlan().root();  // produces the query result
class MultiFragmentPlan {
 public:
  /// Describes options for running a MultiFragmentPlan.
  struct Options {
    /// Query id used as a prefix for tasks ids.
    std::string queryId;

    /// Maximum number of remote partitions for one stage of execution. If 1,
    /// there are no exchanges.
    /// The optimizer may lower it, so a plan carries the count it was planned
    /// for rather than the count available.
    int32_t maxRemotePartitions{1};

    /// Maximum number of local partitions, i.e. threads in a stage in a
    /// worker. If 1, there are no local exchanges.
    int32_t maxLocalPartitions{4};

    /// Controls how query results are delivered from the root stage.
    ///
    /// When true, wraps the root of the root stage in a PartitionedOutputNode
    /// that serializes results for remote consumption via exchange. With
    /// multiple workers, results remain distributed across worker nodes — no
    /// gather is added, so the caller must read from each worker
    /// independently. Even with a single worker, the PartitionedOutputNode is
    /// added to enable remote consumption.
    ///
    /// When false (default), results are consumed locally from the root
    /// stage. With multiple workers, the planner adds a gather stage to
    /// collect results onto a single node. With a single worker, no
    /// additional nodes are added.
    bool remoteOutput{false};

    /// The id of the plan's first node. The plan numbers its nodes from it.
    int32_t firstPlanNodeId{0};

    /// Returns options for single-node, single-threaded local execution.
    /// Used by constant folding and join sampling which always run via
    /// LocalRunner regardless of the query's distributed options.
    static Options singleNode() {
      return Options{
          .maxRemotePartitions = 1,
          .maxLocalPartitions = 1,
          .remoteOutput = false};
    }
  };

  /// Makes a plan of 'stages', which form a velox::core::MultiStagePlan unless
  /// empty; the constructor throws a user error if they do not. 'scans' maps a
  /// stage id to what its scans need, for the stages that need anything.
  MultiFragmentPlan(
      std::vector<velox::core::Stage> stages,
      folly::F14FastMap<int32_t, FragmentScans> scans,
      Options options);

  /// Returns true for a plan that runs no stage.
  bool empty() const {
    return !stagePlan_.has_value();
  }

  /// Returns the stages. Requires !empty().
  const velox::core::MultiStagePlan& stagePlan() const;

  /// Returns the stages in the order they were given; empty when empty().
  const std::vector<velox::core::Stage>& stages() const;

  /// Returns what the scans of stage 'stageId' need; empty for a stage
  /// without grouped or sampled scans.
  const FragmentScans& scans(int32_t stageId) const;

  const Options& options() const {
    return options_;
  }

  /// Returns the largest id of this plan's nodes, or firstPlanNodeId - 1 for
  /// a plan without stages.
  int32_t maxPlanNodeId() const;

  /// @param detailed If true, includes details of each plan node. Otherwise,
  /// only node types are included.
  /// @param addContext Optional lambda to add context to plan nodes. Receives
  /// plan node ID, indentation and std::ostream where to append the context.
  /// Start each line of context with 'indentation' and end with a new-line
  /// character.
  std::string toString(
      bool detailed = true,
      const std::function<void(
          const velox::core::PlanNodeId& nodeId,
          std::string_view indentation,
          std::ostream& out)>& addContext = nullptr) const;

  /// Prints the summary of the plan using PlanNode::toSummaryString() API.
  std::string toSummaryString(
      velox::core::PlanSummaryOptions options = {}) const;

 private:
  const std::optional<velox::core::MultiStagePlan> stagePlan_;
  const folly::F14FastMap<int32_t, FragmentScans> scans_;
  const Options options_;
};

using MultiFragmentPlanPtr = std::shared_ptr<const MultiFragmentPlan>;

} // namespace facebook::axiom::optimizer
