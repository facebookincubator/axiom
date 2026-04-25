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

#include "axiom/common/Enums.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/core/PlanFragment.h"
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

/// Describes how to extract column statistics from Velox write results.
/// Built by the optimizer alongside ColumnStatsSpec and stored on FinishWrite.
struct WriteStatsMapping {
  /// Per-column stat mappings. The aggregate channels appear in this order
  /// in the stats output, after any grouping key columns.
  std::vector<ColumnStatMapping> columns;

  /// Number of grouping key columns in the stats aggregation. These appear
  /// as the first stats columns (after row_count, fragment, context). Zero
  /// when there are no grouping keys.
  uint32_t numGroupingKeys{0};

  /// Table-level names for the grouping key columns, in the same order as
  /// they appear in the stats output. The stats output may use write-input
  /// column names (e.g. "expr_0"), but the grouping keys RowVector passed to
  /// finishWrite must use table column names (e.g. "pk"). Empty when there
  /// are no grouping keys.
  std::vector<std::string> groupingKeyNames;
};

/// Describes an exchange source for an ExchangeNode a non-leaf stage.
struct InputStage {
  // Id of ExchangeNode in the consumer fragment.
  velox::core::PlanNodeId consumerNodeId;

  /// Task prefix of producer stage.
  std::string producerTaskPrefix;
};

/// Callbacks to finalize writing to a connector after the query completes.
/// On success, the runner calls 'commit'. On failure, 'abort'. Only one of the
/// 'commit' or 'abort' is called.
class FinishWrite {
 public:
  FinishWrite() = default;
  FinishWrite(const FinishWrite&) = delete;
  FinishWrite(FinishWrite&& other) noexcept = default;
  FinishWrite& operator=(const FinishWrite&) = delete;
  FinishWrite& operator=(FinishWrite&& other) noexcept = default;

  FinishWrite(
      std::shared_ptr<connector::ConnectorMetadata> metadata,
      connector::ConnectorSessionPtr session,
      connector::ConnectorWriteHandlePtr handle,
      WriteStatsMapping statsMapping = {});

  /// Best-effort abort if not already committed or aborted.
  ~FinishWrite();

  explicit operator bool() const {
    return handle_ != nullptr;
  }

  /// Commits the write by extracting stats from write results and calling
  /// ConnectorMetadata::finishWrite. Returns the number of rows written.
  [[nodiscard]] connector::RowsFuture commit(
      const std::vector<velox::RowVectorPtr>& writeResults) &&;

  /// Aborts the write. Must be called if commit is not called.
  [[nodiscard]] velox::ContinueFuture abort() && noexcept;

 private:
  std::shared_ptr<connector::ConnectorMetadata> metadata_;
  connector::ConnectorSessionPtr session_;
  connector::ConnectorWriteHandlePtr handle_;
  WriteStatsMapping statsMapping_;
};

/// Describes a fragment of a distributed plan. This allows a run
/// time to distribute fragments across workers and to set up
/// exchanges. A complete plan is a vector of these with the last
/// being the fragment that gathers results from the complete
/// plan. Different runtimes, e.g. local, streaming or
/// materialized shuffle can use this to describe exchange
/// parallel execution. Decisions on number of workers, location
/// of workers and mode of exchange are up to the runtime.
struct ExecutableFragment {
  ExecutableFragment() = default;

  explicit ExecutableFragment(std::string taskPrefix)
      : taskPrefix{std::move(taskPrefix)} {}

  std::string taskPrefix;

  int32_t width{0};

  velox::core::PlanFragment fragment;

  /// Source fragments and Exchange node ids for remote shuffles producing input
  /// for 'this'.
  std::vector<InputStage> inputStages;
};

/// Describes a distributed plan handed to a Runner for parallel/distributed
/// execution. The last element of 'fragments' is by convention the stage that
/// gathers the query result. Otherwise the order of 'fragments' is not
/// important since the producer-consumer relations are given by 'inputStages'
/// in each fragment.
class MultiFragmentPlan {
 public:
  /// Describes options for running a MultiFragmentPlan.
  struct Options {
    /// Query id used as a prefix for tasks ids.
    std::string queryId;

    /// Maximum Number of independent Tasks for one stage of execution. If 1,
    /// there are no exchanges.
    int32_t numWorkers{1};

    /// Number of threads in a fragment in a worker. If 1, there are no local
    /// exchanges.
    int32_t numDrivers{4};

    /// Controls how query results are delivered from the final plan fragment.
    ///
    /// When true, wraps the root of the final fragment in a
    /// PartitionedOutputNode that serializes results for remote consumption
    /// via exchange. With multiple workers, results remain distributed across
    /// worker nodes — no gather is added, so the caller must read from each
    /// worker independently. Even with a single worker, the
    /// PartitionedOutputNode is added to enable remote consumption.
    ///
    /// When false (default), results are consumed locally from the final
    /// fragment. With multiple workers, the planner adds a gather stage to
    /// collect results onto a single node. With a single worker, no
    /// additional nodes are added.
    bool remoteOutput{false};

    /// Returns options for single-node, single-threaded local execution.
    /// Used by constant folding and join sampling which always run via
    /// LocalRunner regardless of the query's distributed options.
    static Options singleNode() {
      return Options{.numWorkers = 1, .numDrivers = 1, .remoteOutput = false};
    }
  };

  MultiFragmentPlan(std::vector<ExecutableFragment> fragments, Options options)
      : fragments_{std::move(fragments)}, options_{std::move(options)} {}

  const std::vector<ExecutableFragment>& fragments() const {
    return fragments_;
  }

  const Options& options() const {
    return options_;
  }

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
  const std::vector<ExecutableFragment> fragments_;
  const Options options_;
};

using MultiFragmentPlanPtr = std::shared_ptr<const MultiFragmentPlan>;

} // namespace facebook::axiom::optimizer
