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
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/runner/MultiFragmentPlan.h"

namespace facebook::velox::optimizer {

/// A map from PlanNodeId of an executable plan to a key for
/// recording the execution for use in cost model. The key is a
/// canonical summary of the node and its inputs.
using NodeHistoryMap = std::unordered_map<core::PlanNodeId, std::string>;

using NodePredictionMap = std::unordered_map<core::PlanNodeId, NodePrediction>;

/// Plan and specification for recording execution history amd planning ttime
/// predictions.
struct PlanAndStats {
  axiom::runner::MultiFragmentPlanPtr plan;
  NodeHistoryMap history;
  NodePredictionMap prediction;
};

class ToVelox {
 public:
  ToVelox(
      const axiom::runner::MultiFragmentPlan::Options& options,
      const OptimizerOptions& optimizerOptions)
      : options_{options},
        optimizerOptions_{optimizerOptions},
        isSingle_{options.numWorkers == 1} {}

  /// Converts physical plan (a tree of RelationOp) to an executable
  /// multi-fragment Velox plan.
  PlanAndStats toVeloxPlan(
      RelationOpPtr plan,
      const axiom::runner::MultiFragmentPlan::Options& options);

  std::pair<connector::ConnectorTableHandlePtr, std::vector<core::TypedExprPtr>>
  leafHandle(int32_t id) {
    auto it = leafHandles_.find(id);
    return it != leafHandles_.end()
        ? it->second
        : std::make_pair<
              std::shared_ptr<velox::connector::ConnectorTableHandle>,
              std::vector<core::TypedExprPtr>>(nullptr, {});
  }

  velox::core::TypedExprPtr toTypedExpr(ExprCP expr);

  RowTypePtr subfieldPushdownScanType(
      BaseTableCP baseTable,
      const ColumnVector& leafColumns,
      ColumnVector& topColumns,
      std::unordered_map<ColumnCP, TypePtr>& typeMap);

  // Returns a new PlanNodeId.
  velox::core::PlanNodeId nextId() {
    return fmt::format("{}", nodeCounter_++);
  }

  void filterUpdated(BaseTableCP baseTable, bool updateSelectivity = true);

  // Returns column name to use in the Velox plan.
  static std::string outputName(ColumnCP column) {
    return column->alias() ? column->alias() : column->toString();
  }

 private:
  void setLeafHandle(
      int32_t id,
      const connector::ConnectorTableHandlePtr& handle,
      const std::vector<core::TypedExprPtr>& extraFilters) {
    leafHandles_[id] = std::make_pair(handle, extraFilters);
  }

  /// True if a scan should expose 'column' of 'table' as a struct only
  /// containing the accessed keys. 'column' must be a top level map column.
  bool isMapAsStruct(Name table, Name column) {
    return optimizerOptions_.isMapAsStruct(table, column);
  }

  // Makes an output type for use in PlanNode et al. If 'columnType' is set,
  // only considers base relation columns of the given type.
  velox::RowTypePtr makeOutputType(const ColumnVector& columns);

  // Produces a scan output type with only top level columns. Returns
  // these in scanColumns. The scan->columns() is the leaf columns,
  // not the top level ones if subfield pushdown.
  RowTypePtr scanOutputType(
      const TableScan& scan,
      ColumnVector& scanColumns,
      std::unordered_map<ColumnCP, TypePtr>& typeMap);

  // Makes a getter path over a top level column and can convert the top
  // map getter into struct getter if maps extracted as structs.
  core::TypedExprPtr
  pathToGetter(ColumnCP column, PathCP path, core::TypedExprPtr source);

  // Returns a filter expr that ands 'exprs'. nullptr if 'exprs' is empty.
  velox::core::TypedExprPtr toAnd(const ExprVector& exprs);

  // Translates 'exprs' and returns them in 'result'. If an expr is
  // other than a column, adds a projection node to evaluate the
  // expression. The projection is added on top of 'source' and
  // returned. If no projection is added, 'source' is returned.
  velox::core::PlanNodePtr maybeProject(
      const ExprVector& exprs,
      velox::core::PlanNodePtr source,
      std::vector<velox::core::FieldAccessTypedExprPtr>& result);

  // Makes a Velox AggregationNode for a RelationOp.
  velox::core::PlanNodePtr makeAggregation(
      const Aggregation& agg,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  // Makes partial + final order by fragments for order by with and without
  // limit.
  velox::core::PlanNodePtr makeOrderBy(
      const OrderBy& op,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  // Makes partial + final limit fragments.
  velox::core::PlanNodePtr makeLimit(
      const Limit& op,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  // @pre op.sNoLimit() is true.
  velox::core::PlanNodePtr makeOffset(
      const Limit& op,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeScan(
      const TableScan& scan,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeFilter(
      const Filter& filter,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeProject(
      const Project& project,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeJoin(
      const Join& join,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeRepartition(
      const Repartition& repartition,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages,
      std::shared_ptr<core::ExchangeNode>& exchange);

  // Makes a union all with a mix of remote and local inputs. Combines all
  // remote inputs into one ExchangeNode.
  velox::core::PlanNodePtr makeUnionAll(
      const UnionAll& unionAll,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  core::PlanNodePtr makeValues(
      const Values& values,
      axiom::runner::ExecutableFragment& fragment);

  // Makes a tree of PlanNode for a tree of
  // RelationOp. 'fragment' is the fragment that 'op'
  // belongs to. If op or children are repartitions then the
  // source of each makes a separate fragment. These
  // fragments are referenced from 'fragment' via
  // 'inputStages' and are returned in 'stages'.
  velox::core::PlanNodePtr makeFragment(
      const RelationOpPtr& op,
      axiom::runner::ExecutableFragment& fragment,
      std::vector<axiom::runner::ExecutableFragment>& stages);

  // Records the prediction for 'node' and a history key to update history
  // after the plan is executed.
  void makePredictionAndHistory(
      const core::PlanNodeId& id,
      const RelationOp* op);

  // Returns a stack of parallel project nodes if parallelization makes sense.
  // nullptr means use regular ProjectNode in output.
  velox::core::PlanNodePtr maybeParallelProject(
      const Project* op,
      core::PlanNodePtr input);

  core::PlanNodePtr makeParallelProject(
      const core::PlanNodePtr& input,
      const PlanObjectSet& topExprs,
      const PlanObjectSet& placed,
      const PlanObjectSet& extraColumns);

  // Makes projections for subfields as top level columns.
  core::PlanNodePtr makeSubfieldProjections(
      const TableScan& scan,
      const core::TableScanNodePtr& scanNode);

  axiom::runner::ExecutableFragment newFragment();

  // TODO Move this into MultiFragmentPlan::Options.
  const VectorSerde::Kind exchangeSerdeKind_{VectorSerde::Kind::kPresto};

  axiom::runner::MultiFragmentPlan::Options options_;

  const OptimizerOptions& optimizerOptions_;

  const bool isSingle_;

  // Map filled in with a PlanNodeId and history key for measurement points
  // for
  // history recording.
  NodeHistoryMap nodeHistory_;

  // Predicted cardinality and memory for nodes to record in history.
  NodePredictionMap prediction_;

  // On when producing a remaining filter for table scan, where columns must
  // correspond 1:1 to the schema.
  bool makeVeloxExprWithNoAlias_{false};

  bool getterForPushdownSubfield_{false};

  // Map from top level map column  accessed as struct to the struct type.
  // Used only when generating a leaf scan for result Velox plan.
  std::unordered_map<ColumnCP, TypePtr> columnAlteredTypes_;

  // When generating parallel projections with intermediate assignment for
  // common subexpressions, maps from ExprCP to the FieldAccessTypedExppr with
  // the value.
  std::unordered_map<ExprCP, core::TypedExprPtr> projectedExprs_;

  // Map from plan object id to pair of handle with pushdown filters and list
  // of
  // filters to eval on the result from the handle.
  std::unordered_map<
      int32_t,
      std::pair<
          connector::ConnectorTableHandlePtr,
          std::vector<core::TypedExprPtr>>>
      leafHandles_;

  // Serial number for plan nodes in executable plan.
  int32_t nodeCounter_{0};

  // Serial number for stages in executable plan.
  int32_t stageCounter_{0};
};

} // namespace facebook::velox::optimizer
