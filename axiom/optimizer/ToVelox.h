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

namespace facebook::axiom::optimizer {

/// A map from PlanNodeId of an executable plan to a key for
/// recording the execution for use in cost model. The key is a
/// canonical summary of the node and its inputs.
using NodeHistoryMap = folly::F14FastMap<velox::core::PlanNodeId, std::string>;

using NodePredictionMap =
    folly::F14FastMap<velox::core::PlanNodeId, NodePrediction>;

/// Plan and specification for recording execution history amd planning ttime
/// predictions.
struct PlanAndStats {
  runner::MultiFragmentPlanPtr plan;
  NodeHistoryMap history;
  NodePredictionMap prediction;

  /// Returns a string representation of the plan annotated with estimates from
  /// 'prediction'.
  std::string toString() const;
};

class ToVelox {
 public:
  ToVelox(
      const runner::MultiFragmentPlan::Options& options,
      const OptimizerOptions& optimizerOptions);

  /// Converts physical plan (a tree of RelationOp) to an executable
  /// multi-fragment Velox plan.
  PlanAndStats toVeloxPlan(
      RelationOpPtr plan,
      const runner::MultiFragmentPlan::Options& options);

  std::pair<
      velox::connector::ConnectorTableHandlePtr,
      std::vector<velox::core::TypedExprPtr>>
  leafHandle(int32_t id) {
    auto it = leafHandles_.find(id);
    return it != leafHandles_.end()
        ? it->second
        : std::make_pair<
              std::shared_ptr<velox::connector::ConnectorTableHandle>,
              std::vector<velox::core::TypedExprPtr>>(nullptr, {});
  }

  velox::core::TypedExprPtr toTypedExpr(ExprCP expr);

  velox::RowTypePtr subfieldPushdownScanType(
      BaseTableCP baseTable,
      const ColumnVector& leafColumns,
      ColumnVector& topColumns,
      folly::F14FastMap<ColumnCP, velox::TypePtr>& typeMap);

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
      velox::connector::ConnectorTableHandlePtr handle,
      std::vector<velox::core::TypedExprPtr> extraFilters) {
    leafHandles_[id] = {std::move(handle), std::move(extraFilters)};
  }

  /// True if a scan should expose 'column' of 'table' as a struct only
  /// containing the accessed keys. 'column' must be a top level map column.
  bool isMapAsStruct(Name table, Name column) {
    return optimizerOptions_.isMapAsStruct(table, column);
  }

  // Makes an output type for use in PlanNode et al. If 'columnType' is set,
  // only considers base relation columns of the given type.
  velox::RowTypePtr makeOutputType(const ColumnVector& columns) const;

  // Makes a getter path over a top level column and can convert the top
  // map getter into struct getter if maps extracted as structs.
  velox::core::TypedExprPtr
  pathToGetter(ColumnCP column, PathCP path, velox::core::TypedExprPtr field);

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
      const Aggregation& op,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  // Makes partial + final order by fragments for order by with and without
  // limit.
  velox::core::PlanNodePtr makeOrderBy(
      const OrderBy& op,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  // Makes partial + final limit fragments.
  velox::core::PlanNodePtr makeLimit(
      const Limit& op,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  // @pre op.sNoLimit() is true.
  velox::core::PlanNodePtr makeOffset(
      const Limit& op,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeScan(
      const TableScan& scan,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeFilter(
      const Filter& filter,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeProject(
      const Project& project,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeJoin(
      const Join& join,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeRepartition(
      const Repartition& repartition,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages,
      std::shared_ptr<velox::core::ExchangeNode>& exchange);

  // Makes a union all with a mix of remote and local inputs. Combines all
  // remote inputs into one ExchangeNode.
  velox::core::PlanNodePtr makeUnionAll(
      const UnionAll& unionAll,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeValues(
      const Values& values,
      runner::ExecutableFragment& fragment);

  // Makes a tree of PlanNode for a tree of
  // RelationOp. 'fragment' is the fragment that 'op'
  // belongs to. If op or children are repartitions then the
  // source of each makes a separate fragment. These
  // fragments are referenced from 'fragment' via
  // 'inputStages' and are returned in 'stages'.
  velox::core::PlanNodePtr makeFragment(
      const RelationOpPtr& op,
      runner::ExecutableFragment& fragment,
      std::vector<runner::ExecutableFragment>& stages);

  // Records the prediction for 'node' and a history key to update history
  // after the plan is executed.
  void makePredictionAndHistory(
      const velox::core::PlanNodeId& id,
      const RelationOp* op);

  // Returns a stack of parallel project nodes if parallelization makes sense.
  // nullptr means use regular ProjectNode in output.
  velox::core::PlanNodePtr maybeParallelProject(
      const Project* project,
      velox::core::PlanNodePtr input);

  velox::core::PlanNodePtr makeParallelProject(
      const velox::core::PlanNodePtr& input,
      const PlanObjectSet& topExprs,
      const PlanObjectSet& placed,
      const PlanObjectSet& extraColumns);

  // Makes projections for subfields as top level columns.
  // @param scanNode TableScan or Filter input node.
  velox::core::PlanNodePtr makeSubfieldProjections(
      const TableScan& scan,
      const velox::core::PlanNodePtr& scanNode);

  runner::ExecutableFragment newFragment();

  // TODO Move this into MultiFragmentPlan::Options.
  const velox::VectorSerde::Kind exchangeSerdeKind_{
      velox::VectorSerde::Kind::kPresto};

  runner::MultiFragmentPlan::Options options_;

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
  folly::F14FastMap<ColumnCP, velox::TypePtr> columnAlteredTypes_;

  // When generating parallel projections with intermediate assignment for
  // common subexpressions, maps from ExprCP to the FieldAccessTypedExppr with
  // the value.
  folly::F14FastMap<ExprCP, velox::core::TypedExprPtr> projectedExprs_;

  // Map from plan object id to pair of handle with pushdown filters and list
  // of filters to eval on the result from the handle.
  folly::F14FastMap<
      int32_t,
      std::pair<
          velox::connector::ConnectorTableHandlePtr,
          std::vector<velox::core::TypedExprPtr>>>
      leafHandles_;

  // Serial number for plan nodes in executable plan.
  int32_t nodeCounter_{0};

  // Serial number for stages in executable plan.
  int32_t stageCounter_{0};

  const std::optional<std::string> subscript_;
};

} // namespace facebook::axiom::optimizer
