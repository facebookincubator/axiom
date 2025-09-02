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
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/ToGraph.h"
#include "axiom/optimizer/ToVelox.h"
#include "axiom/runner/MultiFragmentPlan.h"

namespace facebook::velox::optimizer {

/// Instance of query optimization. Converts a plan and schema into an
/// optimized plan. Depends on QueryGraphContext being set on the
/// calling thread. There is one instance per query to plan. The
/// instance must stay live as long as a returned plan is live.
class Optimization {
 public:
  Optimization(
      const logical_plan::LogicalPlanNode& logicalPlan,
      const Schema& schema,
      History& history,
      std::shared_ptr<core::QueryCtx> veloxQueryCtx,
      velox::core::ExpressionEvaluator& evaluator,
      OptimizerOptions options = {},
      axiom::runner::MultiFragmentPlan::Options runnerOptions = {});

  // Simplified API for usage in testing and tooling.
  static PlanAndStats toVeloxPlan(
      const logical_plan::LogicalPlanNode& logicalPlan,
      velox::memory::MemoryPool& pool,
      OptimizerOptions options = {},
      axiom::runner::MultiFragmentPlan::Options runnerOptions = {});

  Optimization(const Optimization& other) = delete;
  Optimization& operator=(const Optimization& other) = delete;

  /// Returns the optimized RelationOp plan for 'plan' given at construction.
  PlanP bestPlan();

  /// Returns a set of per-stage Velox PlanNode trees. If 'historyKeys' is
  /// given, these can be used to record history data about the execution of
  /// each relevant node for costing future queries.
  PlanAndStats toVeloxPlan(RelationOpPtr plan) {
    return toVelox_.toVeloxPlan(std::move(plan), runnerOptions_);
  }

  std::pair<connector::ConnectorTableHandlePtr, std::vector<core::TypedExprPtr>>
  leafHandle(int32_t id) {
    return toVelox_.leafHandle(id);
  }

  /// Translates from Expr to Velox.
  velox::core::TypedExprPtr toTypedExpr(ExprCP expr) {
    return toVelox_.toTypedExpr(expr);
  }

  RowTypePtr subfieldPushdownScanType(
      BaseTableCP baseTable,
      const ColumnVector& leafColumns,
      ColumnVector& topColumns,
      std::unordered_map<ColumnCP, TypePtr>& typeMap) {
    return toVelox_.subfieldPushdownScanType(
        baseTable, leafColumns, topColumns, typeMap);
  }

  /// Sets 'filterSelectivity' of 'baseTable' from history. Returns true if set.
  /// 'scanType' is the set of sampled columns with possible map to struct cast.
  bool setLeafSelectivity(BaseTable& baseTable, const RowTypePtr& scanType) {
    return history_.setLeafSelectivity(baseTable, scanType);
  }

  void filterUpdated(BaseTableCP baseTable, bool updateSelectivity = true) {
    toVelox_.filterUpdated(baseTable, updateSelectivity);
  }

  auto& memo() {
    return memo_;
  }

  auto& existenceDts() {
    return existenceDts_;
  }

  /// Lists the possible joins based on 'state.placed' and adds each on top of
  /// 'plan'. This is a set of plans extending 'plan' by one join (single table
  /// or bush). Calls itself on the interesting next plans. If all tables have
  /// been used, adds postprocess and adds the plan to 'plans' in 'state'. If
  /// 'state' enables cutoff and a partial plan is worse than the best so far,
  /// discards the candidate.
  void makeJoins(RelationOpPtr plan, PlanState& state);

  void makeJoins(PlanState& state);

  const std::shared_ptr<core::QueryCtx>& veloxQueryCtx() const {
    return veloxQueryCtx_;
  }

  velox::core::ExpressionEvaluator* evaluator() const {
    return toGraph_.evaluator();
  }

  Name newCName(const std::string& prefix) {
    return toGraph_.newCName(prefix);
  }

  const OptimizerOptions& options() const {
    return options_;
  }

  const axiom::runner::MultiFragmentPlan::Options& runnerOptions() const {
    return runnerOptions_;
  }

  History& history() const {
    return history_;
  }

  /// Retain a reference to the provided TablePtr to ensure
  /// the Table is retained for the duration of Optimization, even if
  /// dropped by the corresponding ConnectorMetadata.
  void retainConnectorTable(connector::TablePtr table) {
    retainedTables_.insert(std::move(table));
  }

  /// If false, correlation names are not included in Column::toString(). Used
  /// for canonicalizing join cache keys.
  bool& cnamesInExpr() {
    return cnamesInExpr_;
  }

  bool cnamesInExpr() const {
    return cnamesInExpr_;
  }

  BuiltinNames& builtinNames() {
    return toGraph_.builtinNames();
  }

  /// Returns a dedupped left deep reduction with 'func' for the
  /// elements in set1 and set2. The elements are sorted on plan object
  /// id and then combined into a left deep reduction on 'func'.
  ExprCP
  combineLeftDeep(Name func, const ExprVector& set1, const ExprVector& set2);

  /// Produces trace output if event matches 'traceFlags_'.
  void trace(uint32_t event, int32_t id, const Cost& cost, RelationOp& plan)
      const;

 private:
  // Retrieves or makes a plan from 'key'. 'key' specifies a set of top level
  // joined tables or a hash join build side table or join.
  //
  // @param distribution the desired output distribution or a distribution with
  // no partitioning if this does not matter.
  // @param boundColumns a set of columns that are lookup keys for an index
  // based path through the joins in 'key'.
  // #param existsFanout the selectivity for the 'existences' in 'key', i.e.
  // extra reducing joins for a hash join build side, reflecting reducing joins
  // on the probe side. 1 if none.
  // @param state the state of the caller, empty for a top level call and the
  // state with the planned objects so far if planning a derived table.
  // @param needsShuffle set to true if a shuffle is needed to align the result
  // of the made plan with 'distribution'.
  PlanP makePlan(
      const MemoKey& key,
      const Distribution& distribution,
      const PlanObjectSet& boundColumns,
      float existsFanout,
      PlanState& state,
      bool& needsShuffle);

  PlanP makeUnionPlan(
      const MemoKey& key,
      const Distribution& distribution,
      const PlanObjectSet& boundColumns,
      float existsFanout,
      PlanState& state,
      bool& needsShuffle);

  PlanP makeDtPlan(
      const MemoKey& key,
      const Distribution& distribution,
      float existsFanout,
      PlanState& state,
      bool& needsShuffle);

  // Returns a sorted list of candidates to add to the plan in 'state'. The
  // joinable tables depend on the tables already present in 'plan'. A candidate
  // will be a single table for all the single tables that can be joined.
  // Additionally, when the single table can be joined to more tables not in
  // 'state' to form a reducing join, this is produced as a candidate for a
  // bushy hash join. When a single table or join to be used as a hash build
  // side is made, we further check if reducing joins applying to the probe can
  // be used to further reduce the build. These last joins are added as
  // 'existences' in the candidate.
  std::vector<JoinCandidate> nextJoins(PlanState& state);

  // Adds group by, order by, top k, limit to 'plan'. Updates 'plan' if
  // relation ops added. Sets cost in 'state'.
  void addPostprocess(DerivedTableCP dt, RelationOpPtr& plan, PlanState& state)
      const;

  // Places a derived table as first table in a plan. Imports possibly reducing
  // joins into the plan if can.
  void placeDerivedTable(DerivedTableCP from, PlanState& state);

  // Adds the items from 'dt.conjuncts' that are not placed in 'state'
  // and whose prerequisite columns are placed. If conjuncts can be
  // placed, adds them to 'state.placed' and calls makeJoins()
  // recursively to make the rest of the plan. Returns false if no
  // unplaced conjuncts were found and plan construction should proceed.
  bool placeConjuncts(
      RelationOpPtr plan,
      PlanState& state,
      bool allowNondeterministic);

  // Helper function that calls makeJoins recursively for each of
  // 'nextJoins'. The point of making 'nextJoins' first and only then
  // calling makeJoins is to allow detecting a star pattern of a fact
  // table and independently joined dimensions. These can be ordered
  // based on partitioning and size and we do not need to evaluate
  // their different permutations.
  void tryNextJoins(PlanState& state, const std::vector<NextJoin>& nextJoins);

  // Adds a cross join to access a single row from a non-correlated subquery.
  RelationOpPtr placeSingleRowDt(
      RelationOpPtr plan,
      DerivedTableCP subquery,
      ExprCP filter,
      PlanState& state);

  // Adds the join represented by 'candidate' on top of 'plan'. Tries index and
  // hash based methods and adds the index and hash based plans to 'result'. If
  // one of these is clearly superior, only adds the better one.
  void addJoin(
      const JoinCandidate& candidate,
      const RelationOpPtr& plan,
      PlanState& state,
      std::vector<NextJoin>& result);

  // If 'candidate' can be added on top 'plan' as a merge/index lookup, adds the
  // plan to 'toTry'. Adds any necessary repartitioning.
  void joinByIndex(
      const RelationOpPtr& plan,
      const JoinCandidate& candidate,
      PlanState& state,
      std::vector<NextJoin>& toTry);

  // Adds 'candidate' on top of 'plan' as a hash join. Adds possibly needed
  // repartitioning to both probe and build and makes a broadcast build if
  // indicated. If 'candidate' calls for a join on the build side, plans a
  // derived table with the build side tables and optionl 'existences' from
  // 'candidate'.
  void joinByHash(
      const RelationOpPtr& plan,
      const JoinCandidate& candidate,
      PlanState& state,
      std::vector<NextJoin>& toTry);

  // Tries a right hash join variant of left outer or left semijoin.
  void joinByHashRight(
      const RelationOpPtr& plan,
      const JoinCandidate& candidate,
      PlanState& state,
      std::vector<NextJoin>& toTry);

  void crossJoin(
      const RelationOpPtr& plan,
      const JoinCandidate& candidate,
      PlanState& state,
      std::vector<NextJoin>& toTry);

  const OptimizerOptions options_;

  const axiom::runner::MultiFragmentPlan::Options runnerOptions_;

  const bool isSingleWorker_;

  // Top level plan to optimize.
  const logical_plan::LogicalPlanNode* const logicalPlan_;

  // Source of historical cost/cardinality information.
  History& history_;

  std::shared_ptr<core::QueryCtx> veloxQueryCtx_;

  // Set of tables in use by the Optimizer.
  std::unordered_set<connector::TablePtr> retainedTables_;

  // Top DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP root_;

  std::unordered_map<MemoKey, PlanSet> memo_;

  // Set of previously planned dts for importing probe side reducing joins to a
  // build side
  std::unordered_map<MemoKey, DerivedTableP> existenceDts_;

  // The top level PlanState. Contains the set of top level interesting plans.
  // Must stay alive as long as the Plans and RelationOps are reeferenced.
  PlanState topState_{*this, nullptr};

  // Controls tracing.
  int32_t traceFlags_{0};

  // Generates unique ids for build sides.
  int32_t buildCounter_{0};

  bool cnamesInExpr_{true};

  ToGraph toGraph_;

  ToVelox toVelox_;
};

} // namespace facebook::velox::optimizer
