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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/RelationOp.h"
#include "axiom/optimizer/ToGraph.h"
#include "axiom/optimizer/ToVelox.h"
#include "axiom/runner/MultiFragmentPlan.h"
#include "velox/connectors/Connector.h"

/// Planning-time data structures. Represent the state of the planning process
/// plus utilities.
namespace facebook::velox::optimizer {

struct Plan;
struct PlanState;

using PlanP = Plan*;

/// A set of build sides. A candidate plan tracks all builds so that they can be
/// reused.
using HashBuildVector = std::vector<HashBuildCP>;

/// Item produced by optimization and kept in memo. Corresponds to
/// pre-costed physical plan with costs and data properties.
struct Plan {
  Plan(RelationOpPtr op, const PlanState& state);

  /// True if 'state' has a lower cost than 'this'. If 'perRowMargin' is given,
  /// then 'other' must win by margin per row.
  bool isStateBetter(const PlanState& state, float perRowMargin = 0) const;

  /// Root of the plan tree.
  const RelationOpPtr op;

  /// Total cost of 'op'. Setup costs and memory sizes are added up. The unit
  /// cost is the sum of the unit costs of the left-deep branch of 'op', where
  /// each unit cost is multiplied by the product of the fanouts of its inputs.
  const Cost cost;

  /// The tables from original join graph that are included in this
  /// plan. If this is a derived table in the original plan, the
  /// covered object is the derived table, not its constituent tables.
  const PlanObjectSet tables;

  /// The produced columns. Includes input columns.
  const PlanObjectSet columns;

  /// Columns that are fixed on input. Applies to index path for a derived
  /// table, e.g. a left (t1 left t2) dt on dt.t1pk = a.fk. In a memo of dt
  /// inputs is dt.pkt1.
  PlanObjectSet input;

  /// Hash join builds placed in the plan. Allows reusing a build.
  HashBuildVector builds;

  /// The tables/derived tables that are contained in this plan and need not be
  /// addressed by enclosing plans. This is all the tables in a build side join
  /// but not necessarily all tables that were added to a group by derived
  /// table.
  PlanObjectSet fullyImported;

  std::string printCost() const;

  std::string toString(bool detail) const;
};

/// The set of plans produced for a set of tables and columns. The plans may
/// have different output orders and distributions.
struct PlanSet {
  /// Interesting equivalent plans.
  std::vector<std::unique_ptr<Plan>> plans;

  /// Cost of lowest-cost plan plus shuffle. If a cutoff is applicable, nothing
  /// more expensive than this should be tried.
  float bestCostWithShuffle{0};

  /// Returns the best plan that produces 'distribution'. If the best plan has
  /// some other distribution, sets 'needsShuffle ' to true.
  PlanP best(const Distribution& distribution, bool& needShuffle);

  /// Compares 'plan' to already seen plans and retains it if it is
  /// interesting, e.g. better than the best so far or has an interesting
  /// order. Returns the plan if retained, nullptr if not.
  PlanP addPlan(RelationOpPtr plan, PlanState& state);
};

/// Represents the next table/derived table to join. May consist of several
/// tables for a bushy build side.
struct JoinCandidate {
  JoinCandidate() = default;

  JoinCandidate(JoinEdgeP _join, PlanObjectCP _right, float _fanout)
      : join(_join), tables({_right}), fanout(_fanout) {}

  /// Returns the join side info for 'table'. If 'other' is set, returns the
  /// other side.
  JoinSide sideOf(PlanObjectCP side, bool other = false) const;

  /// Adds 'other' to the set of joins between the new table and already placed
  /// tables. a.k = b.k and c.k = b.k2 and c.k3 = a.k2. When placing c after a
  /// and b the edges to both a and b must be combined.
  void addEdge(PlanState& state, JoinEdgeP other);

  /// True if 'other' has all the equalities to placed columns that 'join' of
  /// 'this' has and has more equalities.
  bool isDominantEdge(PlanState& state, JoinEdgeP other);

  std::string toString() const;

  /// The join between already placed tables and the table(s) in 'this'.
  JoinEdgeP join{nullptr};

  /// Tables to join on the build side. The tables must not be already placed in
  /// the plan.
  std::vector<PlanObjectCP> tables;

  /// Joins imported from the left side for reducing a build
  /// size. These could be ignored without affecting the result but can
  /// be included to restrict the size of build, e.g. lineitem join
  /// part left (partsupp exists part) would have the second part in
  /// 'existences' and partsupp in 'tables' because we know that
  /// partsupp will not be probed with keys that are not in part, so
  /// there is no point building with these. This may involve tables already
  /// placed in the plan.
  std::vector<PlanObjectSet> existences;

  /// Number of right side hits for one row on the left. The join
  /// selectivity in 'tables' affects this but the selectivity in
  /// 'existences' does not.
  float fanout;

  /// The selectivity from 'existences'. 0.2 means that the join of 'tables' is
  /// reduced 5x.
  float existsFanout{1};

  JoinEdgeP compositeEdge{nullptr};
};

/// Represents a join to add to a partial plan. One join candidate can make
/// many NextJoins, e.g, for different join methods. If one is clearly best,
/// not all need be tried. If many NextJoins are disconnected (no JoinEdge
/// between them), these may be statically orderable without going through
/// permutations.
struct NextJoin {
  NextJoin(
      const JoinCandidate* candidate,
      const RelationOpPtr& plan,
      const Cost& cost,
      const PlanObjectSet& placed,
      const PlanObjectSet& columns,
      const HashBuildVector& builds)
      : candidate(candidate),
        plan(plan),
        cost(cost),
        placed(placed),
        columns(columns),
        newBuilds(builds) {}

  const JoinCandidate* candidate;
  RelationOpPtr plan;
  Cost cost;
  PlanObjectSet placed;
  PlanObjectSet columns;
  HashBuildVector newBuilds;

  /// If true, only 'other' should be tried. Use to compare equivalent joins
  /// with different join method or partitioning.
  bool isWorse(const NextJoin& other) const;
};

class Optimization;

/// Tracks the set of tables / columns that have been placed or are still needed
/// when constructing a partial plan.
struct PlanState {
  PlanState(Optimization& optimization, DerivedTableCP dt)
      : optimization(optimization), dt(dt) {}

  PlanState(Optimization& optimization, DerivedTableCP dt, PlanP plan)
      : optimization(optimization), dt(dt), cost(plan->cost) {}

  Optimization& optimization;

  /// The derived table from which the tables are drawn.
  DerivedTableCP dt;

  /// The tables that have been placed so far.
  PlanObjectSet placed;

  /// The columns that have a value from placed tables.
  PlanObjectSet columns;

  /// The columns that need a value at the end of the plan. A dt can be
  /// planned for just join/filter columns or all payload. Initially,
  /// columns the selected columns of the dt depend on.
  PlanObjectSet targetColumns;

  /// lookup keys for an index based derived table.
  PlanObjectSet input;

  /// The total cost for the PlanObjects placed thus far.
  Cost cost;

  /// All the hash join builds in any branch of the partial plan constructed so
  /// far.
  HashBuildVector builds;

  /// True if we should backtrack when 'cost' exceeds the best cost with
  /// shuffle from already generated plans.
  bool hasCutoff{true};

  /// Interesting completed plans for the dt being planned. For
  /// example, best by cost and maybe plans with interesting orders.
  PlanSet plans;

  /// Caches results of downstreamColumns(). This is a pure function of
  /// 'placed', 'targetColumns' and 'dt'.
  mutable std::unordered_map<PlanObjectSet, PlanObjectSet>
      downstreamPrecomputed;

  /// Ordered set of tables placed so far. Used for setting a
  /// breakpoint before a specific join order gets costed.
  std::vector<int32_t> debugPlacedTables;

  /// Updates 'cost' to reflect 'op' being placed on top of the partial plan.
  void addCost(RelationOp& op);

  /// Adds 'added' to all hash join builds.
  void addBuilds(const HashBuildVector& added);

  /// Specifies that the plan-to-make only references 'target' columns and
  /// whatever these depend on. These refer to 'columns' of 'dt'.
  void setTargetColumnsForDt(const PlanObjectSet& target);

  /// Returns the set of columns referenced in unplaced joins/filters union
  /// targetColumns. Gets smaller as more tables are placed.
  const PlanObjectSet& downstreamColumns() const;

  /// Adds a placed join to the set of partial queries to be developed. No-op if
  /// cost exceeds best so far and cutoff is enabled.
  void addNextJoin(
      const JoinCandidate* candidate,
      RelationOpPtr plan,
      HashBuildVector builds,
      std::vector<NextJoin>& toTry) const;

  std::string printCost() const;

  /// Makes a string of 'op' with 'details'. Costs are annotated with percentage
  /// of total in 'this->cost'.
  std::string printPlan(RelationOpPtr op, bool detail) const;

  /// True if the costs accumulated so far are so high that this should not be
  /// explored further.
  bool isOverBest() const {
    return hasCutoff && plans.bestCostWithShuffle != 0 &&
        cost.unitCost + cost.setupCost > plans.bestCostWithShuffle;
  }

  void debugSetFirstTable(int32_t id);
};

/// A scoped guard that restores fields of PlanState on destruction.
struct PlanStateSaver {
 public:
  explicit PlanStateSaver(PlanState& state)
      : state_(state),
        placed_(state.placed),
        columns_(state.columns),
        cost_(state.cost),
        numBuilds_(state.builds.size()),
        numPlaced_(state.debugPlacedTables.size()) {}

  PlanStateSaver(PlanState& state, const JoinCandidate& candidate);

  ~PlanStateSaver() {
    state_.placed = std::move(placed_);
    state_.columns = std::move(columns_);
    state_.cost = cost_;
    state_.builds.resize(numBuilds_);
    state_.debugPlacedTables.resize(numPlaced_);
  }

 private:
  PlanState& state_;
  PlanObjectSet placed_;
  PlanObjectSet columns_;
  const Cost cost_;
  const int32_t numBuilds_;
  const int32_t numPlaced_;
};

/// Key for collection of memoized partial plans. Any table or derived
/// table with a particular set of projected out columns and an
/// optional set of reducing joins and semijoins (existences) is
/// planned once. The plan is then kept in a memo for future use. The
/// memo may hold multiple plans with different distribution
/// properties for one MemoKey. The first table is the table or
/// derived table to be planned. The 'tables' set is the set of
/// reducing joins applied to 'firstTable', including the table
/// itself. 'existences' is another set of reducing joins that are
/// semijoined to the join of 'tables' in order to restrict the
/// result. For example, if a reducing join is moved below a group by,
/// unless it is known never to have duplicates, it must become a
/// semijoin and the original join must still stay in place in case
/// there were duplicates.
struct MemoKey {
  bool operator==(const MemoKey& other) const;

  size_t hash() const;

  PlanObjectCP firstTable;
  PlanObjectSet columns;
  PlanObjectSet tables;
  std::vector<PlanObjectSet> existences;
};

} // namespace facebook::velox::optimizer

namespace std {
template <>
struct hash<::facebook::velox::optimizer::MemoKey> {
  size_t operator()(const ::facebook::velox::optimizer::MemoKey& key) const {
    return key.hash();
  }
};
} // namespace std

namespace facebook::velox::optimizer {

/// Instance of query optimization. Converts a plan and schema into an
/// optimized plan. Depends on QueryGraphContext being set on the
/// calling thread. There is one instance per query to plan. The
/// instance must stay live as long as a returned plan is live.
class Optimization {
 public:
  Optimization(
      const logical_plan::LogicalPlanNode& plan,
      const Schema& schema,
      History& history,
      std::shared_ptr<core::QueryCtx> queryCtx,
      velox::core::ExpressionEvaluator& evaluator,
      OptimizerOptions options = OptimizerOptions(),
      axiom::runner::MultiFragmentPlan::Options runnerOptions =
          axiom::runner::MultiFragmentPlan::Options{
              .numWorkers = 5,
              .numDrivers = 5});

  Optimization(const Optimization& other) = delete;

  void operator==(Optimization& other) = delete;

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
  bool setLeafSelectivity(BaseTable& baseTable, RowTypePtr scanType) {
    return history_.setLeafSelectivity(baseTable, std::move(scanType));
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

  const std::shared_ptr<core::QueryCtx>& queryCtxShared() const {
    return queryCtx_;
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
  void trace(int32_t event, int32_t id, const Cost& cost, RelationOp& plan);

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
  void addPostprocess(DerivedTableCP dt, RelationOpPtr& plan, PlanState& state);

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

  std::shared_ptr<core::QueryCtx> queryCtx_;

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

const JoinEdgeVector& joinedBy(PlanObjectCP table);

/// Returns  the inverse join type, e.g. right outer from left outer.
/// TODO Move this function to Velox.
core::JoinType reverseJoinType(core::JoinType joinType);

} // namespace facebook::velox::optimizer
