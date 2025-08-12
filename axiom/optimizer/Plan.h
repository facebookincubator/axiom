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
#include "velox/connectors/Connector.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/runner/MultiFragmentPlan.h"

/// Planning-time data structures. Represent the state of the planning process
/// plus utilities.
namespace facebook::velox::optimizer {

inline bool isSpecialForm(
    const logical_plan::Expr* expr,
    logical_plan::SpecialForm form) {
  return expr->isSpecialForm() &&
      expr->asUnchecked<logical_plan::SpecialFormExpr>()->form() == form;
}

/// Represents a path over an Expr of complex type. Used as a key
/// for a map from unique step+optionl subscript expr pairs to the
/// dedupped Expr that is the getter.
struct PathExpr {
  Step step;
  ExprCP subscriptExpr{nullptr};
  ExprCP base;

  bool operator==(const PathExpr& other) const {
    return step == other.step && subscriptExpr == other.subscriptExpr &&
        base == other.base;
  }
};

struct PathExprHasher {
  size_t operator()(const PathExpr& expr) const {
    size_t hash = bits::hashMix(expr.step.hash(), expr.base->id());
    return expr.subscriptExpr ? bits::hashMix(hash, expr.subscriptExpr->id())
                              : hash;
  }
};

struct ITypedExprHasher {
  size_t operator()(const velox::core::ITypedExpr* expr) const {
    return expr->hash();
  }
};

struct ITypedExprComparer {
  bool operator()(
      const velox::core::ITypedExpr* lhs,
      const velox::core::ITypedExpr* rhs) const {
    return *lhs == *rhs;
  }
};

// Map for deduplicating ITypedExpr trees.
using ExprDedupMap = folly::F14FastMap<
    const velox::core::ITypedExpr*,
    ExprCP,
    ITypedExprHasher,
    ITypedExprComparer>;

struct ExprDedupKey {
  Name func;
  const ExprVector* args;

  bool operator==(const ExprDedupKey& other) const {
    return func == other.func && *args == *other.args;
  }
};

struct ExprDedupHasher {
  size_t operator()(const ExprDedupKey& key) const {
    size_t h =
        folly::hasher<uintptr_t>()(reinterpret_cast<uintptr_t>(key.func));
    for (auto& a : *key.args) {
      h = bits::hashMix(h, folly::hasher<ExprCP>()(a));
    }
    return h;
  }
};

using FunctionDedupMap =
    std::unordered_map<ExprDedupKey, ExprCP, ExprDedupHasher>;

struct VariantPtrHasher {
  size_t operator()(const std::shared_ptr<const Variant>& value) const {
    return value->hash();
  }
};

struct VariantPtrComparer {
  bool operator()(
      const std::shared_ptr<const Variant>& left,
      const std::shared_ptr<const Variant>& right) const {
    return *left == *right;
  }
};

/// Set of accessed subfields given ordinal of output column or function
/// argument.
struct ResultAccess {
  // Key in 'resultPaths' to indicate the path is applied to the function
  // itself, not the ith argument.
  static constexpr int32_t kSelf = -1;
  std::map<int32_t, BitSet> resultPaths;
};

/// PlanNode output columns and function arguments with accessed subfields.
struct LogicalPlanSubfields {
  std::unordered_map<const logical_plan::LogicalPlanNode*, ResultAccess>
      nodeFields;
  std::unordered_map<const velox::logical_plan::Expr*, ResultAccess> argFields;

  bool hasColumn(const logical_plan::LogicalPlanNode* node, int32_t ordinal)
      const {
    auto it = nodeFields.find(node);
    if (it == nodeFields.end()) {
      return false;
    }
    return it->second.resultPaths.count(ordinal) != 0;
  }

  std::string toString() const;
};

/// Struct for resolving which logical PlanNode or Lambda defines which
/// field for column and subfield tracking.
struct LogicalContextSource {
  const logical_plan::LogicalPlanNode* planNode{nullptr};
  const logical_plan::CallExpr* call{nullptr};
  int32_t lambdaOrdinal{-1};
};

/// Utility for making a getter from a Step.
core::TypedExprPtr stepToGetter(Step, core::TypedExprPtr arg);

logical_plan::ExprPtr stepToLogicalPlanGetter(
    Step,
    const logical_plan::ExprPtr& arg);

/// Lists the subfield paths physically produced by a source. The
/// source can be a column or a complex type function. This is empty
/// if the whole object corresponding to the type of the column or
/// function is materialized. Suppose a type of map<int, float>. If
/// we have a function that adds 1 to every value in a map and we
/// only access [1] and [2] then the projection has [1] = 1 +
/// arg[1], [2] = 1 + arg[2]. If we have a column of the type and
/// only [1] and [2] are accessed, then we could have [1] = xx1, [2]
/// = xx2, where xx is the name of a top level column returned by
/// the scan.
struct SubfieldProjections {
  std::unordered_map<PathCP, ExprCP> pathToExpr;
};

struct Plan;
struct PlanState;

using PlanPtr = Plan*;

/// A set of build sides. a candidate plan tracks all builds so that they can be
/// reused
using HashBuildVector = std::vector<HashBuildCP>;

/// Item produced by optimization and kept in memo. Corresponds to
/// pre-costed physical plan with costs and data properties.
struct Plan {
  Plan(RelationOpPtr op, const PlanState& state);

  /// True if 'state' has a lower cost than 'this'. If 'perRowMargin' is given,
  /// then 'other' must win by margin per row.
  bool isStateBetter(const PlanState& state, float perRowMargin = 0) const;

  // Root of the plan tree.
  RelationOpPtr op;

  // Total cost of 'op'. Setup costs and memory sizes are added up. The unit
  // cost is the sum of the unit costs of the left-deep branch of 'op', where
  // each unit cost is multiplied by the product of the fanouts of its inputs.
  Cost cost;

  // The tables from original join graph that are included in this
  // plan. If this is a derived table in the original plan, the
  // covered object is the derived table, not its constituent
  // tables.
  PlanObjectSet tables;

  // The produced columns. Includes input columns.
  PlanObjectSet columns;

  // Columns that are fixed on input. Applies to index path for a derived
  // table, e.g. a left (t1 left t2) dt on dt.t1pk = a.fk. In a memo of dt
  // inputs is dt.pkt1.
  PlanObjectSet input;

  // hash join builds placed in the plan. Allows reusing a build.
  HashBuildVector builds;

  // the tables/derived tables that are contained in this plan and need not be
  // addressed by enclosing plans. This is all the tables in a build side join
  // but not necessarily all tables that were added to a group by derived table.
  PlanObjectSet fullyImported;

  std::string printCost() const;
  std::string toString(bool detail) const;
};

/// The set of plans produced for a set of tables and columns. The plans may
/// have different output orders and  distributions.
struct PlanSet {
  // Interesting equivalent plans.
  std::vector<std::unique_ptr<Plan>> plans;

  // Cost of lowest cost  plan plus shuffle. If a cutoff is applicable, nothing
  // more expensive than this should be tried.
  float bestCostWithShuffle{0};

  // Returns the best plan that produces 'distribution'. If the best plan has
  // some other distribution, sets 'needsShuffle ' to true.
  PlanPtr best(const Distribution& distribution, bool& needShuffle);

  /// Compares 'plan' to already seen plans and retains it if it is
  /// interesting, e.g. better than the best so far or has an interesting
  /// order. Returns the plan if retained, nullptr if not.
  PlanPtr addPlan(RelationOpPtr plan, PlanState& state);
};

// Represents the next table/derived table to join. May consist of several
// tables for a bushy build side.
struct JoinCandidate {
  JoinCandidate() = default;

  JoinCandidate(JoinEdgeP _join, PlanObjectCP _right, float _fanout)
      : join(_join), tables({_right}), fanout(_fanout) {}

  // Returns the join side info for 'table'. If 'other' is set, returns the
  // other side.
  JoinSide sideOf(PlanObjectCP side, bool other = false) const;

  /// Adds 'other' to the set of joins between the new table and already placed
  /// tables. a.k = b.k and c.k = b.k2 and c.k3 = a.k2. When placing c after a
  /// and b the edges to both a and b must be combined.
  void addEdge(PlanState& state, JoinEdgeP other);

  /// True if 'other' has all the equalities to placed columns that 'join' of
  /// 'this' has and has more equalities.
  bool isDominantEdge(PlanState& state, JoinEdgeP other);

  std::string toString() const;

  // The join between already placed tables and the table(s) in 'this'.
  JoinEdgeP join{nullptr};

  // Tables to join on the build side. The tables must not be already placed in
  // the plan.
  std::vector<PlanObjectCP> tables;

  // Joins imported from the left side for reducing a build
  // size. These could be ignored without affecting the result but can
  // be included to restrict the size of build, e.g. lineitem join
  // part left (partsupp exists part) would have the second part in
  // 'existences' and partsupp in 'tables' because we know that
  // partsupp will not be probed with keys that are not in part, so
  // there is no point building with these. This may involve tables already
  // placed in the plan.
  std::vector<PlanObjectSet> existences;

  // Number of right side hits for one row on the left. The join
  // selectivity in 'tables' affects this but the selectivity in
  // 'existences' does not.
  float fanout;

  // the selectivity from 'existences'. 0.2 means that the join of 'tables' is
  // reduced 5x.
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

  PlanState(Optimization& optimization, DerivedTableCP dt, PlanPtr plan)
      : optimization(optimization), dt(dt), cost(plan->cost) {}

  Optimization& optimization;

  // The derived table from which the tables are drawn.
  DerivedTableCP dt{nullptr};

  // The tables that have been placed so far.
  PlanObjectSet placed;

  // The columns that have a value from placed tables.
  PlanObjectSet columns;

  // The columns that need a value at the end of the plan. A dt can be
  // planned for just join/filter columns or all payload. Initially,
  // columns the selected columns of the dt depend on.
  PlanObjectSet targetColumns;

  // lookup keys for an index based derived table.
  PlanObjectSet input;

  // The total cost for the PlanObjects placed thus far.
  Cost cost;

  // All the hash join builds in any branch of the partial plan constructed so
  // far.
  HashBuildVector builds;

  // True if we should backtrack when 'costs' exceeds the best cost with shuffle
  // from already generated plans.
  bool hasCutoff{true};

  // Interesting completed plans for the dt being planned. For
  // example, best by cost and maybe plans with interesting orders.
  PlanSet plans;

  // Caches results of downstreamColumns(). This is a pure function of
  // 'placed' a'targetColumns' and 'dt'.
  mutable std::unordered_map<PlanObjectSet, PlanObjectSet>
      downstreamPrecomputed;

  // Ordered set of tables placed so far. Used for setting a
  // breakpoint before a specific join order gets costed.
  std::vector<int32_t> debugPlacedTables;

  /// Updates 'cost_' to reflect 'op' being placed on top of the partial plan.
  void addCost(RelationOp& op);

  /// Adds 'added' to all hash join builds.
  void addBuilds(const HashBuildVector& added);

  // Specifies that the plan to make only references 'target' columns and
  // whatever these depend on. These refer to 'columns' of 'dt'.
  void setTargetColumnsForDt(const PlanObjectSet& target);

  /// Returns the  set of columns referenced in unplaced joins/filters union
  /// targetColumns. Gets smaller as more tables are placed.
  const PlanObjectSet& downstreamColumns() const;

  // Adds a placed join to the set of partial queries to be developed. No op if
  // cost exceeds best so far and cutoff is enabled.
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

  void setFirstTable(int32_t id);
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

struct OptimizerOptions {
  /// Parallelizes independent projections over this many threads. 1 means no
  /// parallel projection.
  int32_t parallelProjectWidth = 1;

  /// Produces skyline subfield sets of complex type columns as top level
  /// columns in table scan.
  bool pushdownSubfields{false};

  /// Map from table name to  list of map columns to be read as structs unless
  /// the whole map is accessed as a map.
  std::unordered_map<std::string, std::vector<std::string>> mapAsStruct;

  bool sampleJoins{true};

  /// Produce trace of plan candidates.
  int32_t traceFlags{0};
};

/// A map from PlanNodeId of an executable plan to a key for
/// recording the execution for use in cost model. The key is a
/// canonical summary of the node and its inputs.
using NodeHistoryMap = std::unordered_map<core::PlanNodeId, std::string>;

using NodePredictionMap = std::unordered_map<core::PlanNodeId, NodePrediction>;

/// Plan and specification for recording execution history amd planning ttime
/// predictions.
struct PlanAndStats {
  runner::MultiFragmentPlanPtr plan;
  NodeHistoryMap history;
  NodePredictionMap prediction;
};

struct BuiltinNames {
  BuiltinNames();

  Name reverse(Name op) const;

  bool isCanonicalizable(Name name) const {
    return canonicalizable.find(name) != canonicalizable.end();
  }

  Name eq;
  Name lt;
  Name lte;
  Name gt;
  Name gte;
  Name plus;
  Name multiply;
  Name _and;
  Name _or;

  folly::F14FastSet<Name> canonicalizable;
};

/// Instance of query optimization. Converts a plan and schema into an
/// optimized plan. Depends on QueryGraphContext being set on the
/// calling thread. There is one instance per query to plan. The
/// instance must stay live as long as a returned plan is live.
class Optimization {
 public:
  static constexpr int32_t kRetained = 1;
  static constexpr int32_t kExceededBest = 2;
  static constexpr int32_t kSample = 4;

  Optimization(
      const logical_plan::LogicalPlanNode& plan,
      const Schema& schema,
      History& history,
      std::shared_ptr<core::QueryCtx> queryCtx,
      velox::core::ExpressionEvaluator& evaluator,
      OptimizerOptions opts = OptimizerOptions(),
      runner::MultiFragmentPlan::Options options =
          runner::MultiFragmentPlan::Options{.numWorkers = 5, .numDrivers = 5});

  Optimization(const Optimization& other) = delete;

  void operator==(Optimization& other) = delete;

  /// Returns the optimized RelationOp plan for 'plan' given at construction.
  PlanPtr bestPlan();

  /// Returns a set of per-stage Velox PlanNode trees. If 'historyKeys' is
  /// given, these can be used to record history data about the execution of
  /// each relevant node for costing future queries.
  PlanAndStats toVeloxPlan(
      RelationOpPtr plan,
      const velox::runner::MultiFragmentPlan::Options& options);

  void setLeafHandle(
      int32_t id,
      const connector::ConnectorTableHandlePtr& handle,
      const std::vector<core::TypedExprPtr>& extraFilters) {
    leafHandles_[id] = std::make_pair(handle, extraFilters);
  }

  std::pair<connector::ConnectorTableHandlePtr, std::vector<core::TypedExprPtr>>
  leafHandle(int32_t id) {
    auto it = leafHandles_.find(id);
    return it != leafHandles_.end()
        ? it->second
        : std::make_pair<
              std::shared_ptr<velox::connector::ConnectorTableHandle>,
              std::vector<core::TypedExprPtr>>(nullptr, {});
  }

  // Translates from Expr to Velox.
  velox::core::TypedExprPtr toTypedExpr(ExprCP expr);

  // Returns a new PlanNodeId.
  velox::core::PlanNodeId nextId();

  // Makes a getter path over a top level column and can convert the top map
  // getter into struct getter if maps extracted as structs.
  core::TypedExprPtr
  pathToGetter(ColumnCP column, PathCP path, core::TypedExprPtr source);

  // Produces a scan output type with only top level columns. Returns
  // these in scanColumns. The scan->columns() is the leaf columns,
  // not the top level ones if subfield pushdown.
  RowTypePtr scanOutputType(
      const TableScan& scan,
      ColumnVector& scanColumns,
      std::unordered_map<ColumnCP, TypePtr>& typeMap);

  RowTypePtr subfieldPushdownScanType(
      BaseTableCP baseTable,
      const ColumnVector& leafColumns,
      ColumnVector& topColumns,
      std::unordered_map<ColumnCP, TypePtr>& typeMap);

  // Makes projections for subfields as top level columns.
  core::PlanNodePtr makeSubfieldProjections(
      const TableScan& scan,
      const core::TableScanNodePtr& scanNode);

  /// Sets 'filterSelectivity' of 'baseTable' from history. Returns True if set.
  /// 'scanType' is the set of sampled columns with possible map to struct cast.
  bool setLeafSelectivity(BaseTable& baseTable, RowTypePtr scanType) {
    return history_.setLeafSelectivity(baseTable, std::move(scanType));
  }

  auto& memo() {
    return memo_;
  }

  auto& existenceDts() {
    return existenceDts_;
  }

  // Lists the possible joins based on 'state.placed' and adds each on top of
  // 'plan'. This is a set of plans extending 'plan' by one join (single table
  // or bush). Calls itself on the interesting next plans. If all tables have
  // been used, adds postprocess and adds the plan to 'plans' in 'state'. If
  // 'state' enables cutoff and a partial plan is worse than the best so far,
  // discards the candidate.
  void makeJoins(RelationOpPtr plan, PlanState& state);

  std::shared_ptr<core::QueryCtx> queryCtxShared() const {
    return queryCtx_;
  }

  velox::core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

  Name newCName(const std::string& prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  bool& makeVeloxExprWithNoAlias() {
    return makeVeloxExprWithNoAlias_;
  }

  bool& getterForPushdownSubfield() {
    return getterForPushdownSubfield_;
  }

  // Makes an output type for use in PlanNode et al. If 'columnType' is set,
  // only considers base relation columns of the given type.
  velox::RowTypePtr makeOutputType(const ColumnVector& columns);

  const OptimizerOptions& opts() const {
    return opts_;
  }

  std::unordered_map<ColumnCP, TypePtr>& columnAlteredTypes() {
    return columnAlteredTypes_;
  }

  /// True if a scan should expose 'column' of 'table' as a struct only
  /// containing the accessed keys. 'column' must be a top level map column.
  bool isMapAsStruct(Name table, Name column);

  History& history() const {
    return history_;
  }

  /// If false, correlation names are not included in Column::toString(),. Used
  /// for canonicalizing join cache keys.
  bool& cnamesInExpr() {
    return cnamesInExpr_;
  }

  /// Map for canonicalizing correlation names when making history cache keys.
  std::unordered_map<Name, Name>*& canonicalCnames() {
    return canonicalCnames_;
  }

  runner::MultiFragmentPlan::Options& options() {
    return options_;
  }

  BuiltinNames& builtinNames();

  // Returns a dedupped left deep reduction with 'func' for the
  // elements in set1 and set2. The elements are sorted on plan object
  // id and then combined into a left deep reduction on 'func'.
  ExprCP
  combineLeftDeep(Name func, const ExprVector& set1, const ExprVector& set2);

  /// Produces trace output if event matches 'traceFlags_'.
  void trace(int32_t event, int32_t id, const Cost& cost, RelationOp& plan);

 private:
  static constexpr uint64_t kAllAllowedInDt = ~0UL;

  // True if 'op' is in 'mask.
  static bool contains(uint64_t mask, PlanType op) {
    return 0 != (mask & (1UL << static_cast<int32_t>(op)));
  }

  // Returns a mask that allows 'op' in the same derived table.
  uint64_t allow(PlanType op) {
    return 1UL << static_cast<int32_t>(op);
  }

  // Removes 'op' from the set of operators allowed in the current derived
  // table. makeQueryGraph() starts a new derived table if it finds an operator
  // that does not belong to the mask.
  static uint64_t makeDtIf(uint64_t mask, PlanType op) {
    return mask & ~(1UL << static_cast<int32_t>(op));
  }

  // Initializes a tree of DerivedTables with JoinEdges from 'plan' given at
  // construction. Sets 'root_' to the root DerivedTable.
  DerivedTableP makeQueryGraph();

  DerivedTableP makeQueryGraphFromLogical();

  // Converts 'plan' to PlanObjects and records join edges into
  // 'currentSelect_'. If 'node' does not match  allowedInDt, wraps 'node' in a
  // new DerivedTable.
  PlanObjectP makeQueryGraph(
      const logical_plan::LogicalPlanNode& node,
      uint64_t allowedInDt);

  // Converts a table scan into a BaseTable wen building a DerivedTable.
  PlanObjectP makeBaseTable(const core::TableScanNode* tableScan);

  PlanObjectP makeBaseTable(const logical_plan::TableScanNode& tableScan);

  PlanObjectP makeValuesTable(const logical_plan::ValuesNode& values);

  // Decomposes complex type columns into parts projected out as top
  // level if subfield pushdown is on.
  void makeSubfieldColumns(
      BaseTable* baseTable,
      ColumnCP column,
      const BitSet& paths);

  // Interprets a Project node and adds its information into the DerivedTable
  // being assembled.
  void addProjection(const core::ProjectNode* project);

  PlanObjectP addProjection(const logical_plan::ProjectNode* project);

  // Interprets a Filter node and adds its information into the DerivedTable
  // being assembled.
  void addFilter(const core::FilterNode* Filter);

  PlanObjectP addFilter(const logical_plan::FilterNode* Filter);

  // Interprets an AggregationNode and adds its information to the DerivedTable
  // being assembled.
  PlanObjectP addAggregation(
      const core::AggregationNode& aggNode,
      uint64_t allowedInDt);

  PlanObjectP addAggregation(const logical_plan::AggregateNode& aggNode);

  PlanObjectP addLimit(const logical_plan::LimitNode& limitNode);

  PlanObjectP addOrderBy(const logical_plan::SortNode& order);

  // Sets the columns to project out from the root DerivedTable  based on
  // 'plan'.
  void setDerivedTableOutput(
      DerivedTableP dt,
      const velox::logical_plan::LogicalPlanNode& planNode);

  // Returns a literal from applying 'call' or 'cast' to 'literals'. nullptr if
  // not successful.
  ExprCP tryFoldConstant(
      const velox::core::CallTypedExpr* call,
      const velox::core::CastTypedExpr* cast,
      const ExprVector& literals);

  ExprCP tryFoldConstant(
      const logical_plan::CallExpr* call,
      const logical_plan::SpecialFormExpr* cast,
      const ExprVector& literals);

  // Folds a logical expr to a constant if can. Should be called only if 'expr'
  // only depends on constants. Identifier scope will may not be not set at time
  // of call. This is before regular constant folding because subscript
  // expressions must be folded for subfield resolution.
  logical_plan::ConstantExprPtr maybeFoldLogicalConstant(
      const logical_plan::ExprPtr expr);

  // Returns a constant expression if 'typedExprcan be folded, nullptr
  // otherwise.
  std::shared_ptr<const exec::ConstantExpr> foldConstant(
      const core::TypedExprPtr& typedExpr);

  // Returns the ordinal positions of actually referenced outputs of 'node'.
  std::vector<int32_t> usedChannels(const logical_plan::LogicalPlanNode* node);

  // Returns the ordinal position of used arguments for a function call that
  // produces a complex type.
  std::vector<int32_t> usedArgs(const core::ITypedExpr* call);

  std::vector<int32_t> usedArgs(const logical_plan::Expr* call);

  void markFieldAccessed(
      const LogicalContextSource& source,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources);

  void markSubfields(
      const logical_plan::Expr* expr,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources);

  void markSubfields(
      const logical_plan::ExprPtr& expr,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources) {
    markSubfields(expr.get(), steps, isControl, context, sources);
  }

  void markAllSubfields(
      const RowType* type,
      const logical_plan::LogicalPlanNode* node);

  void markControl(const logical_plan::LogicalPlanNode* node);

  void markColumnSubfields(
      const logical_plan::LogicalPlanNodePtr& source,
      const std::vector<logical_plan::ExprPtr>& columns);

  bool isSubfield(
      const core::ITypedExpr* expr,
      Step& step,
      core::TypedExprPtr& input);

  bool isSubfield(
      const logical_plan::Expr* expr,
      Step& step,
      logical_plan::ExprPtr& input);

  // if 'step' applied to result of the function of 'metadata'
  // corresponds to an argument, returns the ordinal of the argument/
  std::optional<int32_t> stepToArg(
      const Step& step,
      const FunctionMetadata* metadata);

  BitSet functionSubfields(
      const core::CallTypedExpr* call,
      bool controlOnly,
      bool payloadOnly);

  BitSet functionSubfields(
      const logical_plan::CallExpr* call,
      bool controlOnly,
      bool payloadOnly);

  // Makes a deduplicated Expr tree from 'expr'.
  ExprCP translateExpr(const velox::core::TypedExprPtr& expr);

  ExprCP translateExpr(const logical_plan::ExprPtr& expr);

  // For comparisons, swaps the args to have a canonical form for
  // deduplication. E.g column op constant, and Smaller plan object id
  // to the left.
  void canonicalizeCall(Name& name, ExprVector& args);

  // Creates or returns pre-existing function call with name+args. If
  // deterministic, a new ExprCP is remembered for reuse.
  ExprCP
  deduppedCall(Name name, Value value, ExprVector args, FunctionSet flags);

  // Returns a deduplicated Literal from the value in 'constant'.
  ExprCP makeConstant(const core::ConstantTypedExprPtr& constant);

  ExprCP makeConstant(const logical_plan::ConstantExpr& constant);

  ExprCP translateLambda(const velox::core::LambdaTypedExpr* lambda);

  ExprCP translateLambda(const logical_plan::LambdaExpr* lambda);

  // If 'expr' is not a subfield path, returns std::nullopt. If 'expr'
  // is a subfield path that is subsumed by a projected subfield,
  // returns nullptr. Else returns an optional subfield path on top of
  // the base of the subfield. Suppose column c is map<int,
  // map<int,array<int>>>. Suppose the only access is
  // c[1][1][0]. Suppose that the subfield projections are [1][1] =
  // xx. Then c[1] resolves to nullptr,c[1][1] to xx and c[1][1][1]
  // resolves to xx[1]. If no subfield projections, c[1][1] is c[1][1] etc.
  std::optional<ExprCP> translateSubfield(const logical_plan::ExprPtr& expr);

  void getExprForField(
      const logical_plan::Expr* expr,
      logical_plan::ExprPtr& resultExpr,
      ColumnCP& resultColumn,
      const logical_plan::LogicalPlanNode*& context);

  // Translates a complex type function where the generated Exprs  depend on the
  // accessed subfields.
  std::optional<ExprCP> translateSubfieldFunction(
      const core::CallTypedExpr* call,
      const FunctionMetadata* metadata);

  std::optional<ExprCP> translateSubfieldFunction(
      const logical_plan::CallExpr* call,
      const FunctionMetadata* metadata);

  // Calls translateSubfieldFunction() if not already called.
  void ensureFunctionSubfields(const core::TypedExprPtr& expr);

  void ensureFunctionSubfields(const logical_plan::ExprPtr& expr);

  // Makes dedupped getters for 'steps'. if steps is below skyline,
  // nullptr. If 'steps' intersects 'skyline' returns skyline wrapped
  // in getters that are not in skyline. If no skyline, puts dedupped
  // getters defined by 'steps' on 'base' or 'column' if 'base' is
  // nullptr.
  ExprCP makeGettersOverSkyline(
      const std::vector<Step>& steps,
      const SubfieldProjections* skyline,
      const core::TypedExprPtr& base,
      ColumnCP column);

  ExprCP makeGettersOverSkyline(
      const std::vector<Step>& steps,
      const SubfieldProjections* skyline,
      const logical_plan::ExprPtr& base,
      ColumnCP column);

  // Adds conjuncts combined by any number of enclosing ands from 'input' to
  // 'flat'.
  void translateConjuncts(
      const velox::core::TypedExprPtr& input,
      ExprVector& flat);

  void translateConjuncts(const logical_plan::ExprPtr& input, ExprVector& flat);

  // Converts 'name' to a deduplicated ExprCP. If 'name' is assigned to an
  // expression in a projection, returns the deduplicated ExprPtr of the
  // expression.
  ExprCP translateColumn(const std::string& name);

  //  Applies translateColumn to a 'source'.
  ExprVector translateColumns(
      const std::vector<velox::core::FieldAccessTypedExprPtr>& source);

  ExprVector translateColumns(const std::vector<logical_plan::ExprPtr>& source);

  // Adds a JoinEdge corresponding to 'join' to the enclosing DerivedTable.
  void translateJoin(const velox::core::AbstractJoinNode& join);

  void translateJoin(const logical_plan::JoinNode& join);

  DerivedTableP translateSetJoin(
      const logical_plan::SetNode& set,
      DerivedTableP setDt);

  // Updates the distribution and column stats of 'setDt', which must
  // be a union. 'innerDt' should be null on top level call. Adds up
  // the cardinality of union branches and their columns.
  void makeUnionDistributionAndStats(
      DerivedTableP setDt,
      DerivedTableP innerDt = nullptr);

  DerivedTableP translateUnion(
      const logical_plan::SetNode& set,
      DerivedTableP setDt,
      bool isTopLevel,
      bool& isLeftLeaf);

  // Makes an extra column for existence flag.
  ColumnCP makeMark(const velox::core::AbstractJoinNode& join);

  // Adds a join edge for a join with no equalities.
  void translateNonEqualityJoin(const velox::core::NestedLoopJoinNode& join);

  // Adds order by information to the enclosing DerivedTable.
  OrderByCP translateOrderBy(const velox::core::OrderByNode& order);

  // Adds aggregation information to the enclosing DerivedTable.
  AggregationP translateAggregation(
      const velox::core::AggregationNode& aggregation);

  AggregationP translateAggregation(
      const logical_plan::AggregateNode& aggregation);

  // Adds 'node' and descendants to query graph wrapped inside a
  // DerivedTable. Done for joins to the right of non-inner joins,
  // group bys as non-top operators, whenever descendents of 'node'
  // are not freely reorderable with its parents' descendents.
  PlanObjectP wrapInDt(const logical_plan::LogicalPlanNode& node);

  DerivedTableP newDt();

  /// Retrieves or makes a plan from 'key'. 'key' specifies a set of
  /// top level joined tables or a hash join build side table or
  /// join. 'distribution' is the desired output distribution or a
  /// distribution with no partitioning if this does
  /// matter. 'boundColumns' is a set of columns that are lookup keys
  /// for an index based path through the joins in
  /// 'key'. 'existsFanout' is the selectivity for the 'existences' in
  /// 'key', i.e. extra reducing joins for a hash join build side,
  /// reflecting reducing joins on the probe side, 1 if none. 'state'
  /// is the state of the caller, empty for a top level call and the
  /// state with the planned objects so far if planning a derived
  /// table. 'needsShuffle' is set to true if a shuffle is needed to
  /// align the result of the made plan with 'distribution'.
  PlanPtr makePlan(
      const MemoKey& key,
      const Distribution& distribution,
      const PlanObjectSet& boundColumns,
      float existsFanout,
      PlanState& state,
      bool& needsShuffle);

  // Non-union case of makePlan().
  PlanPtr makeDtPlan(
      const MemoKey& key,
      const Distribution& distribution,
      const PlanObjectSet& boundColumns,
      float existsFanout,
      PlanState& state,
      bool& needsShuffle);

  // Returns a sorted list of candidates to add to the plan in
  // 'state'. The joinable tables depend on the tables already present
  // in 'plan'. A candidate will be a single table for all the single
  // tables that can be joined. Additionally, when the single table
  // can be joined to more tables not in 'state' to form a reducing
  // join, this is produced as a candidate for a bushy hash join. When
  // a single table or join to be used as a hash build side is made,
  // we further check if reducing joins applying to the probe can be
  // used to furthr reduce the build. These last joins are added as
  // 'existences' in the candidate.
  std::vector<JoinCandidate> nextJoins(PlanState& state);

  // Adds group by, order by, top k to 'plan'. Updates 'plan' if
  // relation ops added.  Sets cost in 'state'.
  void addPostprocess(DerivedTableCP dt, RelationOpPtr& plan, PlanState& state);

  // Places a derived table as first table in a plan. Imports possibly reducing
  // joins into the plan if can.
  void placeDerivedTable(DerivedTableCP from, PlanState& state);

  // Adds the items from 'dt.conjuncts' that are not placed in 'state'
  // and whose prerequisite columns are placed. If conjuncts can be
  // placed, adds them to 'state.placed' and calls makeJoins()
  // recursively to make the rest of the plan. Returns false if no
  // unplaced conjuncts were found and and plan construction should
  // proceed.
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

  // Adds the join represented by'candidate' on top of 'plan'. Tries index and
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

  // Adds 'candidate' on top of 'plan as a hash join. Adds possibly needed
  // repartitioning to both probe and build and makes a broadcast build if
  // indicated. If 'candidate' calls for a join on the build ide, plans a
  // derived table with the build side tables and optionl 'existences' from
  // 'candidate'.
  void joinByHash(
      const RelationOpPtr& plan,
      const JoinCandidate& candidate,
      PlanState& state,
      std::vector<NextJoin>& toTry);

  /// Tries a right hash join variant of left outer or left semijoin.
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
      Aggregation& agg,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  // Makes partial + final order by fragments for order by with and without
  // limit.
  velox::core::PlanNodePtr makeOrderBy(
      const OrderBy& op,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  // Makes partial + final limit fragments.
  velox::core::PlanNodePtr makeLimit(
      const Limit& op,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  // @pre op.sNoLimit() is true.
  velox::core::PlanNodePtr makeOffset(
      const Limit& op,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeScan(
      const TableScan& scan,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeFilter(
      const Filter& filter,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeProject(
      const Project& project,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeJoin(
      const Join& join,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  velox::core::PlanNodePtr makeRepartition(
      const Repartition& repartition,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages,
      std::shared_ptr<core::ExchangeNode>& exchange);

  // Makes a union all with a mix of remote and local inputs. Combines all
  // remote inputs into one ExchangeNode.
  velox::core::PlanNodePtr makeUnionAll(
      const UnionAll& unionAll,
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  core::PlanNodePtr makeValues(
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
      velox::runner::ExecutableFragment& fragment,
      std::vector<velox::runner::ExecutableFragment>& stages);

  // Records the prediction for 'node' and a history key to update history after
  // the plan is executed.
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

  runner::ExecutableFragment newFragment();

  PlanObjectCP findLeaf(const logical_plan::LogicalPlanNode* node) {
    auto* leaf = logicalPlanLeaves_[node];
    VELOX_CHECK_NOT_NULL(leaf);
    return leaf;
  }

  const Schema& schema_;

  OptimizerOptions opts_;

  // Top level plan to optimize.
  const logical_plan::LogicalPlanNode* logicalPlan_{nullptr};

  // Source of historical cost/cardinality information.
  History& history_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  velox::core::ExpressionEvaluator& evaluator_;
  // Top DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP root_;

  // Innermost DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP currentSelect_;

  // Source PlanNode when inside addProjection() or 'addFilter().
  const logical_plan::LogicalPlanNode* logicalExprSource_{nullptr};

  // Maps names in project noes of 'inputPlan_' to deduplicated Exprs.
  std::unordered_map<std::string, ExprCP> renames_;

  // Holds transient ConstantTypedExprs etc. Must be after 'exprDedup_' in
  // destruction order.
  std::vector<core::TypedExprPtr> tempExprs_;

  // Maps unique core::TypedExprs from 'inputPlan_' to deduplicated Exps. Use
  // for leaves, e.g. constants.
  ExprDedupMap exprDedup_;

  std::unordered_map<
      std::shared_ptr<const Variant>,
      ExprCP,
      VariantPtrHasher,
      VariantPtrComparer>
      constantDedup_;

  // Reverse map from dedupped literal to the shared_ptr. We put the
  // shared ptr back into the result plan so the variant never gets
  // copied.
  std::map<ExprCP, std::shared_ptr<const Variant>> reverseConstantDedup_;

  // Dedup map fr om name+ExprVector to corresponding Call Expr.
  FunctionDedupMap functionDedup_;

  // Counter for generating unique correlation names for BaseTables and
  // DerivedTables.
  int32_t nameCounter_{0};

  // Serial number for columns created for projections that name Exprs, e.g. in
  // join or grouping keys.
  int32_t resultNameCounter_{0};

  // Serial number for stages in executable plan.
  int32_t stageCounter_{0};

  std::unordered_map<MemoKey, PlanSet> memo_;

  // Set of previously planned dts for importing probe side reducing joins to a
  // build side
  std::unordered_map<MemoKey, DerivedTableP> existenceDts_;

  // The top level PlanState. Contains the set of top level interesting plans.
  // Must stay alive as long as the Plans and RelationOps are reeferenced.
  PlanState topState_{*this, nullptr};

  // Column and subfield access info for filters, joins, grouping and other
  // things affecting result row selection.
  LogicalPlanSubfields logicalControlSubfields_;

  // Column and subfield info for items that only affect column values.
  LogicalPlanSubfields logicalPayloadSubfields_;

  /// Expressions corresponding to skyline paths over a subfield decomposable
  /// function.
  std::unordered_map<const core::ITypedExpr*, SubfieldProjections>
      functionSubfields_;

  std::unordered_map<const logical_plan::CallExpr*, SubfieldProjections>
      logicalFunctionSubfields_;

  // Every unique path step, expr pair. For paths c.f1.f2 and c.f1.f3 there are
  // 3 entries: c.f1 and c.f1.f2 and c1.f1.f3, where the two last share the same
  // c.f1.
  std::unordered_map<PathExpr, ExprCP, PathExprHasher> deduppedGetters_;

  // Complex type functions that have been checke for explode and
  // 'functionSubfields_'.
  std::unordered_set<const core::CallTypedExpr*> translatedSubfieldFuncs_;

  std::unordered_set<const logical_plan::CallExpr*>
      logicalTranslatedSubfieldFuncs_;

  /// If subfield extraction is pushed down, then these give the skyline
  /// subfields for a column for control and payload situations. The same column
  /// may have different skylines in either. For example if the column is
  /// struct<a int, b int> and only c.a is accessed, there may be no
  /// representation for c, but only for c.a. In this case the skyline is .a =
  /// xx where xx is a synthetic leaf column name for c.a.
  std::unordered_map<ColumnCP, SubfieldProjections> allColumnSubfields_;
  std::unordered_map<ColumnCP, SubfieldProjections> controlColumnSubfields_;
  std::unordered_map<ColumnCP, SubfieldProjections> payloadColumnSubfields_;

  // Controls tracing.
  int32_t traceFlags_{0};

  // Generates unique ids for build sides.
  int32_t buildCounter_{0};

  // When making a graph from 'inputPlan_' the output of an aggregation comes
  // from the topmost (final) and the input from the lefmost (whichever consumes
  // raw values). Records the output type of the final aggregation.
  velox::RowTypePtr aggFinalType_;

  // Map from leaf PlanNode to corresponding PlanObject
  std::unordered_map<const logical_plan::LogicalPlanNode*, PlanObjectCP>
      logicalPlanLeaves_;

  // Map from plan object id to pair of handle with pushdown filters and list of
  // filters to eval on the result from the handle.
  std::unordered_map<
      int32_t,
      std::pair<
          connector::ConnectorTableHandlePtr,
          std::vector<core::TypedExprPtr>>>
      leafHandles_;

  class PlanNodeIdGenerator {
   public:
    explicit PlanNodeIdGenerator(int startId = 0) : nextId_{startId} {}

    core::PlanNodeId next() {
      return fmt::format("{}", nextId_++);
    }

    void reset(int startId = 0) {
      nextId_ = startId;
    }

   private:
    int nextId_;
  };

  velox::runner::MultiFragmentPlan::Options options_;

  // TODO Move this into MultiFragmentPlan::Options.
  const VectorSerde::Kind exchangeSerdeKind_{VectorSerde::Kind::kPresto};

  PlanNodeIdGenerator idGenerator_;

  // Limit for a possible limit/top k order by for while making a Velox plan. -1
  // means no limit.
  int32_t toVeloxLimit_{-1};
  int32_t toVeloxOffset_{0};

  // On when producing a remaining filter for table scan, where columns must
  // correspond 1:1 to the schema.
  bool makeVeloxExprWithNoAlias_{false};

  bool getterForPushdownSubfield_{false};

  // Map from top level map column  accessed as struct to the struct type. Used
  // only when generating a leaf scan for result Velox plan.
  std::unordered_map<ColumnCP, TypePtr> columnAlteredTypes_;

  // When generating parallel projections with intermediate assignment for
  // common subexpressions, maps from ExprCP to the FieldAccessTypedExppr with
  // the value.
  std::unordered_map<ExprCP, core::TypedExprPtr> projectedExprs_;

  // Map filled in with a PlanNodeId and history key for measurement points for
  // history recording.
  NodeHistoryMap nodeHistory_;

  // Predicted cardinality and memory for nodes to record in history.
  NodePredictionMap prediction_;

  bool cnamesInExpr_{true};

  std::unordered_map<Name, Name>* canonicalCnames_{nullptr};

  const bool isSingle_;

  // True if wrapping a nondeterministic filter inside a DT in ToGraph.
  bool isNondeterministicWrap_{false};

  std::unique_ptr<BuiltinNames> builtinNames_;
};

const JoinEdgeVector& joinedBy(PlanObjectCP table);

void filterUpdated(BaseTableCP baseTable, bool updateSelectivity = true);

/// Returns a struct with fields for skyline map keys of 'column' in
/// 'baseTable'. This is the type to return from the table reader
/// for the map column.
RowTypePtr skylineStruct(BaseTableCP baseTable, ColumnCP column);

/// Returns  the inverse join type, e.g. right outer from left outer.
/// TODO Move this function to Velox.
core::JoinType reverseJoinType(core::JoinType joinType);

PathCP stepsToPath(const std::vector<Step>& steps);

Variant* subscriptLiteral(TypeKind kind, const Step& step);

} // namespace facebook::velox::optimizer
