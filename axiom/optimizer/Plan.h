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

#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/RelationOp.h"

/// Planning-time data structures. Represent the state of the planning process
/// plus utilities.
namespace facebook::axiom::optimizer {

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
  PlanP best(const Distribution& distribution, bool& needsShuffle);

  /// Retruns the best plan when we're ok with any distribution.
  PlanP best() {
    bool ignore = false;
    return best(Distribution{}, ignore);
  }

  /// Compares 'plan' to already seen plans and retains it if it is
  /// interesting, e.g. better than the best so far or has an interesting
  /// order. Returns the plan if retained, nullptr if not.
  PlanP addPlan(RelationOpPtr plan, PlanState& state);
};

/// Represents the next table/derived table to join. May consist of several
/// tables for a bushy build side.
struct JoinCandidate {
  JoinCandidate(JoinEdgeP join, PlanObjectCP right, float fanout)
      : join(join), tables({right}), fanout(fanout) {}

  /// Returns two join sides. First is the side that contains 'tables' (build).
  /// Second is the other side.
  std::pair<JoinSide, JoinSide> joinSides() const;

  /// Adds 'edge' to the set of joins between the new table and already placed
  /// tables. a.k = b.k and c.k = b.k2 and c.k3 = a.k2. When placing c after a
  /// and b the edges to both a and b must be combined.
  void addEdge(PlanState& state, JoinEdgeP edge);

  /// True if 'edge' has all the equalities to placed columns that 'join' of
  /// 'this' has and has more equalities.
  bool isDominantEdge(PlanState& state, JoinEdgeP edge);

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
      RelationOpPtr plan,
      const Cost& cost,
      PlanObjectSet placed,
      PlanObjectSet columns,
      HashBuildVector builds)
      : candidate{candidate},
        plan{std::move(plan)},
        cost{cost},
        placed{std::move(placed)},
        columns{std::move(columns)},
        newBuilds{std::move(builds)} {}

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
  PlanState(Optimization& optimization, DerivedTableCP dt);

  PlanState(Optimization& optimization, DerivedTableCP dt, PlanP plan);

  Optimization& optimization;

  /// The derived table from which the tables are drawn.
  DerivedTableCP dt;

  /// The tables that have been placed so far.
  PlanObjectSet placed;

  /// The columns that have a value from placed tables.
  PlanObjectSet columns;

  /// The expressions that need a value at the end of the plan. A dt can be
  /// planned for just join/filter columns or all payload. Initially, the
  /// selected expressions of the dt.
  PlanObjectSet targetExprs;

  /// A mapping of expressions to pre-computed columns. See
  /// PrecomputeProjection.
  folly::F14FastMap<ExprCP, ExprCP> exprToColumn;

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

  /// Ordered set of tables placed so far. Used for setting a
  /// breakpoint before a specific join order gets costed.
  std::vector<int32_t> debugPlacedTables;

  /// Updates 'cost' to reflect 'op' being placed on top of the partial plan.
  void addCost(RelationOp& op);

  /// Adds 'added' to all hash join builds.
  void addBuilds(const HashBuildVector& added);

  /// Specifies that the plan-to-make only produces 'target' expressions and.
  /// These refer to 'exprs' of 'dt'.
  void setTargetExprsForDt(const PlanObjectSet& target);

  /// Returns the set of columns referenced in unplaced joins/filters union
  /// targetColumns. Gets smaller as more tables are placed.
  const PlanObjectSet& downstreamColumns() const;

  /// Replace expression with pre-computed column using 'exprToColumn'
  /// mapping.
  ExprCP toColumn(ExprCP expr) const;

  /// If OptimizerOptions::syntacticJoinOrder is true, returns true if all
  /// tables that must be placed before 'table' have been placed. If
  /// OptimizerOptions::syntacticJoinOrder is false, returns true
  /// unconditionally.
  bool mayConsiderNext(PlanObjectCP table) const;

  /// Adds a placed join to the set of partial queries to be developed.
  /// No-op if cost exceeds best so far and cutoff is enabled.
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

 private:
  /// Caches results of downstreamColumns(). This is a pure function of
  /// 'placed', 'targetExprs' and 'dt'.
  mutable folly::F14FastMap<PlanObjectSet, PlanObjectSet>
      downstreamColumnsCache_;

  const bool syntacticJoinOrder_;
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
  const uint32_t numBuilds_;
  const uint32_t numPlaced_;
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

} // namespace facebook::axiom::optimizer

namespace std {
template <>
struct hash<::facebook::axiom::optimizer::MemoKey> {
  size_t operator()(const ::facebook::axiom::optimizer::MemoKey& key) const {
    return key.hash();
  }
};
} // namespace std

namespace facebook::axiom::optimizer {

const JoinEdgeVector& joinedBy(PlanObjectCP table);

/// Returns  the inverse join type, e.g. right outer from left outer.
/// TODO Move this function to Velox.
velox::core::JoinType reverseJoinType(velox::core::JoinType joinType);

} // namespace facebook::axiom::optimizer
