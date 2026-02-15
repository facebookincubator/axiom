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
struct PlanStateSaver;

using PlanP = Plan*;

/// A set of build sides. A candidate plan tracks all builds so that they can be
/// reused.
using HashBuildVector = std::vector<HashBuildCP>;

/// Item produced by optimization and kept in memo. Corresponds to
/// pre-costed physical plan with costs and data properties.
struct Plan {
  Plan(RelationOpPtr op, const PlanState& state);

  /// True if 'state' has a lower cost than 'this'. If 'margin' is given,
  /// then 'other' must win by margin.
  bool isStateBetter(const PlanState& state, float margin = 0) const;

  /// Root of the plan tree.
  const RelationOpPtr op;

  /// Total cost of 'op'. Setup costs and memory sizes are added up. The unit
  /// cost is the sum of the unit costs of the left-deep branch of 'op', where
  /// each unit cost is multiplied by the product of the fanouts of its inputs.
  const PlanCost cost;

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

  /// Derived constraints on output columns.
  const ConstraintMap* constraints;

  /// Hash join builds placed in the plan. Allows reusing a build.
  HashBuildVector builds;

  std::string printCost() const;

  std::string toString(bool detail) const;
};

/// The set of plans produced for a set of tables and columns. The plans may
/// have different output orders and distributions.
struct PlanSet {
  /// Interesting equivalent plans.
  std::vector<std::unique_ptr<Plan>> plans;

  /// Cost of lowest-cost plan plus shuffle. If a cutoff is applicable,
  /// nothing more expensive than this should be tried.
  float bestCostWithShuffle{std::numeric_limits<float>::infinity()};

  /// Returns the best plan that produces 'distribution'. If the best plan has
  /// some other distribution, sets 'needsShuffle ' to true.
  PlanP best(
      const std::optional<DesiredDistribution>& distribution,
      bool& needsShuffle);

  /// Returns the best plan when we're ok with any distribution.
  PlanP best() {
    bool ignore = false;
    return best(std::nullopt, ignore);
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
  void addEdge(PlanState& state, JoinEdgeP edge, PlanObjectCP joined);

  /// True if 'edge' has all the equalities to placed columns that 'join' of
  /// 'this' has and has more equalities.
  bool isDominantEdge(PlanState& state, JoinEdgeP edge);

  std::string toString() const;

  /// The join between already placed tables and the table(s) in 'this'.
  JoinEdgeP join{nullptr};

  /// Tables to join on the build side. The first entry is the primary
  /// table (the original join candidate). Subsequent entries, if any,
  /// are reducing tables discovered by findReducingBushyJoins that are
  /// bundled into a bushy build side. Must not include already-placed
  /// tables.
  std::vector<PlanObjectCP> tables;

  /// Semi-joins imported from the probe side to reduce the build size.
  /// Each entry is a set of tables forming one semi-join. These do not
  /// affect correctness — the inner join already filters — but shrink
  /// the build by excluding rows that would never be probed. May
  /// reference tables already placed in the plan.
  ///
  /// Example: for lineitem ⨝ part ⨝ partsupp, if partsupp (build) is
  /// filtered by an existence on part, 'tables' = {partsupp} and
  /// 'existences' = {{part}}.
  std::vector<PlanObjectSet> existences;

  /// Number of right side hits for one row on the left. The join
  /// selectivity in 'tables' affects this but the selectivity in
  /// 'existences' does not.
  float fanout;

  /// Product of the selectivities from 'existences'. Used to set
  /// the fanout on the exists-DT join. 0.2 means existences
  /// reduce cardinality 5x.
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
      PlanCost cost,
      PlanObjectSet placed,
      PlanObjectSet columns)
      : candidate{candidate},
        plan{std::move(plan)},
        cost{std::move(cost)},
        placed{std::move(placed)},
        columns{std::move(columns)} {}

  const JoinCandidate* candidate;
  RelationOpPtr plan;
  PlanCost cost;
  PlanObjectSet placed;
  PlanObjectSet columns;

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

  /// The expressions that need a value at the end of the plan. A dt can be
  /// planned for just join/filter columns or all payload. Initially, the
  /// selected expressions of the dt.
  PlanObjectSet targetExprs;

  /// The total cost for the PlanObjects placed thus far.
  PlanCost cost;

  /// Interesting completed plans for the dt being planned. For
  /// example, best by cost and maybe plans with interesting orders.
  PlanSet plans;

  /// Ordered set of tables placed so far. Used for setting a
  /// breakpoint before a specific join order gets costed.
  std::vector<int32_t> debugPlacedTables;

  /// The set of tables/objects that have been placed so far.
  const PlanObjectSet& placed() const {
    return placed_;
  }

  /// Returns true if 'object' has been placed. Use to check for tables,
  /// conjuncts, aggregation, order keys, and derived tables. Do not use
  /// to check for columns; columns are tracked separately via 'columns()').
  bool isPlaced(PlanObjectCP object) const {
    return placed_.contains(object);
  }

  /// The set of columns that have a value from placed tables.
  const PlanObjectSet& columns() const {
    return columns_;
  }

  /// Updates 'cost' to reflect 'op' being placed on top of the partial plan.
  void addCost(RelationOp& op) {
    cost.add(op);
  }

  /// Specifies that the plan-to-make only produces 'target' expressions and.
  /// These refer to 'exprs' of 'dt'.
  void setTargetExprsForDt(const PlanObjectSet& target);

  /// Returns the set of columns referenced in unplaced joins/filters union
  /// targetColumns. Gets smaller as more tables are placed.
  const PlanObjectSet& downstreamColumns() const;

  /// Checks if 'column' is used downstream just for filtering and that usage is
  /// limited to a single conjunct. Returns that conjunct or nullptr.
  ExprCP isDownstreamFilterOnly(ColumnCP column) const;

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
      std::vector<NextJoin>& toTry) const;

  std::string printCost() const;

  /// Makes a string of 'op' with 'details'. Costs are annotated with percentage
  /// of total in 'this->cost'.
  std::string printPlan(RelationOpPtr op, bool detail) const;

  /// True if the costs accumulated so far are so high that this should not be
  /// explored further.
  bool isOverBest() const {
    return hasCutoff_ && cost.cost > plans.bestCostWithShuffle;
  }

  void debugSetFirstTable(int32_t id);

  /// Saves the current state to 'saver' for later restoration.
  void save(PlanStateSaver& saver) const;

  /// Restores the state from 'saver'. Uses std::move for efficiency.
  void restore(PlanStateSaver& saver);

  /// Restores the state from 'nextJoin'.
  void restore(const NextJoin& nextJoin);

  /// Adds 'object' to placed. Use to place tables (BaseTable, ValuesTable,
  /// UnnestTable, DerivedTable), conjuncts/filters, aggregation plans, and
  /// order keys. Do not use to place columns; use placeColumn(s) instead.
  void place(PlanObjectCP object);

  /// Adds all objects in 'objects' to placed. See place(PlanObjectCP) for
  /// details on what types of objects should be placed.
  void place(const PlanObjectSet& objects);
  void place(const ExprVector& objects);

  /// Adds 'column' to columns.
  void placeColumn(PlanObjectCP column);

  /// Adds all columns in 'columns' to the set of placed columns.
  void placeColumns(const PlanObjectSet& columns);
  void placeColumns(const ColumnVector& columns);

  /// Replaces columns with 'newColumns'. Used after join projection.
  void replaceColumns(PlanObjectSet newColumns);

  /// Add a mapping from expression to pre-computed column.
  void addExprToColumn(ExprCP expr, ExprCP column);

  /// Replace expression with pre-computed column using 'exprToColumn_'
  /// mapping. Returns the original 'expr' if no mapping exists.
  ExprCP toColumn(ExprCP expr) const;

 private:
  PlanObjectSet computeDownstreamColumns(bool includeFilters) const;

  // Caches results of downstreamColumns(). This is a pure function of
  // 'placed_', 'targetExprs' and 'dt'.
  mutable folly::F14FastMap<PlanObjectSet, PlanObjectSet>
      downstreamColumnsCache_;

  const bool syntacticJoinOrder_;

  // True if we should backtrack when 'cost' exceeds the best cost with
  // shuffle from already generated plans.
  const bool hasCutoff_{true};

  /// The set of tables/objects that have been placed so far.
  PlanObjectSet placed_;

  /// The set of columns that have a value from placed tables.
  PlanObjectSet columns_;

  /// A mapping of expressions to pre-computed columns. See
  /// PrecomputeProjection.
  folly::F14FastMap<ExprCP, ExprCP> exprToColumn_;
};

/// A scoped guard that restores fields of PlanState on destruction.
/// Has public members for saved state; PlanState controls save/restore logic.
struct PlanStateSaver {
 public:
  explicit PlanStateSaver(PlanState& state);

  PlanStateSaver(PlanState& state, const JoinCandidate& candidate);

  ~PlanStateSaver();

  PlanObjectSet placed;
  PlanObjectSet columns;
  PlanCost cost;
  folly::F14FastMap<ExprCP, ExprCP> exprToColumn;
  size_t numDebugPlacedTables{0};

 private:
  PlanState& state_;
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
///
/// MemoKey is immutable after construction. Use the static create() method
/// to construct instances.
struct MemoKey {
  static MemoKey create(
      PlanObjectCP firstTable,
      PlanObjectSet columns,
      PlanObjectSet tables,
      std::vector<PlanObjectSet> existences = {}) {
    VELOX_CHECK_NOT_NULL(firstTable);
    VELOX_CHECK(tables.contains(firstTable));
    return MemoKey{
        firstTable,
        std::move(columns),
        std::move(tables),
        std::move(existences)};
  }

  bool operator==(const MemoKey& other) const;

  size_t hash() const;

  std::string toString() const;

  const PlanObjectCP firstTable;
  const PlanObjectSet columns;
  const PlanObjectSet tables;
  const std::vector<PlanObjectSet> existences;

 private:
  MemoKey(
      PlanObjectCP firstTable,
      PlanObjectSet columns,
      PlanObjectSet tables,
      std::vector<PlanObjectSet> existences)
      : firstTable(firstTable),
        columns(std::move(columns)),
        tables(std::move(tables)),
        existences(std::move(existences)) {}
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

/// Memoization cache for partial plans. Wraps the underlying map
/// to provide controlled access for adding entries and looking them up.
class Memo {
 public:
  /// Inserts a plan set for the given key. Throws if the key already exists.
  void insert(const MemoKey& key, PlanSet plans) {
    VELOX_CHECK(!plans.plans.empty());

    bool inserted = entries_.emplace(key, std::move(plans)).second;
    VELOX_CHECK(inserted, "Duplicate memo key: {}", key.toString());
  }

  bool erase(const MemoKey& key) {
    return entries_.erase(key) == 1;
  }

  /// Finds plans for the given key. Returns nullptr if not found.
  PlanSet* find(const MemoKey& key) {
    auto it = entries_.find(key);
    return it != entries_.end() ? &it->second : nullptr;
  }

  const PlanSet* find(const MemoKey& key) const {
    auto it = entries_.find(key);
    return it != entries_.end() ? &it->second : nullptr;
  }

  /// Returns true if the key exists in the memo.
  bool contains(const MemoKey& key) const {
    return entries_.contains(key);
  }

  /// Returns the number of entries in the memo.
  size_t size() const {
    return entries_.size();
  }

  /// Clears all entries.
  void clear() {
    entries_.clear();
  }

 private:
  folly::F14FastMap<MemoKey, PlanSet> entries_;
};

const JoinEdgeVector& joinedBy(PlanObjectCP table);

/// Returns  the inverse join type, e.g. right outer from left outer.
/// TODO Move this function to Velox.
velox::core::JoinType reverseJoinType(velox::core::JoinType joinType);

} // namespace facebook::axiom::optimizer
