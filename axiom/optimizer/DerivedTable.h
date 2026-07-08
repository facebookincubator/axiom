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

#include <folly/Function.h>
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/MemoKey.h"
#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

struct Distribution;
using DistributionP = Distribution*;

class JoinEdge;
using JoinEdgeP = JoinEdge*;
using JoinEdgeCP = const JoinEdge*;
using JoinEdgeVector = QGVector<JoinEdgeP>;

class RelationOp;

class AggregationPlan;
using AggregationPlanCP = const AggregationPlan*;

class Aggregate;
using AggregateCP = const Aggregate*;

enum class OrderType;
using OrderTypeVector = QGVector<OrderType>;

class WritePlan;
using WritePlanCP = const WritePlan*;

class WindowPlan;
using WindowPlanCP = const WindowPlan*;

class SetDt;

/// Represents a derived table, i.e. a SELECT in a FROM clause. Derived tables
/// are the basic unit of planning and can be merged and split apart from other
/// ones. Join types and orders are decided within each derived table. A
/// derived table is likewise a reorderable unit inside its parent derived
/// table. Joins can move between derived tables within limits, considering the
/// semantics of e.g. group by.
///
/// A derived table represets an ordered list of operations that matches SQL
/// semantics. Some operations might be missing. The logical order of operations
/// is:
///
///   1. FROM (scans, joins)
///   2. WHERE (filters)
///   3. GROUP BY (aggregation)
///   4. HAVING (more filters)
///   5. Window functions
///   6. SELECT (projections)
///   7. ORDER BY (sort)
///   8. OFFSET and LIMIT (limit)
///   9. WRITE (create/insert/delete/update)
///
/// See docs/DerivedTableLayers.md for the layered column ownership model
/// and dependency rules validated by checkConsistency.
///
/// Direct-instantiated `DerivedTable` is a regular SELECT (tables, joins,
/// conjuncts, exprs, postprocessing). `SetDt` (below) is the UNION ALL
/// variant. `asUnion()` distinguishes them at runtime.
struct DerivedTable : public TableObject {
  DerivedTable() : TableObject{PlanType::kDerivedTableNode} {}

 protected:
  // Ctor for subclasses sharing the DerivedTable PlanType.
  explicit DerivedTable(PlanType kind) : TableObject{kind} {}

 public:
  /// True if this DT is guaranteed to produce exactly one row: a global
  /// aggregation (no grouping keys) with no HAVING clause and a non-zero
  /// limit.
  bool isSingleRow() const;

  /// Estimated number of rows this DerivedTable produces. Set during
  /// planning by `initializePlans()` / `makeInitialPlan()`. `nullopt`
  /// until planning has run.
  std::optional<float> cardinality() const override {
    return cardinality_;
  }

  /// Returns `this` as a `SetDt` if the DT is in Set shape (SetDt with
  /// non-empty inputs — today, a UNION ALL with or without a dedup
  /// aggregation), or nullptr otherwise. A SetDt whose inputs have been
  /// cleared by `SetDt::addFilter` is in regular-DT shape and does not
  /// qualify.
  virtual SetDt* asUnion() {
    return nullptr;
  }
  virtual const SetDt* asUnion() const {
    return nullptr;
  }

  /// Correlation name.
  Name cname{nullptr};

  /// Exprs projected out. 1:1 to 'columns' (or empty on `SetDt`, where
  /// each leg carries its own exprs).
  ExprVector exprs;

  /// Ordered list of columns this DT produces as output. Subset of 'columns'.
  /// Determines the schema visible to the enclosing query. Uninitialized (null)
  /// means output columns have not been set (keep all columns from the plan).
  /// An empty vector means produce zero output columns.
  std::optional<ColumnVector> outputColumns;

  /// True if this DT is expected to produce exactly 1 row and must be validated
  /// at runtime. Set for scalar subqueries that don't naturally guarantee
  /// single-row output (no global aggregation).
  bool enforceSingleRow{false};

  /// All tables in FROM, either Table or DerivedTable. If Table, all
  /// filters resolvable with the table alone are in single column filters or
  /// 'filter' of BaseTable.
  QGVector<PlanObjectCP> tables;

  /// Repeats the contents of 'tables'. Used for membership check. A
  /// DerivedTable can be a subset of another, for example when planning a join
  /// for a build side. In this case joins that refer to tables not in
  /// 'tableSet' are not considered.
  PlanObjectSet tableSet;

  /// Single-row DTs (see isSingleRow()) that have no join dependencies in
  /// this DT — not referenced by any join's keys, sides, or filter.
  /// These can be appended at the end of the plan via cross-join, or
  /// skipped entirely if no downstream column needs them. Single-row DTs
  /// referenced by a join — including by a non-inner join's filter — are
  /// not in this set; they're placed at the join site that needs them.
  PlanObjectSet singleRowDts;

  /// Tables that are not to the right sides of non-commutative joins.
  PlanObjectSet startTables;

  /// Joins between 'tables'. Does not contain RIGHT joins because these are
  /// normalized to LEFT joins.
  JoinEdgeVector joins;

  /// Filters in WHERE that are not single table expressions and not join
  /// conditions of explicit joins and not equalities between columns of joined
  /// tables.
  ExprVector conjuncts;

  /// Set of reducing joined tables imported to reduce build size. Set if 'this'
  /// represents a build side join.
  PlanObjectSet importedExistences;

  /// True if this dt is already a reducing join imported to a build side. Do
  /// not try to further restrict this with probe side.
  bool noImportOfExists{false};

  /// A list of PlanObject IDs for 'tables' in the order of appearance in the
  /// query. Used to produce syntactic join order if requested. Table with id
  /// joinOrder[i] can only be placed after tables before it are placed.
  std::vector<int32_t, QGAllocator<int32_t>> joinOrder;

  /// Postprocessing clauses: group by, having, order by, limit, offset.

  AggregationPlanCP aggregation{nullptr};

  /// Window functions and their output columns. Set during ToGraph when the
  /// projection contains window function expressions.
  WindowPlanCP windowPlan{nullptr};

  ExprVector having;

  /// Order by.
  ExprVector orderKeys;
  OrderTypeVector orderTypes;

  /// Limit and offset.
  int64_t limit{-1};
  int64_t offset{0};

  // Write.
  WritePlanCP write{nullptr};

  /// Initializes 'this' and all nested DerivedTables recursively. Processes the
  /// tree in two passes:
  ///   1. Pre-order (top-down): pushes filter conjuncts down to children.
  ///   2. Post-order (bottom-up): finalizes join graphs and computes initial
  ///      physical plans.
  ///
  /// Sets 'cardinality' for each DerivedTable. For non-union DTs, sets it to
  /// resultCardinality() of the initial physical plan. For union DTs, sums
  /// resultCardinality() across all children.
  ///
  /// As a side effect, computing the physical plan updates constraints
  /// (type, cardinality, min, max, etc.) on output columns. Finalizing the
  /// join graph estimates left-to-right and right-to-left fanouts for each
  /// join edge.
  void initializePlans();

  /// Populates 'this' as a sub-DT of 'super' and pushes existence semijoins
  /// into the subquery. Three steps:
  ///   1. Copy a subset of tables and joins from 'super' (filtered to
  ///      'superTables').
  ///   2. Add 'existences' as existence semijoins alongside 'primaryTable'.
  ///   3. If 'primaryTable' is a subquery, push existence tables inside it
  ///      below the aggregation boundary.
  ///
  /// Requires:
  ///   - 'this' must be empty (no tables, no joins).
  ///   - 'super' must not be a union DT (union children are planned
  ///     individually via importUnionChild).
  ///   - 'superTables' must not be empty.
  ///   - 'primaryTable' must be in 'superTables'.
  ///
  /// @param super The outer DerivedTable that owns the tables and joins.
  /// Joins connecting 'superTables' are copied from 'super'. Must not be a
  /// set operation (UNION ALL, etc.).
  /// @param superTables Subset of tables to import. These become 'this'
  /// DT's tables. Joins from 'super' where both sides are in 'superTables'
  /// are copied.
  /// @param primaryTable The main table in 'superTables'. Existence semijoins
  /// are attached to this table. If this table is a subquery with aggregation,
  /// existence tables are pushed inside it below the aggregation boundary.
  /// @param existences Groups of reducing tables to add as existence
  /// semijoins. Can be empty. Each group is a PlanObjectSet of tables that
  /// form a single existence semijoin. Single-table groups are added directly;
  /// multi-table groups are wrapped in their own DerivedTable.
  /// @param existsFanout Cumulative fanout estimate for the existence
  /// semijoins. Used to set fanout on multi-table existence DT joins.
  void import(
      const DerivedTable& super,
      const PlanObjectSet& superTables,
      PlanObjectCP primaryTable,
      const std::vector<PlanObjectSet>& existences,
      float existsFanout);

  /// Populates 'this' by flattening a union child DT. Used when planning
  /// individual branches of a UNION ALL — the child DT is self-contained
  /// and does not need tables or joins from an outer DT.
  ///
  /// @param child A child from a union DT's 'children' list.
  void importUnionChild(PlanObjectCP child);

  /// Return a copy of 'expr', replacing references to this DT's 'columns' with
  /// corresponding 'exprs'.
  ExprCP importExpr(ExprCP expr) const;

  /// Invokes 'cb(outerColumn, translatedFilter)' for each single-column
  /// conjunct in this DT whose underlying source column maps to one of
  /// this DT's output 'columns'. 'translatedFilter' is the conjunct with
  /// the inner source column substituted by the corresponding outer column.
  void forEachExportableSingleColumnConjunct(
      folly::FunctionRef<void(ColumnCP, ExprCP)> cb) const;

  /// Adds a filter conjunct to this DT. Handles LIMIT (returns false),
  /// aggregation (adds to 'having'), and set operations (adds to all children).
  /// Returns true if the conjunct was successfully added, false otherwise.
  virtual bool addFilter(ExprCP conjunct);

  /// Returns a copy of 'expr', replacing references to this DT's 'exprs' with
  /// the corresponding 'columns'. If 'expr' references columns not present in
  /// DT's output, those columns are added.
  ExprCP exportExpr(ExprCP expr);

  /// Applies 'exportExpr' to each expression in 'exprs' in place.
  void exportExprs(ExprVector& exprs);

  /// Exports a single aggregate function for non-equi correlation
  /// decorrelation.
  ///
  /// This method is used when decorrelating scalar subqueries with non-equi
  /// correlation predicates. It transforms the aggregation by:
  /// 1. Adding a mark column (boolean true literal) to track which rows
  /// came from the subquery.
  /// 2. Exporting the aggregate's arguments and sorting keys using
  /// 'exportExpr'.
  /// 3. Combining the mark column with any existing aggregate condition.
  /// 4. Clearing the aggregation from this DerivedTable.
  ///
  /// Requires: hasAggregation() == true, no grouping keys, no HAVING clause,
  /// exactly one aggregate function, no LIMIT, no ORDER BY.
  ///
  /// @param markName Name for the mark column to be added.
  /// @return The exported aggregate with updated condition that includes the
  /// mark column.
  AggregateCP exportSingleAggregate(Name markName);

  void addTable(PlanObjectCP table) {
    tables.push_back(table);
    tableSet.add(table);

    joinOrder.push_back(table->id());
  }

  void removeLastTable(PlanObjectCP table) {
    VELOX_CHECK(!tables.empty());
    VELOX_CHECK(tables.back() == table);

    tables.pop_back();

    if (std::ranges::find(tables, table) == tables.end()) {
      tableSet.erase(table);
    }

    joinOrder.pop_back();
  }

  /// True if 'table' is of 'this'.
  bool hasTable(PlanObjectCP table) const {
    return tableSet.contains(table);
  }

  /// True if 'join' exists in 'this'. Tables link to joins that may be
  /// in different speculative candidate dts. So only consider joins
  /// inside the current dt when planning.
  bool hasJoin(JoinEdgeP join) const {
    return std::find(joins.begin(), joins.end(), join) != joins.end();
  }

  bool hasAggregation() const {
    return aggregation != nullptr;
  }

  bool hasWindow() const {
    return windowPlan != nullptr;
  }

  bool hasOrderBy() const {
    return !orderKeys.empty();
  }

  bool hasLimit() const {
    return limit >= 0;
  }

  bool hasOffset() const {
    return offset > 0;
  }

  bool hasUnnestTable() const {
    return std::ranges::any_of(tables, [](PlanObjectCP table) {
      return table->is(PlanType::kUnnestTableNode);
    });
  }

  // Returns true if any join in this DT is not an inner join (e.g. semi-join,
  // anti-join, left join).
  bool hasNonInnerJoin() const;

  /// Returns true if this DT is known to produce zero rows (e.g., an empty
  /// ValuesTable with no data).
  bool isZeroRows() const;

  /// Returns true if 'tables' is exactly one ValuesTable that produces a
  /// single row with no columns. Does not look at postprocessing.
  bool isSingleRowNoColumnsValues() const;

  /// True when this DT consists of a projection over an optional WHERE,
  /// with no FROM (the only source is the synthetic single-row no-cols
  /// values), no aggregation, no window, no ORDER BY, and no LIMIT/OFFSET.
  bool isProjectFilterOnly() const {
    return isSingleRowNoColumnsValues() && !hasAggregation() && !hasWindow() &&
        !hasOrderBy() && !hasLimit() && !hasOffset();
  }

  /// Returns true if this DT has no postprocessing — no filter,
  /// aggregation, having, window, order by, limit, or offset.
  bool hasNoPostprocess() const;

  /// Returns true if this DT's postprocessing has no filter, HAVING,
  /// LIMIT, or OFFSET. Aggregation, window, or ORDER BY may still be
  /// present.
  ///
  /// Caller contract: the result is cardinality-neutral only when the
  /// input is a single row. Callers must guarantee single-row input
  /// before relying on this method's claim — under multi-row input,
  /// aggregation with GROUP BY reduces cardinality.
  bool hasCardinalityNeutralPostprocess() const;

  /// Sets enforceSingleRow flag if this DT doesn't naturally guarantee
  /// single-row output. A global aggregation (no grouping keys) without
  /// HAVING clause guarantees exactly one row; otherwise, runtime validation
  /// is needed for scalar subqueries.
  void ensureSingleRow();

  /// True if contains one derived table in 'tables' and adds no change to its
  /// result set.
  bool isWrapOnly() const;

  /// Asserts invariants about this DerivedTable.
  virtual void checkConsistency() const;

  /// Recursively distributes conjuncts across the entire DT tree (top-down).
  /// Called as Pass 1 of initializePlans().
  virtual void distributeAllConjuncts();

  /// Recursively finalizes joins and builds initial plans across the entire
  /// DT tree (bottom-up). Called as Pass 3 of initializePlans().
  virtual void finalizeJoinsAndMakePlans();

  /// Returns the memo key for this DT.
  MemoKey memoKey() const;

  std::string toString() const override;

  /// Pushes down filters from 'conjuncts' into join conditions and single-table
  /// predicates, including nested DerivedTables. Must be called before
  /// memoization.
  void distributeConjuncts();

 private:
  // Resets all mutable state to empty defaults.
  void clearState();

  // Adds same-table equality filters that hold on a single table because of
  // its column equivalences. Three paths:
  //   1. Per-column propagation of single-column deterministic filters to
  //      other members of the same equivalence class.
  //   2. Same-table synthesis: for each equivalence class with 2+ members
  //      on the same table (e.g. ON t.a = u.x AND t.b = u.x), emits
  //      anchored equalities (t.a = t.b) on that table.
  //   3. Outer-join slot synthesis: for LEFT and non-null-aware LEFT SEMI
  //      joins, when two right-side join keys belong to the same null-
  //      supplying table and pair with the same left key, emits their
  //      equality on that table.
  void inferImpliedPredicates();

  // Enforces 'expr' on 'target': existing scan filter, pushdown via
  // tryPushdownConjunct, or fallback to this DT's conjuncts above the
  // refusing target.
  void attachPredicate(PlanObjectP target, ExprCP expr);

  // Completes 'joins' with edges implied by column equivalences.
  void addImpliedJoins();

  // After 'joins' is filled in, links tables to their direct and
  // equivalence-implied joins.
  void linkTablesToJoins();

  // Fills in 'startTables_' to 'tables_' that are not to the right of
  // non-commutative joins.
  void setStartTables();

  // Pushes the other tables in 'this' into 'subquery' as existence semijoins
  // below its aggregation boundary. A table can be pushed when the join key
  // maps to a pre-aggregation expression (not an aggregate result) inside the
  // subquery. If any table cannot be pushed, the entire pushdown is skipped.
  void pushExistencesIntoSubquery(const DerivedTable& subquery);

  // Checks whether all tables in 'this' can be pushed into 'subquery'. Returns
  // false if pushdown is blocked (LIMIT, ORDER BY, aggregate key). On success,
  // populates 'validJoins' and 'innerKeyGroups' with the validated joins and
  // their translated inner keys (one ExprVector per join).
  bool validatePushdown(
      const DerivedTable& subquery,
      JoinEdgeVector& validJoins,
      std::vector<ExprVector>& innerKeyGroups);

  // Removes 'table' and all tables in 'chain' from 'tables' and 'tableSet'.
  void removeTables(PlanObjectCP table, const std::vector<PlanObjectCP>& chain);

  // Populates tables, tableSet, joinOrder, and joins from 'super', filtered
  // to 'subsetTables'.
  void copySubset(const DerivedTable& super, const PlanObjectSet& subsetTables);

  // Adds each group in 'existences' as an existence semijoin alongside
  // 'primaryTable'. Single-table groups are added directly; multi-table groups
  // are wrapped in their own DerivedTable.
  void addExistences(
      const DerivedTable& super,
      PlanObjectCP primaryTable,
      const std::vector<PlanObjectSet>& existences,
      float existsFanout);

  // Populates 'this' as a sub-DT of 'super' without adding existences. Used
  // when wrapping tables into a chain DT or multi-table existence DT.
  void import(
      const DerivedTable& super,
      const PlanObjectSet& superTables,
      PlanObjectCP primaryTable);

  // Attempts to convert outer joins to less restrictive join types based on
  // filter predicates. A filter that eliminates NULLs on the optional side of
  // an outer join allows the join to be converted:
  // - LEFT join + filter on right columns → INNER join
  // - FULL join + filter on right columns → RIGHT join
  // - FULL join + filter on left columns → LEFT join
  // - FULL join + filter on both left and right columns → INNER join
  //
  // The filter can reference columns from multiple tables; as long as it
  // references at least one column from the optional side, it will reject
  // NULLs on that side.
  //
  // @param allowNondeterministic If true, non-deterministic conjuncts are
  // considered for join conversion.
  void tryConvertOuterJoins(bool allowNondeterministic);

  // Replaces column references in 'exprs', 'conjuncts', 'orderKeys', and
  // 'aggregation' from 'source' to 'target'. Used when converting outer joins
  // to less restrictive join types and the join output columns need to be
  // translated to the underlying expressions.
  void replaceJoinOutputs(const ColumnVector& source, const ExprVector& target);

  // Attempts to push down a filter conjunct into the specified table.
  // For a DerivedTable, translates column names and adds the condition to
  // conjuncts or having clause (if there's aggregation). For set operations,
  // the filter is added to all children. For a BaseTable, adds the filter
  // directly.
  // Returns false without modifying anything for ValuesTable, UnnestTable,
  // and DerivedTables with LIMIT (which block filter push-down).
  //
  // @param conjunct The filter expression to push down.
  // @param table The target table (BaseTable, DerivedTable, etc.).
  // @return true if the conjunct was successfully pushed down, false otherwise.
  bool tryPushdownConjunct(ExprCP conjunct, PlanObjectP table);

  // Pushes 'expr' to the null-extending side. Drops if target refuses —
  // post-join fallback would filter NULL-padded unmatched rows incorrectly.
  void tryAttachToNullExtendingSide(PlanObjectP target, ExprCP expr);

  // Returns true if 'imported' depends only on columns that are partition keys
  // of every window function in windowPlan.
  bool isPartitionKeyFilter(ExprCP imported) const;

 protected:
  // Replaces this DT's contents with an empty ValuesTable producing zero rows.
  // Preserves 'outputColumns' (external interface referenced by parent DTs).
  void makeEmpty();

  // Replaces 'this' with the contents of 'dt', effectively removing one
  // level of DT nesting. Reconstructs columns that have relation_ == dt
  // (aggregation, window, outer join outputs) so they reference 'this'
  // instead, since 'dt' will no longer be in tableSet after flattening.
  void flattenDt(const DerivedTable* dt);

  // Updates cardinality and column constraints from the plan.
  void updateConstraints(const RelationOp& plan);

  // Completes 'joins' with edges implied by column equivalences, links tables
  // to their joins, estimates fanout for each join, and computes start tables.
  void finalizeJoins();

  std::optional<float> cardinality_{};
};

using DerivedTableP = DerivedTable*;
using DerivedTableCP = const DerivedTable*;

/// UNION ALL DT. UNION DISTINCT arrives here as UNION ALL plus a
/// GROUP-BY-all-columns `aggregation` (base field).
///
/// A UNION ALL produces a single result set — each leg contributes rows
/// to the same output columns. The output schema is defined once on the
/// parent, and all legs feed into it. No leg "owns" the output columns;
/// the parent does. Each leg's `exprs` describes how that leg populates
/// the shared output columns.
///
/// Column structure:
///
///   setDt (parent):
///     columns = [col_a, col_b, ...]  (relation_ == setDt)
///     exprs = {}
///     outputColumns = columns
///
///   leg DTs:
///     columns contains setDt->columns (shared Column objects from parent,
///             plus possibly the leg's own columns from makeQueryGraph)
///     exprs = [expr_a, expr_b, ...]  (1:1 with columns, reference the
///                                     leg's internal tables or its own
///                                     aggregation/window output columns)
///     outputColumns = setDt->columns
///
/// TODO: `SetDt::addFilter` mutates `this` to regular shape when a
/// UNION ALL collapses (via `makeEmpty` / `flattenDt`). Each override
/// guards on `inputs.empty()` and delegates to the base; `asUnion()`
/// similarly gates on `!inputs.empty()`. Cleaner fix (return a
/// replacement, reparent columns) deferred.
class SetDt final : public DerivedTable {
 public:
  SetDt() : DerivedTable{PlanType::kDerivedTableNode} {}

  /// Per-leg DTs.
  QGVector<DerivedTable*> inputs;

  /// True if this DT is a UNION ALL without a dedup aggregation. UNION
  /// DISTINCT is lowered in ToGraph to UNION ALL + an aggregation
  /// grouping on all output columns; `isUnionAll()` returns false in
  /// that case.
  bool isUnionAll() const {
    return !inputs.empty() && aggregation == nullptr;
  }

  void checkConsistency() const override;
  bool addFilter(ExprCP conjunct) override;
  void distributeAllConjuncts() override;
  void finalizeJoinsAndMakePlans() override;

  SetDt* asUnion() override {
    return inputs.empty() ? nullptr : this;
  }
  const SetDt* asUnion() const override {
    return inputs.empty() ? nullptr : this;
  }
};

} // namespace facebook::axiom::optimizer
