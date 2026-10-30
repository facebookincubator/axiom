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

#include "axiom/optimizer/QueryGraph.h"
#include "velox/core/PlanNode.h"

/// Query graph types that only the v1 optimizer uses. The shared ones are in
/// axiom/optimizer/QueryGraph.h.
namespace facebook::axiom::optimizer::v1 {

struct DerivedTable;

/// The join structure is described as a tree of derived tables with
/// base tables as leaves. Joins are described as join graph
/// edges. Edges describe direction for non-inner joins. Scalar and
/// existence subqueries are flattened into derived tables or base
/// tables. The join graph would represent select ... from t where
/// exists(x) or exists(y) as a derived table of three joined tables
/// where the edge from t to x and t to y is directed and qualified as
/// left semijoin. The semijoins project out one column, an existence
/// flag. The filter would be expresssed as a conjunct under the top
/// derived table with x-exists or y-exists.

/// Represents one side of a join. See Join below for the meaning of the
/// members.
struct JoinSide {
  PlanObjectCP table;
  const ExprVector& keys;
  const std::optional<float> fanout;
  const velox::core::JoinType joinType;
  ColumnCP markColumn;
  const bool isUnique;
  const ColumnVector& columns;
  const ExprVector& exprs;

  /// Returns the join type to use if 'this' is the right side.
  velox::core::JoinType leftJoinType() const {
    return joinType;
  }
};

/// Represents a possibly directional equality join edge.
/// 'rightTable' is always set. 'leftTable' is nullptr if 'leftKeys' come from
/// different tables. If so, 'this' must be not inner and not full outer.
/// 'filter' is a list of post join conjuncts. This should be present only in
/// non-inner joins. For inner joins these are representable as freely
/// decomposable and reorderable conjuncts.
class JoinEdge {
 public:
  /// Default is INNER JOIN.
  struct Spec {
    /// Filter conjuncts to be applied after the join. Only for non-inner joins.
    ExprVector filter;

    /// Specifies the join semantics (inner, left, right, full, semi, anti,
    /// etc.).
    velox::core::JoinType joinType{velox::core::JoinType::kInner};

    /// When true, the join semantic is IN / NOT IN. When false, the join
    /// semantic is EXISTS / NOT EXISTS. Applies to semi and anti joins.
    bool nullAwareIn{false};

    /// When true, join keys use IS NOT DISTINCT FROM semantics where NULL
    /// equals NULL. Used to implement SQL set operations (EXCEPT, INTERSECT).
    bool nullAsValue{false};

    /// Marker column produced by 'exists' or 'not exists' join. If set, the
    /// 'rightExists' must be true.
    ColumnCP markColumn{nullptr};

    /// Row number column to be assigned to the non-optional (probe) side using
    /// AssignUniqueId. Used for decorrelating scalar subqueries with non-equi
    /// correlation conditions and scalar subqueries without aggregation. The
    /// join output will be clustered on this column.
    ColumnCP rowNumberColumn{nullptr};

    /// When set, validate at runtime during query execution that the join
    /// produces at most one match for each left-side row. Throws with this
    /// error message if multiple matches are found. Used for decorrelating
    /// scalar subqueries without aggregation. Requires 'rowNumberColumn' to
    /// be set.
    Name multipleMatchesError{nullptr};

    /// Columns produced by the 'left' side of a RIGHT or FULL OUTER join.
    /// Requires 'leftOptional' to be true.
    ColumnVector leftColumns;

    /// Input expressions corresponding 1:1 to 'leftColumns'.
    ExprVector leftExprs;

    /// Columns produced by the 'right' side of a LEFT or FULL OUTER join.
    /// Requires 'rightOptional' to be true.
    ColumnVector rightColumns;

    /// Input expressions corresponding 1:1 to 'rightColumns'.
    ExprVector rightExprs;

    bool directed{false};
  };

  /// @param leftTable The left table of the join. May be nullptr if 'leftKeys'
  /// come from different tables. If so, 'this' must be not inner and not full
  /// outer.
  /// @param rightTable The right table of the join. Cannot be nullptr.
  JoinEdge(
      PlanObjectCP leftTable,
      PlanObjectCP rightTable,
      Spec spec,
      std::optional<logical_plan::JoinType> originalJoinType = std::nullopt)
      : leftTable_(leftTable),
        rightTable_(rightTable),
        filter_(std::move(spec.filter)),
        joinType_(spec.joinType),
        nullAwareIn_(spec.nullAwareIn),
        nullAsValue_(spec.nullAsValue),
        directed_(spec.directed),
        markColumn_(spec.markColumn),
        rowNumberColumn_(spec.rowNumberColumn),
        multipleMatchesError_(spec.multipleMatchesError),
        leftColumns_{spec.leftColumns},
        leftExprs_{spec.leftExprs},
        rightColumns_{spec.rightColumns},
        rightExprs_{spec.rightExprs},
        originalJoinType_{originalJoinType} {
    VELOX_CHECK_NOT_NULL(rightTable);

    if (isInner()) {
      VELOX_CHECK_NOT_NULL(
          leftTable, "Hyper edge is not supported for an inner join");
      VELOX_CHECK(filter_.empty(), "Filter is not allowed for an inner join");
    }

    if (markColumn_) {
      VELOX_CHECK_EQ(
          joinType_,
          velox::core::JoinType::kLeftSemiProject,
          "Mark column requires kLeftSemiProject join type");
    }

    if (rowNumberColumn_) {
      VELOX_CHECK(!leftOptional());
      VELOX_CHECK(rightOptional());
    }

    if (multipleMatchesError_) {
      VELOX_CHECK_NOT_NULL(rowNumberColumn_);
    }

    if (!leftColumns_.empty()) {
      VELOX_CHECK(leftOptional());
      VELOX_CHECK_EQ(leftColumns_.size(), leftExprs_.size());
    }

    if (!rightColumns_.empty()) {
      VELOX_CHECK(rightOptional());
      VELOX_CHECK_EQ(rightColumns_.size(), rightExprs_.size());
    }
  }

  static JoinEdge* makeInner(PlanObjectCP leftTable, PlanObjectCP rightTable) {
    VELOX_CHECK(
        !leftTable->is(PlanType::kUnnestTableNode) &&
            !rightTable->is(PlanType::kUnnestTableNode),
        "UnnestTable cannot participate in inner joins. "
        "Use makeUnnest() instead.");
    return make<JoinEdge>(leftTable, rightTable, Spec{});
  }

  /// Creates a JoinEdge for EXISTS or IN subqueries.
  ///
  /// This factory method creates a semi-join edge used to implement:
  /// - EXISTS subqueries: WHERE EXISTS (SELECT ... FROM rightTable WHERE ...)
  /// - IN subqueries: WHERE leftKey IN (SELECT rightKey FROM rightTable)
  ///
  /// The join keeps rows from the left side that have at least one match on the
  /// right side (semi-join semantics).
  ///
  /// @param leftTable The outer query table. Can be nullptr for hyper-edges
  ///        when the correlation references multiple outer tables.
  /// @param rightTable The subquery's DerivedTable. Must not be null.
  /// @param markColumn Optional boolean column that indicates whether a match
  ///        was found. Used when the EXISTS/IN result needs to be referenced
  ///        in the output (e.g., for conditional expressions). When null, the
  ///        join acts as a pure filter.
  /// @param filter Optional non-equality filter conditions from the subquery's
  ///        correlation predicates that cannot be expressed as equality keys.
  /// @param nullAwareIn When true, the join semantic is IN (nullAware). When
  ///        false, the join semantic is EXISTS.
  /// @return A JoinEdge with rightExists=true representing the semi-join.
  static JoinEdge* makeExists(
      PlanObjectCP leftTable,
      PlanObjectCP rightTable,
      ColumnCP markColumn = nullptr,
      ExprVector filter = {},
      bool nullAwareIn = false,
      bool counting = false,
      bool nullAsValue = false) {
    auto joinType = counting
        ? velox::core::JoinType::kCountingLeftSemiFilter
        : (markColumn ? velox::core::JoinType::kLeftSemiProject
                      : velox::core::JoinType::kLeftSemiFilter);
    return make<JoinEdge>(
        leftTable,
        rightTable,
        Spec{
            .filter = std::move(filter),
            .joinType = joinType,
            .nullAwareIn = nullAwareIn,
            .nullAsValue = nullAsValue,
            .markColumn = markColumn,
        });
  }

  /// Creates a JoinEdge for NOT EXISTS or NOT IN subqueries.
  ///
  /// This factory method creates an anti-join edge used to implement:
  /// - NOT EXISTS subqueries: WHERE NOT EXISTS (SELECT ... FROM rightTable ...)
  /// - NOT IN subqueries: WHERE leftKey NOT IN (SELECT rightKey FROM
  /// rightTable)
  ///
  /// The join keeps rows from the left side that have no matches on the right
  /// side (anti-join semantics).
  ///
  /// @param leftTable The outer query table. Must not be null.
  /// @param rightTable The subquery's DerivedTable. Must not be null.
  /// @return A JoinEdge with rightNotExists=true representing the anti-join.
  static JoinEdge* makeNotExists(
      PlanObjectCP leftTable,
      PlanObjectCP rightTable,
      bool counting = false,
      bool nullAsValue = false) {
    return make<JoinEdge>(
        leftTable,
        rightTable,
        Spec{
            .joinType = counting ? velox::core::JoinType::kCountingAnti
                                 : velox::core::JoinType::kAnti,
            .nullAsValue = nullAsValue,
        });
  }

  /// Creates a JoinEdge for CROSS JOIN UNNEST. Stores 'unnestExprs' in
  /// leftKeys_. These are the MAP or ARRAY expressions to unnest, not
  /// equality keys. Equalities from WHERE-clause filters that reference
  /// unnested columns are left as DT conjuncts and placed as Filter nodes
  /// during optimization.
  static JoinEdge* makeUnnest(
      PlanObjectCP leftTable,
      PlanObjectCP rightTable,
      ExprVector unnestExprs) {
    VELOX_DCHECK_NOT_NULL(leftTable);
    auto* edge = make<JoinEdge>(leftTable, rightTable, Spec{.directed = true});
    edge->leftKeys_ = std::move(unnestExprs);
    // TODO Not sure to what values fanout need to be set,
    // (1, 1) looks ok, but tests don't produce expected plans.
    edge->setFanouts(2, 2);
    return edge;
  }

  PlanObjectCP leftTable() const {
    return leftTable_;
  }

  PlanObjectCP rightTable() const {
    return rightTable_;
  }

  PlanObjectSet allTables() const;

  size_t numKeys() const {
    VELOX_DCHECK_LE(rightKeys_.size(), leftKeys_.size());
    return rightKeys_.size();
  }

  const ExprVector& leftKeys() const {
    return leftKeys_;
  }

  const ExprVector& rightKeys() const {
    return rightKeys_;
  }

  std::optional<float> lrFanout() const {
    return lrFanout_;
  }

  std::optional<float> rlFanout() const {
    return rlFanout_;
  }

  bool leftOptional() const {
    return joinType_ == velox::core::JoinType::kRight ||
        joinType_ == velox::core::JoinType::kFull;
  }

  bool rightOptional() const {
    return joinType_ == velox::core::JoinType::kLeft ||
        joinType_ == velox::core::JoinType::kFull;
  }

  ColumnCP markColumn() const {
    return markColumn_;
  }

  ColumnCP rowNumberColumn() const {
    return rowNumberColumn_;
  }

  Name multipleMatchesError() const {
    return multipleMatchesError_;
  }

  const ColumnVector& leftColumns() const {
    return leftColumns_;
  }

  const ExprVector& leftExprs() const {
    return leftExprs_;
  }

  const ColumnVector& rightColumns() const {
    return rightColumns_;
  }

  const ExprVector& rightExprs() const {
    return rightExprs_;
  }

  bool directed() const {
    return directed_;
  }

  void addEquality(ExprCP left, ExprCP right, bool update = false);

  /// Creates a reversed copy of an inner join with left and right tables
  /// swapped and left-to-right and right-to-left fanouts swapped.
  /// @param join The join edge to reverse. Must be an inner join.
  /// @return A new JoinEdge with swapped tables and fanouts.
  static JoinEdge* reverse(JoinEdge& join);

  /// True if inner join.
  bool isInner() const {
    return joinType_ == velox::core::JoinType::kInner;
  }

  /// True if this is an EXISTS join.
  bool isSemi() const {
    return joinType_ == velox::core::JoinType::kLeftSemiFilter ||
        joinType_ == velox::core::JoinType::kLeftSemiProject ||
        joinType_ == velox::core::JoinType::kCountingLeftSemiFilter;
  }

  /// True if this is a NOT EXISTS join.
  bool isAnti() const {
    return joinType_ == velox::core::JoinType::kAnti ||
        joinType_ == velox::core::JoinType::kCountingAnti;
  }

  /// True if this is an UNNEST join.
  bool isUnnest() const {
    return rightTable_->is(PlanType::kUnnestTableNode);
  }

  /// Returns true for IN semantics (nullAware), false for EXISTS semantics.
  bool isNullAwareIn() const {
    return nullAwareIn_;
  }

  /// Returns true when join keys use IS NOT DISTINCT FROM semantics where
  /// NULL equals NULL. Used for SQL set operations (EXCEPT, INTERSECT).
  bool isNullAsValue() const {
    return nullAsValue_;
  }

  /// Returns true for counting join semantics (kCountingAnti for EXCEPT ALL,
  /// kCountingLeftSemiFilter for INTERSECT ALL).
  bool isCounting() const {
    return joinType_ == velox::core::JoinType::kCountingAnti ||
        joinType_ == velox::core::JoinType::kCountingLeftSemiFilter;
  }

  /// True if this is a LEFT join.
  bool isLeftOuter() const {
    return joinType_ == velox::core::JoinType::kLeft;
  }

  /// Original join type if different from this edge. Used to mark LEFT joins
  /// which were specified as RIGHT joins in the logical plan. Needed to
  /// correctly plan RIGHT join when syntactic join order is requested.
  std::optional<logical_plan::JoinType> originalJoinType() const {
    return originalJoinType_;
  }

  /// Returns the join type (inner, left, right, full, semi, anti, etc.).
  velox::core::JoinType joinType() const {
    return joinType_;
  }

  /// True if all tables referenced from 'leftKeys' must be placed before
  /// placing this.
  bool isNonCommutative() const {
    // Inner and full outer joins are commutative.
    if (joinType_ == velox::core::JoinType::kFull) {
      return false;
    }

    return !leftTable_ || joinType_ != velox::core::JoinType::kInner ||
        directed_;
  }

  /// True if has a hash based variant that builds on the left and probes on the
  /// right.
  bool hasRightHashVariant() const {
    // Velox does not support null-aware right semi project join with extra
    // filter.
    if (nullAwareIn_ && !filter_.empty() && markColumn_ != nullptr) {
      return false;
    }
    // Counting anti joins (EXCEPT ALL) cannot be reversed — they are
    // directional. Counting semi joins (INTERSECT ALL) can be reversed
    // since the operation is symmetric.
    return isNonCommutative() && !isAnti() && rowNumberColumn_ == nullptr;
  }

  /// Returns the join side info for 'table'. If 'other' is set, returns the
  /// other side.
  JoinSide sideOf(PlanObjectCP side, bool other = false) const;

  /// Returns the table on the other side of 'table' and the number of rows in
  /// the returned table for one row in 'table'. Returns {nullptr, 0} if the
  /// 'other' side of the join has multiple tables.
  std::pair<PlanObjectCP, std::optional<float>> otherTable(
      PlanObjectCP table) const {
    VELOX_DCHECK_NOT_NULL(table);
    return leftTable_ == table
        ? std::pair<PlanObjectCP, std::optional<float>>{rightTable_, lrFanout_}
        : rightTable_ == table && leftTable_ != nullptr
        ? std::pair<PlanObjectCP, std::optional<float>>{leftTable_, rlFanout_}
        : std::pair<PlanObjectCP, std::optional<float>>{nullptr, 0};
  }

  PlanObjectCP otherSide(PlanObjectCP side) const {
    if (side == leftTable()) {
      return rightTable();
    }

    if (rightTable() == side) {
      return leftTable();
    }

    return nullptr;
  }

  const ExprVector& filter() const {
    return filter_;
  }

  void setFanouts(
      std::optional<float> leftToRight,
      std::optional<float> rightToLeft) {
    fanoutsFixed_ = true;
    lrFanout_ = leftToRight;
    rlFanout_ = rightToLeft;
  }

  std::string toString() const;

  /// Fills in 'lrFanout' and 'rlFanout', 'leftUnique', 'rightUnique'.
  void guessFanout();

  /// True if a hash join build can be broadcasted. Used when building on the
  /// right. None of the right hash join variants are broadcastable.
  bool isBroadcastableType() const;

  /// Returns a key string for recording a join cardinality sample. The string
  /// is empty if not applicable. The bool is true if the key has right table
  /// before left.
  std::pair<std::string, bool> sampleKey() const;

 private:
  // Leading left side join keys.
  ExprVector leftKeys_;

  // Leading right side join keys, compared equals 1:1 to 'leftKeys'.
  ExprVector rightKeys_;

  PlanObjectCP const leftTable_;

  PlanObjectCP const rightTable_;

  // Join condition for any non-equality conditions for non-inner joins.
  const ExprVector filter_;

  // Number of right side rows selected for one row on the left.
  std::optional<float> lrFanout_{1};

  // Number of left side rows selected for one row on the right.
  std::optional<float> rlFanout_{1};

  // True if 'lrFanout_' and 'rlFanout_' are set by setFanouts.
  bool fanoutsFixed_{false};

  // 'rightKeys' select max 1 'leftTable' row.
  bool leftUnique_{false};

  // 'leftKeys' select max 1 'rightTable' row.
  bool rightUnique_{false};

  // Specifies the join semantics (inner, left, right, full, semi, anti, etc.).
  const velox::core::JoinType joinType_;

  // True for IN semantics (nullAware), false for EXISTS semantics.
  const bool nullAwareIn_;

  // True for IS NOT DISTINCT FROM semantics on join keys (NULL equals NULL).
  const bool nullAsValue_;

  // If directed non-outer edge. For example unnest or inner dependent on
  // optional of outer.
  const bool directed_;

  // Flag to set if right side has a match.
  ColumnCP const markColumn_;

  // Row number column assigned to the probe side using AssignUniqueId.
  ColumnCP const rowNumberColumn_;

  // When set, validate at runtime that the join produces at most one match for
  // each left-side row. Throws with this error message if multiple matches are
  // found.
  const Name multipleMatchesError_;

  const ColumnVector leftColumns_;
  const ExprVector leftExprs_;
  const ColumnVector rightColumns_;
  const ExprVector rightExprs_;

  const std::optional<logical_plan::JoinType> originalJoinType_;
};

struct ValuesTable : public TableObject {
  using Variants = const std::vector<velox::Variant>*;
  using Vectors = const std::vector<velox::RowVectorPtr>*;
  using Data = std::variant<Variants, Vectors>;

  explicit ValuesTable(const velox::Type* _dataType, Data _data)
      : TableObject{PlanType::kValuesTableNode},
        dataType{_dataType},
        data{std::move(_data)},
        cardinality_{cardinality(data)} {
    VELOX_CHECK_NOT_NULL(dataType);
  }

  const velox::Type* dataType;

  const Data data;

  std::optional<float> cardinality() const override {
    return cardinality_;
  }

  std::string toString() const override;

 private:
  static float cardinality(const Data& data) {
    if (const auto* rows = std::get_if<Variants>(&data)) {
      return (*rows)->size();
    }

    float cardinality = 0;
    const auto* vectors = *std::get_if<Vectors>(&data);
    for (auto& vector : *vectors) {
      cardinality += vector->size();
    }

    return cardinality;
  }

  float cardinality_;
};

struct UnnestTable : public TableObject {
  explicit UnnestTable() : TableObject{PlanType::kUnnestTableNode} {}

  // Ordinality column if ordinality clause is present.
  ColumnCP ordinalityColumn{nullptr};

  std::optional<float> cardinality() const override {
    // TODO Should be changed later to actual cardinality.
    return 1;
  }

  std::string toString() const override;
};

using UnnestTableCP = const UnnestTable*;

/// Stores the GROUP BY keys, aggregate functions, and output columns for a
/// DerivedTable. Output column layout: [groupingKeys, groupId (optional),
/// aggregates]. columns_[i] corresponds to groupingKeys_[i] for
/// i < groupingKeys_.size(). When groupId is present, columns_[numKeys] is
/// the groupId column and aggregates start at numKeys + 1. Without groupId,
/// aggregates start immediately after keys.
/// Each grouping set is a list of indices into the grouping keys array.
using GroupingSet = QGVector<int32_t>;
using GroupingSets = QGVector<GroupingSet>;

class AggregationPlan : public PlanObject {
 public:
  /// @param groupingKeys GROUP BY columns referencing the enclosing
  ///   DerivedTable's tableSet. When grouping sets are present, these are
  ///   new columns with auto-generated names (no alias).
  /// @param aggregates Aggregate functions (sum, count, etc.).
  /// @param columns Output columns: groupingKeys[i] maps to columns[i],
  ///   followed by groupId (if present), then aggregate result columns.
  /// @param intermediateColumns Columns with intermediate accumulator types for
  ///   partial aggregation. Same layout as columns.
  /// @param groupingSets Grouping sets for ROLLUP/CUBE/GROUPING SETS. Each set
  ///   is a list of indices into groupingKeys. Empty for regular GROUP BY.
  /// @param groupId Output column for the grouping set ID. Null when
  ///   grouping sets are not used.
  AggregationPlan(
      ExprVector groupingKeys,
      AggregateVector aggregates,
      ColumnVector columns,
      ColumnVector intermediateColumns,
      GroupingSets groupingSets = {},
      ColumnCP groupId = nullptr);

  /// GROUP BY columns. Each references a column from a table in the enclosing
  /// DerivedTable's tableSet or from the DerivedTable itself.
  const ExprVector& groupingKeys() const {
    return groupingKeys_;
  }

  /// Aggregate functions (e.g., sum, count, min). Each references columns from
  /// tables in the enclosing DerivedTable's tableSet.
  const AggregateVector& aggregates() const {
    return aggregates_;
  }

  /// Output columns: [groupingKeys, groupId (if present), aggregates].
  const ColumnVector& columns() const {
    return columns_;
  }

  /// Output columns with intermediate accumulator types, used for partial
  /// aggregation. 1:1 with columns().
  const ColumnVector& intermediateColumns() const {
    return intermediateColumns_;
  }

  /// Checks that grouping keys and aggregates reference only tables in
  /// dt.tableSet or 'dt' itself. Verifies that grouping key output columns
  /// reference tables in dt.tableSet or 'dt', and aggregate output columns
  /// reference 'dt'.
  void checkConsistency(const DerivedTable& dt) const;

  /// Returns true if this aggregation uses grouping sets (ROLLUP/CUBE/GROUPING
  /// SETS).
  bool hasGroupingSets() const {
    return !groupingSets_.empty();
  }

  /// Grouping sets for ROLLUP/CUBE/GROUPING SETS. Empty for regular GROUP BY.
  const GroupingSets& groupingSets() const {
    return groupingSets_;
  }

  /// Returns the column for the grouping set ID, or nullptr if no grouping
  /// sets.
  ColumnCP groupId() const {
    return groupId_;
  }

  /// Returns indices of global (empty) grouping sets within groupingSets().
  QGVector<int32_t> globalGroupingSets() const;

 private:
  const ExprVector groupingKeys_;
  const AggregateVector aggregates_;
  const ColumnVector columns_;
  const ColumnVector intermediateColumns_;
  const GroupingSets groupingSets_;
  const ColumnCP groupId_;
};

using AggregationPlanCP = const AggregationPlan*;

/// Stores window functions and their output columns for a DerivedTable.
/// Each window function produces exactly one output column: functions_[i]
/// corresponds to columns_[i].
class WindowPlan : public PlanObject {
 public:
  WindowPlan(
      WindowFunctionVector functions,
      ColumnVector columns,
      std::optional<int32_t> rankingLimit = std::nullopt)
      : PlanObject(PlanType::kWindowPlanNode),
        functions_(std::move(functions)),
        columns_(std::move(columns)),
        rankingLimit_(rankingLimit) {
    VELOX_CHECK_EQ(functions_.size(), columns_.size());
    if (rankingLimit_.has_value()) {
      VELOX_CHECK_EQ(functions_.size(), 1);
    }
  }

  /// Window function definitions (e.g., row_number, rank, sum). Each
  /// references columns from tables in the enclosing DerivedTable's tableSet.
  const WindowFunctionVector& functions() const {
    return functions_;
  }

  /// Output columns produced by window functions. 1:1 with functions().
  const ColumnVector& columns() const {
    return columns_;
  }

  /// Ranking limit absorbed from a filter predicate (e.g., row_number() <= 5).
  std::optional<int32_t> rankingLimit() const {
    return rankingLimit_;
  }

  /// Returns a new WindowPlan with the given ranking limit.
  const WindowPlan* withRankingLimit(int32_t limit) const {
    return make<WindowPlan>(functions_, columns_, limit);
  }

  /// Returns a new WindowPlan with additional window functions and columns
  /// appended. Used when merging windows from stacked project nodes.
  const WindowPlan* withFunctions(
      WindowFunctionVector functions,
      ColumnVector columns) const;

  /// Checks that window functions reference only tables in dt.tableSet or
  /// 'dt' itself, and that output columns reference 'dt'.
  void checkConsistency(const DerivedTable& dt) const;

 private:
  const WindowFunctionVector functions_;
  const ColumnVector columns_;
  const std::optional<int32_t> rankingLimit_;
};

using WindowPlanCP = const WindowPlan*;

class WritePlan : public PlanObject {
 public:
  /// @param table The table to write to.
  /// @param kind Indicates the type of write (create/insert/delete/update)
  /// @param columnExprs Expressions producing the values to write. 1:1 with the
  /// table schema.
  WritePlan(
      const connector::Table& table,
      connector::WriteKind kind,
      ExprVector columnExprs)
      : PlanObject{PlanType::kWriteNode},
        table_{table},
        kind_{kind},
        columnExprs_{std::move(columnExprs)} {
    VELOX_DCHECK_EQ(columnExprs_.size(), table_.type()->size());
  }

  const connector::Table& table() const {
    return table_;
  }

  connector::WriteKind kind() const {
    return kind_;
  }

  const ExprVector& columnExprs() const {
    return columnExprs_;
  }

 private:
  const connector::Table& table_;
  const connector::WriteKind kind_;
  const ExprVector columnExprs_;
};

using WritePlanCP = const WritePlan*;

} // namespace facebook::axiom::optimizer::v1
