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

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"

/// Plan candidates.
/// A candidate plan is constructed based on the join graph/derived table tree.

namespace facebook::axiom::optimizer {

class RelationOpVisitorContext;
class RelationOpVisitor;
class RelationOp;

/// Represents the cost of a plan.
struct PlanCost {
  /// Total cost of the plan.
  float cost{0};

  /// Number of output rows.
  float cardinality{1};

  void add(RelationOp& op);

  void add(const PlanCost& other) {
    cost += other.cost;
    cardinality = other.cardinality;
  }

  std::string toString() const {
    return fmt::format(
        std::locale("en_US.UTF-8"),
        "cost: {:L}, cardinality: {:L}",
        cost,
        cardinality);
  }
};

/// Represents the cost of a RelationOp. A Cost has a per-row cost and fanout.
/// For example, a hash join probe has a fanout of 0.3 if 3 of 10 input rows are
/// expected to hit and a constant small per-row cost that is fixed. (The one
/// time cost of the build side subplan (a.k.a setup cost) is being accounted
/// for separately in the PlanCost.) The inputCardinality is a precalculated
/// product of the left deep inputs for the hash probe. For a leaf table scan,
/// input cardinality is 1 and the fanout is the estimated cardinality after
/// filters, the unitCost is the cost of the scan and all filters. For an index
/// lookup, the unit cost is a function of the index size and the input spacing
/// and input cardinality. A lookup that hits densely is cheaper than one that
/// hits sparsely. An index lookup has no setup cost.
struct Cost {
  /// Cardinality of the output of the left deep input tree. 1 for a leaf
  /// scan.
  float inputCardinality{1};

  /// Cost of processing one input tuple. Complete cost of the operation for a
  /// leaf.
  float unitCost{0};

  /// 'fanout * inputCardinality' is the number of result rows. For a leaf scan,
  /// this is the number of rows.
  float fanout{1};

  /// Estimate of total data volume for a hash join build or group / order
  /// by / distinct / repartition. The memory footprint may not be this if the
  /// operation is streaming or spills.
  float totalBytes{0};

  /// Shuffle data volume.
  float transferBytes{0};

  float totalCost() const {
    return unitCost * inputCardinality;
  }

  float resultCardinality() const {
    return fanout * inputCardinality;
  }

  /// If 'isUnit' shows the cost/cardinality for one row, else for
  /// 'inputCardinality' rows.
  std::string toString(bool detail, bool isUnit = false) const;
};

/// A std::string with lifetime of the optimization. These are
/// freeable unlike Names but can be held in objects that are
/// dropped without destruction with the optimization arena.
using QGString =
    std::basic_string<char, std::char_traits<char>, QGAllocator<char>>;

/// Identifies the operator type producing the relation.
enum class RelType {
  kTableScan,
  kRepartition,
  kFilter,
  kProject,
  kJoin,
  kHashBuild,
  kAggregation,
  kOrderBy,
  kUnionAll,
  kLimit,
  kValues,
  kUnnest,
  kTableWrite,
  kEnforceSingleRow,
  kAssignUniqueId,
  kEnforceDistinct,
};

AXIOM_DECLARE_ENUM_NAME(RelType)

/// Physical relational operator. This is the common base class of all
/// elements of plan candidates. The immutable Exprs, Columns and
/// BaseTables in the query graph are referenced from
/// these. RelationOp instances are also arena allocated but are
/// reference counted so that no longer interesting candidate plans
/// can be freed, since a very large number of these could be
/// generated. All std containers inside RelationOps must be allocated
/// from the optimization arena, e.g. ExprVector instead of
/// std::vector<Expr>. RelationOps are owned via intrusive_ptr, which
/// is more lightweight than std::shared_ptr and does not require
/// atomics or keeping a separate control block. This is faster and
/// more compact and entirely bypasses malloc.
/// would use malloc.
class RelationOp {
 public:
  RelationOp(
      RelType type,
      boost::intrusive_ptr<RelationOp> input,
      Distribution distribution,
      ColumnVector columns)
      : relType_(type),
        distribution_(std::move(distribution)),
        columns_(std::move(columns)),
        input_(std::move(input)) {
    checkInputCardinality();
  }

  /// Convenience constructor for operators that project all input columns as
  /// is. E.g. Repartition or OrderBy.
  RelationOp(
      RelType type,
      boost::intrusive_ptr<RelationOp> input,
      Distribution distribution)
      : relType_{type},
        distribution_{std::move(distribution)},
        columns_{input->columns()},
        input_{std::move(input)} {
    checkInputCardinality();
  }

  /// Convenience constructor for operators that preserve input's distribution.
  /// E.g. Join or Project.
  RelationOp(
      RelType type,
      boost::intrusive_ptr<RelationOp> input,
      ColumnVector columns)
      : relType_{type},
        distribution_{input->distribution()},
        columns_{std::move(columns)},
        input_{std::move(input)} {
    checkInputCardinality();
  }

  /// Convenience constructor for operators that preserve input's distribution
  /// and project all input columns as is. E.g. Filter.
  RelationOp(RelType type, boost::intrusive_ptr<RelationOp> input)
      : relType_{type},
        distribution_{input->distribution()},
        columns_{input->columns()},
        input_{std::move(input)} {
    checkInputCardinality();
  }

  virtual ~RelationOp() = default;

  void operator delete(void* ptr) {
    queryCtx()->free(ptr);
  }

  RelType relType() const {
    return relType_;
  }

  std::string_view relTypeName() const {
    return RelTypeName::toName(relType_);
  }

  const Distribution& distribution() const {
    return distribution_;
  }

  const ColumnVector& columns() const {
    return columns_;
  }

  const boost::intrusive_ptr<class RelationOp>& input() const {
    return input_;
  }

  bool is(RelType relType) const {
    return relType_ == relType;
  }

  /// Caller must ensure this relType is correct.
  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<RelationOp, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<const T*>(this));
    return static_cast<const T*>(this);
  }

  /// Caller must ensure this relType is correct.
  template <typename T>
  T* as() {
    static_assert(std::is_base_of_v<RelationOp, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<T*>(this));
    return static_cast<T*>(this);
  }

  virtual void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const = 0;

  const Cost& cost() const {
    return cost_;
  }

  /// Returns constraints on output columns. Keys are column IDs. Leaf operators
  /// initialize constraints from schema/column values. Non-leaf operators
  /// compute constraints from input constraints.
  const ConstraintMap& constraints() const {
    return constraints_;
  }

  /// Returns the number of output rows.
  float resultCardinality() const {
    return std::max<float>(1, cost_.resultCardinality());
  }

  /// @return 1 for a leaf node, otherwise returns 'resultCardinality()' of the
  /// input. Nodes with multiple inputs, e.g. Join, UnionAll, return the
  /// 'resultCardinality()' of the left-most input, which is not the same as
  /// 'input cardinality'.
  float inputCardinality() const {
    if (input() == nullptr) {
      return 1;
    }

    return input()->resultCardinality();
  }

  /// Returns a key for retrieving/storing a historical record of execution for
  /// future costing.
  virtual const QGString& historyKey() const {
    VELOX_CHECK_NOT_NULL(input_, "Leaf RelationOps must specify a history key");
    return input_->historyKey();
  }

  std::string toString() const;

  std::string toOneline() const;

 protected:
  // adds a line of cost information to 'out'
  void printCost(bool detail, std::stringstream& out) const;

  const RelType relType_;
  const Distribution distribution_;
  const ColumnVector columns_;

  // Input of filter/project/group by etc., Left side of join, nullptr for a
  // leaf table scan.
  const boost::intrusive_ptr<class RelationOp> input_;

  Cost cost_;

  ConstraintMap constraints_;

  // Cache of history lookup key.
  mutable QGString key_;

 private:
  void checkInputCardinality() const;

  // thread local reference count. PlanObjects are freed when the
  // QueryGraphContext arena is freed, candidate plans are freed when no longer
  // referenced.
  mutable int32_t refCount_{0};

  friend void intrusive_ptr_add_ref(RelationOp* op);
  friend void intrusive_ptr_release(RelationOp* op);
};

using RelationOpPtr = boost::intrusive_ptr<RelationOp>;

inline void intrusive_ptr_add_ref(RelationOp* op) {
  ++op->refCount_;
}

inline void intrusive_ptr_release(RelationOp* op) {
  if (0 == --op->refCount_) {
    delete op;
  }
}

using RelationOpPtrVector = QGVector<RelationOpPtr>;

/// Represents a full table scan or an index lookup.
struct TableScan : public RelationOp {
  /// Constructor for a full table scan.
  TableScan(
      BaseTableCP table,
      ColumnGroupCP index,
      const ColumnVector& columns);

  /// Constructor for an index lookup.
  TableScan(
      RelationOpPtr input,
      Distribution distribution,
      BaseTableCP table,
      ColumnGroupCP index,
      float fanout,
      ColumnVector columns,
      ExprVector lookupKeys,
      velox::core::JoinType joinType,
      ExprVector joinFilter);

  /// Returns the distribution given the table, index and columns. If
  /// partitioning/ordering columns are in the output columns, the
  /// distribution reflects the distribution of the index.
  static Distribution outputDistribution(
      BaseTableCP baseTable,
      ColumnGroupCP index,
      const ColumnVector& columns);

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

  /// The base table reference. May occur in multiple scans if the base
  /// table decomposes into access via secondary index joined to pk or
  /// if doing another pass for late materialization.
  BaseTableCP const baseTable;

  /// Index (or other materialization of table) used for the physical data
  /// access.
  ColumnGroupCP const index;

  /// Lookup keys, empty if full table scan.
  const ExprVector keys;

  /// If this is a lookup, 'joinType' can be inner, left or anti.
  const velox::core::JoinType joinType;

  /// If this is a non-inner join,  extra filter for the join.
  const ExprVector joinFilter;
};

/// Represents a values.
struct Values : RelationOp {
  Values(const ValuesTable& valuesTable, ColumnVector columns);

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

  const ValuesTable& valuesTable;
};

/// Represents a repartition, i.e. query fragment boundary. The distribution of
/// the output is '_distribution'.
class Repartition : public RelationOp {
 public:
  Repartition(
      RelationOpPtr input,
      Distribution distribution,
      ColumnVector columns);

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;
};

using RepartitionCP = const Repartition*;

/// Represents a usually multitable filter not associated with any non-inner
/// join. Non-equality constraints over inner joins become Filters.
class Filter : public RelationOp {
 public:
  Filter(RelationOpPtr input, ExprVector exprs);

  const ExprVector& exprs() const {
    return exprs_;
  }

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  const ExprVector exprs_;
};

/// Assigns names to expressions. Used to rename output from a derived table.
class Project : public RelationOp {
 public:
  /// @param redundant Indicates if this Project node is redundant and is not
  /// expected to appear in the final Velox plan. It is convenient to keep these
  /// nodes in the tree to allow for easier tracking of the origins of
  /// individual symbols.
  Project(
      const RelationOpPtr& input,
      ExprVector exprs,
      const ColumnVector& columns,
      bool redundant);

  const ExprVector& exprs() const {
    return exprs_;
  }

  bool isRedundant() const {
    return redundant_;
  }

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

  /// Returns true if input's columns match 'exprs' and 'columns' exactly.
  /// Specifically, for each input column, input->columns[i] == exprs[i] and
  /// input->columns[i]->outputName() == columns[i]->outputName().
  static bool isRedundant(
      const RelationOpPtr& input,
      const ExprVector& exprs,
      const ColumnVector& columns);

 private:
  const ExprVector exprs_;
  const bool redundant_;
};

enum class JoinMethod { kHash, kMerge, kCross };

AXIOM_DECLARE_ENUM_NAME(JoinMethod);

/// Represents a hash or merge join.
struct Join : public RelationOp {
  /// @param nullAware Applies to semi and anti joins only. When true, the join
  /// semantic is IN / NOT IN. When false, the join semantic is EXISTS / NOT
  /// EXISTS.
  /// @param lhsKeys Left-side join keys from lhs columns, 1:1 with rhsKeys.
  /// @param rhsKeys Right-side join keys from rhs columns, 1:1 with lhsKeys.
  /// @param fanout Raw equi-key fanout: expected number of rhs matches per lhs
  /// row. Computed externally from join graph statistics (index lookups,
  /// execution history, PK/FK relationships) which are not available inside the
  /// Join constructor. Does not include join-type adjustment (e.g. max(1,
  /// fanout) for left join, min(1, fanout) for semi join) or filter
  /// selectivity; both are applied in initCost.
  /// @param rlFanout Raw equi-key fanout in the reverse direction: expected
  /// number of lhs matches per rhs row.
  /// @param columns Output columns for the join. Must be a subset of columns
  /// from lhs and rhs. For kLeftSemiProject, includes an extra boolean mark
  /// column (last) that is not present in either input.
  Join(
      JoinMethod method,
      velox::core::JoinType joinType,
      bool nullAware,
      RelationOpPtr lhs,
      RelationOpPtr rhs,
      ExprVector lhsKeys,
      ExprVector rhsKeys,
      ExprVector filterExprs,
      float fanout,
      float rlFanout,
      ColumnVector columns);

  /// Convenience factory for cross joins (no equi-keys). Sets method to
  /// kCross, nullAware to false, keys to empty, and fanout to
  /// right->resultCardinality().
  static Join* makeCrossJoin(
      RelationOpPtr input,
      RelationOpPtr right,
      velox::core::JoinType joinType,
      ExprVector filter,
      ColumnVector columns);

  const JoinMethod method;
  const velox::core::JoinType joinType;
  const bool nullAware;
  const RelationOpPtr right;
  const ExprVector leftKeys;
  const ExprVector rightKeys;
  const ExprVector filter;

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  // Computes filter selectivity from input constraints.
  float computeFilterSelectivity() const;

  // Initializes constraints_ for output columns.
  void initConstraints(float fanout, float rlFanout, float filterSelectivity);

  // Initializes cost_ fields. Applies filter selectivity and join-type
  // adjustment to the raw fanout.
  void initCost(float fanout, float rlFanout, float filterSelectivity);
};

using JoinCP = const Join*;

/// Occurs as right input of JoinOp with type kHash. Contains the
/// cost and memory specific to building the table. Can be
/// referenced from multiple JoinOps. The unit cost * input
/// cardinality of this is counted as setup cost in the first
/// referencing join and not counted in subsequent ones.
struct HashBuild : public RelationOp {
  HashBuild(RelationOpPtr input, ExprVector keys, PlanP plan);

  const ExprVector keys;

  // The plan producing the build data. Used for deduplicating joins.
  PlanP plan;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;
};

using HashBuildCP = const HashBuild*;

struct Unnest : public RelationOp {
  Unnest(
      RelationOpPtr input,
      ExprVector replicateColumns,
      ExprVector unnestExprs,
      ColumnVector unnestedColumns,
      ColumnCP ordinalityColumn);

  const ExprVector replicateColumns;
  const ExprVector unnestExprs;

  // Columns correspond to expressions but not 1:1,
  // it can be 2:1 (for MAP) and 1:1 (for ARRAY).
  const ColumnVector unnestedColumns;
  const ColumnCP ordinalityColumn;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  void initConstraints();
};

/// Represents aggregation with or without grouping.
struct Aggregation : public RelationOp {
  /// Constructs an Aggregation operator.
  ///
  /// @param input The input relation.
  /// @param groupingKeys Columns from the input that define groups. Each key
  ///   must be a column expression (kColumnExpr). These correspond 1:1 to the
  ///   prefix of output columns (columns[0..groupingKeys.size()-1]).
  /// @param preGroupedKeys Prefix of groupingKeys that input is already sorted
  ///   on. When equal to groupingKeys, enables streaming aggregation.
  /// @param aggregates Aggregate functions to compute per group. These
  ///   correspond 1:1 to the suffix of output columns
  ///   (columns[groupingKeys.size()..]).
  /// @param step Aggregation step (partial, final, single, intermediate).
  /// @param columns Output columns: groupingKeys followed by aggregate result
  ///   columns. Must have size == groupingKeys.size() + aggregates.size().
  Aggregation(
      RelationOpPtr input,
      ExprVector groupingKeys,
      ExprVector preGroupedKeys,
      AggregateVector aggregates,
      velox::core::AggregationNode::Step step,
      ColumnVector columns);

  const ExprVector groupingKeys;
  const AggregateVector aggregates;
  const velox::core::AggregationNode::Step step;

  /// Keys that the input is pre-grouped on. This is a prefix of groupingKeys
  /// that matches the input's clusterKeys. When non-empty, the aggregation
  /// can be executed in streaming manner without building a hash table.
  const ExprVector preGroupedKeys;

  /// Returns true if the aggregation is pre-grouped (streaming).
  bool isPreGrouped() const {
    return !preGroupedKeys.empty() &&
        preGroupedKeys.size() == groupingKeys.size();
  }

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  void initConstraints();
  void setCostWithGroups(int64_t inputBeforePartial);
};

/// Represents an order by. The order is given by the distribution.
struct OrderBy : public RelationOp {
  OrderBy(
      RelationOpPtr input,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      int64_t limit = -1,
      int64_t offset = 0);

  const int64_t limit;
  const int64_t offset;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;
};

using OrderByCP = const OrderBy*;

/// Represents a union all.
struct UnionAll : public RelationOp {
  explicit UnionAll(RelationOpPtrVector inputsVector);

  const QGString& historyKey() const override;

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

  const RelationOpPtrVector inputs;

 private:
  void initConstraints();
};

using UnionAllCP = const UnionAll*;

struct Limit : public RelationOp {
  Limit(RelationOpPtr input, int64_t limit, int64_t offset);

  const int64_t limit;
  const int64_t offset;

  bool isNoLimit() const {
    static const auto kMax = std::numeric_limits<int64_t>::max();
    return limit >= (kMax - offset);
  }

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;
};

using LimitCP = const Limit*;

struct TableWrite : public RelationOp {
  // 'inputColumns' are the columns from 'input' that correspond to
  // the 'write->table()->type()' 1:1.
  TableWrite(
      RelationOpPtr input,
      ExprVector inputColumns,
      const WritePlan* write);

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

  const ExprVector inputColumns;
  const WritePlan* write;
};

using TableWriteCP = const TableWrite*;

/// Validates that input produces exactly one row. Used for scalar subqueries.
/// Fails at runtime if input produces zero or more than one row.
struct EnforceSingleRow : public RelationOp {
  explicit EnforceSingleRow(RelationOpPtr input);

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;
};

using EnforceSingleRowCP = const EnforceSingleRow*;

/// Assigns unique identifiers to each input row. Used for decorrelating
/// subqueries with non-equi correlation conditions. The unique identifier
/// is not necessarily sequential, just guaranteed to be unique across
/// all nodes in a distributed query.
struct AssignUniqueId : public RelationOp {
  /// @param input The input relation.
  /// @param uniqueIdColumn The column to store the unique identifier.
  AssignUniqueId(RelationOpPtr input, ColumnCP uniqueIdColumn);

  ColumnCP uniqueIdColumn() const {
    return uniqueIdColumn_;
  }

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  ColumnCP const uniqueIdColumn_;
};

using AssignUniqueIdCP = const AssignUniqueId*;

/// Enforces that input has unique values for specified key columns. Used for
/// validating correlated scalar subqueries return at most one row per
/// correlation group. Throws with a custom error message if duplicates are
/// found.
struct EnforceDistinct : public RelationOp {
  /// @param input The input relation.
  /// @param distinctKeys The columns to check for distinctness.
  /// @param preGroupedKeys Subset of distinctKeys that are already
  /// sorted/grouped. When preGroupedKeys matches distinctKeys, streaming
  /// enforcement with O(1) memory is used. Otherwise, hash-based enforcement
  /// with O(n) memory is used.
  /// @param errorMessage Error message to throw if duplicates are found.
  EnforceDistinct(
      RelationOpPtr input,
      ExprVector distinctKeys,
      ExprVector preGroupedKeys,
      Name errorMessage);

  const ExprVector& distinctKeys() const {
    return distinctKeys_;
  }

  const ExprVector& preGroupedKeys() const {
    return preGroupedKeys_;
  }

  Name errorMessage() const {
    return errorMessage_;
  }

  void accept(
      const RelationOpVisitor& visitor,
      RelationOpVisitorContext& context) const override;

 private:
  const ExprVector distinctKeys_;
  const ExprVector preGroupedKeys_;
  const Name errorMessage_;
};

using EnforceDistinctCP = const EnforceDistinct*;

} // namespace facebook::axiom::optimizer
