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
#include "axiom/optimizer/Schema.h"

/// Plan candidates.
/// A candidate plan is constructed based on the join graph/derived table tree.

namespace facebook::velox::optimizer {

// Represents the cost and cardinality of a RelationOp or Plan. A Cost has a
// per-row cost, a per-row fanout and a one-time setup cost. For example, a hash
// join probe has a fanout of 0.3 if 3 of 10 input rows are expected to hit, a
// constant small per-row cost that is fixed and a setup cost that is
// the one time cost of the build side subplan. The inputCardinality
// is a precalculated product of the left deep inputs for the hash
// probe. For a leaf table scan, input cardinality is 1 and the fanout
// is the estimated cardinality after filters, the unitCost is the
// cost of the scan and all filters. For an index lookup, the unit
// cost is a function of the index size and the input spacing and
// input cardinality. A lookup that hits densely is cheaper than one
// that hits sparsely. An index lookup has no setup cost.
struct Cost {
  // Cardinality of the output of the left deep input tree. 1 for a leaf
  // scan.
  float inputCardinality{1};

  // Cost of processing one input tuple. Complete cost of the operation for a
  // leaf.
  float unitCost{0};

  // 'fanout * inputCardinality' is the number of result rows. For a leaf scan,
  // this is the number of rows.
  float fanout{1};

  // One time setup cost. Cost of build subplan for the first use of a hash
  // build side. 0 for the second use of a hash build side. 0 for table scan
  // or index access.
  float setupCost{0};

  // Estimate of total data volume  for a hash join build or group/order
  // by/distinct / repartition. The memory footprint may not be this if the
  // operation is streaming or spills.
  float totalBytes{0};

  /// Shuffle data volume
  float transferBytes{0};

  // Maximum memory occupancy. If the operation is blocking, e.g. group by, the
  // amount of spill is 'totalBytes' - 'peakResidentBytes'.
  float peakResidentBytes{0};

  void add(const Cost& other);

  /// If 'isUnit' shows the cost/cardinality for one row, else for
  /// 'inputCardinality' rows.
  std::string toString(bool detail, bool isUnit = false) const;
};

/// A std::string with lifetime of the optimization. These are
/// freeable unlike Names but can be held in objects that are
/// dropped without destruction with the optimization arena.
using QGstring =
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
};

VELOX_DECLARE_ENUM_NAME(RelType)

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
        input_(std::move(input)) {}

  RelationOp(
      RelType type,
      boost::intrusive_ptr<RelationOp> input,
      Distribution distribution)
      : relType_{type},
        distribution_{std::move(distribution)},
        columns_{input->columns()},
        input_{std::move(input)} {}

  RelationOp(
      RelType type,
      boost::intrusive_ptr<RelationOp> input,
      ColumnVector columns)
      : relType_{type},
        distribution_{input->distribution()},
        columns_{std::move(columns)},
        input_{std::move(input)} {}

  RelationOp(RelType type, boost::intrusive_ptr<RelationOp> input)
      : relType_{type},
        distribution_{input->distribution()},
        columns_{input->columns()},
        input_{std::move(input)} {}

  virtual ~RelationOp() = default;

  void operator delete(void* ptr) {
    queryCtx()->free(ptr);
  }

  RelType relType() const {
    return relType_;
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

  template <typename T>
  const T* as() const {
    return static_cast<const T*>(this);
  }

  template <typename T>
  T* as() {
    return static_cast<T*>(this);
  }

  const Cost& cost() const {
    return cost_;
  }

  /// Returns the number of output rows.
  float resultCardinality() const {
    return cost_.inputCardinality * cost_.fanout;
  }

  float inputCardinality() const {
    if (input() == nullptr) {
      return 1;
    }

    return input()->resultCardinality();
  }

  /// Returns the value constraints of 'expr' at the output of
  /// 'this'. For example, a filter or join may limit values. An Expr
  /// will for example have no more distinct values than the number of
  /// rows. This is computed on first use.
  const Value& value(ExprCP expr) const;

  /// Returns a key for retrieving/storing a historical record of execution for
  /// future costing.
  virtual const QGstring& historyKey() const {
    VELOX_CHECK_NOT_NULL(input_, "Leaf RelationOps must specify a history key");
    return input_->historyKey();
  }

  /// Returns human redable string for 'this' and inputs if 'recursive' is true.
  /// If 'detail' is true, includes cost and other details.
  ///
  /// Example,
  ///
  ///   region t3*H  (nation t2  Build ) project 2 columns
  ///
  /// reads as
  ///
  ///   - Project -> 2 columns
  ///     - HashJoin
  ///       - Scan(region as t3)
  ///       - Scan(nation as t2)
  virtual std::string toString(bool recursive, bool detail) const = 0;

 protected:
  // adds a line of cost information to 'out'
  void printCost(bool detail, std::stringstream& out) const;

  const RelType relType_;
  const Distribution distribution_;
  const ColumnVector columns_;

  // Input of filter/project/group by etc., Left side of join, nullptr for a
  // leaf table scan.
  boost::intrusive_ptr<class RelationOp> input_;

  Cost cost_;

  // Cache of history lookup key.
  mutable QGstring key_;

 private:
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

using RelationOpPtrVector =
    std::vector<RelationOpPtr, QGAllocator<RelationOpPtr>>;

/// Represents a full table scan or an index lookup.
struct TableScan : public RelationOp {
  TableScan(
      RelationOpPtr input,
      Distribution distribution,
      BaseTableCP table,
      ColumnGroupCP index,
      float fanout,
      ColumnVector columns,
      ExprVector lookupKeys = {},
      velox::core::JoinType joinType = velox::core::JoinType::kInner,
      ExprVector joinFilter = {});

  /// Returns the distribution given the table, index and columns. If
  /// partitioning/ordering columns are in the output columns, the
  /// distribution reflects the distribution of the index.
  static Distribution outputDistribution(
      BaseTableCP baseTable,
      ColumnGroupCP index,
      const ColumnVector& columns);

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;

  // The base table reference. May occur in multiple scans if the base
  // table decomposes into access via secondary index joined to pk or
  // if doing another pass for late materialization.
  BaseTableCP baseTable;

  // Index (or other materialization of table) used for the physical data
  // access.
  ColumnGroupCP index;

  // Columns read from 'baseTable'. Can be more than 'columns' if
  // there are filters that need columns that are not projected out to
  // next op.
  PlanObjectSet extractedColumns;

  // Lookup keys, empty if full table scan.
  ExprVector keys;

  // If this is a lookup, 'joinType' can  be inner, left or anti.
  velox::core::JoinType joinType{velox::core::JoinType::kInner};

  // If this is a non-inner join,  extra filter for the join.
  const ExprVector joinFilter;
};

/// Represents a values.
struct Values : RelationOp {
  Values(const ValuesTable& valuesTable, ColumnVector columns);

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;

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

  std::string toString(bool recursive, bool detail) const override;
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

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;

 private:
  const ExprVector exprs_;
};

/// Assigns names to expressions. Used to rename output from a derived table.
class Project : public RelationOp {
 public:
  Project(
      const RelationOpPtr& input,
      ExprVector exprs,
      const ColumnVector& columns);

  const ExprVector& exprs() const {
    return exprs_;
  }

  std::string toString(bool recursive, bool detail) const override;

 private:
  const ExprVector exprs_;
};

enum class JoinMethod { kHash, kMerge, kCross };

/// Represents a hash or merge join.
struct Join : public RelationOp {
  Join(
      JoinMethod method,
      velox::core::JoinType joinType,
      RelationOpPtr lhs,
      RelationOpPtr rhs,
      ExprVector lhsKeys,
      ExprVector rhsKeys,
      ExprVector filterExprs,
      float fanout,
      ColumnVector columns);

  static Join*
  makeCrossJoin(RelationOpPtr input, RelationOpPtr right, ColumnVector columns);

  JoinMethod method;
  velox::core::JoinType joinType;
  RelationOpPtr right;
  ExprVector leftKeys;
  ExprVector rightKeys;
  ExprVector filter;

  // Total cost of build side plan. For documentation.
  Cost buildCost;

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;
};

using JoinCP = const Join*;

/// Occurs as right input of JoinOp with type kHash. Contains the
/// cost and memory specific to building the table. Can be
/// referenced from multiple JoinOps. The unit cost * input
/// cardinality of this is counted as setup cost in the first
/// referencing join and not counted in subsequent ones.
struct HashBuild : public RelationOp {
  HashBuild(RelationOpPtr input, int32_t id, ExprVector keys, PlanP plan);

  int32_t buildId{0};

  ExprVector keys;
  // The plan producing the build data. Used for deduplicating joins.
  PlanP plan;

  std::string toString(bool recursive, bool detail) const override;
};

using HashBuildCP = const HashBuild*;

/// Represents aggregation with or without grouping.
struct Aggregation : public RelationOp {
  Aggregation(
      RelationOpPtr input,
      ExprVector groupingKeys,
      AggregateVector aggregates,
      velox::core::AggregationNode::Step step,
      ColumnVector columns);

  const ExprVector groupingKeys;
  const AggregateVector aggregates;
  const velox::core::AggregationNode::Step step;

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;
};

/// Represents an order by. The order is given by the distribution.
struct OrderBy : public RelationOp {
  OrderBy(
      const RelationOpPtr& input,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      int64_t limit = -1,
      int64_t offset = 0);

  const int64_t limit;
  const int64_t offset;

  std::string toString(bool recursive, bool detail) const override;
};

using OrderByCP = const OrderBy*;

/// Represents a union all.
struct UnionAll : public RelationOp {
  explicit UnionAll(RelationOpPtrVector inputsVector);

  const QGstring& historyKey() const override;

  std::string toString(bool recursive, bool detail) const override;

  const RelationOpPtrVector inputs;
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

  std::string toString(bool recursive, bool detail) const override;
};

using LimitCP = const Limit*;

} // namespace facebook::velox::optimizer
