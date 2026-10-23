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

#include "axiom/common/Enums.h"
#include "axiom/common/SchemaTableName.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/GraphPattern.h"
#include "axiom/logical_plan/PropertyGraph.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::logical_plan {

enum class NodeKind {
  kValues = 0,
  kTableScan = 1,
  kFilter = 2,
  kProject = 3,
  kAggregate = 4,
  kJoin = 5,
  kSort = 6,
  kLimit = 7,
  kSet = 8,
  kUnnest = 9,
  kTableWrite = 10,
  kSample = 11,
  kOutput = 12,
  kFixedPoint = 13,
  kRecursiveReference = 14,
  kLateralJoin = 15,
  kGraphMatch = 16,
};

AXIOM_DECLARE_ENUM_NAME(NodeKind)

class LogicalPlanNode;
using LogicalPlanNodePtr = std::shared_ptr<const LogicalPlanNode>;

class PlanNodeVisitor;
class PlanNodeVisitorContext;

/// Base class for all logical plan nodes. Every plan node has a unique ID and
/// zero or more inputs. Leaf nodes like Scan and Values have no inputs. Joins
/// have two inputs; a lateral join has one. Union may have many inputs. Most
/// other nodes have just one input. Every plan node has an output schema (list
/// of names and types of output columns) expressed as a RowType.
///
/// Shape of a plan:
///
/// - Column names are unique across the plan, so a name identifies the one
///   node that produces it.
/// - A node's inputs are the plans whose rows flow into it, each computed
///   before the node runs. The nodes reachable through `inputs()` form a tree:
///   each is the input of one other node.
/// - A plan the node runs itself is one of its `nestedPlans()`: a lateral
///   join's right side, a fixed point's step and convergence, a graph match's
///   table plans.
/// - A subquery's plan hangs off a `SubqueryExpr`, and the same plan may be
///   embedded in several of them.
/// - `forEachNode()` reaches inputs, nested plans and subquery plans, and
///   visits a shared plan once.
class LogicalPlanNode : public velox::ISerializable {
 public:
  LogicalPlanNode(
      NodeKind kind,
      std::string id,
      std::vector<LogicalPlanNodePtr> inputs,
      velox::RowTypePtr outputType)
      : LogicalPlanNode{
            kind,
            std::move(id),
            std::move(inputs),
            /*nestedPlans=*/{},
            std::move(outputType)} {}

  LogicalPlanNode(
      NodeKind kind,
      std::string id,
      std::vector<LogicalPlanNodePtr> inputs,
      std::vector<LogicalPlanNodePtr> nestedPlans,
      velox::RowTypePtr outputType)
      : kind_{kind},
        id_{std::move(id)},
        inputs_{std::move(inputs)},
        nestedPlans_{std::move(nestedPlans)},
        outputType_{std::move(outputType)} {
    VELOX_USER_CHECK_NOT_NULL(outputType_);
    for (const auto& plan : nestedPlans_) {
      VELOX_USER_CHECK_NOT_NULL(plan);
    }
    for (const auto& input : inputs_) {
      VELOX_USER_CHECK_NOT_NULL(input);
      VELOX_USER_CHECK(
          input->kind() != NodeKind::kTableWrite,
          "TableWrite cannot be non-root logical plan node");
      VELOX_USER_CHECK(
          input->kind() != NodeKind::kOutput,
          "Output cannot be non-root logical plan node");
    }
  }

  NodeKind kind() const {
    return kind_;
  }

  std::string_view kindName() const {
    return NodeKindName::toName(kind_);
  }

  bool is(NodeKind kind) const {
    return kind_ == kind;
  }

  /// Caller must ensure this kind is correct.
  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<LogicalPlanNode, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<const T*>(this));
    return static_cast<const T*>(this);
  }

  const std::string& id() const {
    return id_;
  }

  /// Plans whose rows flow into this node, each computed before the node
  /// runs. Walks that follow `inputs()` visit the plan tree; plans the node
  /// runs itself are in nestedPlans().
  const std::vector<LogicalPlanNodePtr>& inputs() const {
    return inputs_;
  }

  /// Plans the node runs itself, besides its inputs: a lateral join's right
  /// side, a fixed point's step and convergence, a graph match's table plans.
  const std::vector<LogicalPlanNodePtr>& nestedPlans() const {
    return nestedPlans_;
  }

  /// Returns the only input. Throws if there are zero or more than one inputs.
  const LogicalPlanNodePtr& onlyInput() const {
    VELOX_USER_CHECK_EQ(1, inputs_.size());
    return inputs_[0];
  }

  /// Convenience getter for the input at the specified index.
  /// A shortcut for inputs().at(index).
  const LogicalPlanNodePtr& inputAt(size_t index) const {
    VELOX_USER_CHECK_LT(index, inputs_.size());
    return inputs_[index];
  }

  const velox::RowTypePtr& outputType() const {
    return outputType_;
  }

  /// Returns true if two plan nodes are structurally equal. Node IDs are
  /// allowed to differ, except for RecursiveReferenceNode, which compares by
  /// id: distinct references are never equal even when structurally identical
  /// (see RecursiveReferenceNode::equalTo).
  bool operator==(const LogicalPlanNode& other) const;

  std::string toString() const;

  virtual void accept(
      const PlanNodeVisitor& visitor,
      PlanNodeVisitorContext& context) const = 0;

  /// Invokes 'visit' once for each node reachable through inputs, nested plans
  /// or subqueries.
  void forEachNode(
      folly::FunctionRef<void(const LogicalPlanNode&)> visit) const;

  /// Registers deserializers for all LogicalPlanNode subclasses.
  static void registerSerDe();

 protected:
  // Returns true if derived-class-specific fields, including nested plans, are
  // equal. Called only when kind, outputType, and inputs already match.
  virtual bool equalTo(const LogicalPlanNode& other) const = 0;

  /// Serializes common base fields (name, id, outputType, inputs).
  folly::dynamic serializeBase(std::string_view name) const;

  const NodeKind kind_;
  const std::string id_;
  const std::vector<LogicalPlanNodePtr> inputs_;
  const std::vector<LogicalPlanNodePtr> nestedPlans_;
  const velox::RowTypePtr outputType_;
};

/// A table whose content is embedded in the plan.
class ValuesNode : public LogicalPlanNode {
 public:
  using Variants = std::vector<velox::Variant>;
  using Vectors = std::vector<velox::RowVectorPtr>;
  using Exprs = std::vector<std::vector<ExprPtr>>;
  using Data = std::variant<Variants, Vectors, Exprs>;

  /// @param rowType Output schema. A list of column names and types. All names
  /// must be non-empty and unique.
  /// @param rows A list of rows. Each row is a list of values, one per column.
  /// The number, order and types of columns must match 'rowType'.
  ValuesNode(std::string id, velox::RowTypePtr rowType, Variants rows);

  /// Memory pools used for RowVector's allocation should outlive the execution
  /// of the plan.
  ValuesNode(std::string id, Vectors values);

  /// @param rowType Output schema. A list of column names and types. All names
  /// must be non-empty and unique.
  /// @param rows A list of rows. Each row is a list of values, one per column.
  /// Each value is a constant expression, i.e. expression that doesn't have any
  /// dependencies. E.g. 1 + 2, cast(1 as real). The number, order and types of
  /// columns must match 'rowType'.
  ValuesNode(std::string id, velox::RowTypePtr rowType, Exprs rows);

  uint64_t cardinality() const {
    return cardinality_;
  }

  const Data& data() const {
    return data_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const uint64_t cardinality_ = 0;
  const Data data_;
};

using ValuesNodePtr = std::shared_ptr<const ValuesNode>;

/// A table accessible through a connector.
class TableScanNode : public LogicalPlanNode {
 public:
  /// @param id Unique ID of the plan node.
  /// @param outputType Output schema. A list of column names and types.
  /// @param tableName Table name with catalog and schema.
  /// @param columnNames A list of column names. Must align with 'outputType',
  /// which may expose columns under different names.
  TableScanNode(
      std::string id,
      velox::RowTypePtr outputType,
      std::string connectorId,
      SchemaTableName tableName,
      std::vector<std::string> columnNames)
      : LogicalPlanNode{NodeKind::kTableScan, std::move(id), {}, std::move(outputType)},
        connectorId_{std::move(connectorId)},
        tableName_{std::move(tableName)},
        columnNames_{std::move(columnNames)} {
    VELOX_USER_CHECK_EQ(
        outputType_->size(),
        columnNames_.size(),
        "TableScan output type and column names must have the same size");

    const auto numColumns = outputType_->size();
    for (size_t i = 0; i < numColumns; ++i) {
      VELOX_USER_CHECK(
          !outputType_->nameOf(i).empty(),
          "TableScan output column name must not be empty: {}",
          i);
      VELOX_USER_CHECK(
          !columnNames_[i].empty(),
          "TableScan column name must not be empty: {}",
          i);
    }
  }

  const std::string& connectorId() const {
    return connectorId_;
  }

  const SchemaTableName& tableName() const {
    return tableName_;
  }

  const std::vector<std::string>& columnNames() const {
    return columnNames_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::string connectorId_;
  const SchemaTableName tableName_;
  const std::vector<std::string> columnNames_;
};

using TableScanNodePtr = std::shared_ptr<const TableScanNode>;

/// Eliminates zero or more rows from the input based on a boolean expression.
/// The output schema is the same as the input schema. This node may reduce the
/// cardinality of the dataset, but it cannot increase it.
class FilterNode : public LogicalPlanNode {
 public:
  FilterNode(std::string id, const LogicalPlanNodePtr& input, ExprPtr predicate)
      : LogicalPlanNode{NodeKind::kFilter, std::move(id), {input}, input->outputType()},
        predicate_{std::move(predicate)} {
    VELOX_USER_CHECK_NOT_NULL(predicate_, "Filter predicate must not be null");
    VELOX_USER_CHECK_EQ(
        predicate_->type()->kind(),
        velox::TypeKind::BOOLEAN,
        "Filter predicate must be boolean");
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const ExprPtr& predicate() const {
    return predicate_;
  }

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const ExprPtr predicate_;
};

using FilterNodePtr = std::shared_ptr<const FilterNode>;

/// Produces one or more columns using specified expressions. The output
/// schema matches the list of names and expressions. This not doesn't change
/// the cardinality of the dataset.
class ProjectNode : public LogicalPlanNode {
 public:
  /// @param names List of output column names. Names must be non-empty and
  /// unique.
  /// @param expressions List of expressions aligned with 'names'. Expressions
  /// may be scalar or window function calls. For any give expression, only
  /// the root expression can be a window function call. Scalar expressions
  /// over window function calls are not supported.
  ProjectNode(
      std::string id,
      LogicalPlanNodePtr input,
      std::vector<std::string> names,
      std::vector<ExprPtr> expressions)
      : LogicalPlanNode{NodeKind::kProject, std::move(id), {std::move(input)}, makeOutputType(names, expressions)},
        names_{std::move(names)},
        expressions_{std::move(expressions)} {}

  const std::vector<std::string>& names() const {
    return names_;
  }

  const std::vector<ExprPtr>& expressions() const {
    return expressions_;
  }

  const ExprPtr& expressionAt(size_t index) const {
    VELOX_USER_CHECK_LT(index, expressions_.size());
    return expressions_[index];
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const std::vector<std::string>& names,
      const std::vector<ExprPtr>& expressions);

  const std::vector<std::string> names_;
  const std::vector<ExprPtr> expressions_;
};

using ProjectNodePtr = std::shared_ptr<const ProjectNode>;

/// Groups input data on one or more sets of grouping keys, calculating each
/// measure for each combination of the grouping keys. The order of columns in
/// the output schema is: grouping keys, followed by aggregations, optionally
/// followed by a grouping set index. This node may decrease the cardinality of
/// the dataset. Unless there are multiple grouping sets, this node cannot
/// increase the cardinality.
class AggregateNode : public LogicalPlanNode {
 public:
  using GroupingSet = std::vector<int32_t>;

  /// @param groupingKeys Zero or more grouping keys. If empty, at least one
  /// aggregate must be specified.
  /// @param groupingSets Zero of more grouping sets. Each set is a list of
  /// grouping keys specified by an index into 'groupingKeys' array.
  /// @param aggregates Zero or more aggregates. Must be aligned with
  /// 'aggregateNames'. If empty, at least one grouping key must be specified.
  /// Each aggregate is a call to an aggregate function. Each aggregate may
  /// specify (1) a filter to apply to the input rows; (2) a sorting order to
  /// apply to input rows; (2) whether to deduplicate input rows. Used to
  /// support agg(DISTINCT x ORDER BY y) FILTER (WHERE f(z)) SQL syntax.
  /// @param outputNames List of names of the output columns: one name for each
  /// grouping key, followed by one name for each aggregate, followed
  /// by the name of a column that contains grouping set index if 'groupingSets'
  /// is not empty. Names must be unique.
  AggregateNode(
      std::string id,
      LogicalPlanNodePtr input,
      std::vector<ExprPtr> groupingKeys,
      std::vector<GroupingSet> groupingSets,
      std::vector<AggregateExprPtr> aggregates,
      std::vector<std::string> outputNames)
      : LogicalPlanNode{NodeKind::kAggregate, std::move(id), {std::move(input)}, makeOutputType(groupingKeys, groupingSets, aggregates, outputNames)},
        groupingKeys_{std::move(groupingKeys)},
        groupingSets_{std::move(groupingSets)},
        aggregates_{std::move(aggregates)},
        outputNames_{std::move(outputNames)} {
    VELOX_USER_CHECK(
        !groupingKeys_.empty() || !aggregates_.empty(),
        "Aggregation node must specify at least one aggregate or grouping key");

    for (const auto& groupingSet : groupingSets_) {
      for (const auto& key : groupingSet) {
        VELOX_USER_CHECK_LT(key, groupingKeys_.size());
      }
    }
  }

  const std::vector<ExprPtr>& groupingKeys() const {
    return groupingKeys_;
  }

  const std::vector<GroupingSet>& groupingSets() const {
    return groupingSets_;
  }

  const std::vector<std::string>& outputNames() const {
    return outputNames_;
  }

  const std::vector<AggregateExprPtr>& aggregates() const {
    return aggregates_;
  }

  const AggregateExprPtr& aggregateAt(size_t index) const {
    VELOX_USER_CHECK_LT(index, aggregates_.size());
    return aggregates_[index];
  }

  /// Returns the output index of the grouping set ID column, or std::nullopt
  /// for regular GROUP BY without grouping sets.
  std::optional<size_t> groupIdIndex() const {
    if (groupingSets_.empty()) {
      return std::nullopt;
    }
    return groupingKeys_.size() + aggregates_.size();
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const std::vector<ExprPtr>& groupingKeys,
      const std::vector<GroupingSet>& groupingSets,
      const std::vector<AggregateExprPtr>& aggregates,
      const std::vector<std::string>& outputNames);

  const std::vector<ExprPtr> groupingKeys_;
  const std::vector<GroupingSet> groupingSets_;
  const std::vector<AggregateExprPtr> aggregates_;
  const std::vector<std::string> outputNames_;
};

using AggregateNodePtr = std::shared_ptr<const AggregateNode>;

enum class JoinType {
  /// For each row on the left, find all matching rows on the right and return
  /// all combinations.
  kInner = 0,

  /// For each row on the left, find all matching rows on the right and return
  /// all combinations. In addition, return all rows from the left that have no
  /// match on the right with right-side columns filled with nulls.
  kLeft = 1,

  /// Opposite of kLeft. For each row on the right, find all matching rows on
  /// the left and return all combinations. In addition, return all rows from
  /// the
  /// right that have no match on the left with left-side columns filled with
  /// nulls.
  kRight = 2,

  /// A "union" of kLeft and kRight. For each row on the left, find all
  /// matching rows on the right and return all combinations. In addition,
  /// return all rows from the left that have no match on the right with
  /// right-side columns filled with nulls. Also, return
  /// all rows from the right that have no match on the left with left-side
  /// columns filled with nulls.
  kFull = 3,
};

AXIOM_DECLARE_ENUM_NAME(JoinType)

/// Combines two separate inputs into a single output, based on a boolean join
/// condition. The output schema contains all columns from the left input
/// followed by all columns from the right input.
class JoinNode : public LogicalPlanNode {
 public:
  /// @param condition Optional join condition.
  /// If nullptr, the output is a cross product of the inputs.
  JoinNode(
      std::string id,
      const LogicalPlanNodePtr& left,
      const LogicalPlanNodePtr& right,
      JoinType joinType,
      ExprPtr condition)
      : LogicalPlanNode{NodeKind::kJoin, std::move(id), {left, right}, makeOutputType(left, right)},
        joinType_{joinType},
        condition_{std::move(condition)} {
    if (condition_ != nullptr) {
      VELOX_USER_CHECK_EQ(condition_->typeKind(), velox::TypeKind::BOOLEAN);
    }
  }

  const LogicalPlanNodePtr& left() const {
    return inputAt(0);
  }

  const LogicalPlanNodePtr& right() const {
    return inputAt(1);
  }

  JoinType joinType() const {
    return joinType_;
  }

  const ExprPtr& condition() const {
    return condition_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const LogicalPlanNodePtr& left,
      const LogicalPlanNodePtr& right);

  const JoinType joinType_;
  const ExprPtr condition_;
};

using JoinNodePtr = std::shared_ptr<const JoinNode>;

/// A lateral (dependent) join: for each row of the left input, the 'right' plan
/// is evaluated with that left row's columns in scope, and the results are
/// joined. The left side is the node's only input. The right side runs per left
/// row and may reference the left input's columns (the correlation). The
/// output schema is all left columns followed by all right columns.
///
/// Only kInner and kLeft are allowed. kRight and kFull would require the right
/// side to be evaluable independently of the left to preserve unmatched right
/// rows, which contradicts the right side depending on the left.
class LateralJoinNode : public LogicalPlanNode {
 public:
  /// @param condition Optional ON predicate, which may reference either side.
  /// If nullptr, the output is the per-left-row cross product with the right.
  LateralJoinNode(
      std::string id,
      const LogicalPlanNodePtr& left,
      const LogicalPlanNodePtr& right,
      JoinType joinType,
      ExprPtr condition)
      : LogicalPlanNode{NodeKind::kLateralJoin, std::move(id), {left}, {right}, makeOutputType(left, right)},
        joinType_{joinType},
        condition_{std::move(condition)} {
    VELOX_USER_CHECK(
        joinType_ == JoinType::kInner || joinType_ == JoinType::kLeft,
        "LATERAL join supports only INNER and LEFT join types");
    if (condition_ != nullptr) {
      VELOX_USER_CHECK_EQ(condition_->typeKind(), velox::TypeKind::BOOLEAN);
    }
  }

  const LogicalPlanNodePtr& left() const {
    return inputAt(0);
  }

  /// Plan evaluated for each left row.
  const LogicalPlanNodePtr& right() const {
    return nestedPlans()[0];
  }

  JoinType joinType() const {
    return joinType_;
  }

  const ExprPtr& condition() const {
    return condition_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const LogicalPlanNodePtr& left,
      const LogicalPlanNodePtr& right);

  const JoinType joinType_;
  const ExprPtr condition_;
};

using LateralJoinNodePtr = std::shared_ptr<const LateralJoinNode>;

/// Sort rows based on one or more sort fields. The output schema for this node
/// matches the input. This node doesn't change the cardinality of the dataset.
class SortNode : public LogicalPlanNode {
 public:
  SortNode(
      std::string id,
      const LogicalPlanNodePtr& input,
      std::vector<SortingField> ordering)
      : LogicalPlanNode{NodeKind::kSort, std::move(id), {input}, input->outputType()},
        ordering_{std::move(ordering)} {
    VELOX_USER_CHECK(!ordering_.empty());
  }

  const std::vector<SortingField>& ordering() const {
    return ordering_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::vector<SortingField> ordering_;
};

using SortNodePtr = std::shared_ptr<const SortNode>;

/// Eliminates rows outside of the desired window. Returns up to 'count' rows
/// starting from 'offset'. The output schema of this node matches the input.
class LimitNode : public LogicalPlanNode {
 public:
  /// @param offset Zero-based index of the first row to return. Must be >= 0.
  /// @param count Maximum number of rows to return. Must be >= 0. If zero, the
  /// node produces empty dataset. Use std::numeric_limits<int64_t>::max() to
  /// indicate no limit, in which case offset must be > 0.
  LimitNode(
      std::string id,
      const LogicalPlanNodePtr& input,
      int64_t offset,
      int64_t count)
      : LogicalPlanNode{NodeKind::kLimit, std::move(id), {input}, input->outputType()},
        offset_{offset},
        count_{count} {
    VELOX_USER_CHECK_GE(offset_, 0);
    VELOX_USER_CHECK_GE(count_, 0);

    if (noLimit()) {
      VELOX_USER_CHECK_NE(
          offset_, 0, "Offset must be > zero if there is no limit");
    }
  }

  int64_t offset() const {
    return offset_;
  }

  int64_t count() const {
    return count_;
  }

  bool noLimit() const {
    return count_ == std::numeric_limits<int64_t>::max();
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const int64_t offset_;
  const int64_t count_;
};

using LimitNodePtr = std::shared_ptr<const LimitNode>;

enum class SetOperation {
  /// Returns all rows from all inputs after removing duplicates.
  kUnion,

  /// Returns all rows from all inputs.
  kUnionAll,

  /// Returns distinct rows that are present in all inputs.
  kIntersect,

  /// Returns all rows that are present in all inputs, preserving duplicates.
  kIntersectAll,

  /// Returns distinct rows in the first input that are not found in any other
  /// input.
  kExcept,

  /// Returns all rows in the first input that are not found in any other
  /// input, preserving duplicates.
  kExceptAll,
};

AXIOM_DECLARE_ENUM_NAME(SetOperation)

/// Set-level operation that supports combining datasets, possibly excluding
/// rows based on various types of row level matching.
///
/// All inputs must have compatible types. Number and types of columns must be
/// the same. Columns names being unique will be different. The output schema of
/// the Set node is the schema of the first input. Column names in the output of
/// the Set match column names in the first input.
///
/// Set operation must specify at least 2 inputs.
class SetNode : public LogicalPlanNode {
 public:
  SetNode(
      std::string id,
      const std::vector<LogicalPlanNodePtr>& inputs,
      SetOperation operation)
      : LogicalPlanNode{NodeKind::kSet, std::move(id), inputs, makeOutputType(inputs)},
        operation_{operation} {}

  SetOperation operation() const {
    return operation_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const std::vector<LogicalPlanNodePtr>& inputs);

  const SetOperation operation_;
};

using SetNodePtr = std::shared_ptr<const SetNode>;

/// Used to expand an ARRAY or MAP into a relation. Arrays are expanded into a
/// single column, and maps are expanded into two columns (key, value). Can also
/// be used to expand multiple arrays and maps, in which case they are expanded
/// into multiple columns, with as many rows as the highest cardinality array or
/// map (the other columns are padded with nulls). Can optionally request to
/// produce an ordinality column.
///
/// If 'flattenArrayOfRows' is true, ARRAY(ROW) is expanded into multiple
/// columns, one per struct field.
///
/// Duplicates each input row by combining it with every row in the relation
/// produced by expanding the unnest expressions.
///
/// Rows with empty arrays or map do not appear in the output.
///
/// This operation typically increases the cardinality of the dataset. However,
/// it may also reduce the cardinality if there are many rows with empty arrays
/// and maps.
///
/// The output schema contains all columns from the input, followed by columns
/// in the relation produced by expanding the unnest expressions, followed by an
/// optional ordinality column of type BIGINT.
class UnnestNode : public LogicalPlanNode {
 public:
  /// @param input Input node.
  /// @param unnestExpressions One or more expressions that produce ARRAYs
  /// or MAPs.
  /// @param unnestedNames Names to use for expanded relations. Must align
  /// with 'unnestExpressions'. Each ARRAY requires one name. Each MAP
  /// requires two maps. If 'flattenArrayOfRows' is true, each ARRAY(ROW)
  /// requires as many names as there are fields in the ROW.
  /// @param ordinalityName Optional name for the ordinality output column.
  /// If not specified, ordinality column is not added. Ordinality is
  /// 1-based.
  UnnestNode(
      std::string id,
      const LogicalPlanNodePtr& input,
      std::vector<ExprPtr> unnestExpressions,
      std::vector<std::vector<std::string>> unnestedNames,
      std::optional<std::string> ordinalityName,
      bool flattenArrayOfRows = false)
      : LogicalPlanNode{NodeKind::kUnnest, std::move(id), {input}, makeOutputType(input, unnestExpressions, unnestedNames, ordinalityName, flattenArrayOfRows)},
        unnestExpressions_{std::move(unnestExpressions)},
        unnestedNames_{std::move(unnestedNames)},
        ordinalityName_{std::move(ordinalityName)},
        flattenArrayOfRows_{flattenArrayOfRows} {
    if (ordinalityName_.has_value()) {
      VELOX_USER_CHECK(
          !ordinalityName_->empty(),
          "Ordinality column name must be not empty");
    }
  }

  const std::vector<ExprPtr>& unnestExpressions() const {
    return unnestExpressions_;
  }

  const std::vector<std::vector<std::string>>& unnestedNames() const {
    return unnestedNames_;
  }

  const std::optional<std::string>& ordinalityName() const {
    return ordinalityName_;
  }

  bool flattenArrayOfRows() const {
    return flattenArrayOfRows_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  static velox::RowTypePtr makeOutputType(
      const LogicalPlanNodePtr& input,
      const std::vector<ExprPtr>& unnestExpressions,
      const std::vector<std::vector<std::string>>& unnestedNames,
      const std::optional<std::string>& ordinalityName,
      bool flattenArrayOfRows);

  const std::vector<ExprPtr> unnestExpressions_;
  const std::vector<std::vector<std::string>> unnestedNames_;
  const std::optional<std::string> ordinalityName_;
  const bool flattenArrayOfRows_;
};

using UnnestNodePtr = std::shared_ptr<const UnnestNode>;

/// Specifies what type of write is intended when initiating or concluding a
/// write operation.
enum class WriteKind {
  // A write operation to a new table which does not yet exist in the connector.
  // Covers both creation of an empty table and create as select operations.
  kCreate = 1,

  // Rows are added and all columns must be specified for the TableWriter.
  // Covers insert, Hive partition replacement or any other operation which adds
  // whole rows.
  kInsert = 2,

  // Individual rows are deleted. Carries no column names or expressions in the
  // logical plan: how a row is identified is a physical property of the target
  // connector, resolved during optimization.
  kDelete = 3,

  // Column values in individual rows are changed. The TableWriter
  // gets first the columns Table::rowIdColumns() names and then new values
  // for the columns being changed. The new values may overlap with the row id
  // if the row id is a set of primary key columns.
  kUpdate = 4,
};

AXIOM_DECLARE_ENUM_NAME(WriteKind);

/// Implements create/insert/delete/update as per 'writeKind'.
///
/// The output schema contains a single BIGINT column named 'rows'. The value of
/// 'rows' is the total number of rows written. The output has exactly one row.
class TableWriteNode : public LogicalPlanNode {
 public:
  /// @param id Unique ID of the plan node.
  /// @param input The child plan node providing data to write.
  /// @param tableName Table name with catalog and schema.
  /// @param writeKind The type of write (create/insert/delete/update).
  /// @param columnNames A subset of columns in the table being written.
  /// Correspond 1:1 to 'columnExpressions'. 'columnNames' must refer to columns
  /// in the table but their number or order does not have to correspond to the
  /// table. Missing columns get their default from the table schema. Column
  /// names must be unique. Must be empty for 'kDelete'.
  /// @param columnExpressions Expressions producing the values to write.
  /// Correspond 1:1 to 'columnNames'.
  /// @param options Writer dependent options. May specify compression or
  /// encoding. Partitioning, is available, comes from the table schema.
  /// 'options' are provided for advanced usage or testing. Supported only for
  /// writeKind of insert or create.
  TableWriteNode(
      std::string id,
      LogicalPlanNodePtr input,
      std::string connectorId,
      SchemaTableName tableName,
      WriteKind writeKind,
      std::vector<std::string> columnNames,
      std::vector<ExprPtr> columnExpressions,
      folly::F14FastMap<std::string, std::string> options = {});

  const std::string& connectorId() const {
    return connectorId_;
  }

  const SchemaTableName& tableName() const {
    return tableName_;
  }

  WriteKind writeKind() const {
    return writeKind_;
  }

  const std::vector<std::string>& columnNames() const {
    return columnNames_;
  }

  const std::vector<ExprPtr>& columnExpressions() const {
    return columnExpressions_;
  }

  const folly::F14FastMap<std::string, std::string>& options() const {
    return options_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::string connectorId_;
  const SchemaTableName tableName_;
  const WriteKind writeKind_;
  const std::vector<std::string> columnNames_;
  const std::vector<ExprPtr> columnExpressions_;
  const folly::F14FastMap<std::string, std::string> options_;
};

using TableWriteNodePtr = std::shared_ptr<const TableWriteNode>;

/// Returns an approximate percentage of rows. The output schema is the same as
/// the input schema.
class SampleNode : public LogicalPlanNode {
 public:
  enum class SampleMethod {
    /// Samples directly on each row of the input (basically does "coin flip"
    /// for each row). This algorithm gives better random distribution but will
    /// be slower for small percentages then kSystem.
    kBernoulli = 0,

    /// Samples splits of the input. This algorithm is faster then kBernoulli,
    /// but results are less accurate. The accuracy depends on the total number
    /// of splits generated by the connector and how even these splits are.
    kSystem = 1,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(SampleMethod);

  /// @param percentage Percentage of rows to return. Must be constant-foldable
  /// expression of type DOUBLE with value between 0.0 and 100.0.
  SampleNode(
      std::string id,
      const LogicalPlanNodePtr& input,
      ExprPtr percentage,
      SampleMethod sampleMethod)
      : LogicalPlanNode{NodeKind::kSample, std::move(id), {input}, input->outputType()},
        percentage_{std::move(percentage)},
        sampleMethod_{sampleMethod} {
    VELOX_USER_CHECK_NOT_NULL(percentage_);
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  const ExprPtr& percentage() const {
    return percentage_;
  }

  SampleMethod sampleMethod() const {
    return sampleMethod_;
  }

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const ExprPtr percentage_;
  const SampleMethod sampleMethod_;
};

using SampleNodePtr = std::shared_ptr<const SampleNode>;

/// Root-only node that names and reorders input columns for the final query
/// output. Can drop or duplicate columns but cannot compute anything new.
/// Unlike ProjectNode, allows duplicate and empty output column names, matching
/// Presto behavior: SELECT a AS x, b AS x or SELECT 1 AS "".
class OutputNode : public LogicalPlanNode {
 public:
  /// Describes a single output column: which input column it comes from and
  /// what name to give it in the output.
  struct Entry {
    /// Index into input's output columns.
    int32_t index;

    /// Output column name. May be empty or duplicate.
    std::string name;

    bool operator==(const Entry&) const = default;
  };

  OutputNode(
      std::string id,
      LogicalPlanNodePtr input,
      std::vector<Entry> entries);

  const std::vector<Entry>& entries() const {
    return entries_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::vector<Entry> entries_;
};

using OutputNodePtr = std::shared_ptr<const OutputNode>;

/// Iterative fixed-point evaluation node. The 'anchor', the node's only input,
/// produces the initial state; the 'step' plan computes the next state from
/// the previous one. Inside 'step' and 'convergence', a RecursiveReferenceNode
/// with matching 'name' reads the state. 'step' and 'convergence' run once per
/// iteration. The node runs one of two loops.
///
/// kAppend returns every iteration's rows, as a linear recursive CTE does:
///
///   delta = anchor;
///   out = anchor;
///   for (i = 0; i < maxIterations; ++i) {
///     delta = step(delta);
///     out = out UNION ALL delta;
///     if (stop(delta)) return out;
///   }
///   if (failAtBound) fail;
///   return out;
///
/// kReplace returns the last state, as PageRank or KMeans do:
///
///   state = anchor;
///   for (i = 0; i < maxIterations; ++i) {
///     state = step(state);
///     if (stop(state)) return state;
///   }
///   if (failAtBound) fail;
///   return state;
///
/// stop(x) is the value of 'convergence' over x, or 'x is empty' without one.
/// 'convergence' produces exactly one row with one non-NULL BOOLEAN column;
/// any other result fails the query.
///
/// The output schema is the anchor's, which is also the state's schema: the
/// step and every RecursiveReferenceNode produce the same columns.
///
/// In kReplace, 'keyColumns' may name a key of the state, such as PageRank's
/// vertex id: in every iteration, the state has at most one row per key, and
/// no key column is null. The plan's producer guarantees this; Axiom may rely
/// on it and does not check it.
///
/// For SQL the mapping is:
///
///   WITH RECURSIVE t(n) AS (
///     SELECT 1                          -- anchor
///     UNION ALL
///     SELECT n + 1 FROM t WHERE n < 10  -- step; the reference to t is a
///                                       --   RecursiveReferenceNode named "t"
///   )
///
/// with kAppend, no convergence, and the session's recursion limit as a bound
/// that fails.
class FixedPointNode : public LogicalPlanNode {
 public:
  /// What the node returns.
  enum class OutputMode {
    /// The anchor's rows and every iteration's rows.
    kAppend,
    /// The state after the last iteration.
    kReplace,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(OutputMode);

  /// @param convergence Decides after each iteration whether the loop stops.
  /// Null to stop once an iteration produces no rows.
  /// @param maxIterations Bound on the number of iterations. Unset to use the
  /// session's recursion limit.
  /// @param failAtBound Whether reaching the bound before the loop stops fails
  /// the query, or returns the current result.
  /// @param keyColumns Names of state columns that form a key of the state.
  /// Empty when the state declares no key.
  ///
  /// Requires:
  /// - `name` is non-empty.
  /// - `anchor` does not reference its enclosing fixed-point state.
  /// - `step` contains at least one RecursiveReferenceNode named `name`.
  /// - `anchor` and `step` produce equivalent output schemas.
  /// - `convergence`, when set, contains at least one RecursiveReferenceNode
  ///   named `name` and produces one BOOLEAN column.
  /// - `maxIterations`, when set, is positive.
  /// - `keyColumns` are distinct columns of the state, and are empty in
  ///   kAppend.
  FixedPointNode(
      std::string id,
      std::string name,
      LogicalPlanNodePtr anchor,
      LogicalPlanNodePtr step,
      OutputMode outputMode,
      LogicalPlanNodePtr convergence,
      std::optional<int32_t> maxIterations,
      bool failAtBound,
      std::vector<std::string> keyColumns);

  /// Returns a linear recursive CTE: kAppend, stops once an iteration produces
  /// no rows, and fails at the session's recursion limit.
  ///
  ///   delta = anchor;
  ///   out = anchor;
  ///   for (i = 0; i < recursionLimit; ++i) {
  ///     delta = step(delta);
  ///     out = out UNION ALL delta;
  ///     if (delta is empty) return out;
  ///   }
  ///   fail;
  static std::shared_ptr<const FixedPointNode> recursiveCte(
      std::string id,
      std::string name,
      LogicalPlanNodePtr anchor,
      LogicalPlanNodePtr step);

  /// Returns a loop that replaces its state 'numIterations' times and returns
  /// it. Like any loop without a convergence, it stops early if the state
  /// becomes empty.
  ///
  ///   state = anchor;
  ///   for (i = 0; i < numIterations; ++i) {
  ///     state = step(state);
  ///     if (state is empty) return state;
  ///   }
  ///   return state;
  static std::shared_ptr<const FixedPointNode> iterate(
      std::string id,
      std::string name,
      LogicalPlanNodePtr anchor,
      LogicalPlanNodePtr step,
      int32_t numIterations,
      std::vector<std::string> keyColumns);

  /// Returns a loop that replaces its state each iteration until 'convergence'
  /// returns true.
  ///
  ///   state = anchor;
  ///   for (i = 0; i < maxIterations; ++i) {
  ///     state = step(state);
  ///     if (convergence(state)) return state;
  ///   }
  ///   if (failAtBound) fail;
  ///   return state;
  static std::shared_ptr<const FixedPointNode> iterateUntil(
      std::string id,
      std::string name,
      LogicalPlanNodePtr anchor,
      LogicalPlanNodePtr step,
      LogicalPlanNodePtr convergence,
      int32_t maxIterations,
      bool failAtBound,
      std::vector<std::string> keyColumns);

  /// Initial (base-case) subplan.
  const LogicalPlanNodePtr& anchor() const {
    return inputs()[0];
  }

  /// Plan that computes the next state.
  const LogicalPlanNodePtr& step() const {
    return nestedPlans()[0];
  }

  /// Stop condition, or null to stop once an iteration produces no rows.
  const LogicalPlanNodePtr& convergence() const {
    // nestedPlans() holds the step, then the convergence when set.
    static const LogicalPlanNodePtr kNone;
    return nestedPlans().size() > 1 ? nestedPlans()[1] : kNone;
  }

  OutputMode outputMode() const {
    return outputMode_;
  }

  /// Unset when the session's recursion limit bounds the loop.
  std::optional<int32_t> maxIterations() const {
    return maxIterations_;
  }

  bool failAtBound() const {
    return failAtBound_;
  }

  /// Names of state columns that form a key of the state, or empty.
  const std::vector<std::string>& keyColumns() const {
    return keyColumns_;
  }

  /// Name of the loop's state. RecursiveReferenceNodes inside the step and the
  /// convergence match this name to read the state.
  const std::string& name() const {
    return name_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::string name_;
  const OutputMode outputMode_;
  const std::optional<int32_t> maxIterations_;
  const bool failAtBound_;
  const std::vector<std::string> keyColumns_;
};

using FixedPointNodePtr = std::shared_ptr<const FixedPointNode>;

/// Leaf inside the step or the convergence of a FixedPointNode that reads the
/// loop's state.
/// Identity equality is by node id, not schema, so passes that fold
/// equal-comparing nodes (memo lookups, plan-tree equality) cannot collapse
/// two distinct self-references in a step body into one leaf (e.g.
/// `SELECT * FROM r r1 JOIN r r2 ON ...`).
class RecursiveReferenceNode : public LogicalPlanNode {
 public:
  /// @param id Unique plan node ID. Each self-reference in a step body must
  ///     receive its own fresh id so distinct references compare as distinct.
  /// @param name Name of the enclosing FixedPointNode whose previous
  ///     iteration this reference reads. Must be non-empty.
  /// @param outputType Schema of the recursive CTE (after alias application).
  RecursiveReferenceNode(
      std::string id,
      std::string name,
      velox::RowTypePtr outputType);

  /// Name of the FixedPointNode whose previous iteration this reference reads.
  const std::string& name() const {
    return name_;
  }

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  // Two RecursiveReferenceNodes are equal only when they share the same id.
  bool equalTo(const LogicalPlanNode& other) const override;

  const std::string name_;
};

using RecursiveReferenceNodePtr = std::shared_ptr<const RecursiveReferenceNode>;

class GraphMatchOutput;
using GraphMatchOutputPtr = std::shared_ptr<const GraphMatchOutput>;

/// How a GraphMatchNode computes an output column from a match. Reads the
/// pattern named 'pattern'.
class GraphMatchOutput {
 public:
  enum class Kind { kProperty, kPathLength };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Kind);

  virtual ~GraphMatchOutput() = default;

  Kind kind() const {
    return kind_;
  }

  bool is(Kind kind) const {
    return kind_ == kind;
  }

  const std::string& pattern() const {
    return pattern_;
  }

  /// Caller must ensure this kind is correct.
  template <typename T>
  const T* as() const {
    static_assert(std::is_base_of_v<GraphMatchOutput, T>);
    VELOX_DCHECK_NOT_NULL(dynamic_cast<const T*>(this));
    return static_cast<const T*>(this);
  }

  bool operator==(const GraphMatchOutput& other) const;

  virtual folly::dynamic serialize() const;

  static GraphMatchOutputPtr create(const folly::dynamic& obj);

 protected:
  GraphMatchOutput(Kind kind, std::string pattern);

  // Returns true if 'other', of the same kind and pattern, is equal.
  virtual bool equals(const GraphMatchOutput& other) const = 0;

 private:
  const Kind kind_;
  const std::string pattern_;
};

/// Property 'property' of the vertex or edge a vertex or edge pattern binds:
/// b.y. If the pattern repeats, an array with one value per repetition.
class GraphPropertyOutput final : public GraphMatchOutput {
 public:
  GraphPropertyOutput(std::string pattern, std::string property);

  const std::string& property() const {
    return property_;
  }

  folly::dynamic serialize() const override;

 private:
  bool equals(const GraphMatchOutput& other) const override;

  const std::string property_;
};

/// Number of edges in the path a path or subpath pattern matches:
/// PATH_LENGTH(p). BIGINT.
class GraphPathLengthOutput final : public GraphMatchOutput {
 public:
  explicit GraphPathLengthOutput(std::string pattern)
      : GraphMatchOutput{Kind::kPathLength, std::move(pattern)} {}

 private:
  bool equals(const GraphMatchOutput& /*other*/) const override {
    return true;
  }
};

/// Matches path patterns against a property graph and returns one row per
/// match. A vertex or edge pattern binds one vertex or edge of the graph, and
/// outputs read properties of the bound elements by pattern name.
///
///   (a IS A WHERE a.x = 1) -[e IS B]->{1,3} (b)
///   outputs: [b.x, e.z], outputNames: [o1, o2]  ->  o1 BIGINT,
///   o2 ARRAY(DOUBLE)
///
/// Term predicates apply while matching, then each path's selector, then
/// the joins between path patterns that share a pattern name, then
/// 'predicate'. The node has no inputs: the graph's element tables carry their
/// own source plans, which are the node's nested plans.
///
/// Invariants:
/// - 'graph' is set, and 'paths' and 'outputs' are not empty. 'outputNames'
///   is aligned with 'outputs', and its names are unique. Each path has
///   at least one term, and its selector satisfies the invariants
///   GraphPathSelector lists.
/// - Every label name in a vertex pattern is a vertex of the graph, and every
///   label name in an edge pattern is an edge.
/// - Every property a predicate or output reads is defined by a table the
///   pattern can match, and all such tables give it the same data type; for a
///   predicate, the data type its ROW input declares.
/// - A name under a repetition appears once. A vertex pattern's name is never
///   reused for an edge pattern. A reused name matches only tables every
///   pattern with that name can match.
/// - Path and subpath names are unique and distinct from vertex and edge
///   pattern names.
/// - 'predicate' is BOOLEAN and reads vertex and edge patterns that are not
///   under a repetition.
/// - A property output reads a vertex or edge pattern. A path length output
///   reads a path or subpath pattern that is not under a repetition.
class GraphMatchNode : public LogicalPlanNode {
 public:
  GraphMatchNode(
      std::string id,
      PropertyGraphPtr graph,
      std::vector<GraphPathPattern> paths,
      ExprPtr predicate,
      std::vector<GraphMatchOutputPtr> outputs,
      std::vector<std::string> outputNames);

  const PropertyGraphPtr& graph() const {
    return graph_;
  }

  const std::vector<GraphPathPattern>& paths() const {
    return paths_;
  }

  /// Condition on a whole match, across paths: MATCH ... WHERE. Null if the
  /// node has none.
  const ExprPtr& predicate() const {
    return predicate_;
  }

  const std::vector<GraphMatchOutputPtr>& outputs() const {
    return outputs_;
  }

  /// Names of the output columns, aligned with 'outputs'.
  const std::vector<std::string>& outputNames() const {
    return outputNames_;
  }

  /// Invokes 'visit' for each expression the node holds: property expressions,
  /// term predicates and 'predicate'.
  void forEachExpression(folly::FunctionRef<void(const Expr&)> visit) const;

  void accept(const PlanNodeVisitor& visitor, PlanNodeVisitorContext& context)
      const override;

  folly::dynamic serialize() const override;

  static LogicalPlanNodePtr create(const folly::dynamic& obj, void* context);

 private:
  bool equalTo(const LogicalPlanNode& other) const override;

  const PropertyGraphPtr graph_;
  const std::vector<GraphPathPattern> paths_;
  const ExprPtr predicate_;
  const std::vector<GraphMatchOutputPtr> outputs_;
  const std::vector<std::string> outputNames_;
};

using GraphMatchNodePtr = std::shared_ptr<const GraphMatchNode>;

} // namespace facebook::axiom::logical_plan

AXIOM_ENUM_FORMATTER(facebook::axiom::logical_plan::SetOperation);
AXIOM_ENUM_FORMATTER(facebook::axiom::logical_plan::WriteKind);
AXIOM_EMBEDDED_ENUM_FORMATTER(
    facebook::axiom::logical_plan::FixedPointNode,
    OutputMode);
