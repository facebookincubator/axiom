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
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Schema.h"
#include "velox/core/PlanNode.h"

/// Defines subclasses of PlanObject for describing the logical
/// structure of queries. These are the constraints that guide
/// generation of plan candidates. These are referenced from
/// candidates but stay immutable acrosss the candidate
/// generation. Sometimes new derived tables may be added for
/// representing constraints on partial plans but otherwise these stay
/// constant.
namespace facebook::axiom::optimizer {

/// Superclass for all expressions.
class Expr : public PlanObject {
 public:
  Expr(PlanType type, const Value& value) : PlanObject(type), value_(value) {}

  bool isExpr() const override {
    return true;
  }

  /// Returns the single base or derived table 'this' depends on, nullptr if
  /// 'this' depends on none or multiple tables.
  PlanObjectCP singleTable() const;

  /// Returns all tables 'this' depends on.
  PlanObjectSet allTables() const;

  /// True if '&other == this' or is recursively equal with column
  /// leaves either same or in same equivalence.
  bool sameOrEqual(const Expr& other) const;

  const PlanObjectSet& columns() const {
    return columns_;
  }

  const PlanObjectSet& subexpressions() const {
    return subexpressions_;
  }

  const Value& value() const {
    return value_;
  }

  bool containsNonDeterministic() const {
    return containsFunction(FunctionSet::kNonDeterministic);
  }

  /// True if 'this' contains any function from 'set'. See FunctionSet.
  virtual bool containsFunction(uint64_t /*set*/) const {
    return false;
  }

  virtual const FunctionSet& functions() const;

 protected:
  // The columns this depends on.
  PlanObjectSet columns_;

  // All expressions 'this' depends on.
  PlanObjectSet subexpressions_;

  // Type Constraints on the value of 'this'.
  Value value_;
};

struct Equivalence;
using EquivalenceP = Equivalence*;

/// Represents a literal.
class Literal : public Expr {
 public:
  Literal(const Value& value, const velox::Variant* literal)
      : Expr(PlanType::kLiteralExpr, value),
        literal_(literal),
        vector_(nullptr) {}

  Literal(const Value& value, const velox::BaseVector* vector)
      : Expr(PlanType::kLiteralExpr, value), literal_{}, vector_(vector) {}

  const velox::Variant& literal() const {
    return *literal_;
  }

  bool hasVector() const {
    return vector_ != nullptr;
  }

  const velox::BaseVector* vector() const {
    return vector_;
  }

  std::string toString() const override;

 private:
  const velox::Variant* const literal_;
  const velox::BaseVector* const vector_;
};

/// Represents a column. A column is always defined by a relation, whether table
/// or derived table.
class Column : public Expr {
 public:
  Column(
      Name name,
      PlanObjectCP relation,
      const Value& value,
      Name alias = nullptr,
      Name nameInTable = nullptr,
      ColumnCP topColumn = nullptr,
      PathCP path = nullptr);

  Name name() const {
    return name_;
  }

  PlanObjectCP relation() const {
    return relation_;
  }

  Name alias() const {
    return alias_;
  }

  ColumnCP schemaColumn() const {
    return schemaColumn_;
  }

  // Returns column name to use in the Velox plan.
  std::string outputName() const {
    return alias_ != nullptr ? alias_ : toString();
  }

  /// Asserts that 'this' and 'other' are joined on equality. This has a
  /// transitive effect, so if a and b are previously asserted equal and c is
  /// asserted equal to b, a and c are also equal.
  void equals(ColumnCP other) const;

  std::string toString() const override;

  EquivalenceP equivalence() const {
    return equivalence_;
  }

  ColumnCP topColumn() const {
    return topColumn_;
  }

  PathCP path() const {
    return path_;
  }

 private:
  // Last part of qualified name.
  Name name_;

  // The defining BaseTable or DerivedTable.
  PlanObjectCP relation_;

  // Optional alias copied from the the logical plan.
  Name alias_;

  // Equivalence class. Lists all columns directly or indirectly asserted equal
  // to 'this'.
  mutable EquivalenceP equivalence_{nullptr};

  // If this is a column of a BaseTable, points to the corresponding
  // column in the SchemaTable. Used for matching with
  // ordering/partitioning columns in the SchemaTable.
  ColumnCP schemaColumn_{nullptr};

  // Containing top level column if 'this' is a subfield projected out as
  // column.
  ColumnCP topColumn_;

  // Path from 'topColumn'.
  PathCP path_;
};

class Field : public Expr {
 public:
  Field(const velox::Type* type, ExprCP base, Name field)
      : Expr(PlanType::kFieldExpr, Value(type, 1)),
        field_(field),
        index_(0),
        base_(base) {
    columns_ = base->columns();
    subexpressions_ = base->subexpressions();
  }

  Field(const velox::Type* type, ExprCP base, int32_t index)
      : Expr(PlanType::kFieldExpr, Value(type, 1)),
        field_(nullptr),
        index_(index),
        base_(base) {
    columns_ = base->columns();
    subexpressions_ = base->subexpressions();
  }

  Name field() const {
    return field_;
  }

  int32_t index() const {
    return index_;
  }

  std::string toString() const override;

  ExprCP base() const {
    return base_;
  }

 private:
  Name field_;
  int32_t index_;
  ExprCP base_;
};

struct SubfieldSet {
  /// Id of an accessed column of complex type.
  QGVector<int32_t> ids;

  /// Set of subfield paths that are accessed for the corresponding 'column'.
  /// empty means that all subfields are accessed.
  QGVector<BitSet> subfields;

  std::optional<BitSet> findSubfields(int32_t id) const;
};

struct FunctionMetadata;
using FunctionMetadataCP = const FunctionMetadata*;

/// Represents a function call or a special form, any expression with
/// subexpressions.
class Call : public Expr {
 public:
  Call(
      PlanType type,
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions);

  Call(Name name, Value value, ExprVector args, FunctionSet functions)
      : Call(PlanType::kCallExpr, name, value, std::move(args), functions) {}

  Name name() const {
    return name_;
  }

  const FunctionSet& functions() const override {
    return functions_;
  }

  bool isFunction() const override {
    return true;
  }

  bool containsFunction(uint64_t set) const override {
    return functions_.contains(set);
  }

  const ExprVector& args() const {
    return args_;
  }

  ExprCP argAt(size_t index) const {
    return args_[index];
  }

  CPSpan<PlanObject> children() const override {
    return {reinterpret_cast<PlanObjectCP const*>(args_.data()), args_.size()};
  }

  std::string toString() const override;

  FunctionMetadataCP metadata() const {
    return metadata_;
  }

 private:
  // name of function.
  Name const name_;

  // Arguments.
  const ExprVector args_;

  // Set of functions used in 'this' and 'args'.
  const FunctionSet functions_;

  FunctionMetadataCP metadata_;
};

using CallCP = const Call*;

struct SpecialFormCallNames {
  static const char* kAnd;
  static const char* kOr;
  static const char* kCast;
  static const char* kTryCast;
  static const char* kTry;
  static const char* kCoalesce;
  static const char* kIf;
  static const char* kSwitch;
  static const char* kIn;

  static const char* toCallName(const logical_plan::SpecialForm& form) {
    switch (form) {
      case logical_plan::SpecialForm::kAnd:
        return SpecialFormCallNames::kAnd;
      case logical_plan::SpecialForm::kOr:
        return SpecialFormCallNames::kOr;
      case logical_plan::SpecialForm::kCast:
        return SpecialFormCallNames::kCast;
      case logical_plan::SpecialForm::kTryCast:
        return SpecialFormCallNames::kTryCast;
      case logical_plan::SpecialForm::kTry:
        return SpecialFormCallNames::kTry;
      case logical_plan::SpecialForm::kCoalesce:
        return SpecialFormCallNames::kCoalesce;
      case logical_plan::SpecialForm::kIf:
        return SpecialFormCallNames::kIf;
      case logical_plan::SpecialForm::kSwitch:
        return SpecialFormCallNames::kSwitch;
      case logical_plan::SpecialForm::kIn:
        return SpecialFormCallNames::kIn;
      default:
        VELOX_FAIL(
            "No function call name for special form: {}",
            logical_plan::SpecialFormName::toName(form));
    }
  }

  static std::optional<logical_plan::SpecialForm> tryFromCallName(
      const char* name) {
    if (name == kAnd) {
      return logical_plan::SpecialForm::kAnd;
    }
    if (name == kOr) {
      return logical_plan::SpecialForm::kOr;
    }
    if (name == kCast) {
      return logical_plan::SpecialForm::kCast;
    }
    if (name == kTryCast) {
      return logical_plan::SpecialForm::kTryCast;
    }
    if (name == kTry) {
      return logical_plan::SpecialForm::kTry;
    }
    if (name == kCoalesce) {
      return logical_plan::SpecialForm::kCoalesce;
    }
    if (name == kIf) {
      return logical_plan::SpecialForm::kIf;
    }
    if (name == kSwitch) {
      return logical_plan::SpecialForm::kSwitch;
    }
    if (name == kIn) {
      return logical_plan::SpecialForm::kIn;
    }

    return std::nullopt;
  }
};

/// True if 'expr' is a call to function 'name'.
inline bool isCallExpr(ExprCP expr, Name name) {
  return expr->is(PlanType::kCallExpr) && expr->as<Call>()->name() == name;
}

/// Represents a lambda. May occur as an immediate argument of selected
/// functions.
class Lambda : public Expr {
 public:
  Lambda(ColumnVector args, const velox::Type* type, ExprCP body)
      : Expr(PlanType::kLambdaExpr, Value(type, 1)),
        args_(std::move(args)),
        body_(body) {}
  const ColumnVector& args() const {
    return args_;
  }

  ExprCP body() const {
    return body_;
  }

 private:
  ColumnVector args_;
  ExprCP body_;
};

/// Represens a set of transitively equal columns.
struct Equivalence {
  /// Each element has a direct or implied equality edge to every other.
  ColumnVector columns;
};

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
  const float fanout;
  const bool isOptional;
  const bool isNonOptionalOfOuter;
  const bool isExists;
  const bool isNotExists;
  ColumnCP markColumn;
  const bool isUnique;

  /// Returns the join type to use if 'this' is the right side.
  velox::core::JoinType leftJoinType() const {
    if (isNotExists) {
      return velox::core::JoinType::kAnti;
    }
    if (isExists) {
      return velox::core::JoinType::kLeftSemiFilter;
    }
    if (isOptional) {
      return velox::core::JoinType::kLeft;
    }
    if (isNonOptionalOfOuter) {
      return velox::core::JoinType::kRight;
    }

    if (markColumn) {
      return velox::core::JoinType::kLeftSemiProject;
    }
    return velox::core::JoinType::kInner;
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
    ExprVector filter;
    bool leftOptional{false};
    bool rightOptional{false};
    bool rightExists{false};
    bool rightNotExists{false};
    ColumnCP markColumn{nullptr};
    bool directed{false};
  };

  JoinEdge(PlanObjectCP leftTable, PlanObjectCP rightTable, Spec spec)
      : leftTable_(leftTable),
        rightTable_(rightTable),
        filter_(std::move(spec.filter)),
        leftOptional_(spec.leftOptional),
        rightOptional_(spec.rightOptional),
        rightExists_(spec.rightExists),
        rightNotExists_(spec.rightNotExists),
        directed_(spec.directed),
        markColumn_(spec.markColumn) {
    VELOX_CHECK_NOT_NULL(rightTable);
    // filter_ is only for non-inner joins.
    VELOX_CHECK(filter_.empty() || !isInner());
  }

  static JoinEdge* makeInner(PlanObjectCP leftTable, PlanObjectCP rightTable) {
    return make<JoinEdge>(leftTable, rightTable, Spec{});
  }

  static JoinEdge* makeExists(PlanObjectCP leftTable, PlanObjectCP rightTable) {
    return make<JoinEdge>(leftTable, rightTable, Spec{.rightExists = true});
  }

  static JoinEdge* makeNotExists(
      PlanObjectCP leftTable,
      PlanObjectCP rightTable) {
    return make<JoinEdge>(leftTable, rightTable, Spec{.rightNotExists = true});
  }

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

  float lrFanout() const {
    return lrFanout_;
  }

  float rlFanout() const {
    return rlFanout_;
  }

  bool leftOptional() const {
    return leftOptional_;
  }

  bool rightOptional() const {
    return rightOptional_;
  }

  bool directed() const {
    return directed_;
  }

  void addEquality(ExprCP left, ExprCP right, bool update = false);

  /// True if inner join.
  bool isInner() const {
    return !leftOptional_ && !rightOptional_ && !rightExists_ &&
        !rightNotExists_;
  }

  bool isSemi() const {
    return rightExists_;
  }

  bool isAnti() const {
    return rightNotExists_;
  }

  /// True if all tables referenced from 'leftKeys' must be placed before
  /// placing this.
  bool isNonCommutative() const {
    // Inner and full outer joins are commutative.
    if (rightOptional_ && leftOptional_) {
      return false;
    }

    return !leftTable_ || rightOptional_ || leftOptional_ || rightExists_ ||
        rightNotExists_ || markColumn_ || directed_;
  }

  /// True if has a hash based variant that builds on the left and probes on the
  /// right.
  bool hasRightHashVariant() const {
    return isNonCommutative() && !rightNotExists_;
  }

  /// Returns the join side info for 'table'. If 'other' is set, returns the
  /// other side.
  JoinSide sideOf(PlanObjectCP side, bool other = false) const;

  /// Returns the table on the other side of 'table' and the number of rows in
  /// the returned table for one row in 'table'. If the join is not inner
  /// returns {nullptr, 0}.
  std::pair<PlanObjectCP, float> otherTable(PlanObjectCP table) const {
    return leftTable_ == table && !leftOptional_
        ? std::pair<PlanObjectCP, float>{rightTable_, lrFanout_}
        : rightTable_ == table && !rightOptional_ && !rightExists_
        ? std::pair<PlanObjectCP, float>{leftTable_, rlFanout_}
        : std::pair<PlanObjectCP, float>{nullptr, 0};
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

  void setFanouts(float rightToLeft, float leftToRight) {
    fanoutsFixed_ = true;
    lrFanout_ = rightToLeft;
    rlFanout_ = leftToRight;
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
  float lrFanout_{1};

  // Number of left side rows selected for one row on the right.
  float rlFanout_{1};

  // True if 'lrFanout_' and 'rlFanout_' are set by setFanouts.
  bool fanoutsFixed_{false};

  // 'rightKeys' select max 1 'leftTable' row.
  bool leftUnique_{false};

  // 'leftKeys' select max 1 'rightTable' row.
  bool rightUnique_{false};

  // True if an unprobed right side row produces a result with right side
  // columns set and left side columns as null (right outer join). Possible only
  // for hash or merge.
  const bool leftOptional_;

  // True if a right side miss produces a row with left side columns
  // and a null for right side columns (left outer join). A full outer
  // join has both left and right optional.
  const bool rightOptional_;

  // True if the right side is only checked for existence of a match. If
  // rightOptional is set, this can project out a null for misses.
  const bool rightExists_;

  // True if produces a result for left if no match on the right.
  const bool rightNotExists_;

  // If directed non-outer edge. For example unnest or inner dependent on
  // optional of outer.
  const bool directed_;

  // Flag to set if right side has a match.
  ColumnCP const markColumn_;
};

using JoinEdgeP = JoinEdge*;
using JoinEdgeVector = QGVector<JoinEdgeP>;

/// Represents a reference to a table from a query. There is one of these
/// for each occurrence of the schema table. A TableScan references one
/// BaseTable but the same BaseTable can be referenced from many TableScans, for
/// example if accessing different indices in a secondary to primary key lookup.
struct BaseTable : public PlanObject {
  BaseTable() : PlanObject(PlanType::kTableNode) {}

  /// Correlation name, distinguishes between uses of the same schema table.
  Name cname{nullptr};

  SchemaTableCP schemaTable{nullptr};

  /// All columns referenced from 'schemaTable' under this correlation name.
  /// Different indices may have to be combined in different TableScans to cover
  /// 'columns'.
  ColumnVector columns;

  /// All joins where 'this' is an end point.
  JoinEdgeVector joinedBy;

  /// Top level conjuncts on single columns and literals, column to the left.
  ExprVector columnFilters;

  /// Multicolumn filters dependent on 'this' alone.
  ExprVector filter;

  /// The fraction of base table rows selected by all filters involving this
  /// table only.
  float filterSelectivity{1};

  SubfieldSet controlSubfields;

  SubfieldSet payloadSubfields;

  bool isTable() const override {
    return true;
  }

  void addJoinedBy(JoinEdgeP join);

  /// Adds 'expr' to 'filters' or 'columnFilters'.
  void addFilter(ExprCP expr);

  std::optional<int32_t> columnId(Name column) const;

  BitSet columnSubfields(int32_t id, bool controlOnly, bool payloadOnly) const;

  /// Returns possible indices for driving table scan of 'table'.
  std::vector<ColumnGroupCP> chooseLeafIndex() const {
    VELOX_DCHECK(!schemaTable->columnGroups.empty());
    return {schemaTable->columnGroups[0]};
  }

  std::string toString() const override;
};

using BaseTableCP = const BaseTable*;

struct ValuesTable : public PlanObject {
  explicit ValuesTable(const logical_plan::ValuesNode& values)
      : PlanObject{PlanType::kValuesTableNode}, values{values} {}

  /// Correlation name, distinguishes between uses of the same values node.
  Name cname{nullptr};

  const logical_plan::ValuesNode& values;

  /// All columns referenced from this 'ValuesNode'.
  ColumnVector columns;

  /// All joins where 'this' is an end point.
  JoinEdgeVector joinedBy;

  float cardinality() const {
    return static_cast<float>(values.cardinality());
  }

  bool isTable() const override {
    return true;
  }

  void addJoinedBy(JoinEdgeP join);

  std::string toString() const override;
};

struct UnnestTable : public PlanObject {
  explicit UnnestTable() : PlanObject{PlanType::kUnnestTableNode} {}

  // Correlation name, distinguishes between uses of the same unnest node.
  Name cname{nullptr};

  /// All unnested columns from corresponding unnest node.
  /// All replicated columns is on other (left) side of the join edge.
  ColumnVector columns;

  // All joins where 'this' is an end point.
  JoinEdgeVector joinedBy;

  float cardinality() const {
    // TODO Should be changed later to actual cardinality.
    return 1;
  }

  bool isTable() const override {
    return true;
  }

  void addJoinedBy(JoinEdgeP join);

  std::string toString() const override;
};

using TypeVector = QGVector<const velox::Type*>;

// Aggregate function. The aggregation and arguments are in the
// inherited Call. The Value pertains to the aggregation
// result or accumulator.
class Aggregate : public Call {
 public:
  Aggregate(
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions,
      bool isDistinct,
      ExprCP condition,
      const velox::Type* intermediateType)
      : Call(
            PlanType::kAggregateExpr,
            name,
            value,
            std::move(args),
            functions | FunctionSet::kAggregate),
        isDistinct_(isDistinct),
        condition_(condition),
        intermediateType_(intermediateType) {
    for (auto& arg : this->args()) {
      rawInputType_.push_back(arg->value().type);
    }
    if (condition_) {
      columns_.unionSet(condition_->columns());
    }
  }

  ExprCP condition() const {
    return condition_;
  }

  bool isDistinct() const {
    return isDistinct_;
  }

  const velox::Type* intermediateType() const {
    return intermediateType_;
  }

  const TypeVector& rawInputType() const {
    return rawInputType_;
  }

 private:
  bool isDistinct_;
  ExprCP condition_;
  const velox::Type* intermediateType_;
  TypeVector rawInputType_;
};

using AggregateCP = const Aggregate*;
using AggregateVector = QGVector<AggregateCP>;

class AggregationPlan : public PlanObject {
 public:
  AggregationPlan(
      ExprVector groupingKeys,
      AggregateVector aggregates,
      ColumnVector columns,
      ColumnVector intermediateColumns)
      : PlanObject(PlanType::kAggregationNode),
        groupingKeys_(std::move(groupingKeys)),
        aggregates_(std::move(aggregates)),
        columns_(std::move(columns)),
        intermediateColumns_(std::move(intermediateColumns)) {
    VELOX_CHECK(!groupingKeys_.empty() || !aggregates_.empty());
    VELOX_CHECK_EQ(groupingKeys_.size() + aggregates_.size(), columns_.size());
    VELOX_CHECK_EQ(columns_.size(), intermediateColumns_.size());
  }

  const ExprVector& groupingKeys() const {
    return groupingKeys_;
  }

  const AggregateVector& aggregates() const {
    return aggregates_;
  }

  const ColumnVector& columns() const {
    return columns_;
  }

  const ColumnVector& intermediateColumns() const {
    return intermediateColumns_;
  }

 private:
  const ExprVector groupingKeys_;
  const AggregateVector aggregates_;
  const ColumnVector columns_;
  const ColumnVector intermediateColumns_;
};

using AggregationPlanCP = const AggregationPlan*;

} // namespace facebook::axiom::optimizer
