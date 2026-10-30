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

#include <cmath>
#include <limits>
#include <span>

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PathSet.h"
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

  /// True if 'this' depends on any table whose id is in 'tables'.
  /// Equivalent to 'allTables().hasIntersection(tables)' but avoids
  /// materializing the intermediate set and short-circuits at the first
  /// match.
  bool hasAnyTableIn(const PlanObjectSet& tables) const;

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

  /// True if 'this' contains a function that does not return NULL for every
  /// NULL input, such as coalesce or IS NULL.
  bool containsNonDefaultNullBehavior() const {
    return containsFunction(FunctionSet::kNonDefaultNullBehavior);
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
  /// Takes the literal's statistics from 'value' and completes them from the
  /// literal itself: a NULL literal is all nulls; any other has no nulls, a
  /// primitive one is its own min and max, and a boolean one is all true or
  /// all false.
  Literal(const Value& value, const velox::Variant* literal);

  const velox::Variant& literal() const {
    return *literal_;
  }

  std::string toString() const override {
    return literal_->toJson(*value().type);
  }

  /// Non-owning identity view for interning a `Literal` by content: result type
  /// (canonical pointer) and the Variant, compared by value.
  struct KeyView {
    const velox::Type* type;
    const velox::Variant* value;
  };

  /// Transparent hasher for interning `Literal`s; `is_transparent` enables
  /// heterogeneous lookup by `KeyView` with no per-lookup allocation.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Literal* literal) const;
    size_t operator()(const KeyView& key) const;
  };

  /// Transparent equality for interning `Literal`s; the Variant is compared by
  /// value, so equal values dedup across distinct Variant objects.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Literal* left, const Literal* right) const;
    bool operator()(const KeyView& key, const Literal* literal) const;
    bool operator()(const Literal* literal, const KeyView& key) const;
  };

 private:
  const velox::Variant* const literal_;
};

/// Represents a column or a subfield.
class Column : public Expr {
 public:
  /// @param value Type and cardinality (number of distinct values / ndv) of
  /// 'this'. Cardinality represents the data at the time it is first produced
  /// by a table. Cardinality may change as the column flows through filters and
  /// joins.
  /// @param nameInTable Name of the column in the BaseTable. Nullptr if
  /// 'relation' is not a BaseTable. Used to populate 'schemaColumn'.
  Column(
      Name name,
      PlanObjectCP relation,
      const Value& value,
      Name alias = nullptr,
      Name nameInTable = nullptr,
      ColumnCP topColumn = nullptr,
      PathCP path = nullptr);

  /// Synthesizes a `Column`. The column's `name` is `prefix` with a
  /// uniqueness suffix appended by `QueryGraphContext::newName`. The
  /// resulting name appears in plan output (EXPLAIN, `NodePrinter`),
  /// so pick a descriptive prefix (`"__rownum"`, `"mark"`, etc.).
  /// `alias` is not set; sites that produce user-visible LP output
  /// names pass an `alias` to the full constructor instead.
  static ColumnCP create(std::string_view prefix, const Value& value);

  /// Creates a `Column` named by the logical-plan `symbol` it carries. The
  /// internal `name` is uniquified for the optimizer's flat namespace, while
  /// `outputName()` returns `symbol` (the alias), so the emitted plan uses the
  /// logical-plan name. Use whenever a translated logical-plan node produces a
  /// named column; synthetic columns use `create`.
  static ColumnCP createForSymbol(Name symbol, const Value& value);

  /// Creates a fresh identity for a NULL-extending boundary. Preserves
  /// subfield provenance and facts about non-null values, and clears facts
  /// affected by added NULLs. Keeps the source's output name because the source
  /// is visible below the boundary and this column replaces it above. The two
  /// identities never coexist in one row schema.
  static ColumnCP createForNullExtendedValue(ColumnCP source);

  /// Creates a fresh identity with the same emitted name, type and subfield
  /// provenance, and unknown value facts. The source is visible below the
  /// value-changing boundary and this column replaces it above. The two
  /// identities never coexist in one row schema.
  static ColumnCP createWithUnknownValue(ColumnCP source);

  /// Synthesizes a `BOOLEAN` column with cardinality 2 (the two valid
  /// boolean values). Convenience for the recurring pattern of
  /// creating eligibility / marker columns in rewrites; equivalent to
  /// `create(prefix, Value(toType(velox::BOOLEAN()), 2))`.
  static ColumnCP createBoolean(std::string_view prefix);

  Name name() const {
    return name_;
  }

  /// A relation that produced the column. Nullptr if 'this' is an argument of a
  /// lambda.
  PlanObjectCP relation() const {
    return relation_;
  }

  Name alias() const {
    return alias_;
  }

  /// Name of the column in the BaseTable. Nullptr if 'relation' is not a
  /// BaseTable.
  ColumnCP schemaColumn() const {
    return schemaColumn_;
  }

  /// Returns the name of the underlying table column.
  Name schemaName() const {
    VELOX_CHECK_NOT_NULL(schemaColumn_);
    return schemaColumn_->name();
  }

  /// Returns column name to use in the Velox plan.
  std::string outputName() const {
    return alias_ != nullptr ? alias_ : toString();
  }

  /// Asserts that 'this' and 'other' hold the same value on every row the
  /// plan produces, so either may stand for the other. The relation is
  /// transitive, so if a and b are previously asserted equal and c is asserted
  /// equal to b, a and c are also equal.
  ///
  /// Only inner joins assert this. An outer join null-pads its non-preserved
  /// side, so its key columns are not equal in its output. A join that emits
  /// one side only proves the equality but cannot supply both columns, so it
  /// registers its class per cover instead of here.
  ///
  /// The relation is recorded on the columns themselves, so it carries no
  /// notion of where it starts holding: below the join that proves it the
  /// columns are unrelated. A consumer must therefore never use it to remove
  /// the join key or filter that enforces it.
  ///
  /// Returns true when this call merged two classes that were separate, so a
  /// caller can act on a newly proved equality exactly once.
  bool equals(ColumnCP other) const;

  std::string toString() const override;

  EquivalenceP equivalence() const {
    return equivalence_;
  }

  /// Set if 'this' is a subfield (a key of a map, an element of an array or a
  /// field of a struct).
  ColumnCP topColumn() const {
    return topColumn_;
  }

  /// Path from 'topColumn'.
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

  // Equivalence class. See Column::equals.
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
        base_(base),
        functions_(base->functions()) {
    columns_ = base->columns();
    subexpressions_ = base->subexpressions();
  }

  Field(const velox::Type* type, ExprCP base, int32_t index)
      : Expr(PlanType::kFieldExpr, Value(type, 1)),
        field_(nullptr),
        index_(index),
        base_(base),
        functions_(base->functions()) {
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

  bool containsFunction(uint64_t set) const override {
    return functions_.contains(set);
  }

  const FunctionSet& functions() const override {
    return functions_;
  }

 private:
  Name field_;
  int32_t index_;
  ExprCP base_;
  const FunctionSet functions_;
};

struct SubfieldSet {
  /// Id of an accessed column of complex type.
  QGVector<int32_t> ids;

  /// Set of subfield paths that are accessed for the corresponding 'column'.
  /// empty means that all subfields are accessed.
  QGVector<PathSet> subfields;

  std::optional<PathSet> findSubfields(int32_t id) const;
};

struct FunctionMetadata;
using FunctionMetadataCP = const FunctionMetadata*;

/// Represents a function call or a special form, any expression with
/// subexpressions.
class Call : public Expr {
 protected:
  /// To be used by derived classes only.
  Call(
      PlanType type,
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions);

 public:
  /// 'functions' should be the call's own function-property bits
  /// (deterministic, default-null-behavior, aggregate kind, etc.)
  /// OR-ed with each arg's 'functions()'. The Call ctor stores
  /// 'functions' as-is and does not aggregate from args, so the caller
  /// is responsible for OR-ing. 'functions' is queried by
  /// 'containsFunction' to decide whether a subtree contains, e.g., a
  /// non-deterministic call. Build the call's own bits with
  /// 'functionBits(name, /*specialForm=*/false)' for regular function
  /// calls and 'functionBits(name, /*specialForm=*/true)' for special
  /// forms.
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

  /// Returns the value of argument 'index', which must be a literal.
  const velox::Variant& literalAt(size_t index) const {
    VELOX_CHECK(args_[index]->is(PlanType::kLiteralExpr));
    return args_[index]->as<Literal>()->literal();
  }

  CPSpan<PlanObject> children() const override {
    return {reinterpret_cast<PlanObjectCP const*>(args_.data()), args_.size()};
  }

  std::string toString() const override;

  FunctionMetadataCP metadata() const {
    return metadata_;
  }

  /// Returns true if this call constructs a row.
  bool isRowConstructor() const;

  /// Returns the values of an IN over a constant list in the form
  /// '__in(value, <literal of type ARRAY(T)>)', where 'value' has type T.
  /// Returns nullptr if this call is not one. In v2, translation and the
  /// simplifier build every IN over constants in this form, so code that runs
  /// after translation sees no other form of an IN whose list is all literals.
  const std::vector<velox::Variant>* constantInList() const;

  /// Non-owning identity view for interning a `Call` by content. Holds only
  /// what defines identity: function name, result type (distinguishes CAST-like
  /// forms that share name + args), and args by pointer.
  struct KeyView {
    Name name;
    const velox::Type* type;
    std::span<const ExprCP> args;
  };

  /// Transparent hasher for interning `Call`s. `is_transparent` enables
  /// heterogeneous lookup by `KeyView`, so a cache hit needs no allocation.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Call* call) const;
    size_t operator()(const KeyView& key) const;
  };

  /// Transparent equality for interning `Call`s by content.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Call* left, const Call* right) const;
    bool operator()(const KeyView& key, const Call* call) const;
    bool operator()(const Call* call, const KeyView& key) const;
  };

  /// Returns 'base' OR'd with the `FunctionSet` of each argument. Used
  /// when constructing a new `Call` to propagate flags
  /// (kNonDeterministic, kAggregate, kWindow, etc.) that downstream
  /// passes consult via `Expr::containsFunction(...)`. Pair with
  /// `functionBits(name, specialForm)` to seed `base` with the
  /// function's own bits.
  static FunctionSet unionArgFunctions(
      FunctionSet base,
      const ExprVector& arguments) {
    for (const auto* argument : arguments) {
      base = base | argument->functions();
    }
    return base;
  }

 private:
  // Name of function.
  Name const name_;

  // Arguments.
  const ExprVector args_;

  // Set of functions used in 'this' and 'args'.
  const FunctionSet functions_;

  FunctionMetadataCP metadata_;
};

using CallCP = const Call*;

/// Canonical names for special-form calls. Each member is a stable
/// 'Name' pointer that 'QueryGraphContext' pre-registers in its
/// interning table, so 'toName("__and")' returns the same address as
/// 'SpecialFormCallNames::kAnd'. Use these constants directly wherever
/// a 'Name' is expected — no 'toName()' wrapper needed.
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

  static const char* kNullIf;
  static const char* kDereference;
  static const char* kRowConstructor;

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
      case logical_plan::SpecialForm::kInArray:
        return SpecialFormCallNames::kIn;
      case logical_plan::SpecialForm::kNullIf:
        return SpecialFormCallNames::kNullIf;
      case logical_plan::SpecialForm::kDereference:
        return SpecialFormCallNames::kDereference;
      case logical_plan::SpecialForm::kRowConstructor:
        return SpecialFormCallNames::kRowConstructor;
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
    if (name == kNullIf) {
      return logical_plan::SpecialForm::kNullIf;
    }
    if (name == kDereference) {
      return logical_plan::SpecialForm::kDereference;
    }
    if (name == kRowConstructor) {
      return logical_plan::SpecialForm::kRowConstructor;
    }

    return std::nullopt;
  }

  static bool isSpecialForm(const char* name) {
    return tryFromCallName(name).has_value();
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
        body_(body),
        functions_(body->functions()) {
    auto columns = body_->columns();
    for (auto arg : args_) {
      columns.erase(arg);
    }

    columns_.unionSet(columns);
  }

  const ColumnVector& args() const {
    return args_;
  }

  ExprCP body() const {
    return body_;
  }

  bool containsFunction(uint64_t set) const override {
    return functions_.contains(set);
  }

  const FunctionSet& functions() const override {
    return functions_;
  }

 private:
  ColumnVector args_;
  ExprCP body_;
  const FunctionSet functions_;
};

/// Represens a set of transitively equal columns.
struct Equivalence {
  /// Each element has a direct or implied equality edge to every other.
  ColumnVector columns;
};

class JoinEdge;
using JoinEdgeP = JoinEdge*;
using JoinEdgeVector = QGVector<JoinEdgeP>;

/// Polymorphic base for the table-shaped query-graph nodes. Callers
/// iterating over `DerivedTable::tables` reach for `columns`, `cardinality`,
/// or `joinedBy` on the base instead of switching on `PlanType`:
///
///   table->as<TableObject>()->columns
class TableObject : public PlanObject {
 public:
  explicit TableObject(PlanType type, Name cname = nullptr)
      : PlanObject{type}, cname{cname} {}

  /// Estimated number of rows in this table's output, or `nullopt` when
  /// not yet known.
  virtual std::optional<float> cardinality() const = 0;

  /// Correlation name, distinguishes between uses of the same table.
  Name cname{nullptr};

  /// Output columns of this table.
  ColumnVector columns;

  /// Joins where this table is an end point.
  JoinEdgeVector joinedBy;

  bool isTable() const final override {
    return true;
  }
};

/// Represents a reference to a table from a query. There is one of these
/// for each occurrence of the schema table. A TableScan references one
/// BaseTable but the same BaseTable can be referenced from many TableScans, for
/// example if accessing different indices in a secondary to primary key lookup.
struct BaseTable : public TableObject {
  BaseTable(Name correlationName, SchemaTableCP schema)
      : TableObject{PlanType::kTableNode, correlationName},
        schemaTable{schema} {
    VELOX_CHECK_NOT_NULL(schemaTable);
  }

  SchemaTableCP schemaTable{nullptr};

  /// Top level conjuncts on single columns and literals, column to the left.
  ExprVector columnFilters;

  /// Multicolumn filters dependent on 'this' alone.
  ExprVector filter;

  /// Estimated number of rows after applying all filters involving this table
  /// only. Initialized to schemaTable->cardinality (no filtering). nullopt if
  /// the cardinality is unknown.
  std::optional<float> filteredCardinality{0};

  /// Estimated rows the scan of this table reads, as reported by the
  /// connector: filters it resolves from metadata without reading are already
  /// reflected, filters it evaluates on rows it has read are not. See
  /// `connector::FilteredTableStats::numRawInputRows` for the full definition.
  /// Scaled by 'sampledPercentage' when the scan is sampled. nullopt when the
  /// connector does not report it.
  std::optional<uint64_t> numRawInputRows;

  /// Estimated size in bytes of a row the scan of this table reads, as reported
  /// by the connector; see
  /// `connector::FilteredTableStats::numRawInputBytesPerRow`. nullopt when the
  /// connector does not report it.
  std::optional<float> numRawInputBytesPerRow;

  /// Estimated number of splits the scan of this table produces, as reported
  /// by the connector; see `connector::FilteredTableStats::numSplits`. Scaled
  /// by 'sampledPercentage' when the scan is sampled. nullopt when the
  /// connector does not report it.
  std::optional<uint64_t> numSplits;

  /// Estimated bytes the scan of this table reads: 'numRawInputRows' times
  /// 'numRawInputBytesPerRow', rounded up and capped at the uint64_t maximum.
  /// nullopt when either is unknown.
  std::optional<uint64_t> numRawInputBytes() const {
    if (!numRawInputRows.has_value() || !numRawInputBytesPerRow.has_value()) {
      return std::nullopt;
    }
    const double bytes = std::ceil(
        static_cast<double>(*numRawInputRows) * *numRawInputBytesPerRow);
    constexpr auto kMax = std::numeric_limits<uint64_t>::max();
    return bytes >= static_cast<double>(kMax) ? kMax
                                              : static_cast<uint64_t>(bytes);
  }

  SubfieldSet controlSubfields;

  SubfieldSet payloadSubfields;

  /// TABLESAMPLE SYSTEM percentage in the open interval (0, 100). When set,
  /// split generation for this scan emits each split with this probability.
  /// nullopt = no sampling.
  std::optional<float> sampledPercentage;

  std::optional<float> cardinality() const override {
    return filteredCardinality;
  }

  /// Adds 'expr' to 'filter' or 'columnFilters'. Returns true if it added
  /// 'expr', false if 'filter' already had it.
  bool addFilter(ExprCP expr);

  std::optional<int32_t> columnId(Name column) const;

  PathSet columnSubfields(int32_t id) const;

  /// Returns possible indices for driving table scan of 'table'. Empty when
  /// every layout is lookup-only, which leaves index lookup as the sole way
  /// to reach the table.
  std::vector<ColumnGroupCP> chooseLeafIndex() const {
    VELOX_DCHECK(!schemaTable->columnGroups.empty());
    auto* group = schemaTable->columnGroups[0];
    if (!group->layout->supportsScan()) {
      return {};
    }
    return {group};
  }

  /// The connector table layout backing this scan. A BaseTable always has
  /// backing, so the schema table, its first column group, and the layout are
  /// all present.
  const connector::TableLayout* layout() const;

  std::string toString() const override;
};

using BaseTableCP = const BaseTable*;

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
      const velox::Type* intermediateType,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      std::optional<logical_plan::SpecialAggregateKind> specialKind =
          std::nullopt,
      const Aggregate* fallback = nullptr)
      : Call(
            PlanType::kAggregateExpr,
            name,
            value,
            std::move(args),
            functions | FunctionSet::kAggregate),
        isDistinct_(isDistinct),
        condition_(condition),
        intermediateType_(intermediateType),
        orderKeys_(std::move(orderKeys)),
        orderTypes_(std::move(orderTypes)),
        specialKind_(specialKind),
        fallback_(fallback) {
    VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());

    for (auto& arg : this->args()) {
      rawInputType_.push_back(arg->value().type);
    }
    if (condition_) {
      columns_.unionSet(condition_->columns());
    }
    for (auto& key : orderKeys_) {
      columns_.unionSet(key->columns());
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

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  /// The metadata-aggregate kind if this is a special-form metadata aggregate,
  /// or std::nullopt for an ordinary aggregate. A metadata aggregate is folded
  /// into constants from table metadata or replaced by 'fallback' before
  /// lowering; its 'name' is the canonical kind name and is never lowered to a
  /// Velox aggregate.
  const std::optional<logical_plan::SpecialAggregateKind>& specialKind() const {
    return specialKind_;
  }

  /// For a metadata aggregate, the ordinary aggregate (e.g. count) to use when
  /// no connector can answer it from metadata, or nullptr if none was supplied.
  const Aggregate* fallback() const {
    return fallback_;
  }

  /// Returns the typed result for empty input from the function registry.
  velox::Variant emptyInputResult() const;

  /// Returns a copy with isDistinct set to false.
  const Aggregate* dropDistinct() const;

  /// Returns a copy with isDistinct cleared and condition replaced by 'marker'.
  /// Any prior FILTER condition is dropped because the marker already encodes
  /// which rows pass the filter.
  const Aggregate* replaceDistinctAndFilterByMarker(ExprCP marker) const;

  std::string toString() const override;

  /// Non-owning identity view for interning an `Aggregate` by content. Result
  /// type and `intermediateType` are determined by (name, args), so they are
  /// excluded; args / orderKeys are compared by pointer.
  struct KeyView {
    Name name;
    std::span<const ExprCP> args;
    bool isDistinct;
    ExprCP condition;
    std::span<const ExprCP> orderKeys;
    std::span<const OrderType> orderTypes;
  };

  /// Transparent hasher for interning `Aggregate`s; `is_transparent` enables
  /// heterogeneous lookup by `KeyView` with no per-lookup allocation.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Aggregate* aggregate) const;
    size_t operator()(const KeyView& key) const;
  };

  /// Transparent equality for interning `Aggregate`s by content.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Aggregate* left, const Aggregate* right) const;
    bool operator()(const KeyView& key, const Aggregate* aggregate) const;
    bool operator()(const Aggregate* aggregate, const KeyView& key) const;
  };

 private:
  bool isDistinct_;
  ExprCP condition_;
  const velox::Type* intermediateType_;
  TypeVector rawInputType_;
  ExprVector orderKeys_;
  OrderTypeVector orderTypes_;
  const std::optional<logical_plan::SpecialAggregateKind> specialKind_;
  const Aggregate* fallback_;
};

using AggregateCP = const Aggregate*;
using AggregateVector = QGVector<AggregateCP>;

/// Describes the sliding window of rows processed by a window function.
/// Mirrors logical_plan::WindowExpr::Frame but uses QueryGraph expression
/// pointers (ExprCP) instead of logical plan expression pointers (ExprPtr).
struct Frame {
  logical_plan::WindowExpr::WindowType type;

  /// Type of the starting frame bound.
  logical_plan::WindowExpr::BoundType startType;

  /// Offset expression for the start bound, or nullptr for UNBOUNDED or
  /// CURRENT ROW.
  ExprCP startValue;

  /// Type of the ending frame bound.
  logical_plan::WindowExpr::BoundType endType;

  /// Offset expression for the end bound, or nullptr for UNBOUNDED or
  /// CURRENT ROW.
  ExprCP endValue;

  bool operator==(const Frame& other) const = default;

  /// True for a k RANGE frame: a RANGE frame with an n PRECEDING or n
  /// FOLLOWING bound, which Velox computes from the current row's ORDER BY key
  /// plus or minus n. Such a frame needs its single ORDER BY key.
  bool isKRangeFrame() const {
    return type == logical_plan::WindowExpr::WindowType::kRange &&
        (startValue != nullptr || endValue != nullptr);
  }

  /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING — every
  /// row sees the whole partition (e.g. a partition-wide aggregate).
  static Frame wholePartition() {
    return Frame{
        logical_plan::WindowExpr::WindowType::kRows,
        logical_plan::WindowExpr::BoundType::kUnboundedPreceding,
        /*startValue=*/nullptr,
        logical_plan::WindowExpr::BoundType::kUnboundedFollowing,
        /*endValue=*/nullptr};
  }

  /// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW — the default
  /// running frame (e.g. for row_number).
  static Frame toCurrentRow() {
    return Frame{
        logical_plan::WindowExpr::WindowType::kRows,
        logical_plan::WindowExpr::BoundType::kUnboundedPreceding,
        /*startValue=*/nullptr,
        logical_plan::WindowExpr::BoundType::kCurrentRow,
        /*endValue=*/nullptr};
  }
};

/// Window function call. Stores partition keys, order keys, frame, and
/// ignoreNulls in addition to inherited Call fields (function name, args).
class WindowFunction : public Call {
 public:
  WindowFunction(
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions,
      ExprVector partitionKeys,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      Frame frame,
      bool ignoreNulls)
      : Call(
            PlanType::kWindowExpr,
            name,
            value,
            std::move(args),
            functions | FunctionSet::kWindow),
        partitionKeys_(std::move(partitionKeys)),
        orderKeys_(std::move(orderKeys)),
        orderTypes_(std::move(orderTypes)),
        frame_(frame),
        ignoreNulls_(ignoreNulls) {
    VELOX_CHECK_EQ(orderKeys_.size(), orderTypes_.size());
    for (auto& key : partitionKeys_) {
      columns_.unionSet(key->columns());
    }
    for (auto& key : orderKeys_) {
      columns_.unionSet(key->columns());
    }
    if (frame_.startValue) {
      columns_.unionSet(frame_.startValue->columns());
    }
    if (frame_.endValue) {
      columns_.unionSet(frame_.endValue->columns());
    }
  }

  const ExprVector& partitionKeys() const {
    return partitionKeys_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  const Frame& frame() const {
    return frame_;
  }

  bool ignoreNulls() const {
    return ignoreNulls_;
  }

  std::string toString() const override;

 private:
  ExprVector partitionKeys_;
  ExprVector orderKeys_;
  OrderTypeVector orderTypes_;
  Frame frame_;
  bool ignoreNulls_;
};

using WindowFunctionCP = const WindowFunction*;
using WindowFunctionVector = QGVector<WindowFunctionCP>;

/// @return cname of 'relation', which must be a table (a TableObject).
Name cname(PlanObjectCP relation);

} // namespace facebook::axiom::optimizer
