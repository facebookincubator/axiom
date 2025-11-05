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
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"

namespace facebook::axiom::optimizer {

/// Struct for resolving which logical PlanNode or Lambda defines which
/// field for column and subfield tracking.
/// Only one of planNode or call + lambdaOrdinal is set.
struct LogicalContextSource {
  const logical_plan::LogicalPlanNode* planNode{nullptr};
  const logical_plan::CallExpr* call{nullptr};
  int32_t lambdaOrdinal{-1};
};

struct MarkFieldsAccessedContext {
  // 1:1 with 'sources'. Either output type of the plan node or signature of a
  // lambda.
  std::span<const velox::RowType* const> rowTypes;
  std::span<const LogicalContextSource> sources;
};

struct ExprDedupKey {
  Name func;
  std::span<const ExprCP> args;

  bool operator==(const ExprDedupKey& other) const {
    return func == other.func && std::ranges::equal(args, other.args);
  }
};

struct ExprDedupHasher {
  size_t operator()(const ExprDedupKey& key) const {
    size_t h =
        folly::hasher<uintptr_t>()(reinterpret_cast<uintptr_t>(key.func));
    for (auto& a : key.args) {
      h = velox::bits::hashMix(h, folly::hasher<ExprCP>()(a));
    }
    return h;
  }
};

using FunctionDedupMap =
    folly::F14FastMap<ExprDedupKey, ExprCP, ExprDedupHasher>;

struct TypedVariant {
  /// Canonical Type pointer returned by QueryGraphContext::toType.
  const velox::Type* type;
  std::shared_ptr<const velox::Variant> value;
};

struct TypedVariantHasher {
  size_t operator()(const TypedVariant& value) const {
    return velox::bits::hashMix(
        std::hash<const velox::Type*>()(value.type), value.value->hash());
  }
};

struct TypedVariantComparer {
  bool operator()(const TypedVariant& left, const TypedVariant& right) const {
    // Types have been deduped, hence, we compare pointers.
    return left.type == right.type && *left.value == *right.value;
  }
};

/// Represents a path over an Expr of complex type. Used as a key
/// for a map from unique step+optional subscript expr pairs to the
/// dedupped Expr that is the getter.
struct PathExpr {
  Step step;
  ExprCP subscriptExpr{nullptr};
  ExprCP base;

  bool operator==(const PathExpr& other) const = default;
};

struct PathExprHasher {
  size_t operator()(const PathExpr& expr) const {
    size_t hash = velox::bits::hashMix(expr.step.hash(), expr.base->id());
    return expr.subscriptExpr
        ? velox::bits::hashMix(hash, expr.subscriptExpr->id())
        : hash;
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
struct PlanSubfields {
  folly::F14FastMap<const logical_plan::LogicalPlanNode*, ResultAccess>
      nodeFields;
  folly::F14FastMap<const logical_plan::Expr*, ResultAccess> argFields;

  bool hasColumn(const logical_plan::LogicalPlanNode* node, int32_t ordinal)
      const {
    auto it = nodeFields.find(node);
    if (it == nodeFields.end()) {
      return false;
    }
    return it->second.resultPaths.contains(ordinal);
  }

  std::string toString() const;
};

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
  folly::F14FastMap<PathCP, ExprCP> pathToExpr;
};

class ToGraph {
 public:
  ToGraph(
      const connector::SchemaResolver& schemaResolver,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerOptions& options);

  /// Converts 'logicalPlan' to a tree of DerivedTables. Returns the root
  /// DerivedTable.
  DerivedTableP makeQueryGraph(
      const logical_plan::LogicalPlanNode& logicalPlan);

  // Sets the columns to project out from the root DerivedTable based on
  // 'logicalPlan'.
  void setDtOutput(DerivedTableP dt, const logical_plan::LogicalPlanNode& node);

  Name newCName(std::string_view prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  /// Creates or returns pre-existing function call with name+args. If
  /// deterministic, a new ExprCP is remembered for reuse.
  ExprCP
  deduppedCall(Name name, Value value, ExprVector args, FunctionSet flags);

  /// True if 'expr' is of the form a = b where a depends on leftTable and b on
  /// rightTable or vice versa. If true, returns the side depending on
  /// 'leftTable' in 'left' and the other in 'right'.
  bool isJoinEquality(
      ExprCP expr,
      PlanObjectCP leftTable,
      PlanObjectCP rightTable,
      ExprCP& left,
      ExprCP& right) const;

  velox::core::ExpressionEvaluator* evaluator() const {
    return &evaluator_;
  }

  template <typename Func>
  void trace(uint32_t event, Func f) {
    if ((options_.traceFlags & event) != 0) {
      f();
    }
  }

 private:
  static bool isSpecialForm(
      const logical_plan::ExprPtr& expr,
      logical_plan::SpecialForm form) {
    return expr->isSpecialForm() &&
        expr->as<logical_plan::SpecialFormExpr>()->form() == form;
  }

  // For comparisons, swaps the args to have a canonical form for
  // deduplication. E.g column op constant, and Smaller plan object id
  // to the left.
  void canonicalizeCall(Name& name, ExprVector& args);

  // Converts 'plan' to PlanObjects and records join edges into
  // 'currentDt_'. If 'node' does not match  allowedInDt, wraps 'node' in
  // a new DerivedTable.
  void makeQueryGraph(
      const logical_plan::LogicalPlanNode& node,
      uint64_t allowedInDt);

  PlanObjectCP findLeaf(const logical_plan::LogicalPlanNode* node) {
    auto* leaf = planLeaves_[node];
    VELOX_CHECK_NOT_NULL(leaf);
    return leaf;
  }

  // Returns the ordinal positions of actually referenced outputs of 'node'.
  std::vector<int32_t> usedChannels(const logical_plan::LogicalPlanNode& node);

  // if 'step' applied to result of the function of 'metadata'
  // corresponds to an argument, returns the ordinal of the argument.
  static std::optional<int32_t> stepToArg(
      const Step& step,
      const FunctionMetadata* metadata);

  // Returns a deduplicated Literal from the value in 'constant'.
  ExprCP makeConstant(const logical_plan::ConstantExpr& constant);

  // Folds a logical expr to a constant if can. Should be called only if
  // 'expr' only depends on constants. Identifier scope will may not be not
  // set at time of call. This is before regular constant folding because
  // subscript expressions must be folded for subfield resolution.
  logical_plan::ConstantExprPtr tryFoldConstant(
      const logical_plan::ExprPtr& expr);

  // Returns a literal by applying the function 'callName' with return type
  // 'returnType' to the input arguments 'literals'. Returns nullptr if not
  // successful. if not successful.
  ExprCP tryFoldConstant(
      const velox::TypePtr& returnType,
      std::string_view callName,
      const ExprVector& literals);

  // Converts 'name' to a deduplicated ExprCP. If 'name' is assigned to an
  // expression in a projection, returns the deduplicated ExprPtr of the
  // expression.
  ExprCP translateColumn(std::string_view name) const;

  //  Applies translateExpr to a 'source'.
  ExprVector translateExprs(const std::vector<logical_plan::ExprPtr>& source);

  // Makes a deduplicated Expr tree from 'expr'.
  ExprCP translateExpr(const logical_plan::ExprPtr& expr);

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

  // Translates a complex type function where the generated Exprs  depend on
  // the
  // accessed subfields.
  std::optional<ExprCP> translateSubfieldFunction(
      const logical_plan::CallExpr* call,
      const FunctionMetadata* metadata);

  // Adds conjuncts combined by any number of enclosing ands from 'input' to
  // 'flat'.
  void translateConjuncts(const logical_plan::ExprPtr& input, ExprVector& flat);

  // Adds a JoinEdge corresponding to 'join' to the enclosing DerivedTable.
  void translateJoin(const logical_plan::JoinNode& join);

  // Given an INTERSECT or an EXCEPT set operation, create derived tables for
  // inputs, add them to 'currentDt_' and connect them with join edges.
  void translateSetJoin(const logical_plan::SetNode& set);

  // Given a UNION or UNION ALL set operation, recursively flattens the inputs
  // and converts these into derived tables.
  // Flattens UNION ALL inside UNION and UNION ALL.
  // Flattens UNION inside UNION.
  // Example: u(a, u(b, c)) -> u(a, b, c)
  void translateUnion(const logical_plan::SetNode& set);

  void translateUnnest(
      const logical_plan::UnnestNode& logicalUnnest,
      bool isNewDt);

  AggregationPlanCP translateAggregation(
      const logical_plan::AggregateNode& aggregation);

  void addProjection(const logical_plan::ProjectNode& project);

  void addFilter(const logical_plan::FilterNode& filter);

  void addLimit(const logical_plan::LimitNode& limit);

  void addOrderBy(const logical_plan::SortNode& order);

  void addWrite(const logical_plan::TableWriteNode& tableWrite);

  bool isSubfield(
      const logical_plan::ExprPtr& expr,
      Step& step,
      logical_plan::ExprPtr& input);

  void getExprForField(
      const logical_plan::Expr* field,
      logical_plan::ExprPtr& resultExpr,
      ColumnCP& resultColumn,
      const logical_plan::LogicalPlanNode*& context);

  // Makes dedupped getters for 'steps'. if steps is below skyline,
  // nullptr. If 'steps' intersects 'skyline' returns skyline wrapped
  // in getters that are not in skyline. If no skyline, puts dedupped
  // getters defined by 'steps' on 'base' or 'column' if 'base' is
  // nullptr.
  ExprCP makeGettersOverSkyline(
      const std::vector<Step>& steps,
      const SubfieldProjections* skyline,
      const logical_plan::ExprPtr& base,
      ColumnCP column);

  void markSubfields(
      const logical_plan::ExprPtr& expr,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markFieldAccessed(
      const logical_plan::ProjectNode& project,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl);

  void markFieldAccessed(
      const logical_plan::UnnestNode& unnest,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl);

  void markFieldAccessed(
      const logical_plan::AggregateNode& agg,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl);

  void markFieldAccessed(
      const logical_plan::SetNode& set,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl);

  void markFieldAccessed(
      const LogicalContextSource& source,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  void markAllSubfields(
      const logical_plan::LogicalPlanNode& node,
      const MarkFieldsAccessedContext& context);

  void markControl(
      const logical_plan::LogicalPlanNode& node,
      const MarkFieldsAccessedContext& context);

  void markColumnSubfields(
      const logical_plan::LogicalPlanNodePtr& source,
      std::span<const logical_plan::ExprPtr> columns,
      bool isControl,
      const MarkFieldsAccessedContext& context);

  BitSet functionSubfields(
      const logical_plan::CallExpr* call,
      bool controlOnly,
      bool payloadOnly);

  // Calls translateSubfieldFunction() if not already called.
  void ensureFunctionSubfields(const logical_plan::ExprPtr& expr);

  void makeBaseTable(const logical_plan::TableScanNode& tableScan);

  void makeValuesTable(const logical_plan::ValuesNode& values);

  // Decomposes complex type columns into parts projected out as top
  // level if subfield pushdown is on.
  void makeSubfieldColumns(
      BaseTable* baseTable,
      ColumnCP column,
      const BitSet& paths);

  // Adds 'node' and descendants to query graph wrapped inside a
  // DerivedTable. Done for joins to the right of non-inner joins,
  // group bys as non-top operators, whenever descendents of 'node'
  // are not freely reorderable with its parents' descendents.
  // @return Newly created DT.
  DerivedTableP wrapInDt(const logical_plan::LogicalPlanNode& node);

  // Start new DT and add 'currentDt_' as a child.
  // Set 'currentDt_' to the new DT.
  // @return The finalized DT (which is now a child of 'currentDt_').
  DerivedTableP finalizeDt(
      const logical_plan::LogicalPlanNode& node,
      DerivedTableP outerDt = nullptr);

  // Finalizes 'currentDt_' if it contains aggregation, limit or anything else
  // that must be wrapped in a DT before being used as a left side of a join.
  void finalizeLeftDtForJoin(const logical_plan::LogicalPlanNode& node);

  // Adds a column 'name' from current DerivedTable to the 'dt'.
  void addDtColumn(DerivedTableP dt, std::string_view name);

  void setDtUsedOutput(
      DerivedTableP dt,
      const logical_plan::LogicalPlanNode& node);

  DerivedTableP newDt();

  // Removes duplicate ordering keys from the input vector of SortingField
  // objects, returning a pair of vectors containing the deduplicated keys and
  // their corresponding order types. It dedups by comparing the expressions of
  // the SortingField objects and ignores order-type. This is correct because
  // if the same expression appears multiple times with different sort orders,
  // only the first occurrence determines the actual sort behavior - subsequent
  // occurrences of the same expression are redundant since the column is
  // already sorted by the first occurrence.
  std::pair<ExprVector, OrderTypeVector> dedupOrdering(
      const std::vector<logical_plan::SortingField>& ordering);

  // Process non-correlated subqueries used in filter's predicate and populate
  // subqueries_ map. For each IN <subquery> expression, create a separate DT
  // for the subquery and add a semi-join edge. Replace the whole IN predicate
  // with a 'mark' column produced by the join. For other <subquery>
  // expressions, create a separate DT and replace the expression with the only
  // column produced by the DT.
  void processSubqueries(const logical_plan::FilterNode& filter);

  DerivedTableP translateSubquery(const logical_plan::LogicalPlanNode& node);

  ColumnCP addMarkColumn();

  // Cache of resolved table schemas.
  Schema schema_;

  velox::core::ExpressionEvaluator& evaluator_;

  const OptimizerOptions& options_;

  // Innermost DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP currentDt_{nullptr};

  // True if wrapping a nondeterministic filter inside a DT in ToGraph.
  bool isNondeterministicWrap_{false};

  // Source PlanNode when inside addProjection() or 'addFilter().
  const logical_plan::LogicalPlanNode* exprSource_{nullptr};

  // Map from lambda argument names to their corresponding columns when
  // translating inside a lambda body.
  folly::F14FastMap<std::string_view, ColumnCP> lambdaSignature_;

  // Maps names in project nodes of input logical plan to deduplicated Exprs.
  folly::F14FastMap<std::string, ExprCP> renames_;

  // Symbols from the 'outer' query. Used when processing correlated subqueries.
  const folly::F14FastMap<std::string, ExprCP>* correlations_;

  // True if expression is allowed to reference symbols from the 'outer' query.
  bool allowCorrelations_{false};

  // Filter conjuncts found in a subquery that reference symbols from the
  // 'outer' query.
  ExprVector correlatedConjuncts_;

  // Maps an expression that contains a subquery to a column that should be used
  // instead. Populated in 'processSubqueries()'.
  folly::F14FastMap<logical_plan::ExprPtr, ColumnCP> subqueries_;

  folly::
      F14FastMap<TypedVariant, ExprCP, TypedVariantHasher, TypedVariantComparer>
          constantDedup_;

  // Dedup map from name + ExprVector to corresponding CallExpr.
  FunctionDedupMap functionDedup_;

  // Counter for generating unique correlation names for BaseTables and
  // DerivedTables.
  int32_t nameCounter_{0};

  // Counter for generating unique names for 'mark' columns produced by semi
  // joins.
  int32_t markCounter_{0};

  // Column and subfield access info for filters, joins, grouping and other
  // things affecting result row selection.
  PlanSubfields controlSubfields_;

  // Column and subfield info for items that only affect column values.
  PlanSubfields payloadSubfields_;

  /// Expressions corresponding to skyline paths over a subfield decomposable
  /// function.
  folly::F14FastMap<const logical_plan::CallExpr*, SubfieldProjections>
      functionSubfields_;

  // Every unique path step, expr pair. For paths c.f1.f2 and c.f1.f3 there
  // are 3 entries: c.f1 and c.f1.f2 and c1.f1.f3, where the last two share
  // the same c.f1.
  folly::F14FastMap<PathExpr, ExprCP, PathExprHasher> deduppedGetters_;

  // Complex type functions that have been checked for explode and
  // 'functionSubfields_'.
  folly::F14FastSet<const logical_plan::CallExpr*> translatedSubfieldFuncs_;

  /// If subfield extraction is pushed down, then these give the skyline
  /// subfields for a column for control and payload situations. The same
  /// column may have different skylines in either. For example if the column
  /// is struct<a int, b int> and only c.a is accessed, there may be no
  /// representation for c, but only for c.a. In this case the skyline is .a =
  /// xx where xx is a synthetic leaf column name for c.a.
  folly::F14FastMap<ColumnCP, SubfieldProjections> allColumnSubfields_;

  // Map from leaf PlanNode to corresponding PlanObject
  folly::F14FastMap<const logical_plan::LogicalPlanNode*, PlanObjectCP>
      planLeaves_;

  Name equality_;
  Name elementAt_{nullptr};
  Name subscript_{nullptr};
  Name cardinality_{nullptr};

  folly::F14FastMap<Name, Name> reversibleFunctions_;
};

} // namespace facebook::axiom::optimizer
