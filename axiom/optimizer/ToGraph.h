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

struct VariantPtrHasher {
  size_t operator()(const std::shared_ptr<const velox::Variant>& value) const {
    return value->hash();
  }
};

struct VariantPtrComparer {
  bool operator()(
      const std::shared_ptr<const velox::Variant>& left,
      const std::shared_ptr<const velox::Variant>& right) const {
    return *left == *right;
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
      const Schema& schema,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerOptions& options);

  /// Converts 'logicalPlan' to a tree of DerivedTables. Returns the root
  /// DerivedTable.
  DerivedTableP makeQueryGraph(
      const logical_plan::LogicalPlanNode& logicalPlan);

  // Sets the columns to project out from the root DerivedTable based on
  // 'logicalPlan'.
  void setDtOutput(
      DerivedTableP dt,
      const logical_plan::LogicalPlanNode& logicalPlan);

  Name newCName(std::string_view prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  /// Creates or returns pre-existing function call with name+args. If
  /// deterministic, a new ExprCP is remembered for reuse.
  ExprCP
  deduppedCall(Name name, Value value, ExprVector args, FunctionSet flags);

  /// True if 'expr' is of the form a = b where a depends on one of 'tables' and
  /// b on the other. If true, returns the side depending on tables[0] in 'left'
  /// and the other in 'right'.
  bool isJoinEquality(
      ExprCP expr,
      std::vector<PlanObjectP>& tables,
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
        expr->asUnchecked<logical_plan::SpecialFormExpr>()->form() == form;
  }

  // For comparisons, swaps the args to have a canonical form for
  // deduplication. E.g column op constant, and Smaller plan object id
  // to the left.
  void canonicalizeCall(Name& name, ExprVector& args);

  // Converts 'plan' to PlanObjects and records join edges into
  // 'currentDt_'. If 'node' does not match  allowedInDt, wraps 'node' in
  // a new DerivedTable.
  PlanObjectP makeQueryGraph(
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
  // corresponds to an argument, returns the ordinal of the argument/
  std::optional<int32_t> stepToArg(
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
  ExprCP translateColumn(std::string_view name);

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

  DerivedTableP translateSetJoin(
      const logical_plan::SetNode& set,
      DerivedTableP setDt);

  // Updates the distribution and column stats of 'setDt', which must
  // be a union. 'innerDt' should be null on top level call. Adds up
  // the cardinality of union branches and their columns.
  void makeUnionDistributionAndStats(
      DerivedTableP setDt,
      DerivedTableP innerDt = nullptr);

  DerivedTableP translateUnion(
      const logical_plan::SetNode& set,
      DerivedTableP setDt,
      bool isTopLevel,
      bool& isLeftLeaf);

  void translateUnnest(
      const logical_plan::UnnestNode& logicalUnnest,
      bool isNewDt);

  AggregationPlanCP translateAggregation(
      const logical_plan::AggregateNode& aggregation);

  PlanObjectP addProjection(const logical_plan::ProjectNode* project);

  // Interprets a Filter node and adds its information into the DerivedTable
  // being assembled.
  PlanObjectP addFilter(const logical_plan::FilterNode* filter);

  // Interprets an AggregationNode and adds its information to the
  // DerivedTable being assembled.
  PlanObjectP addAggregation(const logical_plan::AggregateNode& aggNode);

  PlanObjectP addLimit(const logical_plan::LimitNode& limitNode);

  PlanObjectP addOrderBy(const logical_plan::SortNode& order);

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

  void markAllSubfields(const logical_plan::LogicalPlanNode& node);

  void markControl(const logical_plan::LogicalPlanNode& node);

  void markColumnSubfields(
      const logical_plan::LogicalPlanNodePtr& source,
      std::span<const logical_plan::ExprPtr> columns);

  BitSet functionSubfields(
      const logical_plan::CallExpr* call,
      bool controlOnly,
      bool payloadOnly);

  // Calls translateSubfieldFunction() if not already called.
  void ensureFunctionSubfields(const logical_plan::ExprPtr& expr);

  PlanObjectP makeBaseTable(const logical_plan::TableScanNode& tableScan);

  PlanObjectP makeValuesTable(const logical_plan::ValuesNode& values);

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
  PlanObjectP wrapInDt(const logical_plan::LogicalPlanNode& node);

  // Start new DT and add 'currentDt_' as a child. Set 'currentDt_' to the new
  // DT.
  void finalizeDt(
      const logical_plan::LogicalPlanNode& node,
      DerivedTableP outerDt = nullptr);

  void setDtUsedOutput(
      DerivedTableP dt,
      const logical_plan::LogicalPlanNode& node);

  DerivedTableP newDt();

  static constexpr uint64_t kAllAllowedInDt = ~0UL;

  const Schema& schema_;

  velox::core::ExpressionEvaluator& evaluator_;

  const OptimizerOptions& options_;

  // Innermost DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP currentDt_{nullptr};

  // True if wrapping a nondeterministic filter inside a DT in ToGraph.
  bool isNondeterministicWrap_{false};

  // Source PlanNode when inside addProjection() or 'addFilter().
  const logical_plan::LogicalPlanNode* exprSource_{nullptr};

  // Maps names in project nodes of input logical plan to deduplicated Exprs.
  folly::F14FastMap<std::string, ExprCP> renames_;

  folly::F14FastMap<
      std::shared_ptr<const velox::Variant>,
      ExprCP,
      VariantPtrHasher,
      VariantPtrComparer>
      constantDedup_;

  // Reverse map from dedupped literal to the shared_ptr. We put the
  // shared ptr back into the result plan so the variant never gets
  // copied.
  folly::F14FastMap<ExprCP, std::shared_ptr<const velox::Variant>>
      reverseConstantDedup_;

  // Dedup map from name + ExprVector to corresponding CallExpr.
  FunctionDedupMap functionDedup_;

  // Counter for generating unique correlation names for BaseTables and
  // DerivedTables.
  int32_t nameCounter_{0};

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
