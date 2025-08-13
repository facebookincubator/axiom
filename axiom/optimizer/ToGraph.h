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

namespace facebook::velox::optimizer {

struct BuiltinNames {
  BuiltinNames();

  Name reverse(Name op) const;

  bool isCanonicalizable(Name name) const {
    return canonicalizable.find(name) != canonicalizable.end();
  }

  Name eq;
  Name lt;
  Name lte;
  Name gt;
  Name gte;
  Name plus;
  Name multiply;
  Name _and;
  Name _or;

  folly::F14FastSet<Name> canonicalizable;
};

/// Struct for resolving which logical PlanNode or Lambda defines which
/// field for column and subfield tracking.
struct LogicalContextSource {
  const logical_plan::LogicalPlanNode* planNode{nullptr};
  const logical_plan::CallExpr* call{nullptr};
  int32_t lambdaOrdinal{-1};
};

struct ITypedExprHasher {
  size_t operator()(const core::ITypedExpr* expr) const {
    return expr->hash();
  }
};

struct ITypedExprComparer {
  bool operator()(const core::ITypedExpr* lhs, const core::ITypedExpr* rhs)
      const {
    return *lhs == *rhs;
  }
};

// Map for deduplicating ITypedExpr trees.
using ExprDedupMap = folly::F14FastMap<
    const core::ITypedExpr*,
    ExprCP,
    ITypedExprHasher,
    ITypedExprComparer>;

struct ExprDedupKey {
  Name func;
  const ExprVector* args;

  bool operator==(const ExprDedupKey& other) const {
    return func == other.func && *args == *other.args;
  }
};

struct ExprDedupHasher {
  size_t operator()(const ExprDedupKey& key) const {
    size_t h =
        folly::hasher<uintptr_t>()(reinterpret_cast<uintptr_t>(key.func));
    for (auto& a : *key.args) {
      h = bits::hashMix(h, folly::hasher<ExprCP>()(a));
    }
    return h;
  }
};

using FunctionDedupMap =
    std::unordered_map<ExprDedupKey, ExprCP, ExprDedupHasher>;

struct VariantPtrHasher {
  size_t operator()(const std::shared_ptr<const Variant>& value) const {
    return value->hash();
  }
};

struct VariantPtrComparer {
  bool operator()(
      const std::shared_ptr<const Variant>& left,
      const std::shared_ptr<const Variant>& right) const {
    return *left == *right;
  }
};

/// Represents a path over an Expr of complex type. Used as a key
/// for a map from unique step+optionl subscript expr pairs to the
/// dedupped Expr that is the getter.
struct PathExpr {
  Step step;
  ExprCP subscriptExpr{nullptr};
  ExprCP base;

  bool operator==(const PathExpr& other) const {
    return step == other.step && subscriptExpr == other.subscriptExpr &&
        base == other.base;
  }
};

struct PathExprHasher {
  size_t operator()(const PathExpr& expr) const {
    size_t hash = bits::hashMix(expr.step.hash(), expr.base->id());
    return expr.subscriptExpr ? bits::hashMix(hash, expr.subscriptExpr->id())
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
struct LogicalPlanSubfields {
  std::unordered_map<const logical_plan::LogicalPlanNode*, ResultAccess>
      nodeFields;
  std::unordered_map<const logical_plan::Expr*, ResultAccess> argFields;

  bool hasColumn(const logical_plan::LogicalPlanNode* node, int32_t ordinal)
      const {
    auto it = nodeFields.find(node);
    if (it == nodeFields.end()) {
      return false;
    }
    return it->second.resultPaths.count(ordinal) != 0;
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
  std::unordered_map<PathCP, ExprCP> pathToExpr;
};

class ToGraph {
 public:
  ToGraph(
      const Schema& schema,
      core::ExpressionEvaluator& evaluator,
      const OptimizerOptions& options)
      : schema_{schema}, evaluator_{evaluator}, options_{options} {}

  /// Converts 'logicalPlan' to a tree of DerivedTables. Returns the root
  /// DerivedTable.
  DerivedTableP makeQueryGraph(
      const logical_plan::LogicalPlanNode& logicalPlan);

  // Sets the columns to project out from the root DerivedTable based on
  // 'logicalPlan'.
  void setDerivedTableOutput(
      DerivedTableP dt,
      const logical_plan::LogicalPlanNode& logicalPlan);

  Name newCName(const std::string& prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  BuiltinNames& builtinNames();

  /// Creates or returns pre-existing function call with name+args. If
  /// deterministic, a new ExprCP is remembered for reuse.
  ExprCP
  deduppedCall(Name name, Value value, ExprVector args, FunctionSet flags);

  core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

  static logical_plan::ExprPtr stepToLogicalPlanGetter(
      Step,
      const logical_plan::ExprPtr& arg);

  template <typename Func>
  void trace(int32_t event, Func f) {
    if ((options_.traceFlags & event) != 0) {
      f();
    }
  }

 private:
  // For comparisons, swaps the args to have a canonical form for
  // deduplication. E.g column op constant, and Smaller plan object id
  // to the left.
  void canonicalizeCall(Name& name, ExprVector& args);

  // Converts 'plan' to PlanObjects and records join edges into
  // 'currentSelect_'. If 'node' does not match  allowedInDt, wraps 'node' in
  // a new DerivedTable.
  PlanObjectP makeQueryGraph(
      const logical_plan::LogicalPlanNode& node,
      uint64_t allowedInDt);

  PlanObjectCP findLeaf(const logical_plan::LogicalPlanNode* node) {
    auto* leaf = logicalPlanLeaves_[node];
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
  logical_plan::ConstantExprPtr maybeFoldLogicalConstant(
      const logical_plan::ExprPtr expr);

  // Returns a literal from applying 'call' or 'cast' to 'literals'. nullptr
  // if not successful.
  ExprCP tryFoldConstant(
      const logical_plan::CallExpr* call,
      const logical_plan::SpecialFormExpr* cast,
      const ExprVector& literals);

  // Converts 'name' to a deduplicated ExprCP. If 'name' is assigned to an
  // expression in a projection, returns the deduplicated ExprPtr of the
  // expression.
  ExprCP translateColumn(const std::string& name);

  //  Applies translateColumn to a 'source'.
  ExprVector translateColumns(const std::vector<logical_plan::ExprPtr>& source);

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

  AggregationPlanCP translateAggregation(
      const logical_plan::AggregateNode& aggregation);

  PlanObjectP addProjection(const logical_plan::ProjectNode* project);

  // Interprets a Filter node and adds its information into the DerivedTable
  // being assembled.
  PlanObjectP addFilter(const logical_plan::FilterNode* Filter);

  // Interprets an AggregationNode and adds its information to the
  // DerivedTable being assembled.
  PlanObjectP addAggregation(const logical_plan::AggregateNode& aggNode);

  PlanObjectP addLimit(const logical_plan::LimitNode& limitNode);

  PlanObjectP addOrderBy(const logical_plan::SortNode& order);

  bool isSubfield(
      const logical_plan::Expr* expr,
      Step& step,
      logical_plan::ExprPtr& input);

  void getExprForField(
      const logical_plan::Expr* expr,
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
      const logical_plan::Expr* expr,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources);

  void markSubfields(
      const logical_plan::ExprPtr& expr,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources) {
    markSubfields(expr.get(), steps, isControl, context, sources);
  }

  void markFieldAccessed(
      const LogicalContextSource& source,
      int32_t ordinal,
      std::vector<Step>& steps,
      bool isControl,
      const std::vector<const RowType*>& context,
      const std::vector<LogicalContextSource>& sources);

  void markAllSubfields(
      const RowType* type,
      const logical_plan::LogicalPlanNode& node);

  void markControl(const logical_plan::LogicalPlanNode& node);

  void markColumnSubfields(
      const logical_plan::LogicalPlanNodePtr& source,
      const std::vector<logical_plan::ExprPtr>& columns);

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

  DerivedTableP newDt();

  static constexpr uint64_t kAllAllowedInDt = ~0UL;

  const Schema& schema_;

  core::ExpressionEvaluator& evaluator_;

  const OptimizerOptions& options_;

  // Innermost DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP currentSelect_;

  // Source PlanNode when inside addProjection() or 'addFilter().
  const logical_plan::LogicalPlanNode* logicalExprSource_{nullptr};

  // Maps names in project noes of 'inputPlan_' to deduplicated Exprs.
  std::unordered_map<std::string, ExprCP> renames_;

  std::unordered_map<
      std::shared_ptr<const Variant>,
      ExprCP,
      VariantPtrHasher,
      VariantPtrComparer>
      constantDedup_;

  // Reverse map from dedupped literal to the shared_ptr. We put the
  // shared ptr back into the result plan so the variant never gets
  // copied.
  std::map<ExprCP, std::shared_ptr<const Variant>> reverseConstantDedup_;

  // Dedup map fr om name+ExprVector to corresponding Call Expr.
  FunctionDedupMap functionDedup_;

  // Counter for generating unique correlation names for BaseTables and
  // DerivedTables.
  int32_t nameCounter_{0};

  // Column and subfield access info for filters, joins, grouping and other
  // things affecting result row selection.
  LogicalPlanSubfields logicalControlSubfields_;

  // Column and subfield info for items that only affect column values.
  LogicalPlanSubfields logicalPayloadSubfields_;

  /// Expressions corresponding to skyline paths over a subfield decomposable
  /// function.
  std::unordered_map<const logical_plan::CallExpr*, SubfieldProjections>
      logicalFunctionSubfields_;

  // Every unique path step, expr pair. For paths c.f1.f2 and c.f1.f3 there
  // are 3 entries: c.f1 and c.f1.f2 and c1.f1.f3, where the two last share
  // the same c.f1.
  std::unordered_map<PathExpr, ExprCP, PathExprHasher> deduppedGetters_;

  // Complex type functions that have been checke for explode and
  // 'functionSubfields_'.
  std::unordered_set<const logical_plan::CallExpr*>
      logicalTranslatedSubfieldFuncs_;

  /// If subfield extraction is pushed down, then these give the skyline
  /// subfields for a column for control and payload situations. The same
  /// column may have different skylines in either. For example if the column
  /// is struct<a int, b int> and only c.a is accessed, there may be no
  /// representation for c, but only for c.a. In this case the skyline is .a =
  /// xx where xx is a synthetic leaf column name for c.a.
  std::unordered_map<ColumnCP, SubfieldProjections> allColumnSubfields_;

  // Map from leaf PlanNode to corresponding PlanObject
  std::unordered_map<const logical_plan::LogicalPlanNode*, PlanObjectCP>
      logicalPlanLeaves_;

  // True if wrapping a nondeterministic filter inside a DT in ToGraph.
  bool isNondeterministicWrap_{false};

  std::unique_ptr<BuiltinNames> builtinNames_;
};

} // namespace facebook::velox::optimizer
