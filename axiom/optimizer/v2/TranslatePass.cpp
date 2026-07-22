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

#include "axiom/optimizer/v2/TranslatePass.h"

#include <folly/container/F14Map.h>
#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/JoinCondition.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"

namespace facebook::axiom::optimizer::v2 {
namespace lp = logical_plan;

namespace {

// Set of LP-output column names the consumer needs from this translation.
// Threaded top-down through `translateNode` so producers (Scan, Project,
// Aggregate, etc.) can mint / keep only the columns downstream actually uses.
using LpNameSet = folly::F14FastSet<std::string>;

// Returns the set of every column name in `rowType`. Used as the "everything"
// required-set when the consumer hasn't (yet) narrowed it.
LpNameSet allNames(const velox::RowType& rowType) {
  const auto& names = rowType.names();
  return LpNameSet{names.begin(), names.end()};
}

void collectUsedNames(const logical_plan::Expr& expr, LpNameSet& out);
void collectUsedNames(
    const std::vector<logical_plan::ExprPtr>& exprs,
    LpNameSet& out);

// Walks every expression carried by `node` and its descendants and adds
// every referenced `InputReferenceExpr::name` to `out`. Used to capture
// correlated outer-column references inside subquery bodies — those names
// are the ones the outer node must preserve in its child's required-set.
void collectUsedNamesInPlan(
    const logical_plan::LogicalPlanNode& node,
    LpNameSet& out) {
  switch (node.kind()) {
    case lp::NodeKind::kFilter:
      collectUsedNames(*node.as<lp::FilterNode>()->predicate(), out);
      break;
    case lp::NodeKind::kProject:
      collectUsedNames(node.as<lp::ProjectNode>()->expressions(), out);
      break;
    case lp::NodeKind::kAggregate: {
      const auto* agg = node.as<lp::AggregateNode>();
      collectUsedNames(agg->groupingKeys(), out);
      for (const auto& aggExpr : agg->aggregates()) {
        collectUsedNames(*aggExpr, out);
      }
      break;
    }
    case lp::NodeKind::kJoin: {
      const auto* join = node.as<lp::JoinNode>();
      if (join->condition() != nullptr) {
        collectUsedNames(*join->condition(), out);
      }
      break;
    }
    case lp::NodeKind::kSort:
      for (const auto& field : node.as<lp::SortNode>()->ordering()) {
        collectUsedNames(*field.expression, out);
      }
      break;
    case lp::NodeKind::kUnnest:
      collectUsedNames(node.as<lp::UnnestNode>()->unnestExpressions(), out);
      break;
    default:
      // Scan, Limit, Values, Set, Output: no expression-bearing fields that
      // could reference outer columns.
      break;
  }
  for (const auto& input : node.inputs()) {
    collectUsedNamesInPlan(*input, out);
  }
}

// Returns the single Scan leaf reached through single-input nodes (Filter,
// Project, ...), or nullptr if the subtree branches (join, set) or bottoms out
// in a non-Scan leaf. SYSTEM sampling requires exactly one scan to sample.
ScanCP soleScan(NodeCP node) {
  const auto inputs = node->inputs();
  if (inputs.empty()) {
    return node->is(NodeType::kScan) ? node->as<Scan>() : nullptr;
  }
  if (inputs.size() != 1) {
    return nullptr;
  }
  return soleScan(inputs[0]);
}

// Collects every `InputReferenceExpr::name` referenced anywhere in `expr`.
// Used by translate functions to compute the required-set their input must
// supply, given the expressions the node itself reads. `WindowExpr`,
// `AggregateExpr`, `LambdaExpr`, and `SubqueryExpr` carry sub-expressions
// in side fields outside `inputs()`, so they need explicit handling.
void collectUsedNames(const logical_plan::Expr& expr, LpNameSet& out) {
  if (expr.isInputReference()) {
    out.insert(expr.as<lp::InputReferenceExpr>()->name());
    return;
  }
  for (const auto& child : expr.inputs()) {
    collectUsedNames(*child, out);
  }
  if (expr.isWindow()) {
    const auto* window = expr.as<lp::WindowExpr>();
    for (const auto& key : window->partitionKeys()) {
      collectUsedNames(*key, out);
    }
    for (const auto& field : window->ordering()) {
      collectUsedNames(*field.expression, out);
    }
    if (window->frame().startValue != nullptr) {
      collectUsedNames(*window->frame().startValue, out);
    }
    if (window->frame().endValue != nullptr) {
      collectUsedNames(*window->frame().endValue, out);
    }
  } else if (expr.isAggregate()) {
    const auto* agg = expr.as<lp::AggregateExpr>();
    if (agg->filter() != nullptr) {
      collectUsedNames(*agg->filter(), out);
    }
    for (const auto& field : agg->ordering()) {
      collectUsedNames(*field.expression, out);
    }
  } else if (expr.kind() == lp::ExprKind::kLambda) {
    // Lambda body may reference outer columns (captures). Recurse into the
    // body, then subtract the lambda's signature names (those are
    // introduced by the lambda itself, not consumed from the outer scope).
    const auto* lambda = expr.as<lp::LambdaExpr>();
    LpNameSet bodyNames;
    collectUsedNames(*lambda->body(), bodyNames);
    for (const auto& name : lambda->signature()->names()) {
      bodyNames.erase(name);
    }
    out.insert(bodyNames.begin(), bodyNames.end());
  } else if (expr.kind() == lp::ExprKind::kSubquery) {
    // Walk the subquery body to surface any correlated outer-column
    // references. Names not in the outer's outputType are silently ignored
    // by the consumer (existing policy); names that match outer columns
    // are the correlations the outer must keep so the body's translate
    // can resolve them.
    const auto* subquery = expr.as<lp::SubqueryExpr>();
    collectUsedNamesInPlan(*subquery->subquery(), out);
  }
}

void collectUsedNames(
    const std::vector<logical_plan::ExprPtr>& exprs,
    LpNameSet& out) {
  for (const auto& expr : exprs) {
    collectUsedNames(*expr, out);
  }
}

// Appends columns from `extra` to `base`, skipping any that already
// appear in `base` (by Column* identity).
void appendUnique(ColumnVector& base, const ColumnVector& extra) {
  PlanObjectSet seen = PlanObjectSet::fromObjects(base);
  for (ColumnCP column : extra) {
    if (!seen.contains(column)) {
      seen.add(column);
      base.push_back(column);
    }
  }
}

// True if 'expr' is or transitively contains an `lp::SubqueryExpr`.
// Used to decide whether a parent node's IR shape needs adjusting to
// give the subquery a relational input above which to lift Apply.
//
// TODO: walk side-channel children carried by Lambda body / Window
// partition+order+frame / Aggregate filter+ordering — currently this
// recursion only follows `Expr::inputs()`, so a subquery buried inside
// `filter(arr, x -> x = (SELECT ...))` (or similar HOF) is missed. A
// miss is safe — control falls through to the non-split path with
// `applyTarget=nullptr` and `liftSubquery` throws NYI rather than
// silently miscompiling — but coverage is incomplete.
bool containsSubquery(const logical_plan::Expr& expr) {
  if (expr.kind() == logical_plan::ExprKind::kSubquery) {
    return true;
  }
  for (const auto& child : expr.inputs()) {
    if (containsSubquery(*child)) {
      return true;
    }
  }
  return false;
}

// True if 'column' is one of 'node's output columns (by identity).
bool outputContains(NodeCP node, ColumnCP column) {
  for (ColumnCP candidate : node->outputColumns()) {
    if (candidate == column) {
      return true;
    }
  }
  return false;
}

// Walks 'expr' as a top-level AND chain and appends each non-AND leaf
// to 'out'.
void flattenAndConjuncts(
    const logical_plan::Expr& expr,
    std::vector<const logical_plan::Expr*>& out) {
  if (expr.isSpecialForm() &&
      expr.as<logical_plan::SpecialFormExpr>()->form() ==
          logical_plan::SpecialForm::kAnd) {
    for (const auto& child : expr.inputs()) {
      flattenAndConjuncts(*child, out);
    }
    return;
  }
  out.push_back(&expr);
}

// Resolution context for translating expressions in a fixed scope. Maps
// output column name to the Column that produces it.
using Scope = folly::F14FastMap<std::string, ColumnCP>;

// Populates 'scope' with one entry per field of 'rowType' pointing at the
// matching Column in 'columns'. 'columns' must align 1:1 with 'rowType'.
void populateScope(
    const velox::RowType& rowType,
    const ColumnVector& columns,
    Scope& scope) {
  VELOX_CHECK_EQ(
      rowType.size(), columns.size(), "Scope row type / columns mismatch");
  for (size_t i = 0; i < rowType.size(); ++i) {
    scope[rowType.nameOf(i)] = columns[i];
  }
}

// Builds one fresh Column per field of 'rowType' and populates 'scope' to
// map each field name to its column.
ColumnVector makeOutputColumns(
    const velox::RowType& rowType,
    float cardinality,
    Scope& scope) {
  ColumnVector columns;
  columns.reserve(rowType.size());
  for (size_t i = 0; i < rowType.size(); ++i) {
    Value value(toType(rowType.childAt(i)), cardinality);
    columns.push_back(
        Column::createForSymbol(toName(rowType.nameOf(i)), value));
  }
  populateScope(rowType, columns, scope);
  return columns;
}

struct Translated {
  NodeCP node;
  Scope scope;
};

// Outer-scope chain used while translating subquery bodies, plus the
// per-nesting-level set of correlated columns referenced so far.
// `liftSubquery` consumes the captured correlations as `Apply.correlations`.
//
// Invariant: `outerScopes_.size() == correlationsStack_.size()`.
class SubqueryContext {
 public:
  // Resolves 'name' against enclosing scopes, innermost-first. Returns
  // nullptr if not found at any level. Records the hit as a correlation
  // for the innermost in-flight subquery.
  ColumnCP resolveOuter(std::string_view name);

  // Enters/leaves the body of a subquery. `pop()` returns the correlated
  // columns collected during that body, deduplicated.
  void push(const Scope& outerScope);
  ColumnVector pop();

 private:
  std::vector<const Scope*> outerScopes_;
  std::vector<ColumnVector> correlationsStack_;
};

ColumnCP SubqueryContext::resolveOuter(std::string_view name) {
  for (size_t i = outerScopes_.size(); i > 0; --i) {
    const Scope& scope = *outerScopes_[i - 1];
    auto found = scope.find(name);
    if (found == scope.end()) {
      continue;
    }
    VELOX_CHECK_GE(correlationsStack_.size(), i);
    // Record on every in-flight subquery between the matching outer scope
    // and the innermost so each intermediate Apply carries the column.
    for (size_t j = i - 1; j < correlationsStack_.size(); ++j) {
      correlationsStack_[j].push_back(found->second);
    }
    return found->second;
  }
  return nullptr;
}

void SubqueryContext::push(const Scope& outerScope) {
  outerScopes_.push_back(&outerScope);
  correlationsStack_.emplace_back();
}

ColumnVector SubqueryContext::pop() {
  ColumnVector deduped;
  folly::F14FastSet<ColumnCP> seen;
  for (ColumnCP column : correlationsStack_.back()) {
    if (seen.insert(column).second) {
      deduped.push_back(column);
    }
  }
  correlationsStack_.pop_back();
  outerScopes_.pop_back();
  return deduped;
}

class Translator {
 public:
  Translator(
      optimizer::Schema& schema,
      velox::core::ExpressionEvaluator& evaluator,
      Builder& builder)
      : schema_(schema),
        builder_(builder),
        exprFactory_(builder),
        simplifier_(builder, evaluator) {}

  TranslatePass::Result run(const lp::LogicalPlanNode& plan) {
    // The query output is a list of (sourceName, outputName) pairs, one per
    // output position. An OutputNode names them explicitly and may repeat a
    // source column under several output names; a bare plan (no OutputNode)
    // uses its own outputType field names for both. Each position resolves by
    // name through the translated scope: identical output expressions collapse
    // to one Column, so the scope -- not the node's column list -- carries
    // every output position.
    const lp::LogicalPlanNode* node;
    std::vector<std::pair<std::string, std::string>> outputSpec;
    if (plan.is(lp::NodeKind::kOutput)) {
      const auto& output = *plan.as<lp::OutputNode>();
      node = &*output.onlyInput();
      const auto& childOutputType = *node->outputType();
      outputSpec.reserve(output.entries().size());
      for (const auto& entry : output.entries()) {
        VELOX_CHECK_LT(entry.index, childOutputType.size());
        outputSpec.emplace_back(
            childOutputType.nameOf(entry.index), entry.name);
      }
    } else {
      node = &plan;
      const auto& outputType = *plan.outputType();
      outputSpec.reserve(outputType.size());
      for (size_t i = 0; i < outputType.size(); ++i) {
        outputSpec.emplace_back(outputType.nameOf(i), outputType.nameOf(i));
      }
    }

    // Request only the referenced source names; unreferenced names flow through
    // as unused and get pruned where producers honor the required-set.
    LpNameSet required;
    required.reserve(outputSpec.size());
    for (const auto& [sourceName, outputName] : outputSpec) {
      required.insert(sourceName);
    }

    Translated translated = translateNode(*node, required);

    ColumnVector outputColumns;
    std::vector<std::string> outputNames;
    outputColumns.reserve(outputSpec.size());
    outputNames.reserve(outputSpec.size());
    for (const auto& [sourceName, outputName] : outputSpec) {
      auto it = translated.scope.find(sourceName);
      VELOX_CHECK(
          it != translated.scope.end(),
          "Output name not found in scope: {}",
          sourceName);
      outputColumns.push_back(it->second);
      outputNames.push_back(outputName);
    }
    return {translated.node, std::move(outputColumns), std::move(outputNames)};
  }

 private:
  // 'required' is the set of LP-output column names the consumer needs from
  // this node. Producers (Scan, Project, Aggregate, Window, Unnest, Values)
  // may emit only outputs in `required`; intermediate nodes propagate it
  // down, augmented with the names the node itself reads.
  Translated translateNode(
      const lp::LogicalPlanNode& node,
      const LpNameSet& required);
  Translated translateScan(
      const lp::TableScanNode& scan,
      const LpNameSet& required);
  Translated translateFilter(
      const lp::FilterNode& filter,
      const LpNameSet& required);
  Translated translateProject(
      const lp::ProjectNode& project,
      const LpNameSet& required);
  Translated translateLimit(
      const lp::LimitNode& limit,
      const LpNameSet& required);
  Translated translateSort(const lp::SortNode& sort, const LpNameSet& required);

  // Translates ordering keys, dropping any key that repeats an earlier one. A
  // repeated key cannot refine the order its first occurrence imposes, and
  // Velox rejects Sort/TopN nodes with duplicate keys.
  std::pair<ExprVector, OrderTypeVector> dedupOrdering(
      const std::vector<lp::SortingField>& ordering,
      const Scope& scope,
      NodeCP* applyTarget);

  Translated translateSample(
      const lp::SampleNode& sample,
      const LpNameSet& required);

  // Returns the constant sample percentage (in [0, 100]) of a SampleNode.
  double extractSamplePercentage(const logical_plan::Expr& percentage);

  // Returns a zero-row result with 'outputType''s schema.
  Translated makeEmptyResult(const velox::RowType& outputType);

  Translated translateAggregate(
      const lp::AggregateNode& aggregate,
      const LpNameSet& required);
  // Lowers GROUPING SETS / ROLLUP / CUBE to a GroupId plus a plain aggregate
  // keyed on an explicit group-id column. Returns the aggregation node.
  NodeCP lowerGroupingSets(
      const lp::AggregateNode& aggregate,
      const ExprVector& groupingKeys,
      AggregateCallVector aggregates,
      const ColumnVector& outputColumns,
      NodeCP currentInput,
      Scope& newScope);
  // The result of an aggregate over the empty set (constant false/null FILTER):
  // the aggregate's empty-input value from the function registry.
  ExprCP emptySetResult(const optimizer::Aggregate* call);
  // Wraps 'node' in a Project appending 'columns' (each defined by the matching
  // expr) to its outputs. Returns 'node' unchanged when 'columns' is empty.
  NodeCP appendConstantColumns(
      NodeCP node,
      const ColumnVector& columns,
      const ExprVector& exprs);
  Translated translateValues(
      const lp::ValuesNode& values,
      const LpNameSet& required);
  Translated translateUnnest(
      const lp::UnnestNode& unnest,
      const LpNameSet& required);
  Translated translateSet(const lp::SetNode& set, const LpNameSet& required);
  Translated translateJoin(const lp::JoinNode& join, const LpNameSet& required);
  Translated translateLateralJoin(
      const lp::LateralJoinNode& join,
      const LpNameSet& required);
  Translated translateTableWrite(
      const lp::TableWriteNode& tableWrite,
      const LpNameSet& required);
  NodeCP maybeWrapInWindow(
      NodeCP input,
      const Scope& inputScope,
      const std::vector<lp::ExprPtr>& projectExprs,
      const std::vector<std::string>& projectNames,
      Scope& windowScope);
  Translated buildUnionAll(
      const std::vector<lp::LogicalPlanNodePtr>& inputs,
      const velox::RowTypePtr& outputType,
      const LpNameSet& required);

  // Translates `predicateExpr` against `scope` (lifting any subqueries
  // above `input` via Apply) and lowers it onto `input`. Returns an
  // empty `Values` of `input`'s schema when the predicate is
  // statically `false`, `input` unchanged when statically `true`, or
  // a `Filter` otherwise. `scope` is returned unchanged.
  Translated
  maybeWrapInFilter(NodeCP input, const lp::Expr& predicateExpr, Scope scope);

  // Translates an lp expression against 'scope'. If the expression
  // contains an `lp::SubqueryExpr` (bare, or wrapped in
  // `kExists`/`kIn`/`kNot`), the subquery is materialized as an `Apply`
  // node lifted above `*applyTarget`, and `*applyTarget` is updated to
  // point to the new Apply. The returned `ExprCP` references the Apply's
  // mark / value column at the subquery site.
  //
  // `applyTarget` must be non-null at any expression position that may
  // contain a subquery. Passing `nullptr` declares "no lift possible
  // here" and the lift path throws if a subquery is encountered
  // (e.g., row-literal expressions in `VALUES`).
  ExprCP
  translateExpr(const lp::Expr& expr, const Scope& scope, NodeCP* applyTarget);
  ExprCP translateInputReference(
      const lp::InputReferenceExpr& expr,
      const Scope& scope);
  ExprCP translateConstant(const lp::ConstantExpr& expr);
  ExprCP translateCall(
      const lp::CallExpr& expr,
      const Scope& scope,
      NodeCP* applyTarget);
  ExprCP translateSpecialForm(
      const lp::SpecialFormExpr& expr,
      const Scope& scope,
      NodeCP* applyTarget);

  // Translates an EXISTS special form by lifting an `Apply(kLeftSemiProject)`,
  // with a fast path that folds EXISTS over a scalar Aggregate to constant
  // TRUE.
  ExprCP translateExists(
      const lp::SpecialFormExpr& expr,
      const Scope& scope,
      NodeCP* applyTarget);

  // Normalizes an IN list. Folds a single-element IN to equality (`a IN (b)` ->
  // `a = b`, `a IN (NULL)` -> null boolean literal). For a multi-element
  // constant list, drops duplicate constants from 'args' in place and returns a
  // folded equality when a single distinct constant remains. Returns nullptr to
  // let the caller build the (deduplicated) IN through the generic path,
  // leaving 'args' unchanged when a multi-element list has any non-constant
  // element.
  ExprCP normalizeInList(ExprVector& args);

  // Translates a DEREFERENCE special form, resolving a varchar field name to a
  // numeric field index against the input row type.
  ExprCP
  translateDereference(ExprVector args, Name callName, const Value& value);

  ExprCP translateLambda(const lp::LambdaExpr& expr, const Scope& scope);

  // Translates a subquery body, captures correlations, and lifts an
  // `Apply` above `*applyTarget`. Updates `*applyTarget` to point to
  // the new Apply (with an `EnforceSingleRow` wrap for uncorrelated
  // scalar). Returns Apply's result column.
  //
  // `kind` is the `Apply.kind` to emit. `inLhs` is the IN expression's
  // left-hand side, non-null only when shaping an IN-form Apply
  // (`liftSubquery` stores it together with the body's single output
  // column as `Apply.inLhs` / `Apply.inBodyKey`). Throws if
  // `applyTarget` is null.
  ColumnCP liftSubquery(
      const lp::SubqueryExpr& subqueryExpr,
      velox::core::JoinType kind,
      ExprCP inLhs,
      const Scope& outerScope,
      NodeCP* applyTarget);

  // Translates each lp expression in 'expressions' against 'scope'.
  ExprVector translateAll(
      const std::vector<lp::ExprPtr>& expressions,
      const Scope& scope,
      NodeCP* applyTarget);

  // Translates an lp window frame against 'scope'.
  Frame toFrame(const lp::WindowExpr::Frame& frame, const Scope& scope);

  // Translates an lp aggregate expression into a QueryGraph `Aggregate`
  // call. Resolves args / FILTER mask / ORDER BY keys against 'scope'.
  // `applyTarget` (if non-null) lifts Apply nodes for any subquery in
  // args / FILTER / ORDER BY above the Aggregate's input.
  const optimizer::Aggregate* toAggregateCall(
      const lp::AggregateExpr& aggregateExpr,
      const Scope& scope,
      NodeCP* applyTarget);

  // Translates each expression in 'exprRows' and, if every one folds to a
  // `Literal`, returns the row-wise `Variant`s. Returns nullopt if any
  // entry doesn't fold. `VALUES` expressions cannot reference input
  // columns, so translation runs against an empty scope.
  std::optional<std::vector<velox::Variant>> foldExprRowsToVariants(
      const lp::ValuesNode::Exprs& exprRows);

  SubqueryContext subqueries_;
  Schema& schema_;
  Builder& builder_;
  ExprFactory exprFactory_;
  ExprSimplifier simplifier_;
  int32_t baseTableCounter_{0};

  // Result column of each scalar subquery already lifted, keyed by its inner
  // plan. Identical subqueries share one inner plan (hash-consed), so a
  // repeated reference reuses the lifted column instead of lifting it again
  // (which would place the same column on both sides of the lift's join). A
  // cached column is only reused while it is still in the current lift
  // target's output, so a reference in an unrelated scope re-lifts.
  folly::F14FastMap<const lp::LogicalPlanNode*, ColumnCP>
      scalarSubqueryColumns_;
};

Translated Translator::translateTableWrite(
    const lp::TableWriteNode& tableWrite,
    const LpNameSet& /*required*/) {
  const auto kind = static_cast<connector::WriteKind>(tableWrite.writeKind());
  if (kind != connector::WriteKind::kInsert &&
      kind != connector::WriteKind::kCreate) {
    VELOX_NYI(
        "TableWrite supports only INSERT and CREATE, got {}",
        connector::WriteKindName::toName(kind));
  }

  const auto* schemaTable =
      schema_.findTable(tableWrite.connectorId(), tableWrite.tableName());
  VELOX_CHECK_NOT_NULL(
      schemaTable,
      "Table not found: {} via connector {}",
      tableWrite.tableName().toString(),
      tableWrite.connectorId());
  const auto* connectorTable = schemaTable->connectorTable;
  VELOX_CHECK_NOT_NULL(connectorTable);
  const auto& tableSchema = *connectorTable->type();

  // The child must supply every column a write expression reads.
  LpNameSet childRequired;
  for (const auto& columnExpr : tableWrite.columnExpressions()) {
    collectUsedNames(*columnExpr, childRequired);
  }
  Translated input = translateNode(*tableWrite.onlyInput(), childRequired);
  NodeCP currentInput = input.node;

  // One expression per target column, in table-schema order: the statement's
  // expression where the column is written, else the column's schema default.
  const auto& writtenNames = tableWrite.columnNames();
  ExprVector columnExprs;
  columnExprs.reserve(tableSchema.size());
  for (uint32_t i = 0; i < tableSchema.size(); ++i) {
    const auto& columnName = tableSchema.nameOf(i);
    auto it = std::find(writtenNames.begin(), writtenNames.end(), columnName);
    if (it != writtenNames.end()) {
      const auto nth = it - writtenNames.begin();
      columnExprs.push_back(translateExpr(
          *tableWrite.columnExpressions()[nth], input.scope, &currentInput));
    } else {
      const auto* tableColumn = connectorTable->findColumn(columnName);
      VELOX_CHECK_NOT_NULL(tableColumn);
      columnExprs.push_back(builder_.makeLiteral(
          velox::Variant(tableColumn->defaultValue()),
          toType(tableColumn->type())));
    }
    VELOX_DCHECK(
        *tableSchema.childAt(i) == *toTypePtr(columnExprs.back()->value().type),
        "TableWrite column type does not match schema: column {}, schema {}, expr {}",
        columnName,
        tableSchema.childAt(i)->toString(),
        toTypePtr(columnExprs.back()->value().type)->toString());
  }

  NodeCP writeNode = builder_.make<TableWrite>(
      {currentInput, connectorTable, kind, std::move(columnExprs)});

  // The single output column is the written row count.
  Scope scope;
  const auto& outputType = *tableWrite.outputType();
  if (outputType.size() == 1) {
    scope[outputType.nameOf(0)] = writeNode->outputColumns()[0];
  }
  return {writeNode, std::move(scope)};
}

Translated Translator::translateNode(
    const lp::LogicalPlanNode& node,
    const LpNameSet& required) {
  switch (node.kind()) {
    case lp::NodeKind::kTableScan:
      return translateScan(*node.as<lp::TableScanNode>(), required);
    case lp::NodeKind::kFilter:
      return translateFilter(*node.as<lp::FilterNode>(), required);
    case lp::NodeKind::kProject:
      return translateProject(*node.as<lp::ProjectNode>(), required);
    case lp::NodeKind::kLimit:
      return translateLimit(*node.as<lp::LimitNode>(), required);
    case lp::NodeKind::kSort:
      return translateSort(*node.as<lp::SortNode>(), required);
    case lp::NodeKind::kSample:
      return translateSample(*node.as<lp::SampleNode>(), required);
    case lp::NodeKind::kAggregate:
      return translateAggregate(*node.as<lp::AggregateNode>(), required);
    case lp::NodeKind::kValues:
      return translateValues(*node.as<lp::ValuesNode>(), required);
    case lp::NodeKind::kUnnest:
      return translateUnnest(*node.as<lp::UnnestNode>(), required);
    case lp::NodeKind::kSet:
      return translateSet(*node.as<lp::SetNode>(), required);
    case lp::NodeKind::kJoin:
      return translateJoin(*node.as<lp::JoinNode>(), required);
    case lp::NodeKind::kLateralJoin:
      return translateLateralJoin(*node.as<lp::LateralJoinNode>(), required);
    case lp::NodeKind::kTableWrite:
      return translateTableWrite(*node.as<lp::TableWriteNode>(), required);
    case lp::NodeKind::kOutput:
      VELOX_UNREACHABLE();
    default:
      VELOX_NYI(
          "Unsupported logical plan node kind: {}",
          lp::NodeKindName::toName(node.kind()));
  }
}

Translated Translator::translateScan(
    const lp::TableScanNode& scan,
    const LpNameSet& required) {
  const auto* schemaTable =
      schema_.findTable(scan.connectorId(), scan.tableName());
  VELOX_CHECK_NOT_NULL(schemaTable);

  auto* baseTable = make<BaseTable>();
  baseTable->cname = toName(fmt::format("t{}", baseTableCounter_++));
  baseTable->schemaTable = schemaTable;
  // filteredCardinality stays 0 until estimateLeafStats populates it from
  // connector stats; cardinality estimation falls back to constraint-based
  // selectivity while it is unset.

  const auto& columnNames = scan.columnNames();
  const auto& outputType = scan.outputType();
  ColumnVector outputColumns;
  outputColumns.reserve(columnNames.size());
  Scope scope;
  for (size_t i = 0; i < columnNames.size(); ++i) {
    const auto& outName = outputType->nameOf(i);
    if (!required.contains(outName)) {
      continue;
    }
    Name inTableName = toName(columnNames[i]);
    Name outNameInterned = toName(outName);
    ColumnCP schemaColumn = schemaTable->findColumn(inTableName);
    VELOX_CHECK_NOT_NULL(schemaColumn);
    auto* column = make<Column>(
        inTableName,
        baseTable,
        schemaColumn->value(),
        outNameInterned,
        schemaColumn->name());
    baseTable->columns.push_back(column);
    outputColumns.push_back(column);
    scope[outName] = column;
  }
  ScanCP scanNode = builder_.make<Scan>(
      {baseTable, std::move(outputColumns), /*filters=*/{}});
  return {scanNode, std::move(scope)};
}

Translated Translator::translateFilter(
    const lp::FilterNode& filter,
    const LpNameSet& required) {
  // Filter is a pass-through: its outputType equals the child's. Forward
  // `required` plus the names the predicate reads from the child.
  LpNameSet childRequired = required;
  collectUsedNames(*filter.predicate(), childRequired);
  Translated input = translateNode(*filter.onlyInput(), childRequired);

  // Split conjuncts so no-subquery ones land in a Filter below the
  // Apply, where PushdownAndPrune can carry them into the input
  // subtree. Subquery-bearing conjuncts must stay above the Apply
  // (and any decorrelation aggregate atop it).
  std::vector<const lp::Expr*> conjuncts;
  flattenAndConjuncts(*filter.predicate(), conjuncts);
  std::vector<const lp::Expr*> noSubquery;
  std::vector<const lp::Expr*> withSubquery;
  noSubquery.reserve(conjuncts.size());
  withSubquery.reserve(conjuncts.size());
  for (const auto* conjunct : conjuncts) {
    if (containsSubquery(*conjunct)) {
      withSubquery.push_back(conjunct);
    } else {
      noSubquery.push_back(conjunct);
    }
  }
  if (noSubquery.empty() || withSubquery.empty()) {
    return maybeWrapInFilter(
        input.node, *filter.predicate(), std::move(input.scope));
  }

  // Translates 'conjuncts' and accumulates simplified results.
  // Returns nullopt if any conjunct simplifies to constant false.
  auto translateConjuncts =
      [&](const std::vector<const lp::Expr*>& conjuncts,
          NodeCP* applyTarget) -> std::optional<ExprVector> {
    ExprVector result;
    result.reserve(conjuncts.size());
    for (const auto* conjunct : conjuncts) {
      ExprCP translated = translateExpr(*conjunct, input.scope, applyTarget);
      if (simplifier_.simplifyFilter(translated, result)) {
        return std::nullopt;
      }
    }
    return result;
  };

  auto innerConjuncts = translateConjuncts(noSubquery, /*applyTarget=*/nullptr);
  if (!innerConjuncts.has_value()) {
    return {
        builder_.makeEmptyValues(input.node->outputColumns()),
        std::move(input.scope)};
  }
  NodeCP inner = innerConjuncts->empty()
      ? input.node
      : builder_.make<Filter>({input.node, std::move(*innerConjuncts)});

  auto outerConjuncts = translateConjuncts(withSubquery, &inner);
  if (!outerConjuncts.has_value()) {
    return {
        builder_.makeEmptyValues(inner->outputColumns()),
        std::move(input.scope)};
  }
  if (outerConjuncts->empty()) {
    return {inner, std::move(input.scope)};
  }
  return {
      builder_.make<Filter>({inner, std::move(*outerConjuncts)}),
      std::move(input.scope)};
}

Translated Translator::maybeWrapInFilter(
    NodeCP input,
    const lp::Expr& predicateExpr,
    Scope scope) {
  ExprCP predicate = translateExpr(predicateExpr, scope, &input);

  ExprVector conjuncts;
  if (simplifier_.simplifyFilter(predicate, conjuncts)) {
    return {builder_.makeEmptyValues(input->outputColumns()), std::move(scope)};
  }

  if (conjuncts.empty()) {
    return {input, std::move(scope)};
  }

  return {
      builder_.make<Filter>({input, std::move(conjuncts)}), std::move(scope)};
}

Translated Translator::translateProject(
    const lp::ProjectNode& project,
    const LpNameSet& required) {
  const auto& names = project.names();
  const auto& exprs = project.expressions();
  VELOX_CHECK_EQ(names.size(), exprs.size());

  // Drop output positions not in `required` and compute the names the kept
  // expressions read from the child.
  std::vector<size_t> keptIndices;
  keptIndices.reserve(names.size());
  LpNameSet childRequired;
  for (size_t i = 0; i < names.size(); ++i) {
    if (!required.contains(names[i])) {
      continue;
    }
    keptIndices.push_back(i);
    collectUsedNames(*exprs[i], childRequired);
  }

  Translated input = translateNode(*project.onlyInput(), childRequired);

  // maybeWrapInWindow only needs the kept window-function exprs.
  std::vector<lp::ExprPtr> keptExprs;
  std::vector<std::string> keptNames;
  keptExprs.reserve(keptIndices.size());
  keptNames.reserve(keptIndices.size());
  for (size_t idx : keptIndices) {
    keptExprs.push_back(exprs[idx]);
    keptNames.push_back(names[idx]);
  }
  Scope windowScope = input.scope;
  NodeCP currentInput = maybeWrapInWindow(
      input.node, input.scope, keptExprs, keptNames, windowScope);

  ColumnVector outputColumns;
  outputColumns.reserve(keptIndices.size());
  ExprVector translatedExprs;
  translatedExprs.reserve(keptIndices.size());
  Scope newScope;
  // Output positions producing the same canonical expression collapse to one
  // output entry; both LP names still resolve to that Column via `newScope`.
  folly::F14FastMap<ExprCP, ColumnCP> exprToOutput;
  exprToOutput.reserve(keptIndices.size());
  for (size_t idx : keptIndices) {
    const auto& name = names[idx];
    ExprCP translatedExpr;
    if (exprs[idx]->isWindow()) {
      auto it = windowScope.find(name);
      VELOX_CHECK(it != windowScope.end());
      translatedExpr = it->second;
    } else {
      translatedExpr = translateExpr(*exprs[idx], input.scope, &currentInput);
    }
    if (auto seenIt = exprToOutput.find(translatedExpr);
        seenIt != exprToOutput.end()) {
      newScope[name] = seenIt->second;
      continue;
    }
    ColumnCP column;
    if (translatedExpr->is(PlanType::kColumnExpr)) {
      column = translatedExpr->as<Column>();
    } else {
      Name outName = toName(name);
      column = make<Column>(
          queryCtx()->newName(outName),
          /*relation=*/nullptr,
          translatedExpr->value(),
          /*alias=*/outName);
    }
    exprToOutput.emplace(translatedExpr, column);
    newScope[name] = column;
    outputColumns.push_back(column);
    translatedExprs.push_back(translatedExpr);
  }

  // Identity Project: pass-through outputs in input order. Skip allocating;
  // `newScope` carries any LP renames.
  if (outputColumns.size() == currentInput->outputColumns().size()) {
    bool identity = true;
    for (size_t i = 0; i < outputColumns.size(); ++i) {
      if (outputColumns[i] != currentInput->outputColumns()[i]) {
        identity = false;
        break;
      }
    }
    if (identity) {
      return {currentInput, std::move(newScope)};
    }
  }

  ProjectCP projectNode = builder_.make<Project>(
      {currentInput, std::move(translatedExprs), std::move(outputColumns)});
  return {projectNode, std::move(newScope)};
}

OrderType toOrderType(const logical_plan::SortOrder& order) {
  if (order.isAscending()) {
    return order.isNullsFirst() ? OrderType::kAscNullsFirst
                                : OrderType::kAscNullsLast;
  }
  return order.isNullsFirst() ? OrderType::kDescNullsFirst
                              : OrderType::kDescNullsLast;
}

// Max NDV across 'exprs', propagating unknown: if any operand's NDV is
// unknown, the derived NDV is unknown (nullopt) rather than a fabricated
// floor. An empty input has no operand to derive from and is treated as a
// known 1 (the prior floor), matching a nullary call's degenerate NDV.
std::optional<float> maxCardinality(const ExprVector& exprs) {
  std::optional<float> result{1.0f};
  for (const auto* expr : exprs) {
    result = optimizer::maxOf(result, expr->value().cardinality);
  }
  return result;
}

ExprVector Translator::translateAll(
    const std::vector<lp::ExprPtr>& expressions,
    const Scope& scope,
    NodeCP* applyTarget) {
  ExprVector result;
  result.reserve(expressions.size());
  for (const auto& expression : expressions) {
    result.push_back(translateExpr(*expression, scope, applyTarget));
  }
  return result;
}

Frame Translator::toFrame(
    const lp::WindowExpr::Frame& frame,
    const Scope& scope) {
  // Frame bounds are scalar literals (rows/range offsets); no subquery
  // can appear here, so no lift target needed.
  return Frame{
      frame.type,
      frame.startType,
      frame.startValue ? translateExpr(*frame.startValue, scope, nullptr)
                       : nullptr,
      frame.endType,
      frame.endValue ? translateExpr(*frame.endValue, scope, nullptr) : nullptr,
  };
}

const optimizer::Aggregate* Translator::toAggregateCall(
    const lp::AggregateExpr& aggregateExpr,
    const Scope& scope,
    NodeCP* applyTarget) {
  ExprVector arguments =
      translateAll(aggregateExpr.inputs(), scope, applyTarget);

  if (aggregateExpr.isSpecialFormAgg()) {
    const auto& special = *aggregateExpr.as<lp::SpecialFormAggExpr>();
    // A metadata aggregate does not support these modifiers. ORDER BY is
    // allowed but ignored (order-insensitive), so it is not checked.
    VELOX_USER_CHECK(
        !special.isDistinct(),
        "Metadata aggregate does not support DISTINCT: {}",
        aggregateExpr.name());
    VELOX_USER_CHECK_NULL(
        special.filter(),
        "Metadata aggregate does not support FILTER: {}",
        aggregateExpr.name());
    // The kind name is not a registered Velox aggregate, so skip the registry
    // lookup and carry the kind and translated fallback for the fold pass.
    // Result and intermediate types are BIGINT.
    const optimizer::Aggregate* fallback = special.fallback() != nullptr
        ? toAggregateCall(*special.fallback(), scope, applyTarget)
        : nullptr;
    FunctionSet funcs = Call::unionArgFunctions(FunctionSet{}, arguments);
    return builder_.makeAggregate(
        toName(aggregateExpr.name()),
        Value(toType(aggregateExpr.type())),
        std::move(arguments),
        funcs,
        /*isDistinct=*/false,
        /*condition=*/nullptr,
        toType(velox::BIGINT()),
        /*orderKeys=*/{},
        /*orderTypes=*/{},
        special.kind(),
        fallback);
  }

  std::vector<velox::TypePtr> argTypes;
  argTypes.reserve(aggregateExpr.inputs().size());
  for (const auto& input : aggregateExpr.inputs()) {
    argTypes.push_back(input->type());
  }

  TypeCP intermediateType = toType(
      velox::exec::resolveIntermediateType(aggregateExpr.name(), argTypes));

  ExprCP condition = aggregateExpr.filter() != nullptr
      ? translateExpr(*aggregateExpr.filter(), scope, applyTarget)
      : nullptr;
  // Drop a constant-true FILTER (it masks nothing). A false or null mask stays
  // as a literal condition, folded to the empty-set result during assembly.
  if (condition != nullptr && isConstantTrue(condition)) {
    condition = nullptr;
  }

  auto [orderKeys, orderTypes] =
      dedupOrdering(aggregateExpr.ordering(), scope, applyTarget);
  Value value(toType(aggregateExpr.type()));

  Name aggName = toName(aggregateExpr.name());
  const auto& metadata =
      velox::exec::getAggregateFunctionMetadata(aggregateExpr.name());

  FunctionSet funcs = Call::unionArgFunctions(FunctionSet{}, arguments);
  if (metadata.ignoreDuplicates) {
    funcs = funcs | FunctionSet::kIgnoreDuplicatesAggregate;
  } else if (
      (aggName == toName("min") || aggName == toName("max")) &&
      arguments.size() == 1) {
    // min(x)/max(x) ignore duplicates; min(x, n)/max(x, n) do not.
    // TODO: Push this distinction into per-signature metadata.
    funcs = funcs | FunctionSet::kIgnoreDuplicatesAggregate;
  }
  if (metadata.orderSensitive) {
    funcs = funcs | FunctionSet::kOrderSensitiveAggregate;
  }

  const bool isDistinct =
      !metadata.ignoreDuplicates && aggregateExpr.isDistinct();

  return builder_.makeAggregate(
      aggName,
      value,
      std::move(arguments),
      funcs,
      isDistinct,
      condition,
      intermediateType,
      std::move(orderKeys),
      std::move(orderTypes));
}

NodeCP Translator::maybeWrapInWindow(
    NodeCP input,
    const Scope& inputScope,
    const std::vector<lp::ExprPtr>& projectExprs,
    const std::vector<std::string>& projectNames,
    Scope& windowScope) {
  std::vector<size_t> windowIndices;
  for (size_t i = 0; i < projectExprs.size(); ++i) {
    if (projectExprs[i]->isWindow()) {
      windowIndices.push_back(i);
    }
  }
  if (windowIndices.empty()) {
    return input;
  }

  // Pre-translate each lp WindowExpr into its effective spec (partition
  // keys, order keys). Drop sort keys that duplicate a partition key (every
  // row in a partition shares that value) or repeat an earlier sort key —
  // both are redundant. Grouping below compares the effective specs, so two
  // windows that differ only in a dropped order key — or only in frame, which
  // is per-function — still share a single `Window` node.
  struct Spec {
    ExprVector partitionKeys;
    ExprVector orderKeys;
    OrderTypeVector orderTypes;

    bool operator==(const Spec&) const = default;
  };
  std::vector<Spec> specs(windowIndices.size());
  for (size_t i = 0; i < windowIndices.size(); ++i) {
    const auto* windowExpr =
        projectExprs[windowIndices[i]]->as<lp::WindowExpr>();
    Spec& spec = specs[i];
    // Subqueries in window partition / order keys / frame bounds would
    // need lifting above the Window's input — left as nullptr until
    // that wiring lands. lp's surface allows them; rare in practice.
    spec.partitionKeys = translateAll(
        windowExpr->partitionKeys(), inputScope, /*applyTarget=*/nullptr);

    folly::F14FastSet<ExprCP> seenKeys(
        spec.partitionKeys.begin(), spec.partitionKeys.end());
    spec.orderKeys.reserve(windowExpr->ordering().size());
    spec.orderTypes.reserve(windowExpr->ordering().size());
    for (const auto& field : windowExpr->ordering()) {
      ExprCP key =
          translateExpr(*field.expression, inputScope, /*applyTarget=*/nullptr);
      if (!seenKeys.insert(key).second) {
        continue;
      }
      spec.orderKeys.push_back(key);
      spec.orderTypes.push_back(toOrderType(field.order));
    }
  }

  // Group windows by their effective spec so each group produces one
  // `Window` node. Multiple groups are chained.
  std::vector<std::vector<size_t>> groups;
  for (size_t i = 0; i < windowIndices.size(); ++i) {
    bool placed = false;
    for (auto& group : groups) {
      if (specs[group.front()] == specs[i]) {
        group.push_back(i);
        placed = true;
        break;
      }
    }
    if (!placed) {
      groups.push_back({i});
    }
  }

  NodeCP current = input;
  for (const auto& group : groups) {
    const Spec& spec = specs[group.front()];

    WindowFunctions functions;
    functions.reserve(group.size());
    ColumnVector outputColumns;
    outputColumns.reserve(current->outputColumns().size() + group.size());
    for (ColumnCP column : current->outputColumns()) {
      outputColumns.push_back(column);
    }
    for (size_t i : group) {
      const size_t projectIndex = windowIndices[i];
      const auto* windowExpr = projectExprs[projectIndex]->as<lp::WindowExpr>();
      Value value(toType(windowExpr->type()));
      Name windowName = toName(windowExpr->name());
      ExprVector windowArgs = translateAll(
          windowExpr->inputs(), inputScope, /*applyTarget=*/nullptr);
      // Window functions are non-deterministic with non-default null behavior.
      FunctionSet windowFuncs =
          Call::unionArgFunctions(FunctionSet{}, windowArgs) |
          FunctionSet::kNonDeterministic | FunctionSet::kNonDefaultNullBehavior;
      auto* call = builder_.makeCall(
          windowName, value, std::move(windowArgs), windowFuncs);
      Frame frame = toFrame(windowExpr->frame(), inputScope);
      functions.push_back(
          WindowFunction{call, frame, windowExpr->ignoreNulls()});

      const auto& name = projectNames[projectIndex];
      auto* column = Column::createForSymbol(toName(name), value);
      outputColumns.push_back(column);
      windowScope[name] = column;
    }

    current = builder_.make<Window>(
        {current,
         std::move(functions),
         spec.partitionKeys,
         spec.orderKeys,
         spec.orderTypes,
         std::move(outputColumns)});
  }
  return current;
}

Translated Translator::translateLimit(
    const lp::LimitNode& limit,
    const LpNameSet& required) {
  if (limit.count() == 0) {
    // LIMIT 0 returns no rows. Skip translating the input entirely and
    // replace with an empty `Values` carrying the same schema.
    return makeEmptyResult(*limit.outputType());
  }
  // Limit is a pass-through with no expressions of its own.
  Translated input = translateNode(*limit.onlyInput(), required);
  LimitCP limitNode =
      builder_.make<Limit>({input.node, limit.offset(), limit.count()});
  return {limitNode, std::move(input.scope)};
}

std::pair<ExprVector, OrderTypeVector> Translator::dedupOrdering(
    const std::vector<lp::SortingField>& ordering,
    const Scope& scope,
    NodeCP* applyTarget) {
  ExprVector orderKeys;
  OrderTypeVector orderTypes;
  orderKeys.reserve(ordering.size());
  orderTypes.reserve(ordering.size());
  folly::F14FastSet<ExprCP> seen;
  for (const auto& field : ordering) {
    ExprCP key = translateExpr(*field.expression, scope, applyTarget);
    if (!seen.insert(key).second) {
      continue;
    }
    orderKeys.push_back(key);
    orderTypes.push_back(toOrderType(field.order));
  }
  return {std::move(orderKeys), std::move(orderTypes)};
}

Translated Translator::translateSort(
    const lp::SortNode& sort,
    const LpNameSet& required) {
  // Sort is a pass-through; forward `required` plus the names its order keys
  // read from the child.
  LpNameSet childRequired = required;
  for (const auto& field : sort.ordering()) {
    collectUsedNames(*field.expression, childRequired);
  }
  Translated input = translateNode(*sort.onlyInput(), childRequired);

  NodeCP currentInput = input.node;
  auto [orderKeys, orderTypes] =
      dedupOrdering(sort.ordering(), input.scope, &currentInput);

  SortCP sortNode = builder_.make<Sort>(
      {currentInput, std::move(orderKeys), std::move(orderTypes)});
  return {sortNode, std::move(input.scope)};
}

double Translator::extractSamplePercentage(const lp::Expr& percentage) {
  Scope emptyScope;
  ExprCP folded =
      simplifier_.simplify(translateExpr(percentage, emptyScope, nullptr));
  VELOX_USER_CHECK(
      folded->is(PlanType::kLiteralExpr),
      "Sampling percentage must be constant: {}",
      percentage.toString());
  const velox::Variant& value = folded->as<Literal>()->literal();
  VELOX_USER_CHECK(!value.isNull(), "Sampling percentage must not be null");
  VELOX_USER_CHECK_EQ(
      value.kind(),
      velox::TypeKind::DOUBLE,
      "Sampling percentage must be a double");
  const double result = value.value<double>();
  VELOX_USER_CHECK_GE(result, 0, "Sampling percentage must be >= 0");
  VELOX_USER_CHECK_LE(result, 100, "Sampling percentage must be <= 100");
  return result;
}

Translated Translator::makeEmptyResult(const velox::RowType& outputType) {
  Scope scope;
  ColumnVector outputColumns =
      makeOutputColumns(outputType, /*cardinality=*/0, scope);
  ValuesCP valuesNode =
      builder_.makeValues(nullptr, nullptr, std::move(outputColumns));
  return {valuesNode, std::move(scope)};
}

Translated Translator::translateSample(
    const lp::SampleNode& sample,
    const LpNameSet& required) {
  const double percentage = extractSamplePercentage(*sample.percentage());

  // Handle the endpoints here so the switch below only sees a percentage in
  // the open interval (0, 100).
  if (percentage == 100) {
    return translateNode(*sample.onlyInput(), required);
  }
  if (percentage == 0) {
    return makeEmptyResult(*sample.outputType());
  }

  Translated input = translateNode(*sample.onlyInput(), required);

  switch (sample.sampleMethod()) {
    case lp::SampleNode::SampleMethod::kSystem: {
      // SYSTEM sampling drops whole splits, so it needs a single scan to
      // sample.
      ScanCP scan = soleScan(input.node);
      VELOX_USER_CHECK_NOT_NULL(
          scan, "TABLESAMPLE SYSTEM is only supported directly over a table");
      const_cast<BaseTable*>(scan->baseTable())->sampledPercentage =
          static_cast<float>(percentage);
      return input;
    }
    case lp::SampleNode::SampleMethod::kBernoulli: {
      ExprCP predicate = exprFactory_.makeSamplePredicate(percentage / 100.0);
      NodeCP filtered =
          builder_.make<Filter>({input.node, ExprVector{predicate}});
      return {filtered, std::move(input.scope)};
    }
  }
  VELOX_UNREACHABLE();
}

ExprCP Translator::emptySetResult(const optimizer::Aggregate* call) {
  std::vector<const velox::Type*> argTypes;
  argTypes.reserve(call->args().size());
  for (ExprCP arg : call->args()) {
    argTypes.push_back(arg->value().type);
  }
  velox::Variant emptyValue =
      FunctionRegistry::instance()->aggregateResultForEmptyInput(
          call->name(), argTypes);
  return emptyValue.isNull()
      ? static_cast<ExprCP>(builder_.makeNull(call->value().type))
      : builder_.makeLiteral(std::move(emptyValue), call->value().type);
}

NodeCP Translator::appendConstantColumns(
    NodeCP node,
    const ColumnVector& columns,
    const ExprVector& exprs) {
  if (columns.empty()) {
    return node;
  }
  ExprVector projectExprs;
  ColumnVector projectColumns;
  const auto& nodeOutputs = node->outputColumns();
  projectExprs.reserve(nodeOutputs.size() + columns.size());
  projectColumns.reserve(nodeOutputs.size() + columns.size());
  for (ColumnCP column : nodeOutputs) {
    projectExprs.push_back(column);
    projectColumns.push_back(column);
  }
  appendAll(projectExprs, exprs);
  appendAll(projectColumns, columns);
  return builder_.make<Project>(
      {node, std::move(projectExprs), std::move(projectColumns)});
}

Translated Translator::translateAggregate(
    const lp::AggregateNode& aggregate,
    const LpNameSet& required) {
  const auto& names = aggregate.outputNames();
  const auto& groupingKeyExpressions = aggregate.groupingKeys();
  const auto numGroupingKeys = groupingKeyExpressions.size();
  const auto& groupingSets = aggregate.groupingSets();
  const auto& aggregateExpressions = aggregate.aggregates();
  const size_t groupIdSlots = groupingSets.empty() ? 0 : 1;
  VELOX_CHECK_EQ(
      names.size(),
      numGroupingKeys + aggregateExpressions.size() + groupIdSlots);

  // Decide which aggregates to keep (output not in `required`). Grouping keys
  // and groupId always stay: dropping them would change group cardinality.
  std::vector<size_t> keptAggregateIndices;
  keptAggregateIndices.reserve(aggregateExpressions.size());
  for (size_t i = 0; i < aggregateExpressions.size(); ++i) {
    if (required.contains(names[numGroupingKeys + i])) {
      keptAggregateIndices.push_back(i);
    }
  }

  // Child must supply: every column read by the grouping keys, plus every
  // column read by the kept aggregates (args, FILTER, ORDER BY).
  LpNameSet childRequired;
  collectUsedNames(groupingKeyExpressions, childRequired);
  for (size_t idx : keptAggregateIndices) {
    collectUsedNames(*aggregateExpressions[idx], childRequired);
  }
  Translated input = translateNode(*aggregate.onlyInput(), childRequired);

  ExprVector groupingKeys;
  groupingKeys.reserve(numGroupingKeys);
  ColumnVector outputColumns;
  outputColumns.reserve(
      numGroupingKeys + keptAggregateIndices.size() + groupIdSlots);
  Scope newScope;
  // Grouping-sets case keeps positional alignment between groupingKeys and
  // the set indices; only the simple GROUP BY case can dedup safely here.
  const bool canDedupGroupingKeys = groupingSets.empty();
  folly::F14FastSet<ColumnCP> seenGroupingOutputs;
  if (canDedupGroupingKeys) {
    seenGroupingOutputs.reserve(numGroupingKeys);
  }

  NodeCP currentInput = input.node;
  for (size_t i = 0; i < numGroupingKeys; ++i) {
    ExprCP keyExpr =
        translateExpr(*groupingKeyExpressions[i], input.scope, &currentInput);
    // Reuse the input column when the grouping key is a column reference:
    // Velox `AggregationNode` emits grouping-key output under the input
    // field-access name. For grouping sets, `GroupIdNode` requires output
    // names distinct from the input columns for the NULL-padded copies, so
    // mint a fresh column rather than reusing the (possibly colliding) LP
    // symbol.
    ColumnCP column;
    if (groupingSets.empty() && keyExpr->is(PlanType::kColumnExpr)) {
      column = keyExpr->as<Column>();
    } else if (groupingSets.empty()) {
      column = Column::createForSymbol(toName(names[i]), keyExpr->value());
    } else {
      column = Column::create(toName(names[i]), keyExpr->value());
    }
    newScope[names[i]] = column;
    if (canDedupGroupingKeys && !seenGroupingOutputs.insert(column).second) {
      continue;
    }
    groupingKeys.push_back(keyExpr);
    outputColumns.push_back(column);
  }

  // Aggregates whose FILTER folded to constant false/null see the empty set;
  // their output is the aggregate's empty-input value, materialized by a
  // Project above the aggregation.
  ColumnVector foldedColumns;
  ExprVector foldedExprs;

  AggregateCallVector aggregates;
  aggregates.reserve(keptAggregateIndices.size());
  folly::F14FastMap<const optimizer::Aggregate*, ColumnCP> aggregateToOutput;
  aggregateToOutput.reserve(keptAggregateIndices.size());
  for (size_t idx : keptAggregateIndices) {
    const auto* aggregateCall =
        toAggregateCall(*aggregateExpressions[idx], input.scope, &currentInput);
    const auto& aggregateName = names[numGroupingKeys + idx];
    // A remaining literal condition can only be false or null: fold to the
    // empty-set result.
    if (aggregateCall->condition() != nullptr &&
        aggregateCall->condition()->is(PlanType::kLiteralExpr)) {
      auto* column = Column::createForSymbol(
          toName(aggregateName), aggregateCall->value());
      foldedColumns.push_back(column);
      foldedExprs.push_back(emptySetResult(aggregateCall));
      newScope[aggregateName] = column;
      continue;
    }
    if (auto it = aggregateToOutput.find(aggregateCall);
        it != aggregateToOutput.end()) {
      newScope[aggregateName] = it->second;
      continue;
    }
    aggregates.push_back(aggregateCall);
    auto* column =
        Column::createForSymbol(toName(aggregateName), aggregateCall->value());
    aggregateToOutput.emplace(aggregateCall, column);
    outputColumns.push_back(column);
    newScope[aggregateName] = column;
  }

  if (groupingSets.empty()) {
    AggregateCP aggNode = builder_.make<Aggregate>(Aggregate::Key{
        .input = currentInput,
        .groupingKeys = std::move(groupingKeys),
        .aggregates = std::move(aggregates),
        .outputColumns = std::move(outputColumns)});
    return {
        appendConstantColumns(aggNode, foldedColumns, foldedExprs),
        std::move(newScope)};
  }

  NodeCP aggNode = lowerGroupingSets(
      aggregate,
      groupingKeys,
      std::move(aggregates),
      outputColumns,
      currentInput,
      newScope);
  return {
      appendConstantColumns(aggNode, foldedColumns, foldedExprs),
      std::move(newScope)};
}

NodeCP Translator::lowerGroupingSets(
    const lp::AggregateNode& aggregate,
    const ExprVector& groupingKeys,
    AggregateCallVector aggregates,
    const ColumnVector& outputColumns,
    NodeCP currentInput,
    Scope& newScope) {
  const auto& names = aggregate.outputNames();
  const auto& groupingSets = aggregate.groupingSets();

  // GROUPING SETS / ROLLUP / CUBE lower to GroupId + a plain aggregate keyed on
  // an explicit group-id column: a mechanical translation that lets physical
  // planning treat the aggregate like any other.
  // Grouping sets keep positional alignment with the set indices (no key
  // dedup), so `outputColumns` is exactly the key output columns followed by
  // the aggregate result columns.
  const size_t numKeys = groupingKeys.size();
  ColumnVector keyOutputColumns(
      outputColumns.begin(), outputColumns.begin() + numKeys);
  ColumnVector aggResultColumns(
      outputColumns.begin() + numKeys, outputColumns.end());

  // Columns the aggregates read pass through GroupId unchanged.
  PlanObjectSet aggInputSet;
  for (const auto* call : aggregates) {
    for (ExprCP arg : call->args()) {
      aggInputSet.unionSet(arg->columns());
    }
    if (call->condition() != nullptr) {
      aggInputSet.unionSet(call->condition()->columns());
    }
    for (ExprCP orderKey : call->orderKeys()) {
      aggInputSet.unionSet(orderKey->columns());
    }
  }

  // GroupId groups on columns; materialize any non-column key into a Project.
  ExprVector groupIdInputKeys;
  groupIdInputKeys.reserve(numKeys);
  ColumnVector materializedColumns;
  ExprVector materializedExprs;
  for (ExprCP keyExpr : groupingKeys) {
    if (keyExpr->is(PlanType::kColumnExpr)) {
      groupIdInputKeys.push_back(keyExpr);
    } else {
      ColumnCP keyColumn = Column::create("groupingKey", keyExpr->value());
      materializedColumns.push_back(keyColumn);
      materializedExprs.push_back(keyExpr);
      groupIdInputKeys.push_back(keyColumn);
    }
  }

  NodeCP groupIdInput = currentInput;
  if (!materializedExprs.empty()) {
    PlanObjectSet passthrough = aggInputSet;
    for (ExprCP keyExpr : groupingKeys) {
      if (keyExpr->is(PlanType::kColumnExpr)) {
        passthrough.add(keyExpr->as<Column>());
      }
    }
    ExprVector projectExprs = passthrough.toObjects<Expr>();
    ColumnVector projectColumns;
    projectColumns.reserve(projectExprs.size() + materializedColumns.size());
    for (ExprCP expr : projectExprs) {
      projectColumns.push_back(expr->as<Column>());
    }
    appendAll(projectExprs, materializedExprs);
    appendAll(projectColumns, materializedColumns);
    groupIdInput = builder_.make<Project>(
        {currentInput, std::move(projectExprs), std::move(projectColumns)});
  }

  ExprVector aggregationInputs = aggInputSet.toObjects<Expr>();

  const auto& groupIdName = names.back();
  Value groupIdValue(
      toType(aggregate.outputType()->childAt(names.size() - 1)),
      static_cast<float>(groupingSets.size()));
  ColumnCP groupIdColumn =
      Column::createForSymbol(toName(groupIdName), groupIdValue);
  newScope[groupIdName] = groupIdColumn;

  ColumnVector groupIdOutputs;
  groupIdOutputs.reserve(
      keyOutputColumns.size() + aggregationInputs.size() + 1);
  appendAll(groupIdOutputs, keyOutputColumns);
  for (ExprCP expr : aggregationInputs) {
    groupIdOutputs.push_back(expr->as<Column>());
  }
  groupIdOutputs.push_back(groupIdColumn);

  QGVector<QGVector<int32_t>> setsCopy;
  setsCopy.reserve(groupingSets.size());
  for (const auto& set : groupingSets) {
    setsCopy.emplace_back(set.begin(), set.end());
  }

  NodeCP groupIdNode = builder_.make<GroupId>(GroupId::Key{
      groupIdInput,
      groupIdInputKeys,
      aggregationInputs,
      setsCopy,
      keyOutputColumns,
      groupIdColumn,
      groupIdOutputs});

  // Aggregate keys: the GroupId key output columns plus the group-id column.
  ExprVector aggGroupingKeys;
  aggGroupingKeys.reserve(numKeys + 1);
  appendAll(aggGroupingKeys, keyOutputColumns);
  aggGroupingKeys.push_back(groupIdColumn);

  // Empty sets are the global () sets whose group-id values must emit a default
  // row over empty input; groupId is coupled with them (set together or not).
  QGVector<int32_t> globalGroupingSets;
  for (size_t i = 0; i < setsCopy.size(); ++i) {
    if (setsCopy[i].empty()) {
      globalGroupingSets.push_back(static_cast<int32_t>(i));
    }
  }
  ColumnCP aggGroupId = globalGroupingSets.empty() ? nullptr : groupIdColumn;

  // Physical output order: keys, group-id, aggregate results.
  ColumnVector aggOutputColumns;
  aggOutputColumns.reserve(numKeys + 1 + aggResultColumns.size());
  appendAll(aggOutputColumns, keyOutputColumns);
  aggOutputColumns.push_back(groupIdColumn);
  appendAll(aggOutputColumns, aggResultColumns);

  AggregateCP aggNode = builder_.make<Aggregate>(Aggregate::Key{
      .input = groupIdNode,
      .groupingKeys = std::move(aggGroupingKeys),
      .aggregates = std::move(aggregates),
      .outputColumns = std::move(aggOutputColumns),
      .step = AggregateStep::kSingle,
      .groupId = aggGroupId,
      .globalGroupingSets = std::move(globalGroupingSets)});
  return aggNode;
}

std::optional<std::vector<velox::Variant>> Translator::foldExprRowsToVariants(
    const lp::ValuesNode::Exprs& exprRows) {
  Scope emptyScope;
  std::vector<velox::Variant> rows;
  rows.reserve(exprRows.size());
  for (const auto& exprRow : exprRows) {
    std::vector<velox::Variant> rowValues;
    rowValues.reserve(exprRow.size());
    for (const auto& expr : exprRow) {
      // VALUES literal expressions have no relational input above; lift
      // forbidden. Subqueries inside VALUES are NYI.
      ExprCP translated =
          translateExpr(*expr, emptyScope, /*applyTarget=*/nullptr);
      if (!translated->is(PlanType::kLiteralExpr)) {
        return std::nullopt;
      }
      rowValues.emplace_back(translated->as<Literal>()->literal());
    }
    rows.emplace_back(velox::Variant::row(std::move(rowValues)));
  }
  return rows;
}

Translated Translator::translateValues(
    const lp::ValuesNode& values,
    const LpNameSet& /*required*/) {
  Scope scope;
  ColumnVector outputColumns =
      makeOutputColumns(*values.outputType(), values.cardinality(), scope);

  // Fold `Exprs` data to a single `Variant::array(rows)` at translate
  // time. Constant-foldable rows arrive here as `Literal`s (Call /
  // SpecialForm translation has already simplified). If any entry
  // isn't a literal, fall back to passing the source through — emit
  // will fail loudly on the unsupported shape.
  const velox::Variant* foldedRows = nullptr;
  if (const auto* exprRows =
          std::get_if<lp::ValuesNode::Exprs>(&values.data())) {
    auto rows = foldExprRowsToVariants(*exprRows);
    if (rows.has_value()) {
      foldedRows = queryCtx()->registerVariant(
          std::make_unique<velox::Variant>(
              velox::Variant::array(std::move(*rows))));
    }
  }

  const lp::ValuesNode* passthrough = foldedRows == nullptr ? &values : nullptr;
  ValuesCP valuesNode =
      builder_.makeValues(passthrough, foldedRows, std::move(outputColumns));
  return {valuesNode, std::move(scope)};
}

Translated Translator::translateUnnest(
    const lp::UnnestNode& unnest,
    const LpNameSet& required) {
  // Child must supply: every column the unnest expressions read, plus any
  // replicated input column the consumer still needs.
  LpNameSet childRequired;
  collectUsedNames(unnest.unnestExpressions(), childRequired);
  const auto& inputType = *unnest.onlyInput()->outputType();
  for (size_t i = 0; i < inputType.size(); ++i) {
    const auto& name = inputType.nameOf(i);
    if (required.contains(name)) {
      childRequired.insert(name);
    }
  }
  Translated input = translateNode(*unnest.onlyInput(), childRequired);

  NodeCP currentInput = input.node;
  ExprVector unnestExprs =
      translateAll(unnest.unnestExpressions(), input.scope, &currentInput);

  // TODO: Cardinality of unnested columns also should be multiplied by the
  // average expected number of elements per unnested row. Other Value
  // properties also should be computed.
  const std::optional<float> unnestCardinality = maxCardinality(unnestExprs);
  const auto& outputType = unnest.outputType();
  Scope newScope;

  // LP UnnestNode's outputType layout is
  // [input.outputType ⊕ per-expression unnested-names ⊕ optional ordinality].
  // Walk in that order, dropping replicated input columns the consumer
  // doesn't need; always emit unnest-expression outputs and ordinality.
  ColumnVector replicatedColumns;
  replicatedColumns.reserve(inputType.size());
  for (size_t i = 0; i < inputType.size(); ++i) {
    const auto& name = inputType.nameOf(i);
    if (!required.contains(name)) {
      continue;
    }
    auto it = input.scope.find(name);
    VELOX_CHECK(it != input.scope.end());
    replicatedColumns.push_back(it->second);
    newScope[name] = it->second;
  }

  QGVector<ColumnVector> unnestColumns;
  unnestColumns.reserve(unnest.unnestExpressions().size());
  size_t outputIndex = inputType.size();
  for (const auto& names : unnest.unnestedNames()) {
    ColumnVector perExpr;
    perExpr.reserve(names.size());
    for (const auto& name : names) {
      Value value = clampCardinality(
          Value{toType(outputType->childAt(outputIndex)), unnestCardinality});
      auto* column = Column::createForSymbol(toName(name), value);
      perExpr.push_back(column);
      newScope[name] = column;
      ++outputIndex;
    }
    unnestColumns.push_back(std::move(perExpr));
  }

  ColumnCP ordinalityColumn = nullptr;
  if (unnest.ordinalityName().has_value()) {
    const auto& name = unnest.ordinalityName().value();
    Value value(toType(outputType->childAt(outputIndex)), unnestCardinality);
    ordinalityColumn = Column::createForSymbol(toName(name), value);
    newScope[name] = ordinalityColumn;
  }

  ColumnVector outputColumns = replicatedColumns;
  for (const auto& perExpr : unnestColumns) {
    for (ColumnCP column : perExpr) {
      outputColumns.push_back(column);
    }
  }
  if (ordinalityColumn != nullptr) {
    outputColumns.push_back(ordinalityColumn);
  }

  UnnestCP unnestNode = builder_.make<Unnest>(
      {currentInput,
       std::move(unnestExprs),
       std::move(replicatedColumns),
       std::move(unnestColumns),
       ordinalityColumn,
       std::move(outputColumns)});
  return {unnestNode, std::move(newScope)};
}

Translated Translator::buildUnionAll(
    const std::vector<lp::LogicalPlanNodePtr>& inputs,
    const velox::RowTypePtr& outputType,
    const LpNameSet& required) {
  NodeVector inputNodes;
  inputNodes.reserve(inputs.size());
  QGVector<ColumnVector> legColumns;
  legColumns.reserve(inputs.size());
  // Union output positions parent doesn't require are dropped from the union
  // node entirely. Build a list of kept positions once and reuse it for every
  // leg's column mapping.
  std::vector<size_t> keptPositions;
  keptPositions.reserve(outputType->size());
  for (size_t j = 0; j < outputType->size(); ++j) {
    if (required.contains(outputType->nameOf(j))) {
      keptPositions.push_back(j);
    }
  }
  for (const auto& in : inputs) {
    // Each leg's required-set is the kept union output names, mapped to that
    // leg's positional outputType names (legs align 1:1 with the union's
    // outputType by SQL semantics).
    LpNameSet legRequired;
    legRequired.reserve(keptPositions.size());
    for (size_t j : keptPositions) {
      legRequired.insert(in->outputType()->nameOf(j));
    }
    Translated translated = translateNode(*in, legRequired);
    ColumnVector cols;
    cols.reserve(keptPositions.size());
    for (size_t j : keptPositions) {
      // Resolve LP-name → IR Column* via the leg's scope; the leg's IR
      // `outputColumns` may be narrower than its LP outputType after
      // dup-collapse, but the same Column may legitimately repeat.
      const auto& legName = in->outputType()->nameOf(j);
      auto it = translated.scope.find(legName);
      VELOX_CHECK(
          it != translated.scope.end(),
          "UnionAll leg scope missing column: {}",
          legName);
      cols.push_back(it->second);
    }
    legColumns.push_back(std::move(cols));
    inputNodes.push_back(translated.node);
  }
  Scope scope;
  ColumnVector outputColumns;
  outputColumns.reserve(keptPositions.size());
  for (size_t j : keptPositions) {
    Value value(toType(outputType->childAt(j)));
    auto* column =
        Column::createForSymbol(toName(outputType->nameOf(j)), value);
    outputColumns.push_back(column);
    scope[outputType->nameOf(j)] = column;
  }
  UnionAllCP unionNode = builder_.make<UnionAll>(
      {std::move(inputNodes), std::move(legColumns), std::move(outputColumns)});
  return {unionNode, std::move(scope)};
}

// Set-op outputs reuse the upstream `Column*`s (left side for
// INTERSECT/EXCEPT, UnionAll for UNION). Two distinct nodes' `outputColumns`
// can share Column identity; downstream passes must not assume freshness.
Translated Translator::translateSet(
    const lp::SetNode& set,
    const LpNameSet& required) {
  switch (set.operation()) {
    case lp::SetOperation::kUnionAll:
      return buildUnionAll(set.inputs(), set.outputType(), required);
    case lp::SetOperation::kUnion: {
      Translated all = buildUnionAll(
          set.inputs(), set.outputType(), allNames(*set.outputType()));
      const ColumnVector& cols = all.node->outputColumns();
      Scope newScope;
      populateScope(*set.outputType(), cols, newScope);
      AggregateCP aggNode = builder_.make<Aggregate>(
          {all.node,
           ExprVector{cols.begin(), cols.end()},
           AggregateCallVector{},
           ColumnVector{cols}});
      return {aggNode, std::move(newScope)};
    }
    case lp::SetOperation::kIntersect:
    case lp::SetOperation::kIntersectAll:
    case lp::SetOperation::kExcept:
    case lp::SetOperation::kExceptAll: {
      const bool isAnti = set.operation() == lp::SetOperation::kExcept ||
          set.operation() == lp::SetOperation::kExceptAll;
      const bool isDistinct = set.operation() == lp::SetOperation::kIntersect ||
          set.operation() == lp::SetOperation::kExcept;
      const velox::core::JoinType joinType = isAnti
          ? (isDistinct ? velox::core::JoinType::kAnti
                        : velox::core::JoinType::kCountingAnti)
          : (isDistinct ? velox::core::JoinType::kLeftSemiFilter
                        : velox::core::JoinType::kCountingLeftSemiFilter);

      // Set-op via Join: each leg's columns become join keys, so all of each
      // leg's outputType is consumed.
      Translated first = translateNode(
          *set.inputs().front(), allNames(*set.inputs().front()->outputType()));
      NodeCP node = first.node;
      for (size_t i = 1; i < set.inputs().size(); ++i) {
        NodeCP other =
            translateNode(
                *set.inputs()[i], allNames(*set.inputs()[i]->outputType()))
                .node;
        const auto& leftColumns = node->outputColumns();
        const auto& rightColumns = other->outputColumns();
        VELOX_CHECK_EQ(leftColumns.size(), rightColumns.size());
        ExprVector leftKeys;
        ExprVector rightKeys;
        leftKeys.reserve(leftColumns.size());
        rightKeys.reserve(leftColumns.size());
        for (size_t columnIndex = 0; columnIndex < leftColumns.size();
             ++columnIndex) {
          leftKeys.push_back(leftColumns[columnIndex]);
          rightKeys.push_back(rightColumns[columnIndex]);
        }
        ColumnVector outputColumns{leftColumns};
        node = builder_.make<Join>(
            {node,
             other,
             joinType,
             std::move(leftKeys),
             std::move(rightKeys),
             /*filter=*/ExprVector{},
             /*nullAware=*/false,
             /*nullAsValue=*/true,
             std::move(outputColumns)});
      }

      // Output columns are the first leg's, flowing through the Join unchanged.
      // Resolve each output position by name through the first leg's scope so
      // positions the leg collapsed to one Column (see translateProject) map
      // both symbols to that shared Column.
      Scope scope;
      const auto& firstType = *set.inputs().front()->outputType();
      for (size_t j = 0; j < set.outputType()->size(); ++j) {
        auto it = first.scope.find(firstType.nameOf(j));
        VELOX_CHECK(
            it != first.scope.end(),
            "Set-op leg scope missing column: {}",
            firstType.nameOf(j));
        scope[set.outputType()->nameOf(j)] = it->second;
      }

      if (isDistinct) {
        const ColumnVector& outputColumns = node->outputColumns();
        AggregateCP aggNode = builder_.make<Aggregate>(
            {node,
             ExprVector{outputColumns.begin(), outputColumns.end()},
             AggregateCallVector{},
             outputColumns});
        return {aggNode, std::move(scope)};
      }
      return {node, std::move(scope)};
    }
  }
  VELOX_UNREACHABLE();
}

velox::core::JoinType toVeloxJoinType(lp::JoinType type) {
  switch (type) {
    case lp::JoinType::kInner:
      return velox::core::JoinType::kInner;
    case lp::JoinType::kLeft:
      return velox::core::JoinType::kLeft;
    case lp::JoinType::kRight:
      return velox::core::JoinType::kRight;
    case lp::JoinType::kFull:
      return velox::core::JoinType::kFull;
  }
  VELOX_UNREACHABLE();
}

// True if a subquery lifted onto the left input correlates to a column of
// 'right'. Such a correlation can't be satisfied by an Apply on the left input.
bool correlatesToRight(NodeCP lifted, NodeCP leftBeforeLift, NodeCP right) {
  const PlanObjectSet rightColumns =
      PlanObjectSet::fromObjects(right->outputColumns());
  for (NodeCP node = lifted; node != leftBeforeLift; node = node->inputs()[0]) {
    if (node->nodeType() != NodeType::kApply) {
      continue;
    }
    for (ColumnCP column : node->as<Apply>()->correlationColumns()) {
      if (rightColumns.contains(column)) {
        return true;
      }
    }
  }
  return false;
}

Translated Translator::translateJoin(
    const lp::JoinNode& join,
    const LpNameSet& required) {
  // Names the join condition reads need to be in both side's scopes; partition
  // them by which side each name belongs to.
  LpNameSet conditionNames;
  if (join.condition() != nullptr) {
    collectUsedNames(*join.condition(), conditionNames);
  }
  const auto& leftType = *join.left()->outputType();
  const auto& rightType = *join.right()->outputType();
  LpNameSet leftRequired;
  LpNameSet rightRequired;
  for (size_t i = 0; i < leftType.size(); ++i) {
    const auto& name = leftType.nameOf(i);
    if (required.contains(name) || conditionNames.contains(name)) {
      leftRequired.insert(name);
    }
  }
  for (size_t i = 0; i < rightType.size(); ++i) {
    const auto& name = rightType.nameOf(i);
    if (required.contains(name) || conditionNames.contains(name)) {
      rightRequired.insert(name);
    }
  }
  Translated left = translateNode(*join.left(), leftRequired);
  Translated right = translateNode(*join.right(), rightRequired);

  Scope merged = left.scope;
  for (auto& [name, column] : right.scope) {
    merged[name] = column;
  }

  const velox::core::JoinType joinType = toVeloxJoinType(join.joinType());
  const bool isInner = joinType == velox::core::JoinType::kInner;
  const bool conditionHasSubquery =
      join.condition() != nullptr && containsSubquery(*join.condition());

  // A subquery in the condition can't run inside the join operator;
  // lift it into a Filter above the join. The lifted Filter reads
  // condition columns from the join's output, so when lifting keep
  // the full left+right union as `outputColumns`.
  const bool liftConditionAbove = isInner && conditionHasSubquery;

  PlanObjectSet requiredColumns;
  if (!liftConditionAbove) {
    auto collect = [&](const velox::RowType& type, const Scope& scope) {
      for (size_t i = 0; i < type.size(); ++i) {
        const auto& name = type.nameOf(i);
        if (!required.contains(name)) {
          continue;
        }
        auto it = scope.find(name);
        VELOX_DCHECK(it != scope.end());
        requiredColumns.add(it->second);
      }
    };
    collect(leftType, left.scope);
    collect(rightType, right.scope);
  }

  ColumnVector outputColumns;
  for (ColumnCP column : left.node->outputColumns()) {
    if (liftConditionAbove || requiredColumns.contains(column)) {
      outputColumns.push_back(column);
    }
  }
  for (ColumnCP column : right.node->outputColumns()) {
    if (liftConditionAbove || requiredColumns.contains(column)) {
      outputColumns.push_back(column);
    }
  }

  if (liftConditionAbove) {
    JoinCP joinNode = builder_.make<Join>(
        {left.node,
         right.node,
         joinType,
         /*leftKeys=*/ExprVector{},
         /*rightKeys=*/ExprVector{},
         /*filter=*/ExprVector{},
         /*nullAware=*/false,
         /*nullAsValue=*/false,
         std::move(outputColumns)});
    return maybeWrapInFilter(joinNode, *join.condition(), std::move(merged));
  }

  JoinCondition::Split split;
  if (join.condition() != nullptr) {
    // Lift a subquery in the condition onto the left input; the condition then
    // references the lifted result column.
    NodeCP leftBeforeLift = left.node;
    ExprCP condition =
        translateExpr(*join.condition(), merged, /*applyTarget=*/&left.node);
    // A subquery correlated to the right input, lifted onto the left,
    // references a column the left can't provide.
    if (correlatesToRight(left.node, leftBeforeLift, right.node)) {
      VELOX_NYI(
          "Correlated subquery referencing the right side of an outer join's "
          "ON clause is not supported");
    }

    split = JoinCondition::splitEquiKeys(
        ExprFactory::flattenAnd(condition),
        PlanObjectSet::fromObjects(left.node->outputColumns()),
        PlanObjectSet::fromObjects(right.node->outputColumns()));
  }

  JoinCP joinNode = builder_.make<Join>(
      {left.node,
       right.node,
       joinType,
       std::move(split.leftKeys),
       std::move(split.rightKeys),
       std::move(split.residual),
       /*nullAware=*/false,
       /*nullAsValue=*/false,
       std::move(outputColumns)});
  return {joinNode, std::move(merged)};
}

Translated Translator::translateLateralJoin(
    const lp::LateralJoinNode& join,
    const LpNameSet& /*required*/) {
  // Keep all columns of both sides: the body may correlate on any left
  // column, and the Apply output is input.columns ++ body.columns. Unused
  // columns are pruned in a later pass.
  Translated left =
      translateNode(*join.left(), allNames(*join.left()->outputType()));

  // With the left scope pushed, the body's references to left columns are
  // captured as the Apply's correlation.
  subqueries_.push(left.scope);
  Translated right =
      translateNode(*join.right(), allNames(*join.right()->outputType()));
  ColumnVector correlationColumns = subqueries_.pop();

  // The ON condition may read either side.
  Scope merged = left.scope;
  for (auto& [name, column] : right.scope) {
    merged[name] = column;
  }

  ExprVector filter;
  if (join.condition() != nullptr) {
    if (containsSubquery(*join.condition())) {
      VELOX_NYI("Subquery in a LATERAL join ON condition is not supported");
    }
    ExprCP condition =
        translateExpr(*join.condition(), merged, /*applyTarget=*/nullptr);
    filter = ExprFactory::flattenAnd(condition);
  }

  // CROSS / INNER JOIN LATERAL -> kInner (outers with no body row are
  // dropped, no pad rows). LEFT JOIN LATERAL -> kLeft (NULL-padded).
  const velox::core::JoinType kind = join.joinType() == lp::JoinType::kLeft
      ? velox::core::JoinType::kLeft
      : velox::core::JoinType::kInner;

  ColumnVector outputColumns = left.node->outputColumns();
  appendUnique(outputColumns, right.node->outputColumns());
  ColumnCP includeMarker = nullptr;
  if (kind == velox::core::JoinType::kLeft) {
    includeMarker = Column::createBoolean("_include");
    outputColumns.push_back(includeMarker);
  }

  auto* apply = builder_.make<Apply>(
      {left.node,
       right.node,
       std::move(correlationColumns),
       kind,
       std::move(filter),
       /*enforceSingleRow=*/false,
       /*markColumn=*/nullptr,
       /*inLhs=*/nullptr,
       /*inBodyKey=*/nullptr,
       includeMarker,
       std::move(outputColumns)});
  return {apply, std::move(merged)};
}

ExprCP Translator::translateExpr(
    const lp::Expr& expr,
    const Scope& scope,
    NodeCP* applyTarget) {
  switch (expr.kind()) {
    case lp::ExprKind::kInputReference:
      return translateInputReference(*expr.as<lp::InputReferenceExpr>(), scope);
    case lp::ExprKind::kConstant:
      return translateConstant(*expr.as<lp::ConstantExpr>());
    case lp::ExprKind::kCall:
      return translateCall(*expr.as<lp::CallExpr>(), scope, applyTarget);
    case lp::ExprKind::kSpecialForm:
      return translateSpecialForm(
          *expr.as<lp::SpecialFormExpr>(), scope, applyTarget);
    case lp::ExprKind::kLambda:
      return translateLambda(*expr.as<lp::LambdaExpr>(), scope);
    case lp::ExprKind::kSubquery: {
      // Bare subquery in scalar position. `liftSubquery` decides the
      // lowered shape: uncorrelated → cross-join + EnforceSingleRow
      // (no Apply), correlated → kLeft Apply. Scalar shape is signaled
      // by a null `inLhs` (no IN equi to build).
      const auto& subquery = *expr.as<lp::SubqueryExpr>();
      const auto* innerPlan = subquery.subquery().get();
      if (applyTarget != nullptr) {
        auto it = scalarSubqueryColumns_.find(innerPlan);
        if (it != scalarSubqueryColumns_.end() &&
            outputContains(*applyTarget, it->second)) {
          return it->second;
        }
      }
      ColumnCP column = liftSubquery(
          subquery,
          velox::core::JoinType::kLeft,
          /*inLhs=*/nullptr,
          scope,
          applyTarget);
      if (applyTarget != nullptr) {
        scalarSubqueryColumns_[innerPlan] = column;
      }
      return column;
    }
    default:
      VELOX_NYI(
          "Unsupported expression kind: {}", static_cast<int>(expr.kind()));
  }
}

ExprCP Translator::translateInputReference(
    const lp::InputReferenceExpr& expr,
    const Scope& scope) {
  const auto& name = expr.name();
  auto it = scope.find(name);
  if (it != scope.end()) {
    return it->second;
  }
  if (ColumnCP outer = subqueries_.resolveOuter(name)) {
    return outer;
  }
  VELOX_FAIL("Column not found in scope or outer scopes: {}", name);
}

ExprCP Translator::translateConstant(const lp::ConstantExpr& expr) {
  auto* variant = queryCtx()->registerVariant(
      std::make_unique<velox::Variant>(*expr.value()));
  Value value(toType(expr.type()), 1);
  return builder_.makeLiteral(value, variant);
}

ExprCP Translator::translateCall(
    const lp::CallExpr& expr,
    const Scope& scope,
    NodeCP* applyTarget) {
  ExprVector args = translateAll(expr.inputs(), scope, applyTarget);
  Value value =
      clampCardinality(Value{toType(expr.type()), maxCardinality(args)});
  Name name = toName(expr.name());
  FunctionSet funcs =
      Call::unionArgFunctions(functionBits(name, /*specialForm=*/false), args);
  auto* call = builder_.makeCall(name, value, std::move(args), funcs);
  return simplifier_.simplify(call);
}

// True if 'expr' is `lp::SpecialFormExpr(kIn, [value, SubqueryExpr])` —
// i.e., the IN-subquery form that lifts to Apply, distinguished from
// `IN (literal_list)` which lowers as an ordinary in-list expression.
bool isInSubqueryForm(const lp::Expr& expr) {
  if (!expr.isSpecialForm()) {
    return false;
  }
  const auto& sf = *expr.as<lp::SpecialFormExpr>();
  return sf.form() == lp::SpecialForm::kIn && sf.inputs().size() == 2 &&
      sf.inputAt(1)->isSubquery();
}

ExprCP Translator::translateSpecialForm(
    const lp::SpecialFormExpr& expr,
    const Scope& scope,
    NodeCP* applyTarget) {
  if (expr.form() == lp::SpecialForm::kExists) {
    return translateExists(expr, scope, applyTarget);
  }

  // IN (subquery): translate the lhs, then lift
  // `Apply(kLeftSemiProject)` passing the lhs — `liftSubquery` records
  // the lhs and the body's single output column as `Apply.inLhs` /
  // `Apply.inBodyKey`. `Apply.nullAware()` returns true from
  // their presence. Plain `IN (literals...)` flows through the
  // generic path below.
  if (isInSubqueryForm(expr)) {
    ExprCP lhs = translateExpr(*expr.inputAt(0), scope, applyTarget);
    const auto& subquery = *expr.inputAt(1)->as<lp::SubqueryExpr>();
    return liftSubquery(
        subquery,
        velox::core::JoinType::kLeftSemiProject,
        /*inLhs=*/lhs,
        scope,
        applyTarget);
  }

  // NOT EXISTS / NOT IN come through as `lp::Call("not", [kExists|kIn])`,
  // not as a `SpecialForm::kNot`. The Call recursion handles the wrap:
  // translateCall translates the EXISTS/IN arg (which lifts Apply and
  // returns its mark), then builds `Call("not", [mark])`. The Apply
  // shape is identical for IN vs NOT IN — null-aware semi-project both
  // ways; the surrounding NOT inverts the mark.

  ExprVector args = translateAll(expr.inputs(), scope, applyTarget);
  Value value =
      clampCardinality(Value{toType(expr.type()), maxCardinality(args)});

  // Drop a redundant cast: CAST(x AS t) => x when typeof(x) == t. TRY_CAST too,
  // since a same-type cast never errors. Pointer equality on interned types is
  // exact, so a row-renaming cast (distinct interned type) is kept.
  if (expr.form() == lp::SpecialForm::kCast ||
      expr.form() == lp::SpecialForm::kTryCast) {
    VELOX_CHECK_EQ(args.size(), 1);
    if (args[0]->value().type == value.type) {
      return args[0];
    }
  }

  // Normalize an IN list: a single element folds to equality (`x IN (a)` is
  // `x = a`) and a constant list drops duplicates (`x IN (a, a)` is `x IN
  // (a)`).
  if (expr.form() == lp::SpecialForm::kIn) {
    if (ExprCP folded = normalizeInList(args)) {
      return folded;
    }
  }

  // `SpecialFormCallNames::toCallName` returns a static literal that is
  // pointer-compared in `tryFromCallName`. Do not re-intern via `toName`,
  // which would allocate a fresh arena copy and break the comparison.
  Name callName = SpecialFormCallNames::toCallName(expr.form());

  if (expr.form() == lp::SpecialForm::kDereference) {
    return translateDereference(std::move(args), callName, value);
  }

  FunctionSet funcs = Call::unionArgFunctions(
      functionBits(callName, /*specialForm=*/true), args);
  auto* call = builder_.makeCall(callName, value, std::move(args), funcs);
  return simplifier_.simplify(call);
}

ExprCP Translator::translateExists(
    const lp::SpecialFormExpr& expr,
    const Scope& scope,
    NodeCP* applyTarget) {
  VELOX_CHECK_EQ(expr.inputs().size(), 1);
  const auto& subquery = *expr.inputs()[0]->as<lp::SubqueryExpr>();

  // Fast path — EXISTS over a scalar (non-grouping) Aggregate with no
  // HAVING above it is provably TRUE per SQL: scalar aggregates always
  // emit exactly one row, so EXISTS sees that one row regardless of
  // input cardinality. Fold to a constant before constructing Apply,
  // matching Presto / modern DuckDB on shapes like
  // `EXISTS (SELECT count(*) FROM t WHERE p)`. HAVING is the only
  // operator that can make EXISTS-over-scalar-Agg evaluate to FALSE;
  // when present (Filter above Aggregate), the root is a FilterNode
  // and this fast path doesn't fire — Decorrelate's Rule B-EXISTS
  // Path 1 handles it correctly.
  //
  // Walk past output-shaping Project chains (parser typically wraps
  // `SELECT count(*) FROM ...` as `Project(Aggregate(...))`).
  const lp::LogicalPlanNode* root = subquery.subquery().get();
  while (root != nullptr && root->kind() == lp::NodeKind::kProject) {
    root = root->onlyInput().get();
  }
  if (root != nullptr && root->kind() == lp::NodeKind::kAggregate) {
    const auto* aggNode = root->as<lp::AggregateNode>();
    if (aggNode->groupingKeys().empty() && aggNode->groupingSets().empty()) {
      return builder_.makeLiteral(
          velox::Variant(true), toType(velox::BOOLEAN()));
    }
  }
  return liftSubquery(
      subquery,
      velox::core::JoinType::kLeftSemiProject,
      /*inLhs=*/nullptr,
      scope,
      applyTarget);
}

ExprCP Translator::normalizeInList(ExprVector& args) {
  VELOX_CHECK_GE(args.size(), 2);

  // Single-element folding is sound in three-valued logic: `a IN (b)` and
  // `a = b` agree on true, false, and null. The null-literal case folds to a
  // constant (rather than `eq(a, null)`) so the surrounding expression can fold
  // further without materializing the column.
  auto foldSingleElement = [&](ExprCP element) -> ExprCP {
    if (element->is(PlanType::kLiteralExpr) &&
        element->as<Literal>()->literal().isNull()) {
      return builder_.makeLiteral(
          velox::Variant::null(velox::TypeKind::BOOLEAN),
          toType(velox::BOOLEAN()));
    }
    return simplifier_.simplify(exprFactory_.makeEq(args[0], element));
  };

  if (args.size() == 2) {
    return foldSingleElement(args[1]);
  }

  // A multi-element list can be deduped and lowered to an array constant only
  // when every element is a literal.
  if (!std::all_of(args.begin() + 1, args.end(), [](ExprCP arg) {
        return arg->is(PlanType::kLiteralExpr);
      })) {
    return nullptr;
  }

  // Equal Literals hash-cons to the same Expr, so dedup by identity.
  ExprVector deduped;
  deduped.reserve(args.size());
  deduped.push_back(args[0]);
  folly::F14FastSet<ExprCP> seen;
  seen.reserve(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    if (seen.insert(args[i]).second) {
      deduped.push_back(args[i]);
    }
  }

  // A list of equal literals (`a IN (5, 5)`) collapses to a single value.
  if (deduped.size() == 2) {
    return foldSingleElement(deduped[1]);
  }
  args = std::move(deduped);
  return nullptr;
}

ExprCP Translator::translateDereference(
    ExprVector args,
    Name callName,
    const Value& value) {
  // Resolve a varchar field name to a numeric field index against the input row
  // type, so emit only ever sees an integer selector.
  VELOX_CHECK_EQ(args.size(), 2);
  VELOX_CHECK(args[1]->is(PlanType::kLiteralExpr));
  const auto* selector = args[1]->as<Literal>();
  const auto* baseType = args[0]->value().type;
  VELOX_CHECK(baseType->isRow());
  int32_t index;
  if (selector->literal().kind() == velox::TypeKind::VARCHAR) {
    const auto& name = selector->literal().value<velox::TypeKind::VARCHAR>();
    index = baseType->asRow().getChildIdx(name);
  } else {
    const int64_t raw = integerValue(&selector->literal());
    VELOX_CHECK_GE(raw, 0);
    VELOX_CHECK_LT(raw, baseType->size());
    index = static_cast<int32_t>(raw);
  }
  auto* indexLiteral =
      builder_.makeLiteral(velox::Variant(index), toType(velox::INTEGER()));
  ExprVector resolved{args[0], indexLiteral};
  FunctionSet funcs = Call::unionArgFunctions(
      functionBits(callName, /*specialForm=*/true), resolved);
  auto* call = builder_.makeCall(callName, value, std::move(resolved), funcs);
  return simplifier_.simplify(call);
}

ExprCP Translator::translateLambda(
    const lp::LambdaExpr& expr,
    const Scope& scope) {
  const auto& signature = expr.signature();

  // TODO: mint canonical `Column*` per `(arg position, arg type)` so
  // structurally-equal lambdas share one `Lambda*` (and their bodies share
  // one `Call*` via Call hash-cons). Today each occurrence mints fresh arg
  // Columns, defeating pointer-keyed dedup.
  Scope bodyScope = scope;
  ColumnVector args;
  args.reserve(signature->size());
  for (size_t i = 0; i < signature->size(); ++i) {
    Name argName = toName(signature->nameOf(i));
    Value argValue(toType(signature->childAt(i)), 1);
    auto* column = Column::create(argName, argValue);
    args.push_back(column);
    bodyScope[signature->nameOf(i)] = column;
  }

  // Lambdas appear inside higher-order functions; subqueries inside a
  // lambda body would need lifting above the surrounding higher-order
  // call's relational input — uncommon and not threaded today.
  ExprCP body = translateExpr(*expr.body(), bodyScope, /*applyTarget=*/nullptr);
  return make<Lambda>(std::move(args), toType(expr.type()), body);
}

// Returns true if 'node' provably produces exactly one row. An EnforceSingleRow
// over such a node is a no-op: the >1-row assertion can never fire, and the
// empty-input null row it would synthesize can never be produced. Conservative
// — returns false when it cannot prove exactly one row.
bool producesExactlyOneRow(NodeCP node) {
  switch (node->nodeType()) {
    case NodeType::kAggregate: {
      // A global (ungrouped) aggregate emits exactly one row, even over empty
      // input. A grouping-set aggregate keys on the group-id column, so its
      // grouping keys are non-empty and it is excluded here.
      const auto* aggregate = node->as<Aggregate>();
      return aggregate->groupingKeys().empty();
    }
    case NodeType::kEnforceSingleRow:
      return true;
    case NodeType::kProject:
      return producesExactlyOneRow(node->as<Project>()->input());
    default:
      return false;
  }
}

ColumnCP Translator::liftSubquery(
    const lp::SubqueryExpr& subqueryExpr,
    velox::core::JoinType kind,
    ExprCP inLhs,
    const Scope& outerScope,
    NodeCP* applyTarget) {
  VELOX_USER_CHECK_NOT_NULL(
      applyTarget,
      "Subquery encountered in an expression position with no relational "
      "input above which to lift Apply");

  const bool isSemi = kind == velox::core::JoinType::kLeftSemiProject;
  // IN is the semi shape that has a non-null `inLhs`. EXISTS is semi
  // without one. Scalar is non-semi (kInner or kLeft).
  const bool isIn = isSemi && inLhs != nullptr;
  const bool isExists = isSemi && !isIn;
  // EXISTS reads only row presence — empty required-set lets the body
  // prune all output columns. IN and scalar both need every body
  // output column kept (IN reads it for the equi predicate; scalar
  // reads it as the value).
  const LpNameSet required =
      isExists ? LpNameSet{} : allNames(*subqueryExpr.subquery()->outputType());

  subqueries_.push(outerScope);
  Translated inner = translateNode(*subqueryExpr.subquery(), required);
  ColumnVector correlationColumns = subqueries_.pop();

  NodeCP body = inner.node;
  ColumnCP markColumn = nullptr;
  ColumnCP returnedColumn = nullptr;
  bool enforceSingleRow = false;

  if (isSemi) {
    // EXISTS / IN: synthesize a fresh BOOLEAN mark. Apply emits it via
    // `markColumn`; the parent expression site references it.
    // `enforceSingleRow` is meaningless for semi (cardinality assertion
    // doesn't apply); kept false.
    markColumn = Column::createBoolean("mark");
    returnedColumn = markColumn;
  } else {
    // Scalar: body must produce exactly one column. The single body
    // output col IS the value the parent expression references.
    const auto& bodyOut = body->outputColumns();
    VELOX_USER_CHECK_EQ(
        bodyOut.size(), 1, "Scalar subquery must produce exactly one column");
    returnedColumn = bodyOut[0];
    // Shape C fix: if body's
    // single output column collides by name with any column in
    // applyTarget's outputColumns (e.g., `SELECT (SELECT a) FROM t`
    // where body's output and outer t.a are both named "a"), wrap
    // body in an aliasing Project with a fresh name. The alias becomes
    // returnedColumn.
    //
    // KNOWN LIMITATION: this fix is design-correct at Translate level,
    // but Decorrelate's Project peel currently exposes the original
    // body (under the alias wrap) when peeling, and the resulting
    // inner Apply's outputColumns = input.cols ++ originalBody.cols
    // brings the duplicate back. Full Shape C handling requires
    // Decorrelate's Project peel to apply the same aliasing trick to
    // body cols that collide with input cols. Currently NYI in
    // Decorrelate; tests like `SELECT (SELECT a) FROM t` still fail
    // with "Duplicate output column" at PlanConsistencyChecker.
    {
      const auto& targetCols = (*applyTarget)->outputColumns();
      auto hasNameCollision = [&](std::string_view name) {
        for (ColumnCP c : targetCols) {
          if (c->name() == name) {
            return true;
          }
        }
        return false;
      };
      if (hasNameCollision(returnedColumn->name())) {
        ColumnCP alias = Column::create(
            std::string(returnedColumn->name()) + "__lift",
            returnedColumn->value());
        body = builder_.make<Project>(Project::Key{
            body,
            ExprVector{returnedColumn},
            ColumnVector{alias},
        });
        returnedColumn = alias;
      }
    }

    if (correlationColumns.empty()) {
      // Uncorrelated scalar: cross-join with the body, which must yield a
      // single row. Wrap it in EnforceSingleRow unless it already provably
      // produces exactly one row, in which case the guard is a no-op.
      NodeCP wrapped = producesExactlyOneRow(body)
          ? body
          : builder_.make<EnforceSingleRow>(EnforceSingleRow::Key{body});

      ColumnVector joinOutput = (*applyTarget)->outputColumns();
      appendUnique(joinOutput, wrapped->outputColumns());

      *applyTarget = builder_.make<Join>(Join::Key{
          *applyTarget,
          wrapped,
          velox::core::JoinType::kInner,
          /*leftKeys=*/{},
          /*rightKeys=*/{},
          /*filter=*/ExprVector{},
          /*nullAware=*/false,
          /*nullAsValue=*/false,
          std::move(joinOutput),
      });
      return returnedColumn;
    }

    // Correlated scalar: kLeft Apply with enforceSingleRow=true.
    enforceSingleRow = true;
  }

  // For IN: capture the equi pair `(lhs, body.col)` directly. Apply
  // stores it as `inLhs` / `inBodyKey`; downstream consumers either
  // read the pair (decorrelate's semi-recovery extracts the equi-key
  // pair directly) or reconstruct an `eq` Call (uncorrelated IN
  // collapse uses it as the resulting Join's filter).
  ExprCP inBodyKey = nullptr;
  if (isIn) {
    const auto& bodyOut = body->outputColumns();
    VELOX_USER_CHECK_EQ(
        bodyOut.size(), 1, "IN subquery body must produce exactly one column");
    inBodyKey = bodyOut[0];
  }

  // Apply.outputColumns per kind:
  //   kLeft            : input.outputColumns
  //                      ++ unique(body.outputColumns) ++ includeMarker
  //   kLeftSemiProject : input.outputColumns ++ markColumn
  ColumnVector outputColumns = (*applyTarget)->outputColumns();
  if (isSemi) {
    outputColumns.push_back(markColumn);
  } else {
    appendUnique(outputColumns, body->outputColumns());
  }
  ColumnCP includeMarker = nullptr;
  if (kind == velox::core::JoinType::kLeft) {
    includeMarker = Column::createBoolean("_include");
    outputColumns.push_back(includeMarker);
  }

  auto* apply = builder_.make<Apply>(
      {*applyTarget,
       body,
       std::move(correlationColumns),
       kind,
       /*filter=*/ExprVector{},
       enforceSingleRow,
       markColumn,
       inLhs,
       inBodyKey,
       includeMarker,
       std::move(outputColumns)});
  *applyTarget = apply;
  return returnedColumn;
}

} // namespace

TranslatePass::Result TranslatePass::run(
    const lp::LogicalPlanNode& plan,
    optimizer::Schema& schema,
    velox::core::ExpressionEvaluator& evaluator,
    Builder& builder) {
  Translator translator(schema, evaluator, builder);
  return translator.run(plan);
}

} // namespace facebook::axiom::optimizer::v2
