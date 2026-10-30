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

#include <folly/ScopeGuard.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/NodeSimplifier.h"
#include "axiom/optimizer/v2/PrecomputeProjections.h"
#include "axiom/optimizer/v2/TranslatePass.h"

// Internals of TranslatePass, shared by the files that implement it. Other
// passes do not include this header.

namespace facebook::axiom::optimizer::v2 {

/// Translates a logical plan into Node IR. TranslatePass::run creates one per
/// plan.
class Translator {
 public:
  /// Set of LP-output column names the consumer needs from this translation.
  /// Threaded top-down through `translateNode` so producers (Scan, Project,
  /// Aggregate, etc.) can mint / keep only the columns downstream actually
  /// uses.
  using LpNameSet = folly::F14FastSet<std::string>;

  /// Node that lifted subqueries attach to, plus the lifts not yet joined onto
  /// it. Attaching is deferred so that a reference from a subquery body can
  /// join the same pending lifts, which are then evaluated once and attached
  /// once.
  ///
  /// Created by 'Translator::withLiftTarget', which attaches the pending lifts
  /// once translation is done and returns the result. Code outside the lift
  /// machinery reads 'node' from there, never from here, since it is not a
  /// usable plan while lifts are pending.
  struct LiftTarget {
    /// Node the lifts attach to.
    NodeCP node{nullptr};

    /// Lifted subqueries not yet joined onto 'node', joined to each other.
    /// Every member produces a single row, and the group reads nothing from
    /// 'node' — which is what lets it attach with a cross join. Members are
    /// uncorrelated scalar lifts, plus bodies correlated only to columns
    /// already in here, which join them and so keep both properties.
    NodeCP pendingLifts{nullptr};

    /// True if 'column' is readable by an expression lifting onto this target.
    bool outputs(ColumnCP column) const;
  };

  /// Resolution context for translating expressions in a fixed scope. Maps
  /// output column name to the Column that produces it.
  using Scope = folly::F14FastMap<std::string, ExprCP>;

  /// A translated logical plan node and the expressions its output names bind
  /// to.
  struct Translated {
    NodeCP node{nullptr};
    Scope scope;
  };

  /// Outer-scope chain used while translating subquery bodies, plus the
  /// per-nesting-level set of correlated columns referenced so far.
  /// `liftSubquery` consumes the captured correlations as `Apply.correlations`.
  ///
  /// Invariant: `outerScopes_`, `liftTargets_` and `correlationsStack_` all
  /// have one entry per body in flight.
  class SubqueryContext {
   public:
    /// Resolves 'name' against enclosing scopes, innermost-first, and records
    /// the hit as a correlation. Returns nullptr if not found at any level.
    ExprCP correlateOuter(std::string_view name);

    /// Records 'column', produced by an enclosing scope's lift, as a
    /// correlation of every body in flight below the scope that outputs it.
    /// Returns false when no enclosing scope outputs it, meaning the body in
    /// flight cannot read it.
    bool correlateLifted(ColumnCP column);

    /// Prevents lifted-result reuse from reaching targets outside the current
    /// set-operation branch or subquery body. Reuse within scopes entered after
    /// the barrier is still allowed.
    void pushLiftedCorrelationBarrier();
    void popLiftedCorrelationBarrier();

    /// True if the body in flight has referenced an enclosing scope. A
    /// correlation recorded at an outer level is recorded at every level below
    /// it, so the innermost entry is the whole set the body can see.
    bool hasCorrelations() const;

    /// Enters/leaves the body of a subquery. 'liftTarget' is the node the
    /// body will be lifted onto, i.e. the plan of the scope enclosing it.
    /// `pop()` returns the correlated columns collected during that body,
    /// deduplicated.
    void push(const Scope& outerScope, LiftTarget* liftTarget);
    ColumnVector pop();

   private:
    // Records 'column', held by the scope at 'level', as a correlation of every
    // body in flight below it.
    void recordCorrelation(ExprCP expr, size_t level);

    std::vector<const Scope*> outerScopes_;
    // Lift target of the scope enclosing each in-flight body. Held by pointer
    // and read on use because a target is replaced as lifts are added to it;
    // the pointee belongs to the caller that pushed and outlives the body.
    std::vector<LiftTarget*> liftTargets_;
    std::vector<ColumnVector> correlationsStack_;

    // First lift-target level visible within each active barrier.
    std::vector<size_t> liftedCorrelationBarriers_;
  };

  Translator(
      optimizer::Schema& schema,
      const connector::SchemaResolver& schemaResolver,
      velox::core::ExpressionEvaluator& evaluator,
      Builder& builder,
      const OptimizerSession& session,
      const ConstantPlanRunner& constantPlanRunner)
      : schema_(schema),
        schemaResolver_(schemaResolver),
        builder_(builder),
        exprFactory_(builder),
        simplifier_(builder, evaluator),
        nodeSimplifier_(builder, simplifier_),
        evaluator_(evaluator),
        session_(session),
        constantPlanRunner_(constantPlanRunner) {}

  /// Translates 'plan' into Node IR.
  TranslatePass::Result run(const logical_plan::LogicalPlanNode& plan);

 private:
  // 'required' is the set of LP-output column names the consumer needs from
  // this node. Producers (Scan, Project, Aggregate, Window, Unnest, Values)
  // may emit only outputs in `required`; intermediate nodes propagate it
  // down, augmented with the names the node itself reads.
  Translated translateNode(
      const logical_plan::LogicalPlanNode& node,
      const LpNameSet& required);

  // Translates 'node' keeping every column it outputs, for consumers that
  // read all of them.
  Translated translateNode(const logical_plan::LogicalPlanNode& node);

  // The scan a delete removes rows from, and the columns that scan must
  // produce to identify them. The columns are empty when the connector cannot
  // delete row by row; the scan is set for every delete.
  struct DeleteTarget {
    const logical_plan::TableScanNode* scan{nullptr};
    std::vector<Name> rowIdColumns;
  };

  // Finds the target scan under the optional DELETE filter.
  DeleteTarget resolveDeleteTarget(
      const logical_plan::TableWriteNode& tableWrite,
      const connector::Table& connectorTable) const;

  Translated translateScan(
      const logical_plan::TableScanNode& scan,
      const LpNameSet& required);
  // Translates the DELETE target and returns its row identity in connector
  // order through 'rowIdColumns'.
  Translated translateDeleteScan(
      const logical_plan::TableScanNode& scan,
      const LpNameSet& required,
      const std::vector<Name>& rowIdColumnNames,
      ColumnVector& rowIdColumns);

  // A column a scan reads: the name of the column in the table, and the name
  // the scan's scope binds it to.
  struct ScanColumn {
    std::string schemaName;
    std::string outputName;
  };

  // Translates a scan of table 'tableName' in connector 'connectorId' that
  // reads 'columns'.
  Translated translateScan(
      const std::string& connectorId,
      const SchemaTableName& tableName,
      const std::vector<ScanColumn>& columns);
  Translated translateFilter(
      const logical_plan::FilterNode& filter,
      const LpNameSet& required);
  // Translates the condition of 'filter' over 'input', the translation of its
  // input.
  Translated translateFilter(
      const logical_plan::FilterNode& filter,
      Translated input);
  Translated translateProject(
      const logical_plan::ProjectNode& project,
      const LpNameSet& required);

  // Translates the expressions of 'project' in 'required' over 'input', the
  // translation of its input.
  Translated translateProject(
      const logical_plan::ProjectNode& project,
      const LpNameSet& required,
      Translated input);
  Translated translateLimit(
      const logical_plan::LimitNode& limit,
      const LpNameSet& required);
  Translated translateSort(
      const logical_plan::SortNode& sort,
      const LpNameSet& required);

  // See the definitions below.
  ColumnCP materializeColumn(NodeCP* node, ExprCP expr, std::string_view name);

  // Applies node simplification facts to every expression published by a
  // translated scope.
  void applySubstitutions(const PlanSubstitutions& substitutions, Scope& scope);

  // Applies node simplification facts to each of 'exprs'.
  void applySubstitutions(
      const PlanSubstitutions& substitutions,
      ExprVector& exprs);

  // Returns 'expr' with node simplification facts applied, simplified.
  ExprCP substitute(const PlanSubstitutions& substitutions, ExprCP expr);

  ColumnVector narrowToNames(
      NodeCP* node,
      const Scope& scope,
      const std::vector<std::string>& names);

  void narrowToColumns(NodeCP* node, const ColumnVector& columns);

  void materializeScope(NodeCP* node, Scope& scope, const velox::RowType& type);

  // Rebinds exact column bindings in `scope` from `sources` to the aligned
  // fresh `outputs` created by a value-changing node.
  void rebindScope(
      Scope& scope,
      const ColumnVector& sources,
      const ColumnVector& outputs);

  ColumnCP
  columnInScope(NodeCP* node, const Scope& scope, const std::string& name);

  ExprCP materializeInto(
      PrecomputeProjections& precompute,
      ExprCP expr,
      bool allowConstant = false);

  // Translates ordering keys and drops the redundant ones (`RedundantKeys`).
  // Velox rejects Sort and TopN nodes with duplicate keys.
  std::pair<ExprVector, OrderTypeVector> translateOrdering(
      const std::vector<logical_plan::SortingField>& ordering,
      const Scope& scope,
      LiftTarget* liftTarget);

  Translated translateSample(
      const logical_plan::SampleNode& sample,
      const LpNameSet& required);

  // Returns the constant sample percentage (in [0, 100]) of a SampleNode.
  double extractSamplePercentage(const logical_plan::Expr& percentage);

  // Returns a zero-row result with 'outputType''s schema.
  Translated makeEmptyResult(const velox::RowType& outputType);

  Translated translateAggregate(
      const logical_plan::AggregateNode& aggregate,
      const LpNameSet& required);

  // Translates 'aggregate''s grouping keys into 'keys' and the columns they are
  // published under into 'columns', binding every grouping-key name in 'scope'.
  void translateGroupingKeys(
      const logical_plan::AggregateNode& aggregate,
      const Scope& inputScope,
      NodeCP& currentInput,
      ExprVector& keys,
      ColumnVector& columns,
      Scope& scope);

  // Returns the column an Aggregate publishes the key at 'index' of 'keys'
  // under, or nullptr when the Aggregate does not keep the key
  // (`NodeSimplifier::keepsGroupingKey`). A column key is its own output, since
  // Velox `AggregationNode` emits a grouping key under its input field-access
  // name; any other kept key gets a column named 'name'.
  ColumnCP groupingKeyOutput(
      const ExprVector& keys,
      size_t index,
      std::string_view name);

  // Lowers GROUPING SETS / ROLLUP / CUBE to a GroupId plus a plain aggregate
  // keyed on an explicit group-id column. Returns the aggregation node.
  NodeCP lowerGroupingSets(
      const logical_plan::AggregateNode& aggregate,
      const ExprVector& groupingKeys,
      AggregateCallVector aggregates,
      const ColumnVector& outputColumns,
      NodeCP currentInput,
      Scope& newScope);

  Translated translateValues(
      const logical_plan::ValuesNode& values,
      const LpNameSet& required);
  Translated translateUnnest(
      const logical_plan::UnnestNode& unnest,
      const LpNameSet& required);
  Translated translateSet(
      const logical_plan::SetNode& set,
      const LpNameSet& required);

  // Dedups the rows 'node' produces for a set operation. Groups by what 'scope'
  // binds each name of 'outputType' to: a column of 'node' or a literal.
  Translated
  dedupSetOutputs(NodeCP node, Scope scope, const velox::RowType& outputType);

  Translated translateJoin(
      const logical_plan::JoinNode& join,
      const LpNameSet& required);

  // Builds a simplified Join and applies its equivalences to the output scope.
  Translated simplifyJoin(Join::Key key, Scope scope);

  Translated translateLateralJoin(
      const logical_plan::LateralJoinNode& join,
      const LpNameSet& required);
  Translated translateTableWrite(
      const logical_plan::TableWriteNode& tableWrite,
      const LpNameSet& required);
  Translated translateFixedPoint(
      const logical_plan::FixedPointNode& fixedPoint,
      const LpNameSet& required);
  // Reads the enclosing fixed point's state under fresh columns, so two reads
  // of one state are distinct relations.
  Translated translateRecursiveRef(
      const logical_plan::RecursiveReferenceNode& ref,
      const LpNameSet& required);

  // Lowers a graph match to joins of its element tables' plans on element
  // identity, with the term predicates and the match predicate above them.
  Translated translateGraphMatch(
      const logical_plan::GraphMatchNode& match,
      const LpNameSet& required);

  class GraphMatchLowering;

  // Records a translation of 'plan' and returns a guard that ends it. A plan
  // translated more than once, such as a subquery referenced twice or a table
  // two graph patterns read, names its columns after the plan's symbols only
  // the first time, since two Columns cannot answer to one name where the
  // translations meet.
  auto translatingPlan(const logical_plan::LogicalPlanNode& plan) {
    const bool duplicate = !translatedPlans_.insert(&plan).second;
    const bool wasTranslatingDuplicate = std::exchange(
        translatingDuplicate_, translatingDuplicate_ || duplicate);
    return folly::makeGuard([this, wasTranslatingDuplicate] {
      translatingDuplicate_ = wasTranslatingDuplicate;
    });
  }

  // Lifts 'call' into an `Inference` node on 'liftTarget' and returns the
  // column carrying its result. Velox evaluates a remote inference call in its
  // own operator, so it cannot stay inside the expression that reads it.
  ExprCP liftInferenceCall(const Call* call, LiftTarget* liftTarget);

  NodeCP maybeWrapInWindow(
      NodeCP input,
      const Scope& inputScope,
      const std::vector<logical_plan::ExprPtr>& projectExprs,
      const std::vector<std::string>& projectNames,
      Scope& windowScope);

  // Flattens nested UNION ALL inputs and UNION inputs whose duplicate removal
  // is provided by an ancestor.
  std::vector<logical_plan::LogicalPlanNodePtr> flattenUnionInputs(
      const std::vector<logical_plan::LogicalPlanNodePtr>& inputs,
      bool dedupAbove) const;

  // Translates the retained positional outputs of each union input.
  std::vector<Translated> translateUnionInputs(
      const std::vector<logical_plan::LogicalPlanNodePtr>& inputs,
      const std::vector<size_t>& keptPositions);

  // `dedupAbove` says the caller dedups the result, which subsumes a nested
  // UNION's own dedup and so allows flattening such a leg.
  Translated buildUnionAll(
      const std::vector<logical_plan::LogicalPlanNodePtr>& inputs,
      const velox::RowTypePtr& outputType,
      const LpNameSet& required,
      bool dedupAbove);

  // Concatenates 'legs'. 'legExprs[i][j]' is the expression leg i produces at
  // output position j, which leg i materializes as a column named
  // 'legNames[i][j]'. The result's scope binds 'names[j]' to a column
  // 'makeColumn' creates from the position's value of type 'types[j]', or to
  // what simplifying the UnionAll substitutes for it: a literal, or a column of
  // the one remaining leg.
  Translated makeUnionAll(
      std::vector<NodeCP> legs,
      const std::vector<ExprVector>& legExprs,
      const std::vector<std::vector<std::string>>& legNames,
      const std::vector<std::string>& names,
      const std::vector<velox::TypePtr>& types,
      folly::FunctionRef<ColumnCP(std::string_view name, const Value& value)>
          makeColumn);

  // Translates `predicateExpr` against `scope` (lifting any subqueries
  // above `input` via Apply) and lowers it onto `input`. Returns an
  // empty `Values` of `input`'s schema when the predicate is
  // statically `false`, `input` unchanged when statically `true`, or a
  // `Filter` otherwise. When `propagateConstants` is true, output names fixed
  // by equality predicates bind to their literals in the returned scope.
  Translated maybeWrapInFilter(
      NodeCP input,
      const logical_plan::Expr& predicateExpr,
      Scope scope,
      bool propagateConstants);

  // Builds a simplified Filter and publishes any resulting constants through
  // its scope.
  Translated simplifyFilter(
      NodeCP input,
      ExprVector predicates,
      Scope scope,
      bool propagateConstants);

  // Rebinds scope expressions fixed by equality predicates to their literals.
  void propagateFilterConstants(const ExprVector& predicates, Scope& scope);

  // Translates an lp expression against 'scope'. If the expression
  // contains an `logical_plan::SubqueryExpr` (bare, or wrapped in
  // `kExists`/`kIn`/`kNot`), the subquery is lifted onto 'liftTarget' —
  // either deferred into its pending lifts or attached as an `Apply` — and
  // the returned `ExprCP` references the column it produces at the subquery
  // site.
  //
  // `liftTarget` must be non-null at any expression position that may
  // contain a subquery. Passing `nullptr` declares "no lift possible
  // here" and the lift path throws if a subquery is encountered
  // (e.g., row-literal expressions in `VALUES`).
  ExprCP translateExpr(
      const logical_plan::Expr& expr,
      const Scope& scope,
      LiftTarget* liftTarget);
  ExprCP translateInputReference(
      const logical_plan::InputReferenceExpr& expr,
      const Scope& scope);
  ExprCP translateConstant(const logical_plan::ConstantExpr& expr);
  ExprCP translateCall(
      const logical_plan::CallExpr& expr,
      const Scope& scope,
      LiftTarget* liftTarget);
  ExprCP translateSpecialForm(
      const logical_plan::SpecialFormExpr& expr,
      const Scope& scope,
      LiftTarget* liftTarget);

  // Translates an EXISTS special form by lifting an `Apply(kLeftSemiProject)`,
  // with a fast path that folds EXISTS over a scalar Aggregate to constant
  // TRUE.
  ExprCP translateExists(
      const logical_plan::SpecialFormExpr& expr,
      const Scope& scope,
      LiftTarget* liftTarget);

  // Normalizes an IN list. A list of literals takes ExprFactory::makeIn's
  // form, and a single non-literal element folds to equality. One-field row
  // constructors compare their field. Returns nullptr to let the caller build
  // the IN from 'args' through the generic path.
  ExprCP normalizeInList(ExprVector& args);

  // Builds ExprFactory::makeIn over 'values', comparing the field of a
  // one-field row constructor 'value'.
  ExprCP makeConstantIn(ExprCP value, std::vector<velox::Variant> values);

  // Returns scalar IN arguments for a one-field row IN, or nullopt when the
  // expression cannot be unwrapped.
  std::optional<ExprVector> unwrapSingleFieldRowIn(const ExprVector& args);

  // Translates a DEREFERENCE special form, resolving a varchar field name to a
  // numeric field index against the input row type.
  ExprCP
  translateDereference(ExprVector args, Name callName, const Value& value);

  ExprCP translateLambda(
      const logical_plan::LambdaExpr& expr,
      const Scope& scope,
      LiftTarget* liftTarget);

  // Joins a scalar body that reads only pending lifts into those lifts. Returns
  // the body's output at the new boundary, or nullptr when the body shape
  // cannot be joined.
  ColumnCP tryJoinIntoPendingLifts(
      NodeCP body,
      ColumnCP returnedColumn,
      LiftTarget& target);

  // Creates the Column a translated logical-plan node outputs under 'symbol'.
  // Names it after 'symbol', except while translating a plan an earlier
  // reference already translated, whose columns are named synthetically.
  ColumnCP columnForSymbol(Name symbol, const Value& value);

  // Builds one fresh Column per field of 'rowType' and populates 'scope' to
  // map each field name to its column.
  ColumnVector makeOutputColumns(
      const velox::RowType& rowType,
      float cardinality,
      Scope& scope);

  // Joins 'right' onto 'left', emitting both sides' columns.
  NodeCP crossJoin(NodeCP left, NodeCP right);

  // Joins 'right' onto 'left' where 'leftKeys' equal 'rightKeys', emitting
  // both sides' columns.
  NodeSimplifier::SimplifiedNode innerJoin(
      NodeCP left,
      NodeCP right,
      ExprVector leftKeys,
      ExprVector rightKeys);

  // Invokes 'translate' with a lift target over 'input', so subqueries in the
  // expressions it translates lift onto that target, and returns 'input' with
  // those lifts joined on.
  //
  // 'translate' is called as `translate(LiftTarget&)`, exactly once, so a
  // result it reports through a capture is always assigned. It passes the
  // target to the translate* methods.
  template <typename Translate>
  NodeCP withLiftTarget(NodeCP input, Translate&& translate) {
    LiftTarget target{input};
    translate(target);
    flushLifts(target);
    return target.node;
  }

  // Joins any pending lifts onto 'target.node'. Called by 'withLiftTarget'
  // when translation finishes, and by a lift that has to build over the node
  // itself. Reading 'target.node' before this runs gives a plan with the
  // pending lifts missing.
  void flushLifts(LiftTarget& target);

  // Returns an earlier lift of 'plan' that a reference lifting onto
  // 'liftTarget' can read: one already in that target's output, or one on an
  // enclosing scope's plan, which this records as a correlation so the Applies
  // in between carry it inwards. Returns nullptr when no lift is reachable and
  // 'plan' has to be lifted again.
  ExprCP reuseScalarSubquery(
      const logical_plan::LogicalPlanNode* plan,
      const LiftTarget& liftTarget);

  // Translates a subquery body, captures correlations, and lifts it onto
  // 'liftTarget'. An uncorrelated scalar body joins the target's pending
  // lifts, under an `EnforceSingleRow` wrap unless it provably yields one
  // row; everything else becomes an `Apply`, over the pending lifts when the
  // body reads only those and over the node otherwise. Returns the column
  // the lift produces, or a `Literal` for an uncorrelated scalar body that
  // folds to a constant, which lifts nothing.
  //
  // `kind` is the `Apply.kind` to emit. `inLhs` is the IN expression's
  // left-hand side, non-null only when shaping an IN-form Apply
  // (`liftSubquery` stores it together with the body's single output
  // column as `Apply.inLhs` / `Apply.inBodyKey`). Throws if
  // `liftTarget` is null.
  ExprCP liftSubquery(
      const logical_plan::SubqueryExpr& subqueryExpr,
      velox::core::JoinType kind,
      ExprCP inLhs,
      const Scope& outerScope,
      LiftTarget* liftTarget);

  // Returns the value a scalar subquery over 'body' produces, when 'body' is a
  // constant `Values` or a `Project` over one: the single row's value, or NULL
  // if it has no rows. Returns nullptr for any other 'body', and for a
  // `Project` whose expression is not a literal. Fails if the body has more
  // than one row, which no scalar subquery may.
  ExprCP tryScalarFromValues(NodeCP body);

  // Evaluates 'aggregate' from the listed discrete-predicate (e.g. partition)
  // values, returning one row Variant per result row, or nullopt when it is not
  // foldable: its aggregates must all ignore duplicate inputs, and its input
  // must be an optional Filter over a Scan of only discrete-predicate columns
  // on a connector that can list them.
  std::optional<std::vector<velox::Variant>> tryEvaluateOverDiscreteValues(
      const Aggregate* aggregate);

  // Translates each lp expression in 'expressions' against 'scope'.
  ExprVector translateAll(
      const std::vector<logical_plan::ExprPtr>& expressions,
      const Scope& scope,
      LiftTarget* liftTarget);

  // Translates an lp window frame against 'scope'.
  Frame toFrame(
      const logical_plan::WindowExpr::Frame& frame,
      const Scope& scope);

  // Translates an lp aggregate expression into a QueryGraph `Aggregate`
  // call. Resolves args / FILTER mask / ORDER BY keys against 'scope'.
  // `liftTarget` (if non-null) lifts Apply nodes for any subquery in
  // args / FILTER / ORDER BY above the Aggregate's input.
  const optimizer::Aggregate* toAggregateCall(
      const logical_plan::AggregateExpr& aggregateExpr,
      const Scope& scope,
      LiftTarget* liftTarget);

  // Evaluates each expression in 'exprRows' to its value and returns the
  // row-wise `Variant`s. `VALUES` expressions cannot reference input columns,
  // so evaluation runs against an empty scope; each is evaluated exactly once,
  // so non-deterministic expressions are allowed.
  std::vector<velox::Variant> evaluateExprRowsToVariants(
      const logical_plan::ValuesNode::Exprs& exprRows);

  SubqueryContext subqueries_;
  Schema& schema_;
  const connector::SchemaResolver& schemaResolver_;
  bool connectorPushdownSupported_{false};
  // True once a graph match lowered a repeated pattern to a Repeat.
  bool hasRepeat_{false};
  // Column materialized for an expression, consulted before materializing
  // another.
  folly::F14FastMap<ExprCP, ColumnCP> materialized_;

  Builder& builder_;
  ExprFactory exprFactory_;
  ExprSimplifier simplifier_;
  NodeSimplifier nodeSimplifier_;
  velox::core::ExpressionEvaluator& evaluator_;
  const OptimizerSession& session_;
  const ConstantPlanRunner& constantPlanRunner_;
  int32_t baseTableCounter_{0};

  // Lifted results of each scalar subquery, keyed by its inner plan.
  // Identical subqueries share one inner plan (hash-consed), so a repeated
  // reference reuses a lift instead of lifting again. A lift is reusable
  // where its column can be read: from the lift target it landed on, or from
  // a scope entered after it. Lifts on unrelated plans are not
  // interchangeable, so every lift is kept and the usable one is chosen per
  // reference. A folded constant (Literal) is on no plan and always
  // reusable.
  folly::F14FastMap<const logical_plan::LogicalPlanNode*, std::vector<ExprCP>>
      scalarSubqueryColumns_;

  // Subquery and graph element table plans translated so far. Each reference
  // to such a plan translates it again.
  folly::F14FastSet<const logical_plan::LogicalPlanNode*> translatedPlans_;

  // True while translating a plan already in 'translatedPlans_'.
  bool translatingDuplicate_{false};

  // Result column of every inference call already lifted, so a call appearing
  // more than once is evaluated once. A column is reusable only where the
  // lift target can read it.
  folly::F14FastMap<const Call*, ColumnCP> inferenceColumns_;

  // Depth of lambda bodies being translated. A call inside one runs per
  // element, so it cannot be lifted to a node.
  int32_t lambdaDepth_{0};

  struct ActiveFixedPoint {
    Name stateName;
    ColumnVector stateColumns;
    int32_t numReferences{0};
  };
  std::optional<ActiveFixedPoint> activeFixedPoint_;
};

} // namespace facebook::axiom::optimizer::v2
