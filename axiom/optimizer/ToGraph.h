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

#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/PathSet.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/SubfieldTracker.h"

namespace facebook::axiom::optimizer {

struct ExprDedupKey {
  Name func;
  std::span<const ExprCP> args;
  // Canonical return type pointer. Distinguishes calls like CAST(x AS double)
  // and CAST(x AS varchar) which share the same function name and arguments.
  const velox::Type* type;

  bool operator==(const ExprDedupKey& other) const {
    return func == other.func && type == other.type &&
        std::ranges::equal(args, other.args);
  }
};

struct ExprDedupHasher {
  size_t operator()(const ExprDedupKey& key) const {
    size_t h = folly::hasher<Name>()(key.func);
    for (auto& a : key.args) {
      h = velox::bits::hashMix(h, folly::hasher<ExprCP>()(a));
    }
    h = velox::bits::hashMix(h, folly::hasher<const velox::Type*>()(key.type));
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
/// for a map from unique step to the dedupped Expr that is the getter.
struct PathExpr {
  Step step;
  ExprCP base;

  bool operator==(const PathExpr& other) const = default;
};

struct PathExprHasher {
  size_t operator()(const PathExpr& expr) const {
    return velox::bits::hashMix(expr.step.hash(), expr.base->id());
  }
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
      const OptimizerOptions& options,
      std::shared_ptr<QueryRuntimeStats> runtimeStats = nullptr);

  /// Converts 'logicalPlan' to a tree of DerivedTables. Returns the root
  /// DerivedTable.
  DerivedTableP makeQueryGraph(
      const logical_plan::LogicalPlanNode& logicalPlan);

  Name newCName(std::string_view prefix) {
    return toName(fmt::format("{}{}", prefix, ++nameCounter_));
  }

  /// Creates a new column with a unique name using the given prefix.
  ColumnCP makeColumn(std::string_view prefix, Value value) {
    return make<Column>(newCName(prefix), currentDt_, value);
  }

  /// Creates or returns pre-existing function call with name+args. If
  /// deterministic, a new ExprCP is remembered for reuse. 'functions'
  /// on the resulting Call is computed as 'functionBits(name,
  /// specialForm) | <each arg's functions()>'.
  ExprCP deduppedCall(
      Name name,
      Value value,
      ExprVector args,
      bool specialForm = false);

  /// Shorthand for 'deduppedCall(name, value, args, /*specialForm=*/
  /// true)' — use at sites that construct a named special form (kIf,
  /// kAnd, kCoalesce, ...).
  ExprCP deduppedSpecialForm(Name name, Value value, ExprVector args) {
    return deduppedCall(name, value, std::move(args), /*specialForm=*/true);
  }

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
  // State for translating a (possibly correlated) subquery's body.
  // Held as a single 'applyContext_' member on 'ToGraph'. Populated by
  // producers during body translation; drained by the outer scope's
  // 'process*Subqueries' code after 'translateSubquery' returns.
  // 'outerDt' is save/restored by 'translateSubqueryBody' on body entry;
  // 'lifted' is save/restored by call sites that invoke
  // 'translateSubquery' while their own scope already has pending lifted
  // state (nested subqueries inside a body that already lifted state of
  // its own). All three lifted fields are asserted empty at
  // 'translateSubqueryBody' entry.
  struct ApplyContext {
    // The DT one scope out: where this body's lifted (correlated)
    // accumulators will eventually be drained into as join predicates /
    // outer aggregation / projection substitutions.
    DerivedTableP outerDt{nullptr};

    // The three lifted accumulators for this scope's body. Bundled in
    // one struct so a single value captures the full snapshot for
    // save/restore across nested 'translateSubquery' calls.
    struct Lifted {
      // Filter conjuncts found in the subquery body that reference
      // outer-scope columns. Drained by the outer scope's
      // 'processCorrelatedScalarSubquery' (and friends) to become join
      // keys or filter on the decorrelating LEFT JOIN.
      ExprVector predicates;

      // Residual projection expressions from the subquery's SELECT that
      // reference outer-scope columns. Each entry combines pure-inner
      // exported columns of the inner DT with outer-column references
      // and is evaluated above the decorrelated join (typically by
      // being substituted as the subquery's value in the outer
      // expression).
      ExprVector projections;

      // An aggregation from the subquery body whose aggregate body
      // references outer-scope columns. It is not attached to the inner
      // DT (its body would fail the inner DT's tableSet check). Its
      // output columns are created with relation set to 'outerDt' so
      // the SELECT projection split classifies them as correlated.
      // 'processCorrelatedScalarSubquery' attaches it to the outer DT
      // above the decorrelating LEFT JOIN.
      AggregationPlanCP aggregation{nullptr};
    };
    Lifted lifted;

    // Moves the lifted-state fields into a snapshot and clears them
    // in place so the next nested 'translateSubquery' starts with a
    // clean slate. Restore via `applyContext_.lifted = std::move(saved)`
    // — that overwrites any current state, which is the desired
    // behavior during exceptional unwind when the inner's state was
    // not drained by its consumer.
    Lifted saveAndClearLifted();
  };

  // For each column referenced by 'outerLifted->predicates' that
  // lives in 'currentDt_'s tableSet, append an 'arbitrary()' aggregate
  // over it to 'aggregates'/'columns', add the resulting post-aggregation
  // column to 'currentDt_->columns', and rewrite the predicates in place
  // to reference the post-aggregation column. No-op when 'outerLifted' is
  // null or has empty predicates. Used by
  // 'appendOuterAggregationCarryForwards' to thread the enclosing scope's
  // correlation keys through an injected outer aggregation.
  void carryForwardOuterCorrelationKeys(
      ApplyContext::Lifted* outerLifted,
      AggregateVector& aggregates,
      ColumnVector& columns);

  // Outer-side join keys, inner-side join keys, and remaining non-equi
  // conjuncts extracted from a subquery's correlated WHERE conjuncts.
  // 'leftTable' is the single outer table the conjuncts reference, or
  // null when there are no correlated conjuncts.
  struct CorrelationKeys {
    PlanObjectCP leftTable{nullptr};
    ExprVector leftKeys;
    ExprVector rightKeys;
    ExprVector nonEquiConjuncts;
  };

  // Extracts equi join keys + non-equi conjuncts from
  // 'correlatedConjuncts' into a CorrelationKeys, classifying each
  // conjunct's tables against 'subqueryDt'.
  static CorrelationKeys extractCorrelationKeys(
      const FunctionNames& funcs,
      const ExprVector& correlatedConjuncts,
      DerivedTableP subqueryDt);

  // For comparisons, swaps the args to have a canonical form for
  // deduplication. E.g column op constant, and smaller plan object id
  // to the left.
  void canonicalizeCall(Name& name, ExprVector& args);

  // Handles correlation processing for aggregations in correlated subqueries.
  // Returns the aggregation plan to use, or nullptr if the aggregation should
  // be skipped (e.g., DISTINCT in EXISTS).
  //
  // Side effects for equi-correlated aggregations:
  // - Clears 'liftedPredicates' and repopulates it with new equality
  //   expressions that reference the new grouping key columns.
  // - Adds grouping key columns to currentDt_->exprs and currentDt_->columns.
  AggregationPlanCP processCorrelatedAggregation(
      AggregationPlanCP agg,
      ExprVector& liftedPredicates);

  // Converts 'plan' to PlanObjects and records join edges into
  // 'currentDt_'. Wraps 'node' in a new Derived table f 'node' does not match
  // allowedInDt or 'node' is an outer join and 'excludeOuterJoins' is true.
  //
  // 'orderObservedAbove' is true when some consumer above this node would
  // observe an order produced here. A Sort encountered with this bit false
  // is skipped — its order is dead.
  void makeQueryGraph(
      const logical_plan::LogicalPlanNode& node,
      uint64_t allowedInDt,
      bool orderObservedAbove,
      bool excludeOuterJoins = false,
      bool excludeWindows = false);

  // Handles kFilter in makeQueryGraph. Creates DT boundaries for
  // nondeterministic filters and filters on window function outputs.
  void makeFilterQueryGraph(
      const logical_plan::FilterNode& filter,
      uint64_t allowedInDt,
      bool orderObservedAbove,
      bool excludeOuterJoins,
      bool excludeWindows);

  // Handles kProject in makeQueryGraph. Creates DT boundaries for
  // window-on-window dependencies and windows over LIMIT.
  void makeProjectQueryGraph(
      const logical_plan::ProjectNode& project,
      uint64_t allowedInDt,
      bool orderObservedAbove,
      bool excludeOuterJoins,
      bool excludeWindows);

  PlanObjectCP findLeaf(const logical_plan::LogicalPlanNode* node) {
    auto* leaf = planLeaves_[node];
    VELOX_CHECK_NOT_NULL(leaf);
    return leaf;
  }

  // Returns the ordinal positions of actually referenced outputs of 'node'.
  std::vector<int32_t> usedChannels(const logical_plan::LogicalPlanNode& node);

  // Returns a deduplicated Literal from the value in 'constant'.
  ExprCP makeConstant(const logical_plan::ConstantExpr& constant);

  // Returns a deduplicated null Literal of 'type'.
  ExprCP makeNullConstant(const velox::TypePtr& type);

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

  // Makes a deduplicated Expr tree from 'expr'. May return null when 'expr'
  // is a subfield path that is not materialized.
  // TODO: Return std::optional<ExprCP> instead of allowing null returns.
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

  // Adds a join to 'currentDt_'. For inner joins, processes the condition as a
  // cross join followed by a filter, which handles subqueries using the same
  // infrastructure as WHERE clause predicates. For left or full joins, creates
  // a JoinEdge with appropriate equi-join keys and remaining filter.
  // @param left Left side of the join. Must have been added to the graph
  // already.
  // @param right Right side of the join. Must have been added to the graph
  // already.
  // @param joinType Inner, left or full. Right join must have been normalized
  // into a left join.
  // @param condition Join condition. Can be nullptr for a cross join.
  // @param originalJoinType The original join type from the logical plan
  // (before normalization).
  void translateJoin(
      const logical_plan::LogicalPlanNodePtr& left,
      const logical_plan::LogicalPlanNodePtr& right,
      logical_plan::JoinType joinType,
      const logical_plan::ExprPtr& condition,
      logical_plan::JoinType originalJoinType);

  // Eliminates join when the ON clause contains a constant false condition.
  // Returns true if the join was eliminated.
  bool eliminateJoinOnConstantFalse(
      logical_plan::JoinType joinType,
      const logical_plan::LogicalPlanNodePtr& left,
      const logical_plan::LogicalPlanNodePtr& right,
      PlanObjectCP rightTable);

  // For LEFT JOIN with subquery conjuncts in the ON clause, processes the right
  // side inside a container DT and applies the subquery conjuncts as filters.
  // Returns the remaining non-subquery condition (possibly nullptr).
  logical_plan::ExprPtr processLeftJoinSubqueries(
      const logical_plan::LogicalPlanNode& right,
      const logical_plan::ExprPtr& condition);

  // Given an INTERSECT or an EXCEPT set operation, create derived tables for
  // inputs, add them to 'currentDt_' and connect them with join edges.
  void translateSetJoin(const logical_plan::SetNode& set);

  // Given a UNION or UNION ALL set operation, recursively flattens the inputs
  // and converts these into derived tables.
  // Flattens UNION ALL inside UNION and UNION ALL.
  // Flattens UNION inside UNION.
  // Example: u(a, u(b, c)) -> u(a, b, c)
  void translateUnion(const logical_plan::SetNode& set);

  void addUnnest(const logical_plan::UnnestNode& unnest);

  AggregationPlanCP translateAggregation(
      const logical_plan::AggregateNode& aggregation);

  // Translates a logical WindowExpr to a QueryGraph WindowFunction.
  ExprCP translateWindowExpr(const logical_plan::WindowExpr& window);

  void addProjection(const logical_plan::ProjectNode& project);

  void addFilter(
      const logical_plan::LogicalPlanNode& input,
      const logical_plan::ExprPtr& predicate);

  // Returns true if any window expression in the project references another
  // window function output through renames_. Used to detect window-on-window
  // dependencies that require a DT boundary.
  bool windowReferencesWindow(const logical_plan::ProjectNode& project) const;

  void addLimit(const logical_plan::LimitNode& limit);

  void addOrderBy(const logical_plan::SortNode& order);

  void addWrite(const logical_plan::TableWriteNode& tableWrite);

  void applySampling(
      const logical_plan::SampleNode& sample,
      uint64_t allowedInDt,
      bool orderObservedAbove);

  bool isSubfield(
      const logical_plan::ExprPtr& expr,
      Step& step,
      logical_plan::ExprPtr& input);

  void getExprForField(
      const logical_plan::InputReferenceExpr* field,
      logical_plan::ExprPtr& resultExpr,
      ColumnCP& resultColumn,
      const logical_plan::LogicalPlanNode*& context);

  // Constructs an expression that applies 'steps' to 'base' in reversed order.
  ExprCP makeGetter(const Step& step, ExprCP base);

  // Constructs an expression that applies 'steps' to 'base'.
  ExprCP makeGetters(std::span<const Step> steps, ExprCP base);

  // Makes dedupped getters for 'steps'. Returns nullptr if 'steps' are below
  // skyline. If 'steps' intersects 'skyline' returns skyline wrapped in getters
  // that are not in skyline. If no skyline, puts dedupped getters defined by
  // 'steps' on 'base' or 'column' if 'base' is nullptr.
  //
  // @param steps Steps in reverse order. E.g. [a, b, c] means c.b.a.
  // @param skyline Skyline to lookup the base of the path.
  // @param base Base of the path. Used if 'skyline' and 'column' are null.
  // @param column Base of the path as a column. Used if 'skyline' is null.
  ExprCP makeGettersOverSkyline(
      std::span<const Step> steps,
      const SubfieldProjections* skyline,
      const logical_plan::ExprPtr& base,
      ColumnCP column);

  PathSet functionSubfields(const logical_plan::CallExpr* call);

  // Calls translateSubfieldFunction() if not already called.
  SubfieldProjections* FOLLY_NULLABLE
  ensureFunctionSubfields(const logical_plan::ExprPtr& expr);

  void makeBaseTable(const logical_plan::TableScanNode& tableScan);

  void makeValuesTable(const logical_plan::ValuesNode& values);

  // Creates a ValuesTable with the output schema of 'node' using 'data'.
  // The 'node' does not need to be a ValuesNode - used when replacing a
  // subtree with a ValuesTable, e.g. for empty result optimizations.
  ValuesTable* makeValuesTable(
      const logical_plan::LogicalPlanNode& node,
      ValuesTable::Data data);

  void makeEmptyValuesTable(const logical_plan::LogicalPlanNode& node);

  // Adds 'node' and descendants to query graph wrapped inside a
  // DerivedTable. Done for joins to the right of non-inner joins,
  // group bys as non-top operators, whenever descendents of 'node'
  // are not freely reorderable with its parents' descendents.
  // Wraps 'node' in a fresh inner DT and attaches it to the current outer
  // DT. 'orderObservedAbove' is forwarded to the inner traversal — set per
  // the caller's consumer semantics.
  void wrapInDt(
      const logical_plan::LogicalPlanNode& node,
      bool orderObservedAbove);

  // Finalizes 'currentDt_' by setting its output columns based on 'node',
  // then nests it inside 'outerDt' (or a new DT if 'outerDt' is nullptr).
  // Updates 'currentDt_' to point to the outer DT.
  void finalizeDt(
      const logical_plan::LogicalPlanNode& node,
      DerivedTableP outerDt = nullptr);

  // Wraps currentDt_ via finalizeDt, preserving any pending
  // applyContext_.lifted.predicates and applyContext_.lifted.projections. After
  // wrapping, rewrites their column references so they point to the wrapped
  // DT's output columns.
  void finalizeDtPreservingCorrelations(
      const logical_plan::LogicalPlanNode& node);

  // Creates a wrapper DerivedTable with a COUNT(*) aggregation over 'inputDt'.
  // Returns the count column. The wrapper DT is added to currentDt_.
  ColumnCP makeCountStarWrapper(DerivedTableP inputDt);

  // Adds a column 'name' from current DerivedTable to 'dt'. The looked-up
  // expression usually references only tables owned by 'dt'; that case
  // appends to 'dt->exprs' / 'dt->columns' and updates 'renames_'. In
  // subquery scope the expression may also reference outer-query tables
  // via 'correlations_'; in that case it is routed to
  // 'applyContext_.lifted.projections' (via 'splitCorrelatedProjection' for the
  // mixed-reference variant), and 'dt' is not extended with the unsplit
  // form (which would violate DerivedTable::checkConsistency).
  void addDtColumn(DerivedTableP dt, std::string_view name);

  // Splits a subquery projection expression that references both 'dt's
  // own tables and tables from an enclosing scope. Local-only maximal
  // sub-trees are appended to 'dt->exprs' / 'dt->columns' as new columns;
  // the returned residual replaces each such sub-tree with a reference to
  // its new column, and still carries the original correlated references
  // for evaluation above the decorrelated join. The walk does not descend
  // into aggregate bodies, so an outer-column reference inside an
  // aggregate cannot be split out and triggers a VELOX_NYI.
  ExprCP splitCorrelatedProjection(ExprCP expr, DerivedTableP dt);

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

  // State threaded between consecutive subqueries (scalar, IN, EXISTS)
  // within a single project / aggregate / filter scope (set up by
  // addProjection, addAggregation, or addFilter). Only the heavy-path
  // non-equi correlated-with-aggregation scalar subquery installs the
  // aggregation that motivates the chain; once that has happened, every
  // subsequent subquery in the scope must wrap currentDt_ before adding
  // its own joins (joins cannot follow aggregation). The wrap site re-
  // exports this state so the new outer DT can keep using it — reusing
  // the row identity as the next aggregation's grouping key (skipping a
  // redundant AssignUniqueId) and carrying prior subquery results forward
  // via arbitrary() aggregates.
  struct SubqueryChain {
    // Per-outer-row identity column to use as the next aggregation's
    // grouping key. The heavy path sets this when it creates a fresh
    // column; the wrap site rewrites it to the column's re-exported form
    // each time the chain crosses a wrap. A non-null value signals an
    // in-progress chain that requires the next subquery iteration to wrap.
    ColumnCP rowNumberColumn{nullptr};

    // Subquery keys whose result must survive subsequent wraps and
    // aggregations. Each entry's current expression lives in subqueries_:
    // the wrap site rewrites that expression to the re-exported form on
    // each wrap, and the next heavy path wraps it in arbitrary() so it
    // survives the new aggregation. Every subquery type appends in
    // encounter order; the vector is never cleared within a chain's
    // lifetime.
    std::vector<logical_plan::ExprPtr> carryForwards;
  };

  // Appends carry-forward aggregates needed by an injected outer
  // aggregation: 'arbitrary()' over downstream-referenced outer columns
  // ('appendArbitraryAggregates'); 'arbitrary()' over prior chain-mate
  // subquery results (updating their 'subqueries_' entries); and the
  // enclosing scope's correlation keys via
  // 'carryForwardOuterCorrelationKeys'. Used by
  // 'processCorrelatedScalarSubquery' and 'buildCorrelatedAggregation'
  // to keep their post-join aggregation construction in lock-step.
  void appendOuterAggregationCarryForwards(
      const logical_plan::LogicalPlanNode& input,
      SubqueryChain& chain,
      ApplyContext::Lifted* outerLifted,
      AggregateVector& aggregates,
      ColumnVector& columns);

  // Process subqueries used in filter's predicate or projection expressions
  // and populate subqueries_ map. For each IN <subquery> expression, create a
  // separate DT for the subquery and add a semi-join edge. Replace the whole IN
  // predicate with a 'mark' column produced by the join. For other <subquery>
  // expressions, create a separate DT and replace the expression with the only
  // column produced by the DT.
  //
  // @param input The logical plan node to use when finalizing the DT.
  // @param expr The expression to scan for subqueries.
  // @param mayFinalize In/out flag. If true on entry and the current DT has
  // non-inner joins, aggregation, or unnest tables, wraps the DT before
  // processing subqueries. Set to false after finalization or after processing
  // subqueries to prevent subsequent calls from finalizing the DT again.
  // @param chain In/out state for chaining subqueries within a logical
  // scope (see SubqueryChain). Callers that may invoke processSubqueries
  // multiple times for the same scope (e.g. multiple project expressions)
  // pass a single chain across all calls; single-call sites pass a
  // temporary.
  void processSubqueries(
      const logical_plan::LogicalPlanNode& input,
      const logical_plan::ExprPtr& expr,
      bool& mayFinalize,
      SubqueryChain& chain);

  // Processes scalar subqueries, creating DTs and joins for each.
  // Populates subqueries_ with mappings from subquery expressions to columns.
  void processScalarSubqueries(
      const logical_plan::LogicalPlanNode& input,
      const std::vector<logical_plan::SubqueryExprPtr>& scalars,
      SubqueryChain& chain);

  // If 'chain' has pending state from a prior heavy-path scalar
  // subquery, wraps currentDt_ inside a new outer DT and re-exports the
  // chain so the new outer DT can reference the prior heavy-path's row-
  // identity column (as the next aggregation's grouping key) and the
  // prior subqueries' results (via the entries in subqueries_). No-op
  // otherwise. Called at the top of each subquery iteration (scalar, IN,
  // EXISTS) before any work that adds joins to currentDt_: joins cannot
  // be added to a DT after its aggregation, and outer-column renames now
  // point to columns owned by currentDt_, which would yield self-
  // referencing JoinEdges.
  void maybeWrapForChainedSubquery(
      const logical_plan::LogicalPlanNode& input,
      SubqueryChain& chain);

  // Attaches a scalar subquery's translated body ('subqueryDt') to the
  // outer scope, picking the decorrelation shape from 'applyContext_':
  // pure uncorrelated, correlated WHERE / outer-referencing aggregate, or
  // correlated projection. Returns the expression to substitute for the
  // subquery in the outer expression. Drains 'applyContext_'.
  //
  // 'outerLifted' holds the enclosing scope's pending lifted state saved
  // before this call. See 'carryForwardOuterCorrelationKeys' for how it
  // is used by the branches that inject an aggregation into 'currentDt_'.
  ExprCP attachScalarSubqueryToOuter(
      const logical_plan::LogicalPlanNode& input,
      DerivedTableP subqueryDt,
      SubqueryChain& chain,
      ApplyContext::Lifted* outerLifted);

  // Processes an uncorrelated scalar subquery. Attempts constant folding,
  // otherwise ensures single row. Returns the expression to map to the
  // subquery.
  ExprCP processUncorrelatedScalarSubquery(DerivedTableP subqueryDt);

  // Processes a correlated scalar subquery. Creates LEFT join with
  // decorrelation, handling both equi-only and non-equi correlation.
  // Returns the expression to map to the subquery. 'chain' carries state
  // between consecutive heavy-path subqueries in the caller's loop; the
  // heavy path reads it on entry and updates it on exit. The caller is
  // responsible for appending the produced expression to chain.carryForwards
  // so subsequent wraps and aggregations preserve the result. 'outerLifted'
  // is the enclosing scope's pending lifted state; the non-equi-with-
  // aggregation branch routes it through 'carryForwardOuterCorrelationKeys'.
  ExprCP processCorrelatedScalarSubquery(
      const logical_plan::LogicalPlanNode& input,
      DerivedTableP subqueryDt,
      SubqueryChain& chain,
      ApplyContext::Lifted* outerLifted);

  // Builds the AssignUniqueId + LEFT JOIN + outer aggregation that
  // decorrelates a scalar subquery whose aggregate body references outer
  // columns. The aggregate is held in 'applyContext_.lifted.aggregation'
  // (set by 'translateAggregation'). Drains 'applyContext_.lifted.aggregation'
  // and 'applyContext_.lifted.predicates' on return. 'outerLifted' is the
  // enclosing scope's pending lifted state, routed through
  // 'carryForwardOuterCorrelationKeys' so its correlation keys are
  // exposed on the synthesized aggregation.
  ExprCP buildCorrelatedAggregation(
      const logical_plan::LogicalPlanNode& input,
      DerivedTableP subqueryDt,
      SubqueryChain& chain,
      const CorrelationKeys& correlation,
      ApplyContext::Lifted* outerLifted);

  // Processes a scalar subquery whose SELECT references outer-query
  // columns. Returns the expression to substitute for the subquery: a
  // residual that combines pure-inner exports of subqueryDt with
  // outer-column references, evaluated above the decorrelated join.
  // The inner DT is integrated into the join graph (or dropped) as
  // appropriate for its shape. Must be called only when
  // 'applyContext_.lifted.projections' is non-empty; drains it on return.
  // 'outerLifted' is forwarded to 'processCorrelatedScalarSubquery' when
  // this method delegates to the heavy path.
  ExprCP processProjectionCorrelatedScalarSubquery(
      const logical_plan::LogicalPlanNode& input,
      DerivedTableP subqueryDt,
      SubqueryChain& chain,
      ApplyContext::Lifted* outerLifted);

  // Processes IN <subquery> predicates, creating semi-joins with mark columns.
  // Populates subqueries_ with mappings from IN predicates to mark columns.
  // 'chain' carries the same wrap/carry-forward state used by scalar
  // subquery processing; an in-progress chain is wrapped before each new
  // join is added, and the resulting mark column is appended to the chain.
  void processInPredicates(
      const logical_plan::LogicalPlanNode& input,
      const std::vector<logical_plan::ExprPtr>& inPredicates,
      SubqueryChain& chain);

  // Processes an uncorrelated IN <subquery> predicate.
  // Returns the mark column to map to the predicate.
  ExprCP processUncorrelatedInPredicate(
      DerivedTableP subqueryDt,
      ColumnCP markColumn,
      ExprCP leftKey,
      PlanObjectCP leftTable);

  // Processes a correlated IN <subquery> predicate.
  // Returns the mark column to map to the predicate.
  ExprCP processCorrelatedInPredicate(
      DerivedTableP subqueryDt,
      ColumnCP markColumn,
      ExprCP leftKey,
      PlanObjectCP leftTable);

  // Processes EXISTS subqueries, creating mark joins or cross joins.
  // Populates subqueries_ with mappings from EXISTS expressions to mark columns
  // or NOT(COUNT(*) = 0) expressions. 'chain' carries the same
  // wrap/carry-forward state used by scalar subquery processing.
  void processExistsSubqueries(
      const logical_plan::LogicalPlanNode& input,
      const std::vector<logical_plan::ExprPtr>& exists,
      SubqueryChain& chain);

  // Processes an uncorrelated EXISTS subquery. Uses COUNT(*) wrapper
  // with cross join and NOT(count == 0) expression.
  // Returns the expression to map to the EXISTS.
  ExprCP processUncorrelatedExists(DerivedTableP subqueryDt);

  // Processes a correlated EXISTS subquery. Creates mark join with
  // decorrelation. Returns the mark column to map to the EXISTS.
  ExprCP processCorrelatedExists(DerivedTableP subqueryDt);

  // Translates a subquery into a DerivedTable. Sets up correlations_ to allow
  // the subquery to reference columns from the outer query. After translation,
  // applyContext_.lifted.predicates contains any correlated predicates found.
  //
  // @param node The logical plan node representing the subquery.
  // @param finalize If true (default), adds the subquery DT to currentDt_ and
  // calls initializePlans(). If false, the caller is responsible for adding
  // the DT and calling initializePlans(). Use finalize=false when you need to
  // modify the DT before memoization (e.g., applying LIMIT 1 for EXISTS).
  // @return The translated DerivedTable for the subquery.
  DerivedTableP translateSubquery(
      const logical_plan::LogicalPlanNode& node,
      bool finalize = true);

  // Translates the body of a subquery. Asserts that 'applyContext_' has
  // no leftover lifted accumulators on entry, swaps 'currentDt_' for a
  // fresh inner DT (save/restoring 'applyContext_.outerDt'), recursively
  // translates 'node' into it, then restores 'currentDt_' and returns
  // the inner DT.
  DerivedTableP translateSubqueryBody(
      const logical_plan::LogicalPlanNode& node);

  // Appends `arbitrary` aggregates for all columns used from 'input'.
  // Used when decorrelating non-equi correlated subqueries. Since the
  // decorrelation strategy groups by a unique ID (one group per outer row),
  // we need to preserve the outer row's columns through the aggregation.
  // The `arbitrary` aggregate is used because each group contains exactly
  // one distinct value for these columns. Updates 'renames_' to map original
  // column names to the new aggregate output columns.
  void appendArbitraryAggregates(
      const logical_plan::LogicalPlanNode& input,
      AggregateVector& aggregates,
      ColumnVector& columns);

  // Appends a single 'arbitrary(expr)' aggregate to 'aggregates' and a
  // corresponding output column owned by currentDt_ to 'columns'. Returns
  // the new column. Used to carry a value through an aggregation when
  // decorrelating correlated subqueries.
  ColumnCP appendArbitraryAggregate(
      ExprCP expr,
      AggregateVector& aggregates,
      ColumnVector& columns);

  ColumnCP addMarkColumn();

  void addJoinColumns(
      const logical_plan::LogicalPlanNode& joinSide,
      ColumnVector& columns,
      ExprVector& exprs);

  // Cache of resolved table schemas.
  Schema schema_;

  velox::core::ExpressionEvaluator& evaluator_;

  const OptimizerOptions& options_;

  // Innermost DerivedTable when making a QueryGraph from PlanNode.
  DerivedTableP currentDt_{nullptr};

  // Source LogicalPlanNode's for the node currently being processed. Used to
  // translate expressions that involve subfields. Contains just one node if
  // current node if Project or Filter. Contains two nodes if current node is a
  // Join.
  std::vector<const logical_plan::LogicalPlanNode*> exprSources_;

  // Map from lambda argument names to their corresponding columns when
  // translating inside a lambda body.
  folly::F14FastMap<std::string_view, ColumnCP> lambdaSignature_;

  // Maps names in project nodes of input logical plan to deduplicated Exprs.
  // Values may be null when a subfield path is not materialized (see
  // intersectWithSkyline in makeGettersOverSkyline).
  folly::F14FastMap<std::string, ExprCP> renames_;

  // Symbols from the 'outer' query. Used when processing correlated subqueries.
  const folly::F14FastMap<std::string, ExprCP>* correlations_{nullptr};

  // True if expression is allowed to reference symbols from the 'outer' query.
  bool allowCorrelations_{false};

  // Correlated-subquery accumulators for the currently active subquery
  // body translation. Populated by producers (correlated predicates,
  // projections, aggregations) and drained by the outer scope's
  // 'process*Subqueries' code after 'translateSubquery' returns. At top
  // level (outside any subquery scope), this is empty.
  ApplyContext applyContext_;

  // Maps an expression that contains a subquery to a column or constant that
  // should be used instead. Populated in 'processSubqueries()'.
  folly::F14FastMap<logical_plan::ExprPtr, ExprCP> subqueries_;

  folly::
      F14FastMap<TypedVariant, ExprCP, TypedVariantHasher, TypedVariantComparer>
          constantDedup_;

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

  const FunctionNames functionNames_;

  folly::F14FastMap<Name, Name> reversibleFunctions_;
};

} // namespace facebook::axiom::optimizer
