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

#include "axiom/optimizer/v2/PushdownAndPrunePass.h"

#include "axiom/optimizer/v2/PrecomputeProjections.h"

#include "axiom/optimizer/ToSubfield.h"
#include "axiom/optimizer/v2/ColumnAccess.h"
#include "axiom/optimizer/v2/JoinPredicatePlacement.h"
#include "axiom/optimizer/v2/MarkFilterFusion.h"
#include "axiom/optimizer/v2/OuterJoinReduction.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"

#include "axiom/optimizer/v2/TableAccessHandle.h"

#include <limits>
#include <optional>

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/ImpliedFilters.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/NodeSimplifier.h"

#include <folly/container/F14Set.h>

namespace facebook::axiom::optimizer::v2 {
namespace {

// Per-call state for the pushdown visitor.
struct PushdownContext {
  // In collection mode, the visitor records predicates guaranteed by this
  // subtree without changing the plan or consulting a connector.
  bool collectingFilters{false};
  PlanObjectSet requestedFilterColumns;
  ExprVector collectedFilters;

  // True if replacing multiple rows that are identical after pruning with one
  // cannot affect consumers above. Distinct emitted rows and the difference
  // between zero and one row remain significant.
  bool mayDropDuplicates{false};

  // Conjuncts collected from the path above this node that the visitor
  // will try to push further down.
  ExprVector pending;

  // Columns the consumer above this node actually reads. Used to narrow
  // each node's `outputColumns` (and to drop unused exprs / aggregates
  // / window functions / leg columns). Seeded at the root with
  // `root->outputColumns()` so the root's output schema is preserved.
  // Includes columns referenced by `pending` (they must survive to where
  // a conjunct lands).
  PlanObjectSet required;

  // Columns that consumers strictly above this node read, not counting the
  // conjuncts in `pending`. A conjunct in `pending` that ends up in a Filter
  // above this node reads its rows too; `blockAt` adds those columns back for
  // its callback. Paths that do not refine this set default to `required`,
  // which can keep a mark, a Sort, a TopN or a column that is not needed but
  // never drops one that is.
  PlanObjectSet requiredAbove;

  // Per-partition row cap implied by a `Limit` or `TopN` directly above a
  // ranking `Window`: it keeps at most `offset + count` rows overall, so no
  // partition can contribute more than that many. Set only for that immediate
  // parent-child pair.
  std::optional<int32_t> rankLimit;

  // Set when the consumer drops the columns it does not read, so a node below
  // may leave them in its output.
  bool consumerDropsExtraColumns{false};

  // `Column*`s for which an ancestor drops every row holding NULL (derived
  // from default-null-behavior equi-keys of an ancestor inner join). A node
  // passes a column to its input only when removing the input rows where it
  // is NULL removes only output rows where it is NULL and leaves the other
  // output rows unchanged. A TopN or a Window over all rows fails this: its
  // other output rows read the removed rows. Never inserted into the plan.
  PlanObjectSet nonNullColumns;

  // Expression identities established in this subtree. A parent applies them
  // while rebuilding on the way back up.
  PlanSubstitutions outputSubstitutions;
};

// Applies a column-to-column substitution. Other substitutions cannot appear
// in a ColumnVector and leave the column unchanged.
ColumnCP rewriteColumn(
    ExprFactory& exprs,
    ColumnCP column,
    const PlanSubstitutions& substitutions) {
  ExprCP rewritten = substitutions.apply(column, exprs);
  return rewritten->isColumn() ? rewritten->as<Column>() : column;
}

// Returns 'columns' with each column rewritten by 'substitutions'.
// With 'dropDuplicates', a replacement already in the list is not added again.
ColumnVector rewriteColumns(
    ExprFactory& exprs,
    const ColumnVector& columns,
    const PlanSubstitutions& substitutions,
    bool dropDuplicates) {
  ColumnVector result;
  result.reserve(columns.size());
  PlanObjectSet added;
  for (ColumnCP column : columns) {
    ColumnCP rewritten = rewriteColumn(exprs, column, substitutions);
    if (dropDuplicates && added.contains(rewritten)) {
      continue;
    }
    added.add(rewritten);
    result.push_back(rewritten);
  }
  return result;
}

// Replaces columns according to `substitutions`.
PlanObjectSet rewriteColumnSet(
    ExprFactory& exprs,
    const PlanObjectSet& columns,
    const PlanSubstitutions& substitutions) {
  PlanObjectSet result;
  columns.forEach<Column>([&](ColumnCP column) {
    result.add(rewriteColumn(exprs, column, substitutions));
  });
  return result;
}

// Returns the columns a consumer reads in place of 'columns' once it applies
// 'substitutions'.
PlanObjectSet replacementColumns(
    ExprFactory& exprs,
    const PlanObjectSet& columns,
    const PlanSubstitutions& substitutions) {
  PlanObjectSet result;
  columns.forEach<Column>([&](ColumnCP column) {
    result.unionColumns(substitutions.apply(column, exprs));
  });
  return result;
}

// Splits `expressions` by whether each one's column set overlaps
// `columns`. Returns `{noOverlap, overlap}`.
std::pair<ExprVector, ExprVector> partition(
    const ExprVector& expressions,
    const PlanObjectSet& columns) {
  ExprVector noOverlap;
  ExprVector overlap;
  for (ExprCP expression : expressions) {
    if (expression->columns().hasIntersection(columns)) {
      overlap.push_back(expression);
    } else {
      noOverlap.push_back(expression);
    }
  }
  return {std::move(noOverlap), std::move(overlap)};
}

// A nondeterministic conjunct may not cross a node whose output row count
// differs from its input's: one evaluation on either side of the node stands
// in for a different number of evaluations on the other, which admits a
// different set of rows.
void blockNondeterministic(ExprVector& pushable, ExprVector& blocked) {
  std::erase_if(pushable, [&](ExprCP conjunct) {
    if (!conjunct->containsNonDeterministic()) {
      return false;
    }
    blocked.push_back(conjunct);
    return true;
  });
}

// A single-ranking-function `Window` that a specialized ranking node can
// replace, with the per-partition row cap that applies to it.
struct RankFusion {
  // The conjunct consumed as the cap, or null when the cap came from a bound
  // above the Window or there is no cap.
  ExprCP consumedPredicate;
  // The part of a consumed predicate that does not constrain the upper bound.
  ExprCP remainingPredicate;
  // The window function's output column (the rank value).
  ColumnCP rankColumn;
  velox::core::TopNRowNumberNode::RankFunction rankFunction;
  // Per-partition row cap; absent only for an unordered row_number with no
  // bound.
  std::optional<int32_t> limit;
};

std::optional<RankFusion> rankFusionFromPredicate(
    ExprCP predicate,
    ColumnCP rankColumn,
    velox::core::TopNRowNumberNode::RankFunction rankFunction,
    const FunctionNames& names,
    ExprFactory& exprs) {
  if (!predicate->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }
  const Call* comparison = predicate->as<Call>();
  const auto& args = comparison->args();
  if (args.empty() || args[0] != rankColumn) {
    return std::nullopt;
  }

  const Name name = comparison->name();
  ExprCP remainingPredicate{nullptr};
  int64_t bound;
  if (name == names.between && args.size() == 3 &&
      args[1]->is(PlanType::kLiteralExpr) &&
      args[2]->is(PlanType::kLiteralExpr)) {
    const int64_t lower = integerValue(&args[1]->as<Literal>()->literal());
    bound = integerValue(&args[2]->as<Literal>()->literal());
    if (lower > 1) {
      remainingPredicate = exprs.makeGreaterThanOrEqual(rankColumn, args[1]);
    }
  } else if (args.size() == 2 && args[1]->is(PlanType::kLiteralExpr)) {
    bound = integerValue(&args[1]->as<Literal>()->literal());
  } else {
    return std::nullopt;
  }

  int64_t limit;
  if ((name == names.lte || name == names.between) && bound > 0) {
    limit = bound;
  } else if (name == names.lt && bound > 1) {
    limit = bound - 1;
  } else if (name == names.equality && bound == 1) {
    limit = 1;
  } else {
    return std::nullopt;
  }
  if (limit > std::numeric_limits<int32_t>::max()) {
    return std::nullopt;
  }
  return RankFusion{
      predicate,
      remainingPredicate,
      rankColumn,
      rankFunction,
      static_cast<int32_t>(limit)};
}

// Returns the specialization for a single ranking function (row_number / rank /
// dense_rank), taking its cap from a 'pending' conjunct on the rank column or
// from 'rankLimit'.
std::optional<RankFusion> detectRankFusion(
    const Window* node,
    const ExprVector& pending,
    std::optional<int32_t> rankLimit,
    const FunctionNames& names,
    ExprFactory& exprs) {
  if (node->functions().size() != 1) {
    return std::nullopt;
  }
  const ExprCP call = node->functions()[0].call;
  if (!call->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }
  const Name functionName = call->as<Call>()->name();
  velox::core::TopNRowNumberNode::RankFunction rankFunction;
  if (functionName == names.rowNumber) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kRowNumber;
  } else if (functionName == names.rank) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kRank;
  } else if (functionName == names.denseRank) {
    rankFunction = velox::core::TopNRowNumberNode::RankFunction::kDenseRank;
  } else {
    return std::nullopt;
  }

  const size_t numInputColumns = node->input()->outputColumns().size();
  const ColumnCP rankColumn = node->outputColumns()[numInputColumns];

  // An unordered rank / dense_rank ties every row of a partition at rank 1,
  // which no specialized node computes.
  const bool ordered = !node->orderKeys().empty();
  if (!ordered &&
      rankFunction !=
          velox::core::TopNRowNumberNode::RankFunction::kRowNumber) {
    return std::nullopt;
  }

  for (ExprCP conjunct : pending) {
    if (auto fusion = rankFusionFromPredicate(
            conjunct, rankColumn, rankFunction, names, exprs)) {
      return fusion;
    }
  }

  if (ordered) {
    // A `Limit` above bounds every partition just as a rank predicate would.
    if (rankLimit.has_value()) {
      return RankFusion{/*consumedPredicate=*/nullptr,
                        /*remainingPredicate=*/nullptr,
                        rankColumn,
                        rankFunction,
                        rankLimit};
    }
    // An unbounded ordered ranking still has to sort each partition.
    return std::nullopt;
  }
  // An unordered row_number numbers rows as they arrive, so it specializes with
  // or without a bound.
  return RankFusion{/*consumedPredicate=*/nullptr,
                    /*remainingPredicate=*/nullptr,
                    rankColumn,
                    rankFunction,
                    /*limit=*/std::nullopt};
}

// With no ORDER BY every row of a partition ties, so `rank` and `dense_rank`
// are 1 for every row. Returns that column when 'node' computes such a
// constant ranking, else null.
ColumnCP constantRankColumn(const Window* node, const FunctionNames& names) {
  if (!node->orderKeys().empty() || node->functions().size() != 1) {
    return nullptr;
  }
  const ExprCP call = node->functions()[0].call;
  if (!call->is(PlanType::kCallExpr)) {
    return nullptr;
  }
  const Name name = call->as<Call>()->name();
  if (name != names.rank && name != names.denseRank) {
    return nullptr;
  }
  return node->outputColumns()[node->input()->outputColumns().size()];
}

// True when 'conjunct' compares 'rankColumn' with a literal and holds for a
// rank of 1 — with the rank constant, the predicate selects every row. Only
// (column, literal) order is matched: `Builder` canonicalizes a reversible
// comparison to put the literal second.
bool holdsAtRankOne(
    ExprCP conjunct,
    ColumnCP rankColumn,
    const FunctionNames& names) {
  if (!conjunct->is(PlanType::kCallExpr)) {
    return false;
  }
  const Call* comparison = conjunct->as<Call>();
  if (comparison->args().size() != 2 || comparison->args()[0] != rankColumn ||
      !comparison->args()[1]->is(PlanType::kLiteralExpr)) {
    return false;
  }
  const int64_t bound =
      integerValue(&comparison->args()[1]->as<Literal>()->literal());
  const Name name = comparison->name();
  if (name == names.equality) {
    return bound == 1;
  }
  if (name == names.lt) {
    return 1 < bound;
  }
  if (name == names.lte) {
    return 1 <= bound;
  }
  if (name == names.gt) {
    return 1 > bound;
  }
  if (name == names.gte) {
    return 1 >= bound;
  }
  return false;
}

// Top-down filter pushdown visitor. Every node kind is overridden
// explicitly so each kind decides whether and how to propagate pending
// conjuncts.

class Pushdown : public NodeRewriter<PushdownContext> {
 public:
  Pushdown(
      Builder& builder,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerSession& session,
      PushdownAndPrunePass::ConnectorPushdown connectorPushdown,
      const ColumnVector& outputColumns)
      : NodeRewriter(builder),
        exprs_(builder),
        evaluator_(evaluator),
        session_(session),
        connectorPushdown_(connectorPushdown),
        simplifier_(builder, evaluator),
        nodeSimplifier_(builder, simplifier_) {
    // The query returns these, so they are read whole however narrowly an
    // expression below reads them.
    for (ColumnCP column : outputColumns) {
      access_.add(column);
    }
  }

  using NodeRewriter::rewrite;

  // Records what each node reads on the way down, so that a node producing a
  // column is reached after the consumers whose paths it extends, and a Scan
  // sees the finished access when it negotiates with its connector.
  NodeCP rewrite(NodeCP node, PushdownContext& context) override {
    if (context.collectingFilters) {
      collectFilters(node, context);
      return node;
    }
    access_.add(*node);
    return narrowed(NodeRewriter::rewrite(node, context), context);
  }

 protected:
  // Predicates guaranteed by each join input and by the join output.
  struct CollectedJoinFilters {
    ExprVector leftInput;
    ExprVector rightInput;
    ExprVector output;
  };

  static void appendDistinct(
      ExprVector& destination,
      const ExprVector& source) {
    PlanObjectSet seen = PlanObjectSet::fromObjects(destination);
    for (ExprCP expression : source) {
      if (!seen.contains(expression)) {
        destination.push_back(expression);
        seen.add(expression);
      }
    }
  }

  static void appendDistinct(ExprVector& destination, ExprCP expression) {
    if (std::find(destination.begin(), destination.end(), expression) ==
        destination.end()) {
      destination.push_back(expression);
    }
  }

  // Runs one read-only collection walk and returns its response.
  ExprVector collectGuaranteedFilters(NodeCP node, PlanObjectSet requested) {
    PushdownContext context;
    context.collectingFilters = true;
    context.requestedFilterColumns = std::move(requested);
    rewrite(node, context);
    return std::move(context.collectedFilters);
  }

  void collectThroughInput(NodeCP input, PushdownContext& context) {
    PlanObjectSet requested;
    for (ColumnCP column : input->outputColumns()) {
      if (context.requestedFilterColumns.contains(column)) {
        requested.add(column);
      }
    }
    context.collectedFilters =
        collectGuaranteedFilters(input, std::move(requested));
  }

  // Produces every requested output-column combination for a filter whose
  // input columns may each have multiple aliases.
  static std::vector<ExprFactory::ExprSubstitution> aliasSubstitutions(
      ExprCP filter,
      const folly::F14FastMap<ColumnCP, ColumnVector>& outputAliases) {
    std::vector<ExprFactory::ExprSubstitution> substitutions(1);
    filter->columns().forEach<Column>([&](ColumnCP inputColumn) {
      const auto aliasIt = outputAliases.find(inputColumn);
      VELOX_DCHECK(aliasIt != outputAliases.end());

      auto partial = std::move(substitutions);
      substitutions.clear();
      substitutions.reserve(partial.size() * aliasIt->second.size());
      for (const auto& mapping : partial) {
        for (ColumnCP outputColumn : aliasIt->second) {
          auto extended = mapping;
          extended.emplace(inputColumn, outputColumn);
          substitutions.push_back(std::move(extended));
        }
      }
    });
    return substitutions;
  }

  // Maps predicates through direct column aliases. General expressions are
  // not invertible, so they form a conservative collection barrier.
  void collectThroughMapping(
      NodeCP input,
      const ExprVector& inputExpressions,
      const ColumnVector& outputColumns,
      PushdownContext& context) {
    folly::F14FastMap<ColumnCP, ColumnVector> outputAliases;
    PlanObjectSet requestedInputs;
    for (size_t i = 0; i < outputColumns.size(); ++i) {
      if (!context.requestedFilterColumns.contains(outputColumns[i]) ||
          !inputExpressions[i]->is(PlanType::kColumnExpr)) {
        continue;
      }
      ColumnCP inputColumn = inputExpressions[i]->as<Column>();
      requestedInputs.add(inputColumn);
      outputAliases[inputColumn].push_back(outputColumns[i]);
    }

    for (ExprCP filter :
         collectGuaranteedFilters(input, std::move(requestedInputs))) {
      for (const auto& mapping : aliasSubstitutions(filter, outputAliases)) {
        appendDistinct(
            context.collectedFilters, exprs_.replace(filter, mapping));
      }
    }
  }

  // Returns only predicates present on every union leg after mapping each
  // leg's columns to the union outputs.
  void collectFromUnion(const UnionAll* node, PushdownContext& context) {
    ExprVector common;
    bool firstInput{true};
    for (size_t i = 0; i < node->inputs().size(); ++i) {
      ExprVector inputExpressions(
          node->legColumns()[i].begin(), node->legColumns()[i].end());
      PushdownContext inputContext;
      inputContext.collectingFilters = true;
      inputContext.requestedFilterColumns = context.requestedFilterColumns;
      collectThroughMapping(
          node->inputs()[i],
          inputExpressions,
          node->outputColumns(),
          inputContext);
      if (firstInput) {
        common = std::move(inputContext.collectedFilters);
        firstInput = false;
      } else {
        const PlanObjectSet present =
            PlanObjectSet::fromObjects(inputContext.collectedFilters);
        std::erase_if(
            common, [&](ExprCP filter) { return !present.contains(filter); });
      }
    }
    context.collectedFilters = std::move(common);
  }

  // Collects, for each requested column of 'values', the values it holds:
  // 'column IN (values)', with 'column IS NULL OR' when it also holds NULL.
  void collectFromValues(const Values& values, PushdownContext& context) {
    if (values.rows() == nullptr) {
      return;
    }
    const auto& columns = values.outputColumns();
    for (size_t channel = 0; channel < columns.size(); ++channel) {
      ColumnCP column = columns[channel];
      if (!context.requestedFilterColumns.contains(column)) {
        continue;
      }
      ExprVector distinctValues;
      folly::F14FastSet<ExprCP> seen;
      bool hasNull{false};
      for (size_t row = 0; row < values.cardinality(); ++row) {
        const velox::Variant& value = values.valueAt(row, channel);
        if (value.isNull()) {
          hasNull = true;
          continue;
        }
        ExprCP literal =
            builder().makeLiteral(velox::Variant(value), column->value().type);
        if (seen.insert(literal).second) {
          distinctValues.push_back(literal);
        }
      }
      ExprCP inValues = nullptr;
      if (!distinctValues.empty()) {
        inValues = distinctValues.size() == 1
            ? exprs_.makeEq(column, distinctValues.front())
            : exprs_.makeIn(column, std::move(distinctValues));
      }
      ExprCP isNull = hasNull ? exprs_.makeIsNull(column) : nullptr;
      if (inValues != nullptr || isNull != nullptr) {
        appendDistinct(
            context.collectedFilters,
            inValues == nullptr     ? isNull
                : isNull == nullptr ? inValues
                                    : exprs_.makeOr(isNull, inValues));
      }
    }
  }

  // Collects predicates guaranteed to hold for every row emitted by `node`.
  void collectFilters(NodeCP node, PushdownContext& context) {
    switch (node->nodeType()) {
      case NodeType::kScan:
      case NodeType::kIndexLookupJoin:
      case NodeType::kWorkingTable:
        return;
      case NodeType::kValues:
        collectFromValues(*node->as<Values>(), context);
        return;
      case NodeType::kFilter: {
        const auto* filter = node->as<Filter>();
        collectThroughInput(filter->input(), context);
        for (ExprCP predicate : filter->predicates()) {
          ExprVector conjuncts;
          ExprFactory::flattenAnd(predicate, conjuncts);
          for (ExprCP conjunct : conjuncts) {
            if (!conjunct->containsNonDeterministic() &&
                !conjunct->columns().empty() &&
                conjunct->columns().isSubset(context.requestedFilterColumns)) {
              appendDistinct(context.collectedFilters, conjunct);
            }
          }
        }
        return;
      }
      case NodeType::kProject: {
        const auto* project = node->as<Project>();
        collectThroughMapping(
            project->input(),
            project->exprs(),
            project->outputColumns(),
            context);
        return;
      }
      case NodeType::kAggregate: {
        const auto* aggregate = node->as<Aggregate>();
        if (aggregate->groupingKeys().empty() ||
            !aggregate->globalGroupingSets().empty()) {
          return;
        }
        const size_t numKeys = aggregate->groupingKeys().size();
        collectThroughMapping(
            aggregate->input(),
            aggregate->groupingKeys(),
            ColumnVector(
                aggregate->outputColumns().begin(),
                aggregate->outputColumns().begin() + numKeys),
            context);
        return;
      }
      case NodeType::kUnionAll:
        collectFromUnion(node->as<UnionAll>(), context);
        return;
      case NodeType::kJoin: {
        const auto& filters = joinFilters(node->as<Join>());
        context.collectedFilters = filters.output;
        std::erase_if(context.collectedFilters, [&](ExprCP filter) {
          return !filter->columns().isSubset(context.requestedFilterColumns);
        });
        return;
      }
      case NodeType::kLimit:
      case NodeType::kSort:
      case NodeType::kTopN:
      case NodeType::kMarkDistinct:
      case NodeType::kUnnest:
      case NodeType::kWindow:
      case NodeType::kInference:
      case NodeType::kRowNumber:
      case NodeType::kTopNRowNumber:
      case NodeType::kAssignUniqueId:
      case NodeType::kEnforceDistinct:
      case NodeType::kExchange:
        collectThroughInput(node->inputs().front(), context);
        return;
      case NodeType::kGroupId:
      case NodeType::kApply:
      case NodeType::kEnforceSingleRow:
      case NodeType::kTableWrite:
      case NodeType::kFixedPoint:
        return;
    }
    VELOX_UNREACHABLE();
  }

  // Places repeated source columns in separate maps so every equivalent
  // target receives a derived filter.
  static std::vector<ExprFactory::ExprSubstitution> makeSubstitutions(
      const ColumnVector& sources,
      const ColumnVector& targets) {
    std::vector<ExprFactory::ExprSubstitution> result;
    folly::F14FastMap<ColumnCP, size_t> nextSubstitution;
    for (size_t i = 0; i < sources.size(); ++i) {
      const size_t index = nextSubstitution[sources[i]]++;
      if (index == result.size()) {
        result.emplace_back();
      }
      result[index].emplace(sources[i], targets[i]);
    }
    return result;
  }

  // Substitutes equivalent join-key columns into deterministic filters. Unless
  // the join matches NULL to NULL ('nullAsValue'), rows agree on a key only
  // when it is not NULL, so the targets are non-null in what a derived filter
  // is about. Returns true if a derived filter simplifies to false.
  bool deriveFilters(
      const ExprVector& filters,
      const ColumnVector& sources,
      const ColumnVector& targets,
      bool nullAsValue,
      ExprVector& derived) {
    const auto substitutions = makeSubstitutions(sources, targets);
    const PlanObjectSet nonNullTargets =
        nullAsValue ? PlanObjectSet{} : PlanObjectSet::fromObjects(targets);
    for (ExprCP filter : filters) {
      if (filter->containsNonDeterministic()) {
        continue;
      }
      for (const auto& mapping : substitutions) {
        ExprCP substituted = exprs_.replace(filter, mapping);
        if (substituted == filter || isSelfEquality(substituted)) {
          continue;
        }
        if (simplifier_.simplifyFilter(
                simplifier_.simplify(substituted, nonNullTargets), derived)) {
          return true;
        }
      }
    }
    return false;
  }

  // Appends derived filters that can be evaluated entirely by one input.
  void deriveForInput(
      const ExprVector& filters,
      const ColumnVector& sourceKeys,
      const ColumnVector& targetKeys,
      const PlanObjectSet& targetColumns,
      bool nullAsValue,
      ExprVector& targetFilters) {
    ExprVector derived;
    if (deriveFilters(filters, sourceKeys, targetKeys, nullAsValue, derived)) {
      appendDistinct(targetFilters, builder().makeBoolean(false));
      return;
    }
    std::erase_if(derived, [&](ExprCP filter) {
      return filter->columns().empty() ||
          !filter->columns().isSubset(targetColumns);
    });
    appendDistinct(targetFilters, derived);
  }

  // Caches predicates independently of a caller's requested columns. Map
  // presence records completion even when all three result vectors are empty.
  const CollectedJoinFilters& joinFilters(JoinCP node) {
    const auto found = collectedJoinFilters_.find(node);
    if (found != collectedJoinFilters_.end()) {
      return found->second;
    }

    const PlanObjectSet leftColumns =
        PlanObjectSet::fromObjects(node->left()->outputColumns());
    const PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(node->right()->outputColumns());
    auto [leftKeys, rightKeys] = JoinPredicatePlacement::equiColumnPairs(
        node, leftColumns, rightColumns, {});

    CollectedJoinFilters collected;
    collected.leftInput = collectGuaranteedFilters(node->left(), leftColumns);
    collected.rightInput =
        collectGuaranteedFilters(node->right(), rightColumns);

    const auto preserved = Join::preservedSides(node->joinType());
    if (preserved.left) {
      appendDistinct(collected.output, collected.leftInput);
    }
    if (preserved.right) {
      appendDistinct(collected.output, collected.rightInput);
    }
    if (JoinPredicatePlacement::filterPropagation(
            node->joinType(), node->nullAware())
            .emitsOnlyMatchedRows) {
      if (preserved.right) {
        deriveForInput(
            collected.leftInput,
            leftKeys,
            rightKeys,
            rightColumns,
            node->nullAsValue(),
            collected.output);
      }
      if (preserved.left) {
        deriveForInput(
            collected.rightInput,
            rightKeys,
            leftKeys,
            leftColumns,
            node->nullAsValue(),
            collected.output);
      }
      for (ExprCP filter : node->filter()) {
        if (!filter->containsNonDeterministic() && !filter->columns().empty()) {
          appendDistinct(collected.output, filter);
        }
      }
    }

    return collectedJoinFilters_.emplace(node, std::move(collected))
        .first->second;
  }

  static void appendInputFilters(
      const ExprVector& filters,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      ExprVector& leftFilters,
      ExprVector& rightFilters) {
    for (ExprCP filter : filters) {
      if (filter->columns().empty()) {
        continue;
      }
      if (filter->columns().isSubset(leftColumns)) {
        appendDistinct(leftFilters, filter);
      } else if (filter->columns().isSubset(rightColumns)) {
        appendDistinct(rightFilters, filter);
      }
    }
  }

  // Propagates filters only toward an input whose rows the join may discard.
  void propagateAcrossJoin(
      JoinCP node,
      velox::core::JoinType joinType,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      const ExprVector& pending,
      ExprVector& leftPending,
      ExprVector& rightPending) {
    const auto propagation =
        JoinPredicatePlacement::filterPropagation(joinType, node->nullAware());
    if (!propagation.leftToRight && !propagation.rightToLeft) {
      return;
    }

    auto [leftKeys, rightKeys] = JoinPredicatePlacement::equiColumnPairs(
        node, leftColumns, rightColumns, pending);
    if (leftKeys.empty()) {
      return;
    }

    const auto& collected = joinFilters(node);
    ExprVector leftFilters = collected.leftInput;
    ExprVector rightFilters = collected.rightInput;
    if (joinType != velox::core::JoinType::kInner) {
      appendInputFilters(
          pending, leftColumns, rightColumns, leftFilters, rightFilters);
    }
    appendInputFilters(
        node->filter(), leftColumns, rightColumns, leftFilters, rightFilters);

    // A filter an input already guarantees is not added to it again.
    if (propagation.rightToLeft) {
      ExprVector derived;
      deriveForInput(
          rightFilters,
          rightKeys,
          leftKeys,
          leftColumns,
          node->nullAsValue(),
          derived);
      appendNotGuaranteed(derived, collected.leftInput, leftPending);
    }
    if (propagation.leftToRight) {
      ExprVector derived;
      deriveForInput(
          leftFilters,
          leftKeys,
          rightKeys,
          rightColumns,
          node->nullAsValue(),
          derived);
      appendNotGuaranteed(derived, collected.rightInput, rightPending);
    }
  }

  static void appendNotGuaranteed(
      const ExprVector& filters,
      const ExprVector& guaranteed,
      ExprVector& pending) {
    for (ExprCP filter : filters) {
      if (std::find(guaranteed.begin(), guaranteed.end(), filter) ==
          guaranteed.end()) {
        appendDistinct(pending, filter);
      }
    }
  }

  // Adds the `Project` that `Node::emitsInputColumns` describes, here rather
  // than at the root, so the column stays out of everything in between.
  NodeCP narrowed(NodeCP node, PushdownContext& context) {
    if (!node->emitsInputColumns() || context.consumerDropsExtraColumns) {
      return node;
    }
    // A required column may have been replaced below; the parent reads its
    // replacement instead.
    PlanObjectSet required = context.required;
    required.unionSet(replacementColumns(
        exprs_, context.required, context.outputSubstitutions));
    ColumnVector keep;
    for (ColumnCP column : node->outputColumns()) {
      if (required.contains(column)) {
        keep.push_back(column);
      }
    }
    // Nothing required reads this node, so the rows themselves are what a
    // consumer wants -- a semijoin's existence check, say. Narrowing to no
    // columns at all is not a plan Velox can run.
    if (keep.empty() || keep.size() == node->outputColumns().size()) {
      return node;
    }
    ExprVector exprs(keep.begin(), keep.end());
    NodeCP project = PrecomputeProjections::makeProject(
        node, std::move(exprs), keep, builder(), simplifier_);
    context.outputSubstitutions.retainVisible(project->outputColumns(), exprs_);
    return project;
  }

  NodeCP finishSimplifiedNode(
      NodeSimplifier::SimplifiedNode simplified,
      ExprVector blocked,
      PushdownContext& context) {
    if (simplified.empty()) {
      context.outputSubstitutions.clear();
      return nodeSimplifier_.materialize(simplified);
    }
    blocked = simplified.substitutions.apply(blocked, exprs_);
    NodeCP result = maybeWrapFilter(simplified.node, std::move(blocked));
    simplified.substitutions.retainVisible(result->outputColumns(), exprs_);
    context.outputSubstitutions = std::move(simplified.substitutions);
    return result;
  }

  // Filter conjuncts (flattened across AND trees) join `pending`; the
  // Filter node itself is dropped.
  NodeCP rewriteFilter(const Filter* node, PushdownContext& context) override {
    for (ExprCP predicate : node->predicates()) {
      ExprFactory::flattenAnd(predicate, context.pending);
    }
    // Forwards 'context' unchanged: this Filter dissolves into 'pending', so
    // whatever consumes the result is the consumer this node had.
    return rewrite(node->input(), context);
  }

  // Project: substitute pending output-column references through aliases
  // whose backing expression is deterministic. Conjuncts that reference a
  // non-deterministic output are blocked above the Project.
  NodeCP rewriteProject(const Project* node, PushdownContext& context)
      override {
    PlanObjectSet blockedOutputs;
    for (size_t i = 0; i < node->exprs().size(); ++i) {
      if (node->exprs()[i]->functions().contains(
              FunctionSet::kNonDeterministic)) {
        blockedOutputs.add(node->outputColumns()[i]);
      }
    }

    auto [substitutable, blocked] = partition(context.pending, blockedOutputs);
    ExprVector pushable;
    pushable.reserve(substitutable.size());
    for (ExprCP conjunct : substitutable) {
      ExprCP substituted =
          exprs_.substitute(conjunct, node->outputColumns(), node->exprs());
      if (simplifier_.simplifyFilter(substituted, pushable)) {
        return makeEmptyValues(node);
      }
    }

    // A conjunct pushed below this Project reads the expressions it was
    // substituted into, and the projection producing them may not survive.
    access_.addAll(pushable);

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    ExprVector survivingExprs;
    ColumnVector survivingOutputs;
    survivingExprs.reserve(node->exprs().size());
    survivingOutputs.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->exprs().size(); ++i) {
      if (outputsKept.contains(node->outputColumns()[i])) {
        survivingExprs.push_back(node->exprs()[i]);
        survivingOutputs.push_back(node->outputColumns()[i]);
      }
    }

    PlanSubstitutions constants;
    for (ExprCP predicate : pushable) {
      if (const auto equality = exprs_.literalEquality(predicate)) {
        constants.set(equality->first, equality->second);
      }
    }
    for (ExprCP& expression : survivingExprs) {
      expression = simplifier_.simplify(constants.apply(expression, exprs_));
    }

    // Recorded here, not on the way down: an expression this pass prunes must
    // not contribute the paths it reads.
    for (size_t i = 0; i < survivingExprs.size(); ++i) {
      access_.addProducing(survivingExprs[i], survivingOutputs[i]);
    }

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required.unionColumns(survivingExprs);
    // Demand from above the child excludes the conjuncts pushed into
    // `pending`; it is exactly what the surviving projections read.
    childContext.requiredAbove = childContext.required;
    childContext.required.unionColumns(childContext.pending);
    childContext.nonNullColumns = context.nonNullColumns;
    childContext.consumerDropsExtraColumns = true;
    childContext.mayDropDuplicates = context.mayDropDuplicates;
    for (ExprCP expr : survivingExprs) {
      if (expr->containsNonDeterministic()) {
        childContext.mayDropDuplicates = false;
        break;
      }
    }
    NodeCP newInput = rewrite(node->input(), childContext);
    auto simplified = nodeSimplifier_.make(
        Project::Key{
            newInput, std::move(survivingExprs), std::move(survivingOutputs)},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(blocked), context);
  }

  // Aggregate: conjuncts referencing only grouping-key outputs push below
  // (substituted to the grouping-key expressions); conjuncts referencing
  // any aggregate output stay above as a HAVING-equivalent Filter, as do
  // nondeterministic conjuncts, since a group collapses rows.
  //
  // An aggregate that emits a row on empty input — a global aggregate (no
  // grouping keys), or one with a global (empty) grouping set — blocks
  // pushdown entirely: the aggregate manufactures that row regardless of its
  // input, so a filter pushed below it can't suppress the row it was meant to
  // remove. A predicate on a key that GroupId NULLs for some set is handled by
  // the GroupId barrier (see rewriteGroupId), not here.
  //
  // Grouping keys stay; dropping one would re-shape the groups.
  NodeCP rewriteAggregate(const Aggregate* node, PushdownContext& context)
      override {
    const bool emitsRowOnEmptyInput =
        node->groupingKeys().empty() || !node->globalGroupingSets().empty();
    if (emitsRowOnEmptyInput) {
      for (const auto* aggregate : node->aggregates()) {
        access_.add(aggregate);
      }

      PushdownContext childContext;
      childContext.required = context.required;
      childContext.required.unionColumns(context.pending);
      childContext.required.unionColumns(node->groupingKeys());
      for (const auto* aggregate : node->aggregates()) {
        childContext.required.unionColumns(aggregate);
      }
      childContext.requiredAbove = childContext.required;
      childContext.nonNullColumns = context.nonNullColumns;
      NodeCP newInput = rewrite(node->input(), childContext);
      auto simplified = nodeSimplifier_.make(
          Aggregate::Key{
              newInput,
              node->groupingKeys(),
              node->aggregates(),
              node->outputColumns(),
              node->step(),
              node->groupId(),
              node->globalGroupingSets()},
          {newInput, std::move(childContext.outputSubstitutions)});
      return finishSimplifiedNode(
          std::move(simplified), std::move(context.pending), context);
    }

    const size_t numKeys = node->groupingKeys().size();
    ColumnVector groupingKeyOutputs(
        node->outputColumns().begin(), node->outputColumns().begin() + numKeys);
    PlanObjectSet aggregateOutputs;
    for (size_t i = numKeys; i < node->outputColumns().size(); ++i) {
      aggregateOutputs.add(node->outputColumns()[i]);
    }

    auto [substitutable, blocked] =
        partition(context.pending, aggregateOutputs);
    blockNondeterministic(substitutable, blocked);

    ExprVector pushable;
    pushable.reserve(substitutable.size());
    for (ExprCP conjunct : substitutable) {
      ExprCP substituted =
          exprs_.substitute(conjunct, groupingKeyOutputs, node->groupingKeys());
      if (simplifier_.simplifyFilter(substituted, pushable)) {
        return makeEmptyValues(node);
      }
    }

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    AggregateCallVector survivingAggregates;
    ColumnVector survivingOutputs(groupingKeyOutputs);
    survivingAggregates.reserve(node->aggregates().size());
    survivingOutputs.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->aggregates().size(); ++i) {
      const ColumnCP outputColumn = node->outputColumns()[numKeys + i];
      if (outputsKept.contains(outputColumn)) {
        survivingAggregates.push_back(node->aggregates()[i]);
        survivingOutputs.push_back(outputColumn);
      }
    }

    for (const auto* aggregate : survivingAggregates) {
      access_.add(aggregate);
    }

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required.unionColumns(node->groupingKeys());
    for (const auto* aggregate : survivingAggregates) {
      childContext.required.unionColumns(aggregate);
    }
    childContext.requiredAbove = childContext.required;
    childContext.required.unionColumns(childContext.pending);
    childContext.nonNullColumns = context.nonNullColumns;
    NodeCP newInput = rewrite(node->input(), childContext);
    auto simplified = nodeSimplifier_.make(
        Aggregate::Key{
            newInput,
            node->groupingKeys(),
            std::move(survivingAggregates),
            std::move(survivingOutputs),
            node->step(),
            node->groupId(),
            node->globalGroupingSets()},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(blocked), context);
  }

  // True if a conjunct of 'conjuncts' is statically false, so no row passes.
  bool neverMatches(const ExprVector& conjuncts) {
    ExprVector kept;
    for (ExprCP conjunct : conjuncts) {
      kept.clear();
      if (simplifier_.simplifyFilter(conjunct, kept)) {
        return true;
      }
    }
    return false;
  }

  // A join condition no row satisfies decides the join's answer without a
  // join: an inner join produces nothing, and an outer join produces its
  // preserved side with the other side's columns NULL.
  //
  // Only a written ON clause reaches here as a false condition. A subquery
  // whose body is empty is a different shape, still joined against.
  NodeCP simplifyNeverMatchingJoin(const Join* node, PushdownContext& context) {
    if (!neverMatches(node->filter())) {
      return nullptr;
    }

    bool leftContributesNoRows{false};
    switch (node->joinType()) {
      case velox::core::JoinType::kInner:
        return makeEmptyValues(node);
      case velox::core::JoinType::kLeft:
        break;
      case velox::core::JoinType::kRight:
        leftContributesNoRows = true;
        break;
      default:
        // kFull preserves both sides, which is a union rather than one node.
        // A written ON clause reaches the semi and anti kinds as the body of
        // a subquery rather than as a join condition, so a false one does not
        // arrive here.
        return nullptr;
    }
    NodeCP remaining = leftContributesNoRows ? node->right() : node->left();
    auto expressions = builder().paddedExpressions(
        remaining->outputColumns(),
        node->outputColumns(),
        /*falsePadding=*/false);
    return rewrite(
        builder().make<Project>(
            {remaining, std::move(expressions), node->outputColumns()}),
        context);
  }

  // True if nothing reads 'mark': neither a consumer above nor a conjunct
  // still looking for a home here.
  static bool markIsDead(ColumnCP mark, const PushdownContext& context) {
    if (context.requiredAbove.contains(mark)) {
      return false;
    }
    return std::none_of(
        context.pending.begin(), context.pending.end(), [&](ExprCP conjunct) {
          return conjunct->columns().contains(mark);
        });
  }

  // Builds the context for one join input from the columns and predicates the
  // join requires from that input.
  static PushdownContext makeJoinInputContext(
      ExprVector pending,
      const PlanObjectSet& required,
      PlanObjectSet nonNullColumns) {
    PushdownContext context;
    context.pending = std::move(pending);
    context.required = required;
    context.requiredAbove = required;
    context.required.unionColumns(context.pending);
    context.nonNullColumns = std::move(nonNullColumns);
    return context;
  }

  // Allows the non-output side of a filtering join to drop duplicates when
  // every join expression is deterministic.
  static void allowJoinInputDeduplication(
      velox::core::JoinType joinType,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      const ExprVector& filter,
      PushdownContext& leftContext,
      PushdownContext& rightContext) {
    const auto allDeterministic = [](const ExprVector& expressions) {
      return std::ranges::none_of(expressions, [](ExprCP expression) {
        return expression->containsNonDeterministic();
      });
    };
    if (!allDeterministic(leftKeys) || !allDeterministic(rightKeys) ||
        !allDeterministic(filter)) {
      return;
    }
    switch (joinType) {
      case velox::core::JoinType::kLeftSemiFilter:
      case velox::core::JoinType::kLeftSemiProject:
      case velox::core::JoinType::kAnti:
        rightContext.mayDropDuplicates = true;
        break;
      case velox::core::JoinType::kRightSemiFilter:
      case velox::core::JoinType::kRightSemiProject:
      case velox::core::JoinType::kRightAnti:
        leftContext.mayDropDuplicates = true;
        break;
      default:
        break;
    }
  }

  // Holds rewritten join inputs and their output substitutions.
  struct RewrittenJoinInputs {
    NodeCP left;
    NodeCP right;
    ExprVector leftKeys;
    ExprVector rightKeys;
    ExprVector filter;
    PlanSubstitutions leftSubstitutions;
    PlanSubstitutions rightSubstitutions;
    PlanSubstitutions outputSubstitutions;
  };

  // Rewrites both inputs and leaves boundary simplification to NodeSimplifier.
  RewrittenJoinInputs rewriteJoinInputNodes(
      const Join* node,
      PushdownContext& leftContext,
      PushdownContext& rightContext,
      ExprVector leftKeys,
      ExprVector rightKeys,
      ExprVector filter) {
    RewrittenJoinInputs result{
        .left = rewrite(node->left(), leftContext),
        .right = rewrite(node->right(), rightContext),
        .leftKeys = std::move(leftKeys),
        .rightKeys = std::move(rightKeys),
        .filter = std::move(filter),
        .leftSubstitutions = std::move(leftContext.outputSubstitutions),
        .rightSubstitutions = std::move(rightContext.outputSubstitutions),
    };
    return result;
  }

  // Holds the join semantics selected before predicates are routed.
  struct PreparedJoin {
    velox::core::JoinType joinType;
    ColumnCP fusedMark{nullptr};
    bool fusedNullAware{false};
    PlanSubstitutions reductionSubstitutions;
  };

  // Applies mark fusion or outer-join reduction and updates pending predicates
  // for the selected join semantics.
  PreparedJoin prepareJoin(
      const Join* node,
      PushdownContext& context,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns) {
    PreparedJoin result{.joinType = node->joinType()};
    if (auto fusion = MarkFilterFusion::fuse(
            node, context.pending, context.requiredAbove)) {
      result.joinType = fusion->joinType;
      result.fusedMark = node->markColumn();
      // A filtering semi join is never null-aware. An anti join retains the
      // null-aware semantics of NOT IN.
      result.fusedNullAware =
          result.joinType == velox::core::JoinType::kAnti && node->nullAware();
      ExprVector remaining;
      remaining.reserve(context.pending.size() - 1);
      for (ExprCP conjunct : context.pending) {
        if (conjunct != fusion->conjunct) {
          remaining.push_back(conjunct);
        }
      }
      context.pending = std::move(remaining);
      return result;
    }

    const auto sourceSubstitutions = OuterJoinReduction::outputSources(node);
    result.joinType = OuterJoinReduction::reduce(
        node->joinType(),
        sourceSubstitutions.apply(context.pending, exprs_),
        rewriteColumnSet(exprs_, context.nonNullColumns, sourceSubstitutions),
        leftColumns,
        rightColumns);
    result.reductionSubstitutions = OuterJoinReduction::reducedOutputs(
        node, result.joinType, leftColumns, rightColumns);
    if (!result.reductionSubstitutions.empty()) {
      context.pending =
          result.reductionSubstitutions.apply(context.pending, exprs_);
    }
    return result;
  }

  // Holds the join boundary that remains after output pruning.
  struct PrunedJoin {
    ExprVector above;
    ColumnVector outputColumns;
    ColumnVector sourceColumns;
  };

  // Keeps outputs demanded by consumers and by predicates that remain above
  // the join. Join keys and filter columns are demanded only of the children.
  PrunedJoin pruneJoin(
      const Join* node,
      const PushdownContext& context,
      const PreparedJoin& prepared,
      JoinPredicatePlacement::Routed& routed) {
    PrunedJoin result{.above = std::move(routed.aboveJoinPredicates)};
    PlanObjectSet outputsKept = context.requiredAbove;
    outputsKept.unionColumns(result.above);
    result.outputColumns.reserve(node->outputColumns().size());
    result.sourceColumns.reserve(node->sourceColumns().size());
    for (size_t i = 0; i < node->outputColumns().size(); ++i) {
      const ColumnCP column = node->outputColumns()[i];
      // Consumers above name the original output, while the predicates kept
      // above, rewritten by prepareJoin, name the rewritten one.
      const ColumnCP rewritten =
          rewriteColumn(exprs_, column, prepared.reductionSubstitutions);
      // The fused conjunct was the mark's only consumer.
      if (column != prepared.fusedMark &&
          (outputsKept.contains(column) || outputsKept.contains(rewritten))) {
        result.outputColumns.push_back(rewritten);
        result.sourceColumns.push_back(node->sourceColumns()[i]);
      }
    }
    return result;
  }

  struct IndexLookupSides {
    NodeCP probeInput;
    const Scan* lookupScan;
    ExprVector probeKeys;
    ExprVector lookupKeyExpressions;
    ExprVector probePredicates;
    ExprVector lookupPredicates;
  };

  std::optional<IndexLookupSides> selectIndexLookupSides(
      const Join* node,
      const PreparedJoin& prepared,
      JoinPredicatePlacement::Routed& routed) {
    if ((prepared.joinType != velox::core::JoinType::kInner &&
         prepared.joinType != velox::core::JoinType::kLeft) ||
        routed.leftKeys.empty()) {
      return std::nullopt;
    }

    const auto lookupOnlyScan = [](NodeCP input) -> const Scan* {
      if (!input->is(NodeType::kScan)) {
        return nullptr;
      }
      const auto* scan = input->as<Scan>();
      return scan->baseTable()->layout()->supportsScan() ? nullptr : scan;
    };
    const Scan* rightLookup = lookupOnlyScan(node->right());
    const Scan* leftLookup = lookupOnlyScan(node->left());
    VELOX_USER_CHECK(
        rightLookup == nullptr || leftLookup == nullptr,
        "Joining two lookup-only tables is unsupported");
    const bool lookupOnRight = rightLookup != nullptr;
    const Scan* lookupScan = lookupOnRight ? rightLookup : leftLookup;
    if (lookupScan == nullptr ||
        (!lookupOnRight &&
         prepared.joinType != velox::core::JoinType::kInner)) {
      return std::nullopt;
    }

    const ExprVector& lookupKeyExpressions =
        lookupOnRight ? routed.rightKeys : routed.leftKeys;
    if (std::ranges::any_of(lookupKeyExpressions, [](ExprCP key) {
          return !key->isColumn();
        })) {
      return std::nullopt;
    }

    return IndexLookupSides{
        .probeInput = lookupOnRight ? node->left() : node->right(),
        .lookupScan = lookupScan,
        .probeKeys = lookupOnRight ? std::move(routed.leftKeys)
                                   : std::move(routed.rightKeys),
        .lookupKeyExpressions = lookupOnRight ? std::move(routed.rightKeys)
                                              : std::move(routed.leftKeys),
        .probePredicates = lookupOnRight
            ? std::move(routed.leftInputPredicates)
            : std::move(routed.rightInputPredicates),
        .lookupPredicates = lookupOnRight
            ? std::move(routed.rightInputPredicates)
            : std::move(routed.leftInputPredicates),
    };
  }

  void deduplicateLookupKeys(
      velox::core::JoinType joinType,
      IndexLookupSides& sides,
      ExprVector& joinPredicates) {
    folly::F14FastMap<ColumnCP, ExprCP> firstProbeKey;
    ExprVector uniqueProbeKeys;
    ExprVector uniqueLookupKeyExpressions;
    uniqueProbeKeys.reserve(sides.probeKeys.size());
    uniqueLookupKeyExpressions.reserve(sides.lookupKeyExpressions.size());
    for (size_t i = 0; i < sides.lookupKeyExpressions.size(); ++i) {
      ColumnCP lookupKey = sides.lookupKeyExpressions[i]->as<Column>();
      auto [it, inserted] =
          firstProbeKey.try_emplace(lookupKey, sides.probeKeys[i]);
      if (inserted) {
        uniqueProbeKeys.push_back(sides.probeKeys[i]);
        uniqueLookupKeyExpressions.push_back(lookupKey);
      } else if (joinType == velox::core::JoinType::kInner) {
        sides.probePredicates.push_back(
            exprs_.makeEq(sides.probeKeys[i], it->second));
      } else {
        joinPredicates.push_back(exprs_.makeEq(sides.probeKeys[i], it->second));
      }
    }
    sides.probeKeys = std::move(uniqueProbeKeys);
    sides.lookupKeyExpressions = std::move(uniqueLookupKeyExpressions);
  }

  struct OrderedLookupKeys {
    ColumnGroupCP index;
    ColumnVector lookupKeys;
  };

  OrderedLookupKeys orderLookupKeysForIndex(IndexLookupSides& sides) {
    ColumnVector lookupKeys;
    lookupKeys.reserve(sides.lookupKeyExpressions.size());
    for (ExprCP key : sides.lookupKeyExpressions) {
      lookupKeys.push_back(key->as<Column>());
    }
    const ColumnGroupCP index =
        sides.lookupScan->baseTable()->schemaTable->columnGroups[0];
    VELOX_USER_CHECK(
        index->distribution.partitionKeys().empty(),
        "Partitioned index lookup is unsupported: {}",
        index->layout->label());
    const auto& layoutKeys = index->layout->lookupKeys();
    VELOX_USER_CHECK_LE(
        lookupKeys.size(),
        layoutKeys.size(),
        "Lookup has more equality keys than the index: {}",
        index->layout->label());

    ExprVector orderedProbeKeys;
    ExprVector orderedLookupKeyExpressions;
    ColumnVector orderedLookupKeys;
    orderedProbeKeys.reserve(lookupKeys.size());
    orderedLookupKeyExpressions.reserve(lookupKeys.size());
    orderedLookupKeys.reserve(lookupKeys.size());
    for (size_t i = 0; i < lookupKeys.size(); ++i) {
      const auto key = std::ranges::find_if(lookupKeys, [&](ColumnCP column) {
        return column->schemaName() == layoutKeys[i]->name();
      });
      VELOX_USER_CHECK(
          key != lookupKeys.end(),
          "Lookup equality keys must cover an index prefix: {}",
          index->layout->label());
      const size_t queryPosition = key - lookupKeys.begin();
      orderedProbeKeys.push_back(sides.probeKeys[queryPosition]);
      orderedLookupKeyExpressions.push_back(
          sides.lookupKeyExpressions[queryPosition]);
      orderedLookupKeys.push_back(*key);
    }
    sides.probeKeys = std::move(orderedProbeKeys);
    sides.lookupKeyExpressions = std::move(orderedLookupKeyExpressions);
    return {index, std::move(orderedLookupKeys)};
  }

  NodeCP tryRewriteIndexLookupJoin(
      const Join* node,
      PushdownContext& context,
      PreparedJoin& prepared,
      JoinPredicatePlacement::Routed& routed,
      PrunedJoin& pruned) {
    auto sides = selectIndexLookupSides(node, prepared, routed);
    if (!sides.has_value()) {
      return nullptr;
    }
    deduplicateLookupKeys(prepared.joinType, *sides, routed.joinPredicates);
    auto [index, lookupKeys] = orderLookupKeysForIndex(*sides);

    PlanObjectSet sideRequired =
        PlanObjectSet::fromObjects(pruned.sourceColumns);
    sideRequired.unionColumns(sides->probeKeys);
    sideRequired.unionColumns(sides->lookupKeyExpressions);
    sideRequired.unionColumns(routed.joinPredicates);

    PlanObjectSet childNonNullColumns = rewriteColumnSet(
        exprs_, context.nonNullColumns, prepared.reductionSubstitutions);
    if (prepared.joinType == velox::core::JoinType::kInner) {
      for (ExprCP probeKey : sides->probeKeys) {
        if (!probeKey->containsNonDefaultNullBehavior()) {
          childNonNullColumns.unionColumns(probeKey);
        }
      }
    }
    PushdownContext probeContext = makeJoinInputContext(
        std::move(sides->probePredicates),
        sideRequired,
        std::move(childNonNullColumns));
    NodeCP probe = rewrite(sides->probeInput, probeContext);

    ColumnVector lookupOutputs;
    for (ColumnCP column : sides->lookupScan->outputColumns()) {
      if (sideRequired.contains(column)) {
        lookupOutputs.push_back(column);
      }
    }
    const PlanObjectSet visibleLookupOutputs =
        PlanObjectSet::fromObjects(pruned.sourceColumns);
    for (ExprCP filter : routed.joinPredicates) {
      PlanObjectSet lookupFilterColumns = filter->columns();
      lookupFilterColumns.intersect(
          PlanObjectSet::fromObjects(sides->lookupScan->outputColumns()));
      VELOX_USER_CHECK(
          lookupFilterColumns.isSubset(visibleLookupOutputs),
          "Index lookup residual filters can reference only lookup columns in the join output");
    }
    ExprVector rejected;
    const TableAccessHandle* handle = builder().takeTableAccessHandle(
        TableAccessHandle::buildIndexLookup(
            *sides->lookupScan->baseTable(),
            lookupOutputs,
            sides->lookupPredicates,
            lookupKeys,
            [this](ColumnCP column) {
              return toSubfields(
                  column->schemaName(),
                  access_.subfieldsOf(column),
                  /*mapKeysAsFields=*/false);
            },
            session_,
            evaluator_,
            rejected));
    VELOX_USER_CHECK(
        rejected.empty(),
        "Index lookup does not support connector-rejected filters");

    RewrittenJoinInputs rewritten{
        .left = probe,
        .right = sides->lookupScan,
        .leftKeys = std::move(sides->probeKeys),
        .rightKeys = std::move(sides->lookupKeyExpressions),
        .filter = std::move(routed.joinPredicates),
    };
    rewritten.outputSubstitutions.merge(prepared.reductionSubstitutions);
    const PlanObjectSet lookupOutputSet =
        PlanObjectSet::fromObjects(lookupOutputs);
    // Lookup columns originate at this node, so their source identities also
    // represent the values the join produces, including NULL-padded values.
    for (size_t i = 0; i < pruned.outputColumns.size(); ++i) {
      if (lookupOutputSet.contains(pruned.sourceColumns[i]) &&
          pruned.outputColumns[i] != pruned.sourceColumns[i]) {
        rewritten.outputSubstitutions.add(
            pruned.outputColumns[i], pruned.sourceColumns[i]);
        pruned.outputColumns[i] = pruned.sourceColumns[i];
      }
    }
    auto simplified = nodeSimplifier_.make(
        IndexLookupJoin::Key{
            probe,
            sides->lookupScan->baseTable(),
            index,
            std::move(lookupOutputs),
            handle,
            prepared.joinType,
            std::move(rewritten.leftKeys),
            std::move(lookupKeys),
            std::move(rewritten.filter),
            std::move(pruned.outputColumns),
            std::move(pruned.sourceColumns)},
        {probe,
         std::move(probeContext.outputSubstitutions),
         sides->probeInput->outputColumns()});
    NodeCP replacement = nodeSimplifier_.materialize(simplified);
    rewritten.outputSubstitutions.merge(simplified.substitutions);
    pruned.above = rewritten.outputSubstitutions.apply(pruned.above, exprs_);
    return finishJoin(replacement, rewritten, pruned, context);
  }

  // Rewrites both join inputs with the requirements established by routing
  // and pruning, then simplifies the rewritten join keys.
  RewrittenJoinInputs rewriteJoinChildren(
      const Join* node,
      const PushdownContext& context,
      PreparedJoin& prepared,
      JoinPredicatePlacement::Routed routed,
      const PrunedJoin& pruned) {
    PlanObjectSet sideRequired =
        PlanObjectSet::fromObjects(pruned.sourceColumns);
    sideRequired.unionColumns(routed.leftKeys);
    sideRequired.unionColumns(routed.rightKeys);
    sideRequired.unionColumns(routed.joinPredicates);

    // Equi-keys of an inner join prove their default-null-behavior inputs
    // non-NULL, which may let a descendant outer join become inner.
    PlanObjectSet childNonNullColumns = rewriteColumnSet(
        exprs_, context.nonNullColumns, prepared.reductionSubstitutions);
    if (prepared.joinType == velox::core::JoinType::kInner) {
      auto addIfDefaultNull = [&](ExprCP key) {
        if (!key->containsNonDefaultNullBehavior()) {
          childNonNullColumns.unionColumns(key);
        }
      };
      for (size_t i = 0; i < routed.leftKeys.size(); ++i) {
        addIfDefaultNull(routed.leftKeys[i]);
        addIfDefaultNull(routed.rightKeys[i]);
      }
    }

    PushdownContext leftContext = makeJoinInputContext(
        std::move(routed.leftInputPredicates),
        sideRequired,
        childNonNullColumns);
    PushdownContext rightContext = makeJoinInputContext(
        std::move(routed.rightInputPredicates),
        sideRequired,
        std::move(childNonNullColumns));
    allowJoinInputDeduplication(
        prepared.joinType,
        routed.leftKeys,
        routed.rightKeys,
        routed.joinPredicates,
        leftContext,
        rightContext);

    return rewriteJoinInputNodes(
        node,
        leftContext,
        rightContext,
        std::move(routed.leftKeys),
        std::move(routed.rightKeys),
        std::move(routed.joinPredicates));
  }

  // Restores predicates above the replacement and publishes its visible
  // output substitutions to the parent.
  NodeCP finishJoin(
      NodeCP replacement,
      RewrittenJoinInputs& rewritten,
      PrunedJoin& pruned,
      PushdownContext& context) {
    NodeCP result = maybeWrapFilter(replacement, std::move(pruned.above));
    rewritten.outputSubstitutions.retainVisible(
        result->outputColumns(), exprs_);
    context.outputSubstitutions = std::move(rewritten.outputSubstitutions);
    return result;
  }

  // Join: reduce outer to inner when possible, then route each pending
  // conjunct to one of: left input, right input, join filter, or
  // stay-above. The join's own filter conjuncts move according to the join
  // kind's match semantics. Neither path moves a nondeterministic conjunct
  // into an input, which would evaluate it once for rows the join multiplies.
  NodeCP rewriteJoin(const Join* node, PushdownContext& context) override {
    for (size_t i = 0; i < node->outputColumns().size(); ++i) {
      if (node->outputColumns()[i] != node->sourceColumns()[i]) {
        access_.addProducing(
            node->sourceColumns()[i], node->outputColumns()[i]);
      }
    }

    if (node->joinType() != velox::core::JoinType::kInner &&
        node->joinType() != velox::core::JoinType::kLeft &&
        node->joinType() != velox::core::JoinType::kRight &&
        node->joinType() != velox::core::JoinType::kFull &&
        node->joinType() != velox::core::JoinType::kLeftSemiFilter &&
        node->joinType() != velox::core::JoinType::kRightSemiFilter &&
        node->joinType() != velox::core::JoinType::kLeftSemiProject &&
        node->joinType() != velox::core::JoinType::kRightSemiProject &&
        node->joinType() != velox::core::JoinType::kAnti &&
        node->joinType() != velox::core::JoinType::kRightAnti &&
        node->joinType() != velox::core::JoinType::kCountingLeftSemiFilter &&
        node->joinType() != velox::core::JoinType::kCountingAnti) {
      if (NodeCP simplified = simplifyNeverMatchingJoin(node, context)) {
        return simplified;
      }
    }

    // A kLeftSemiProject keeps every left row and adds a mark, so with the
    // mark read by nobody the join has no effect and its left input stands in
    // its place.
    if (velox::core::isLeftSemiProjectJoin(node->joinType()) &&
        markIsDead(node->markColumn(), context)) {
      return rewrite(node->left(), context);
    }

    const PlanObjectSet leftColumns =
        PlanObjectSet::fromObjects(node->left()->outputColumns());
    const PlanObjectSet rightColumns =
        PlanObjectSet::fromObjects(node->right()->outputColumns());

    auto prepared = prepareJoin(node, context, leftColumns, rightColumns);

    ExprVector leftPending;
    ExprVector rightPending;

    propagateAcrossJoin(
        node,
        prepared.joinType,
        leftColumns,
        rightColumns,
        context.pending,
        leftPending,
        rightPending);

    if (prepared.joinType == velox::core::JoinType::kInner) {
      // Inner-join equi-key pairs let pending conjuncts cross to the
      // other side. If derivation produces a literal-false conjunct, the
      // join can't yield any rows.
      if (deriveTransitive(node, leftColumns, rightColumns, context.pending)) {
        return makeEmptyValues(node);
      }
    }

    auto routed = JoinPredicatePlacement::route(
        node,
        prepared.joinType,
        leftColumns,
        rightColumns,
        context.pending,
        std::move(leftPending),
        std::move(rightPending),
        builder());
    context.pending.clear();
    auto pruned = pruneJoin(node, context, prepared, routed);
    if (NodeCP lookup = tryRewriteIndexLookupJoin(
            node, context, prepared, routed, pruned)) {
      return lookup;
    }
    auto rewritten =
        rewriteJoinChildren(node, context, prepared, std::move(routed), pruned);
    auto simplified = nodeSimplifier_.make(
        Join::Key{
            rewritten.left,
            rewritten.right,
            prepared.joinType,
            std::move(rewritten.leftKeys),
            std::move(rewritten.rightKeys),
            std::move(rewritten.filter),
            prepared.fusedMark != nullptr ? prepared.fusedNullAware
                                          : node->nullAware(),
            node->nullAsValue(),
            std::move(pruned.outputColumns),
            std::move(pruned.sourceColumns)},
        {rewritten.left,
         std::move(rewritten.leftSubstitutions),
         node->left()->outputColumns()},
        {rewritten.right,
         std::move(rewritten.rightSubstitutions),
         node->right()->outputColumns()});
    NodeCP replacement = nodeSimplifier_.materialize(simplified);
    rewritten.outputSubstitutions = std::move(prepared.reductionSubstitutions);
    rewritten.outputSubstitutions.merge(simplified.substitutions);
    pruned.above = rewritten.outputSubstitutions.apply(pruned.above, exprs_);
    return finishJoin(replacement, rewritten, pruned, context);
  }

  // Scan: pending conjuncts reach the table, and the connector decides which
  // of them it will evaluate. The ones it rejects become a Filter over the
  // Scan, and the columns they read join the Scan's output.
  NodeCP rewriteScan(const Scan* node, PushdownContext& context) override {
    ExprVector filters = std::move(context.pending);
    context.pending.clear();
    if (std::any_of(filters.begin(), filters.end(), isConstantFalse)) {
      return makeEmptyValues(node);
    }
    PlanObjectSet impliedFilterSet;
    if (connectorPushdown_ == PushdownAndPrunePass::ConnectorPushdown::kOffer) {
      ExprVector impliedFilters;
      for (ExprCP derivedFilter :
           ImpliedFilters::deriveForColumns(filters, exprs_)) {
        if (simplifier_.simplifyFilter(derivedFilter, impliedFilters)) {
          return makeEmptyValues(node);
        }
      }

      PlanObjectSet offeredFilterSet = PlanObjectSet::fromObjects(filters);
      std::erase_if(impliedFilters, [&](ExprCP filter) {
        if (offeredFilterSet.contains(filter)) {
          return true;
        }
        offeredFilterSet.add(filter);
        return false;
      });
      impliedFilterSet.unionObjects(impliedFilters);
      appendAll(filters, impliedFilters);
    }

    ColumnVector survivingOutputs;
    survivingOutputs.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      if (context.requiredAbove.contains(column)) {
        survivingOutputs.push_back(column);
      }
    }

    ExprVector rejected;
    const TableAccessHandle* handle =
        negotiate(*node->baseTable(), survivingOutputs, filters, rejected);
    // Implied filters are optional pushdown opportunities because the
    // original filters that imply them remain in the plan.
    std::erase_if(rejected, [&](ExprCP filter) {
      return impliedFilterSet.contains(filter);
    });
    if (rejected.empty()) {
      return nodeSimplifier_
          .make(
              Scan::Key{node->baseTable(), std::move(survivingOutputs), handle})
          .node;
    }

    // The rejected conjuncts are evaluated above the scan, so the scan must
    // read what they reference.
    PlanObjectSet rejectedColumns;
    rejectedColumns.unionColumns(rejected);
    ColumnVector scanOutputs = survivingOutputs;
    for (ColumnCP column : node->baseTable()->columns) {
      if (rejectedColumns.contains(column) &&
          std::find(scanOutputs.begin(), scanOutputs.end(), column) ==
              scanOutputs.end()) {
        scanOutputs.push_back(column);
      }
    }
    // The Filter this rewrite puts above the scan reads only columns the scan
    // produces.
    const PlanObjectSet scanOutputSet = PlanObjectSet::fromObjects(scanOutputs);
    rejectedColumns.forEach<Column>([&](ColumnCP column) {
      VELOX_CHECK(
          scanOutputSet.contains(column),
          "A rejected filter reads a column the scan does not produce: {}",
          column->name());
    });

    const bool readsExtraColumns = scanOutputs.size() > survivingOutputs.size();
    NodeCP scan =
        nodeSimplifier_
            .make(Scan::Key{node->baseTable(), std::move(scanOutputs), handle})
            .node;
    NodeCP filter = builder().make<Filter>({scan, std::move(rejected)});
    if (!readsExtraColumns || context.consumerDropsExtraColumns) {
      return filter;
    }
    // The columns only the rejected conjuncts read stop here: nothing above
    // asked for them, and carrying them would widen every operator between
    // this scan and the query's output.
    ExprVector passThrough(survivingOutputs.begin(), survivingOutputs.end());
    return builder().make<Project>(
        {filter, std::move(passThrough), std::move(survivingOutputs)});
  }

  // Offers 'filters' to the connector for 'baseTable' read as 'outputColumns'
  // and returns the resulting handle, appending the conjuncts the connector
  // rejected to 'rejected'. Returns null, rejecting everything, when the
  // caller asked for no connector pushdown.
  const TableAccessHandle* negotiate(
      const BaseTable& baseTable,
      const ColumnVector& outputColumns,
      const ExprVector& filters,
      ExprVector& rejected) {
    if (connectorPushdown_ == PushdownAndPrunePass::ConnectorPushdown::kSkip) {
      appendAll(rejected, filters);
      return nullptr;
    }
    const auto it = negotiatedByBaseTableId_.find(baseTable.id());
    if (it != negotiatedByBaseTableId_.end()) {
      // A rewrite that duplicated a subtree can present one Scan twice. The
      // connector is asked once, so the second visit must be offering the same
      // predicates and reading no more columns; otherwise its predicates would
      // be silently dropped or it would read a column with no handle. The
      // paths are not compared: access accumulates as the walk descends, so
      // the second visit legitimately sees a superset of what the handle was
      // built for.
      VELOX_CHECK(
          it->second.filters == filters,
          "Two reads of one table offer the connector different filters: {}",
          baseTable.schemaTable->name());
      for (ColumnCP column : outputColumns) {
        VELOX_CHECK(
            it->second.handle->columnHandles.contains(column),
            "Two reads of one table read different columns: {}.{}",
            baseTable.schemaTable->name(),
            column->name());
      }
      appendAll(rejected, it->second.rejected);
      return it->second.handle;
    }
    ExprVector rejectedHere;
    const TableAccessHandle* handle = builder().takeTableAccessHandle(
        TableAccessHandle::buildScan(
            baseTable,
            outputColumns,
            filters,
            [this](ColumnCP column) {
              return toSubfields(
                  column->schemaName(),
                  access_.subfieldsOf(column),
                  /*mapKeysAsFields=*/false);
            },
            session_,
            evaluator_,
            rejectedHere));
    appendAll(rejected, rejectedHere);
    negotiatedByBaseTableId_.emplace(
        baseTable.id(), Negotiated{handle, filters, std::move(rejectedHere)});
    return handle;
  }

  NodeCP rewriteValues(const Values* node, PushdownContext& context) override {
    // Keep every column the output needs (`required`) plus every column a
    // pending conjunct reads: maybeWrapFilter re-materializes those conjuncts
    // as a Filter over this Values, so their columns must survive.
    PlanObjectSet keep = context.required;
    keep.unionColumns(context.pending);

    ColumnVector survivingOutputs;
    QGVector<velox::column_index_t> survivingChannels;
    const auto& outputColumns = node->outputColumns();
    const auto& channels = node->channels();
    for (size_t i = 0; i < outputColumns.size(); ++i) {
      if (keep.contains(outputColumns[i])) {
        survivingOutputs.push_back(outputColumns[i]);
        survivingChannels.push_back(channels[i]);
      }
    }

    auto simplified = nodeSimplifier_.make(
        Values::Key{
            node->source(),
            node->rows(),
            survivingOutputs,
            std::move(survivingChannels)});
    if (!simplified.empty()) {
      simplified.originalColumns = std::move(survivingOutputs);
    }
    NodeCP valuesNode = simplified.node;
    simplified = nodeSimplifier_.make(
        Filter::Key{valuesNode, std::move(context.pending)},
        std::move(simplified));
    if (simplified.empty()) {
      context.outputSubstitutions.clear();
      return nodeSimplifier_.materialize(simplified);
    }
    context.outputSubstitutions = std::move(simplified.substitutions);
    return simplified.node;
  }

  // Internal nodes — recurse with empty pending via `blockAt`:
  NodeCP rewriteLimit(const Limit* node, PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      const int64_t cap = node->offsetPlusCount();
      if (node->input()->is(NodeType::kWindow) &&
          cap <= std::numeric_limits<int32_t>::max()) {
        empty.rankLimit = static_cast<int32_t>(cap);
      }
      NodeCP newInput = rewrite(node->input(), empty);

      // A row_number over one partition, capped at the same count, already
      // emits exactly the rows this limit would keep.
      if (node->offset() == 0 && newInput->is(NodeType::kTopNRowNumber)) {
        const auto* ranking = newInput->as<TopNRowNumber>();
        if (ranking->partitionKeys().empty() &&
            ranking->limit() == node->count() &&
            ranking->rankFunction() ==
                velox::core::TopNRowNumberNode::RankFunction::kRowNumber) {
          return newInput;
        }
      }

      auto simplified = nodeSimplifier_.make(
          Limit::Key{newInput, node->offset(), node->count()},
          {newInput,
           std::move(empty.outputSubstitutions),
           node->input()->outputColumns()});
      empty.outputSubstitutions = std::move(simplified.substitutions);
      return nodeSimplifier_.materialize(simplified);
    });
  }

  // Sort: pending passes through unchanged — order has no row-set
  // effect. The sort keys must survive in the child's outputs.
  NodeCP rewriteSort(const Sort* node, PushdownContext& context) override {
    // With no column of this node read above, its rows are indistinguishable
    // and their order cannot be observed: `SELECT 1 FROM t ORDER BY x` is
    // `SELECT 1 FROM t`. Dropping the Sort also frees its keys to be pruned.
    if (!context.requiredAbove.containsAny(node->outputColumns())) {
      return rewrite(node->input(), context);
    }
    context.required.unionColumns(node->orderKeys());
    context.requiredAbove.unionColumns(node->orderKeys());
    // A Sort outputs its input's columns, so it is not the consumer the flag
    // describes.
    context.consumerDropsExtraColumns = false;
    NodeCP newInput = rewrite(node->input(), context);
    auto simplified = nodeSimplifier_.make(
        Sort::Key{newInput, node->orderKeys(), node->orderTypes()},
        {newInput, std::move(context.outputSubstitutions)});
    context.outputSubstitutions = std::move(simplified.substitutions);
    return simplified.node;
  }

  // Returns true when 'node' already emits rows in the order 'keys' / 'types'
  // ask for. A Window over a single partition sorts its output, and a
  // pass-through Project above it keeps that order. Pass a rewritten node: a
  // Window specialized into a ranking node does not order its output.
  bool emitsInOrder(
      NodeCP node,
      const ExprVector& keys,
      const OrderTypeVector& types) {
    ExprVector mapped(keys);
    while (node->is(NodeType::kProject)) {
      const auto* project = node->as<Project>();
      const auto& outputs = project->outputColumns();
      for (ExprCP& key : mapped) {
        const auto it = std::find(outputs.begin(), outputs.end(), key);
        if (it == outputs.end()) {
          return false;
        }
        key = project->exprs()[it - outputs.begin()];
      }
      node = project->input();
    }

    if (!node->is(NodeType::kWindow)) {
      return false;
    }
    const auto* window = node->as<Window>();
    return window->partitionKeys().empty() &&
        window->orderKeys().size() >= mapped.size() &&
        std::equal(mapped.begin(), mapped.end(), window->orderKeys().begin()) &&
        std::equal(types.begin(), types.end(), window->orderTypes().begin());
  }

  // TopN: a filter barrier like Limit (its bound depends on the row set), and
  // its order keys must survive in the child like Sort.
  NodeCP rewriteTopN(const TopN* node, PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      // With no column read above, only the row count the TopN keeps is
      // observable, which a Limit keeps as well. The conjuncts blocked here
      // filter the rows the TopN keeps, so `empty.requiredAbove` includes the
      // columns they read.
      if (!empty.requiredAbove.containsAny(node->outputColumns())) {
        return builder().make<Limit>(
            {rewrite(node->input(), empty), node->offset(), node->count()});
      }
      empty.required.unionColumns(node->orderKeys());
      empty.requiredAbove.unionColumns(node->orderKeys());

      // Sorting by what the window below ranks on means a row past rank
      // 'offset + count' in its partition has that many rows before it here
      // too, so it cannot survive this TopN: the window can cap partitions
      // there.
      const int64_t cap = node->offsetPlusCount();
      const bool ordersAgree = node->input()->is(NodeType::kWindow) &&
          node->orderKeys() == node->input()->as<Window>()->orderKeys() &&
          node->orderTypes() == node->input()->as<Window>()->orderTypes();
      if (ordersAgree && cap <= std::numeric_limits<int32_t>::max()) {
        empty.rankLimit = static_cast<int32_t>(cap);
      }

      NodeCP newInput = rewrite(node->input(), empty);
      auto simplified = nodeSimplifier_.make(
          TopN::Key{
              newInput,
              node->orderKeys(),
              node->orderTypes(),
              node->offset(),
              node->count()},
          {newInput,
           std::move(empty.outputSubstitutions),
           node->input()->outputColumns()});
      empty.outputSubstitutions = std::move(simplified.substitutions);
      if (simplified.empty()) {
        return nodeSimplifier_.materialize(simplified);
      }

      // A Window over a single partition emits its rows in order-key order, so
      // a TopN asking for that same order only has to count rows.
      if (simplified.node->is(NodeType::kTopN) &&
          emitsInOrder(
              newInput,
              simplified.node->as<TopN>()->orderKeys(),
              simplified.node->as<TopN>()->orderTypes())) {
        return builder().make<Limit>({newInput, node->offset(), node->count()});
      }

      // The cap only reduces the rows the TopN sees; the TopN itself stays,
      // because a ranking node emits each partition greatest-rank first rather
      // than in order-key order. It can go once
      // https://github.com/facebookincubator/velox/issues/18494 makes a ranking
      // node's output order match a Window's.
      return simplified.node;
    });
  }

  // GroupId is a barrier: a conjunct on a key GroupId NULLs for some set can't
  // push below it. The input must supply the key expressions GroupId replicates
  // on and the columns the aggregates read (passed through).
  NodeCP rewriteGroupId(const GroupId* node, PushdownContext& context)
      override {
    return blockAt(context, [&](PushdownContext& empty) -> NodeCP {
      empty.required.unionColumns(node->groupingKeys());
      empty.required.unionColumns(node->aggregationInputs());
      empty.requiredAbove.unionColumns(node->groupingKeys());
      empty.requiredAbove.unionColumns(node->aggregationInputs());
      NodeCP newInput = rewrite(node->input(), empty);
      auto simplified = nodeSimplifier_.make(
          GroupId::Key{
              newInput,
              node->groupingKeys(),
              node->aggregationInputs(),
              node->groupingSets(),
              node->groupingKeyColumns(),
              node->groupId(),
              node->outputColumns()},
          {newInput, std::move(empty.outputSubstitutions)});
      empty.outputSubstitutions = std::move(simplified.substitutions);
      return simplified.node;
    });
  }

  NodeCP rewriteMarkDistinct(
      const MarkDistinct* /*node*/,
      PushdownContext& /*context*/) override {
    VELOX_FAIL("MarkDistinct is added by physical planning, after pushdown");
  }

  // Unnest: conjuncts referencing only replicated (pre-unnest) columns
  // push below. Conjuncts that touch any unnest-produced column or the
  // ordinality column stay above — those don't exist on the input. So do
  // nondeterministic conjuncts, since one input row yields many output rows.
  NodeCP rewriteUnnest(const Unnest* node, PushdownContext& context) override {
    PlanObjectSet outputOnlyColumns;
    for (const ColumnVector& perExpression : node->unnestColumns()) {
      outputOnlyColumns.unionObjects(perExpression);
    }
    if (node->ordinalityColumn() != nullptr) {
      outputOnlyColumns.add(node->ordinalityColumn());
    }
    if (node->isOuter()) {
      outputOnlyColumns.add(node->markerColumn());
    }
    auto [pushable, blocked] = partition(context.pending, outputOnlyColumns);
    blockNondeterministic(pushable, blocked);

    // Columns this Unnest must still produce: those the consumer requires plus
    // the columns read by conjuncts that stay above. The marker keeps the rows
    // of an empty unnested value rather than carrying a value of its own, so
    // dropping it would drop those rows; keep it whether or not it is read.
    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);
    if (node->isOuter()) {
      outputsKept.add(node->markerColumn());
    }

    // Drop the replicated (pass-through) columns and ordinality column the
    // consumer no longer needs. These must leave the replicated/ordinality
    // fields and the output set together: Unnest requires both to appear in
    // outputColumns. Columns still read by pushed-down conjuncts remain
    // required of the input via 'pushable'.
    ColumnVector survivingReplicated;
    survivingReplicated.reserve(node->replicatedColumns().size());
    for (ColumnCP column : node->replicatedColumns()) {
      if (outputsKept.contains(column)) {
        survivingReplicated.push_back(column);
      }
    }
    ColumnCP survivingOrdinality = node->ordinalityColumn() != nullptr &&
            outputsKept.contains(node->ordinalityColumn())
        ? node->ordinalityColumn()
        : nullptr;

    ColumnCP survivingMarker = node->markerColumn();

    const bool collapseDuplicates = context.mayDropDuplicates &&
        builder().functionNames().cardinality != nullptr &&
        builder().functionNames().lte != nullptr && !node->isOuter() &&
        blocked.empty() && !outputsKept.hasIntersection(outputOnlyColumns);

    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    // Unnest computes the output rows of each input row from that row alone.
    childContext.nonNullColumns = context.nonNullColumns;
    childContext.required.unionColumns(node->unnestExpressions());
    childContext.required.unionObjects(survivingReplicated);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    childContext.mayDropDuplicates = collapseDuplicates;
    // This collapse still evaluates the collection once per input row. A
    // child collapse could remove input rows and change its evaluation count.
    for (ExprCP expr : node->unnestExpressions()) {
      if (expr->containsNonDeterministic()) {
        childContext.mayDropDuplicates = false;
        break;
      }
    }
    NodeCP newInput = rewrite(node->input(), childContext);
    ExprVector unnestExpressions = node->unnestExpressions();

    if (collapseDuplicates) {
      applyOutputSubstitutions(childContext, unnestExpressions, blocked);
      const auto* one = builder().makeLiteral(
          velox::Variant(int64_t{1}), toType(velox::BIGINT()));
      ExprVector nonEmpty;
      nonEmpty.reserve(unnestExpressions.size());
      // UNNEST pads shorter collections to the longest one, so an input row
      // produces output exactly when at least one collection is non-empty.
      for (ExprCP expression : unnestExpressions) {
        nonEmpty.push_back(exprs_.makeLessThanOrEqual(
            one, exprs_.makeCardinality(expression)));
      }
      NodeCP filtered = builder().make<Filter>(
          {newInput, ExprVector{exprs_.orAll(nonEmpty)}});
      return finishSimplifiedNode(
          {filtered, std::move(childContext.outputSubstitutions)}, {}, context);
    }

    // Unnest accepts a subset of structured-field outputs as
    // `outputColumns`; drop entries the consumer doesn't need.
    ColumnVector survivingOutputs;
    survivingOutputs.reserve(node->outputColumns().size());
    for (ColumnCP column : node->outputColumns()) {
      if (outputsKept.contains(column)) {
        survivingOutputs.push_back(column);
      }
    }
    auto simplified = nodeSimplifier_.make(
        Unnest::Key{
            newInput,
            std::move(unnestExpressions),
            std::move(survivingReplicated),
            node->unnestColumns(),
            survivingOrdinality,
            survivingMarker,
            std::move(survivingOutputs)},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(blocked), context);
  }

  // UnionAll: each leg receives the same conjuncts rewritten in terms
  // of that leg's input columns.
  NodeCP rewriteUnionAll(const UnionAll* node, PushdownContext& context)
      override {
    // Positions to keep: outputColumns[i] required by the consumer.
    std::vector<size_t> keptPositions;
    keptPositions.reserve(node->outputColumns().size());
    for (size_t i = 0; i < node->outputColumns().size(); ++i) {
      if (context.required.contains(node->outputColumns()[i])) {
        keptPositions.push_back(i);
      }
    }
    ColumnVector newOutputColumns;
    newOutputColumns.reserve(keptPositions.size());
    for (size_t i : keptPositions) {
      newOutputColumns.push_back(node->outputColumns()[i]);
    }

    NodeVector newInputs;
    newInputs.reserve(node->inputs().size());
    QGVector<ColumnVector> newLegColumns;
    newLegColumns.reserve(node->inputs().size());
    std::vector<NodeSimplifier::SimplifiedNode> simplifiedLegs;
    simplifiedLegs.reserve(node->inputs().size());
    for (size_t legIndex = 0; legIndex < node->inputs().size(); ++legIndex) {
      const ColumnVector& legColumns = node->legColumns()[legIndex];
      const ExprVector legAsExprs = toExprs(legColumns);
      ExprVector legPending =
          exprs_.substitute(context.pending, node->outputColumns(), legAsExprs);
      ColumnVector newLegCols;
      newLegCols.reserve(keptPositions.size());
      for (size_t i : keptPositions) {
        newLegCols.push_back(legColumns[i]);
      }

      PushdownContext legContext;
      legContext.pending = std::move(legPending);
      legContext.required.unionObjects(newLegCols);
      legContext.required.unionColumns(legContext.pending);
      legContext.requiredAbove = legContext.required;
      legContext.nonNullColumns = context.nonNullColumns;
      NodeCP newLeg = rewrite(node->inputs()[legIndex], legContext);
      newInputs.push_back(newLeg);
      newLegColumns.push_back(std::move(newLegCols));
      simplifiedLegs.push_back(
          {newLeg, std::move(legContext.outputSubstitutions)});
    }
    context.pending.clear();
    auto simplified = nodeSimplifier_.make(
        UnionAll::Key{
            std::move(newInputs),
            std::move(newLegColumns),
            std::move(newOutputColumns)},
        std::move(simplifiedLegs));
    context.outputSubstitutions = std::move(simplified.substitutions);
    return nodeSimplifier_.materialize(simplified);
  }

  // Inference: every conjunct stays above, since the common case reads the
  // call's result.
  NodeCP rewriteInference(const Inference* node, PushdownContext& context)
      override {
    return blockAt(context, [&](PushdownContext& child) -> NodeCP {
      // The input supplies what the call reads; the result is produced here.
      child.required.unionColumns(node->call());
      child.requiredAbove = child.required;
      child.nonNullColumns = context.nonNullColumns;
      NodeCP newInput = rewrite(node->input(), child);
      auto simplified = nodeSimplifier_.make(
          Inference::Key{
              newInput, node->call(), node->result(), node->outputColumns()},
          {newInput, std::move(child.outputSubstitutions)});
      child.outputSubstitutions = std::move(simplified.substitutions);
      return simplified.node;
    });
  }

  // Window: conjuncts whose columns are all direct partition-key
  // columns push below — within a partition those values are
  // constant, so a deterministic conjunct keeps or drops the partition whole.
  // All others stay above, as do nondeterministic conjuncts, which would
  // instead thin a partition and change what the window functions read.
  // Non-NULL hints from above pass below only for partition-key columns.
  // A single ranking function then specializes into the cheapest node
  // that computes it; anything else stays a Window with unread functions
  // pruned.
  NodeCP rewriteWindow(const Window* node, PushdownContext& context) override {
    const std::optional<RankFusion> fusion = detectRankFusion(
        node,
        context.pending,
        context.rankLimit,
        builder().functionNames(),
        exprs_);

    PlanObjectSet partitionKeyColumns;
    for (ExprCP key : node->partitionKeys()) {
      if (key->is(PlanType::kColumnExpr)) {
        partitionKeyColumns.add(key->as<Column>());
      }
    }

    // Conjuncts referencing only partition keys push below; the rest stay above
    // (or, for a bound the ranking node absorbs, are consumed).
    const ColumnCP constantRank =
        constantRankColumn(node, builder().functionNames());
    ExprVector pushable;
    ExprVector blocked;
    for (ExprCP conjunct : context.pending) {
      if (fusion && conjunct == fusion->consumedPredicate) {
        if (fusion->remainingPredicate != nullptr) {
          blocked.push_back(fusion->remainingPredicate);
        }
        continue;
      }
      // A predicate that every row satisfies selects nothing away.
      if (constantRank != nullptr &&
          holdsAtRankOne(conjunct, constantRank, builder().functionNames())) {
        continue;
      }
      const auto& columns = conjunct->columns();
      // Exclude constant predicates: `isSubset` is vacuously true on an
      // empty set, so without `!empty()` a constant would push.
      if (!columns.empty() && columns.isSubset(partitionKeyColumns)) {
        pushable.push_back(conjunct);
      } else {
        blocked.push_back(conjunct);
      }
    }
    blockNondeterministic(pushable, blocked);

    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);

    PushdownContext childContext;
    childContext.pending = std::move(pushable);
    childContext.required = context.required;
    childContext.required.unionColumns(node->partitionKeys());
    childContext.required.unionColumns(node->orderKeys());
    childContext.required.unionColumns(blocked);
    // Rows with a NULL partition key form partitions of their own, and every
    // output row of those partitions holds that NULL.
    childContext.nonNullColumns = context.nonNullColumns;
    childContext.nonNullColumns.intersect(partitionKeyColumns);

    NodeCP result;
    if (fusion) {
      result = specializeRanking(
          node, *fusion, childContext, outputsKept, std::move(blocked));
    } else {
      result = pruneWindowFunctions(
          node, childContext, outputsKept, std::move(blocked));
    }
    return finishSimplifiedNode(
        {result, std::move(childContext.outputSubstitutions)}, {}, context);
  }

  // Replaces a single-ranking-function Window with the node that computes it
  // most cheaply: RowNumber when the rows need no ordering, TopNRowNumber when
  // an ordered ranking is bounded by a per-partition limit.
  NodeCP specializeRanking(
      const Window* node,
      const RankFusion& fusion,
      PushdownContext& childContext,
      const PlanObjectSet& outputsKept,
      ExprVector blocked) {
    childContext.required.unionColumns(childContext.pending);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = dropColumnsForWindow(
        rewrite(node->input(), childContext), childContext);
    applyOutputSubstitutions(childContext, blocked);

    // Emit the rank column only when a consumer above still needs it.
    const ColumnCP rankColumn =
        outputsKept.contains(fusion.rankColumn) ? fusion.rankColumn : nullptr;
    ColumnVector newOutputColumns;
    appendAll(newOutputColumns, newInput->outputColumns());
    if (rankColumn != nullptr) {
      newOutputColumns.push_back(rankColumn);
    }

    // With no rank column to emit and no limit, the node would pass its input
    // through unchanged.
    if (node->orderKeys().empty() && rankColumn == nullptr &&
        !fusion.limit.has_value()) {
      return maybeWrapFilter(newInput, std::move(blocked));
    }

    auto simplified = fusion.limit.has_value()
        ? nodeSimplifier_.make(
              TopNRowNumber::Key{
                  newInput,
                  fusion.rankFunction,
                  node->partitionKeys(),
                  node->orderKeys(),
                  node->orderTypes(),
                  *fusion.limit,
                  rankColumn,
                  std::move(newOutputColumns)},
              {newInput, std::move(childContext.outputSubstitutions)})
        : nodeSimplifier_.make(
              RowNumber::Key{
                  newInput,
                  node->partitionKeys(),
                  std::nullopt,
                  rankColumn,
                  std::move(newOutputColumns)},
              {newInput, std::move(childContext.outputSubstitutions)});
    childContext.outputSubstitutions = std::move(simplified.substitutions);
    blocked = childContext.outputSubstitutions.apply(blocked, exprs_);
    return maybeWrapFilter(simplified.node, std::move(blocked));
  }

  // Velox's window operators pass every input column through, so a column
  // that only an operator below the window reads would ride through it. Adds
  // a Project that keeps just the columns the window and its consumers read
  // when the input has more.
  NodeCP dropColumnsForWindow(NodeCP input, PushdownContext& context) {
    const PlanObjectSet required = replacementColumns(
        exprs_, context.required, context.outputSubstitutions);
    ExprVector exprs;
    ColumnVector columns;
    for (ColumnCP column : input->outputColumns()) {
      if (required.contains(column)) {
        exprs.push_back(column);
        columns.push_back(column);
      }
    }

    if (columns.size() == input->outputColumns().size()) {
      return input;
    }

    NodeCP project =
        builder().make<Project>({input, std::move(exprs), std::move(columns)});
    context.outputSubstitutions.retainVisible(project->outputColumns(), exprs_);
    return project;
  }

  // Keeps the Window, dropping the functions no consumer reads.
  NodeCP pruneWindowFunctions(
      const Window* node,
      PushdownContext& childContext,
      const PlanObjectSet& outputsKept,
      ExprVector blocked) {
    const size_t numInputColumns = node->input()->outputColumns().size();

    WindowFunctions survivingFunctions;
    ColumnVector survivingFunctionOutputs;
    for (size_t i = 0; i < node->functions().size(); ++i) {
      const ColumnCP funcOutput = node->outputColumns()[numInputColumns + i];
      if (outputsKept.contains(funcOutput)) {
        survivingFunctions.push_back(node->functions()[i]);
        survivingFunctionOutputs.push_back(funcOutput);
      }
    }
    for (const auto& function : survivingFunctions) {
      childContext.required.unionColumns(function.call);
      if (function.frame.startValue != nullptr) {
        childContext.required.unionColumns(function.frame.startValue);
      }
      if (function.frame.endValue != nullptr) {
        childContext.required.unionColumns(function.frame.endValue);
      }
    }
    childContext.required.unionColumns(childContext.pending);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = dropColumnsForWindow(
        rewrite(node->input(), childContext), childContext);

    // With every function pruned the node computes nothing and emits its
    // input's columns.
    if (survivingFunctions.empty()) {
      applyOutputSubstitutions(childContext, blocked);
      return maybeWrapFilter(newInput, std::move(blocked));
    }

    ColumnVector newOutputColumns;
    newOutputColumns.reserve(numInputColumns + survivingFunctions.size());
    appendAll(newOutputColumns, newInput->outputColumns());
    appendAll(newOutputColumns, survivingFunctionOutputs);

    auto simplified = nodeSimplifier_.make(
        Window::Key{
            newInput,
            std::move(survivingFunctions),
            node->partitionKeys(),
            node->orderKeys(),
            node->orderTypes(),
            std::move(newOutputColumns)},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(blocked), childContext);
  }

  NodeCP rewriteApply(const Apply* /*node*/, PushdownContext& /*context*/)
      override {
    VELOX_FAIL("Apply must be removed by decorrelate before pushdown");
  }

  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      PushdownContext& context) override {
    return blockAt(context, [&](PushdownContext& empty) {
      ColumnVector outputColumns;
      ColumnVector sourceColumns;
      for (size_t i = 0; i < node->outputColumns().size(); ++i) {
        if (empty.required.contains(node->outputColumns()[i])) {
          outputColumns.push_back(node->outputColumns()[i]);
          sourceColumns.push_back(node->sourceColumns()[i]);
        }
      }

      empty.required = PlanObjectSet::fromObjects(sourceColumns);
      empty.requiredAbove = empty.required;
      NodeCP newInput = rewrite(node->input(), empty);
      auto simplified = nodeSimplifier_.make(
          EnforceSingleRow::Key{newInput, std::move(outputColumns)},
          {newInput,
           std::move(empty.outputSubstitutions),
           std::move(sourceColumns)});
      empty.outputSubstitutions = std::move(simplified.substitutions);
      return simplified.node;
    });
  }

  // AssignUniqueId: conjuncts on input columns push freely — the id
  // sequence regenerates over the surviving rows. Conjuncts on the id
  // column stay above. If no consumer reads the id, drop the node.
  NodeCP rewriteAssignUniqueId(
      const AssignUniqueId* node,
      PushdownContext& context) override {
    const PlanObjectSet idColumn = PlanObjectSet::single(node->idColumn());
    auto [pushable, blocked] = partition(context.pending, idColumn);
    PlanObjectSet outputsKept = context.required;
    outputsKept.unionColumns(blocked);

    PushdownContext childContext =
        makeChildContext(std::move(pushable), context);
    // Each input row passes through on its own, with a unique id added.
    childContext.nonNullColumns = context.nonNullColumns;
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    if (!outputsKept.contains(node->idColumn())) {
      NodeCP newInput = rewrite(node->input(), childContext);
      applyOutputSubstitutions(childContext, blocked);
      return finishSimplifiedNode(
          {maybeWrapFilter(newInput, std::move(blocked)),
           std::move(childContext.outputSubstitutions)},
          {},
          context);
    }

    NodeCP newInput = rewrite(node->input(), childContext);
    auto simplified = nodeSimplifier_.make(
        AssignUniqueId::Key{newInput, node->idColumn()},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(blocked), context);
  }

  NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      PushdownContext& context) override {
    // Non-NULL hints stay above: a row they would drop can be the duplicate
    // this node must fail on.
    PushdownContext childContext;
    childContext.required = context.required;
    childContext.required.unionColumns(context.pending);
    childContext.required.unionColumns(node->distinctKeys());
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);
    ExprVector pending = std::move(context.pending);
    auto simplified = nodeSimplifier_.make(
        EnforceDistinct::Key{
            newInput, node->distinctKeys(), node->errorMessage()},
        {newInput, std::move(childContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(pending), context);
  }

  // A write is the plan root: nothing consumes its row-count output, so the
  // only demand on its input is the columns the write's value expressions read.
  // Recurse with exactly those required, or the input scan gets pruned to
  // nothing. There is no pending predicate to route — a root carries none.
  NodeCP rewriteTableWrite(const TableWrite* node, PushdownContext& context)
      override {
    VELOX_DCHECK(context.pending.empty());
    PushdownContext child;
    child.required.unionColumns(node->columnExprs());
    child.requiredAbove = child.required;
    child.nonNullColumns = context.nonNullColumns;
    // A delete reads no column values: the connector's handle says which rows
    // go. An insert writes its input columns, so only a delete can take a
    // wider input.
    child.consumerDropsExtraColumns =
        node->kind() == connector::WriteKind::kDelete;
    NodeCP newInput = rewrite(node->input(), child);
    auto simplified = nodeSimplifier_.make(
        TableWrite::Key{
            newInput, node->table(), node->kind(), node->columnExprs()},
        {newInput, std::move(child.outputSubstitutions)},
        node->outputColumns());
    context.outputSubstitutions = std::move(simplified.substitutions);
    return simplified.node;
  }

  // Pending predicates must remain above the fixed point because pushing them
  // into either branch can change the rows accumulated across iterations.
  // TODO: Push iteration-invariant predicates into the anchor and step.
  NodeCP rewriteFixedPoint(const FixedPoint* node, PushdownContext& context)
      override {
    // Filtering seed rows is sound only when `p(step(x))` implies `p(x)`;
    // otherwise, a rejected seed can still produce matching descendants.
    // Record the step and convergence paths before the anchor's scan
    // negotiates; the descent reaches them only afterwards.
    access_.addSubtree(*node->step());
    access_.addSubtree(*node->convergence());
    for (size_t i = 0; i < node->outputColumns().size(); ++i) {
      access_.addProducing(node->sourceColumns()[i], node->outputColumns()[i]);
    }

    PushdownContext anchorContext;
    anchorContext.required.unionObjects(node->sourceColumns());
    anchorContext.requiredAbove = anchorContext.required;
    NodeCP newAnchor = rewrite(node->anchor(), anchorContext);

    // Required columns use step output identities because they may differ from
    // anchor output identities. The working table carries no outer null
    // guarantees, so `nonNullColumns` remains empty.
    PushdownContext stepContext;
    stepContext.required.unionObjects(node->step()->outputColumns());
    stepContext.requiredAbove = stepContext.required;
    NodeCP newStep = rewrite(node->step(), stepContext);

    // Convergence is an independent Boolean subplan. Seed only its root output;
    // each operator adds its expression dependencies while WorkingTable keeps
    // the complete recursive-state schema.
    PushdownContext convergenceContext;
    convergenceContext.required.unionObjects(
        node->convergence()->outputColumns());
    convergenceContext.requiredAbove = convergenceContext.required;
    NodeCP newConvergence = rewrite(node->convergence(), convergenceContext);

    auto simplified = nodeSimplifier_.make(
        FixedPoint::Key{
            .anchor = node->anchor(),
            .step = node->step(),
            .convergence = node->convergence(),
            .name = node->name(),
            .outputColumns = node->outputColumns(),
            .sourceColumns = node->sourceColumns(),
            .maxIterations = node->maxIterations(),
            .recursiveNumDrivers = node->recursiveNumDrivers(),
        },
        {newAnchor, std::move(anchorContext.outputSubstitutions)},
        {newStep, std::move(stepContext.outputSubstitutions)},
        {newConvergence, std::move(convergenceContext.outputSubstitutions)});
    return finishSimplifiedNode(
        std::move(simplified), std::move(context.pending), context);
  }

  NodeCP rewriteWorkingTable(const WorkingTable* node, PushdownContext& context)
      override {
    return maybeWrapFilter(node, std::move(context.pending));
  }

 private:
  // Builds a child `PushdownContext` whose required column set is
  // `parent.required` plus the columns referenced by any conjunct in
  // `pending`. Callers augment the result with the node's own column
  // reads before recursing. Non-NULL hints stay above; a caller whose node
  // computes the output rows of each input row from that row alone passes them
  // on.
  PushdownContext makeChildContext(
      ExprVector pending,
      const PushdownContext& parent) {
    PushdownContext child;
    child.pending = std::move(pending);
    child.required = parent.required;
    child.required.unionColumns(child.pending);
    // Conservative: callers that push a fusable mark down refine this.
    child.requiredAbove = parent.required;
    return child;
  }

  void applyOutputSubstitutions(
      const PlanSubstitutions& substitutions,
      ExprCP& expression) {
    expression = substitutions.apply(expression, exprs_);
  }

  void applyOutputSubstitutions(
      const PlanSubstitutions& substitutions,
      ExprVector& expressions) {
    expressions = substitutions.apply(expressions, exprs_);
  }

  void applyOutputSubstitutions(
      const PlanSubstitutions& substitutions,
      AggregateCallVector& aggregates) {
    aggregates = substitutions.apply(aggregates, exprs_, builder());
  }

  void applyOutputSubstitutions(
      const PlanSubstitutions& substitutions,
      WindowFunctions& functions) {
    for (auto& function : functions) {
      applyOutputSubstitutions(substitutions, function.call);
      applyOutputSubstitutions(substitutions, function.frame.startValue);
      applyOutputSubstitutions(substitutions, function.frame.endValue);
    }
  }

  template <typename... Rewritable>
  void applyOutputSubstitutions(
      const PushdownContext& child,
      Rewritable&... expressions) {
    if (child.outputSubstitutions.empty()) {
      return;
    }
    (applyOutputSubstitutions(child.outputSubstitutions, expressions), ...);
  }

  // Invokes `recurse` with an empty-pending context — letting the
  // subtree rewrite under its own clean pending state, while still
  // propagating `required` — then wraps the caller's `context.pending`
  // as a Filter above the recursed result. Non-NULL hints stay above as
  // well; a caller whose node computes the output rows of each input row from
  // that row alone restores them in `recurse`.
  template <typename Recurse>
  NodeCP blockAt(PushdownContext& context, Recurse recurse) {
    PushdownContext empty;
    empty.required = context.required;
    empty.required.unionColumns(context.pending);
    // Blocked conjuncts wrap as a Filter above the recursed subtree, so
    // everything is "above" it.
    empty.requiredAbove = empty.required;
    NodeCP recursed = recurse(empty);
    ExprVector blocked = std::move(context.pending);
    applyOutputSubstitutions(empty, blocked);
    return finishSimplifiedNode(
        {maybeWrapFilter(recursed, std::move(blocked)),
         std::move(empty.outputSubstitutions)},
        {},
        context);
  }

  // Routes each pending conjunct to either the input (pushable) or a
  // Filter above the rebuilt node (blocked), based on whether the
  // conjunct touches any column in `outputOnlyColumns` — the set of
  // columns the node produces that don't exist on its input. `rebuild`
  // builds a new node from a rewritten input. Child required carries
  // every input-side column read by blocked conjuncts or pushable.
  template <typename SingleInputNode, typename Rebuild>
  NodeCP splitOnOutputColumns(
      const SingleInputNode* node,
      const PushdownContext& parent,
      const PlanObjectSet& outputOnlyColumns,
      Rebuild rebuild) {
    auto [pushable, blocked] = partition(parent.pending, outputOnlyColumns);
    PushdownContext childContext =
        makeChildContext(std::move(pushable), parent);
    childContext.required.unionColumns(blocked);
    childContext.requiredAbove = childContext.required;
    NodeCP newInput = rewrite(node->input(), childContext);
    NodeCP newNode = (newInput == node->input()) ? static_cast<NodeCP>(node)
                                                 : rebuild(newInput);
    return maybeWrapFilter(newNode, std::move(blocked));
  }

  NodeCP maybeWrapFilter(NodeCP body, ExprVector conjuncts) {
    if (conjuncts.empty()) {
      return body;
    }
    return builder().make<Filter>({body, std::move(conjuncts)});
  }

  // Builds an empty `Values` node with the same output schema as `node`.
  // Used to short-circuit a subtree when pushdown discovers a predicate
  // that folds to literal `false`.
  NodeCP makeEmptyValues(NodeCP node) {
    return builder().makeEmptyValues(node->outputColumns());
  }

  // Substituting a cross-side equality by the join's own equi-pair maps both
  // of its arguments to the same expression, e.g. `t.x = u.x` becomes
  // `t.x = t.x`. That says only that the key is not null, which an equi-join
  // already guarantees, so it adds nothing.
  bool isSelfEquality(ExprCP expr) {
    if (!expr->is(PlanType::kCallExpr)) {
      return false;
    }
    const auto* call = expr->as<Call>();
    return call->name() == builder().functionNames().equality &&
        call->args().size() == 2 && call->args()[0] == call->args()[1];
  }

  // Derives new pending conjuncts via each cross-side
  // `Column == Column` equi-pair: a conjunct that references one side
  // gets duplicated with that side's column substituted for the other
  // side's equivalent column, exposing a push to the other side.
  // Returns true if any derived conjunct simplifies to literal `false`,
  // signalling the caller to short-circuit the join to an empty result.
  bool deriveTransitive(
      JoinCP node,
      const PlanObjectSet& leftColumns,
      const PlanObjectSet& rightColumns,
      ExprVector& pending) {
    auto [leftKeys, rightKeys] = JoinPredicatePlacement::equiColumnPairs(
        node, leftColumns, rightColumns, {});
    if (leftKeys.empty()) {
      return false;
    }
    ExprVector derived;
    if (deriveFilters(
            pending, rightKeys, leftKeys, node->nullAsValue(), derived) ||
        deriveFilters(
            pending, leftKeys, rightKeys, node->nullAsValue(), derived)) {
      return true;
    }
    appendDistinct(pending, derived);
    return false;
  }

  folly::F14FastMap<JoinCP, CollectedJoinFilters> collectedJoinFilters_;

  ExprFactory exprs_;
  velox::core::ExpressionEvaluator& evaluator_;
  const OptimizerSession& session_;
  const PushdownAndPrunePass::ConnectorPushdown connectorPushdown_;

  // Which parts of each column the plan reads, accumulated as the rewrite
  // descends.
  ColumnAccess access_;

  ExprSimplifier simplifier_;
  NodeSimplifier nodeSimplifier_;

  // Outcome of one negotiation with the connector.
  struct Negotiated {
    const TableAccessHandle* handle;
    // The conjuncts offered, and of those the ones the connector rejected,
    // which the plan applies itself.
    ExprVector filters;
    ExprVector rejected;
  };

  // One negotiation per base table, by base-table id.
  folly::F14FastMap<int32_t, Negotiated> negotiatedByBaseTableId_;
};

} // namespace

PushdownAndPrunePass::Result PushdownAndPrunePass::run(
    NodeCP root,
    const ColumnVector& outputColumns,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator,
    const OptimizerSession& session,
    ConnectorPushdown connectorPushdown) {
  Pushdown pass{builder, evaluator, session, connectorPushdown, outputColumns};
  PushdownContext context;
  context.required = PlanObjectSet::fromObjects(outputColumns);
  context.requiredAbove = context.required;
  NodeCP result = pass.rewrite(root, context);

  ExprSimplifier simplifier{builder, evaluator};
  NodeSimplifier nodeSimplifier{builder, simplifier};
  ColumnVector rewrittenOutputs = outputColumns;
  nodeSimplifier.restoreOutputLayout(
      result, rewrittenOutputs, context.outputSubstitutions);
  return {result, std::move(rewrittenOutputs)};
}

} // namespace facebook::axiom::optimizer::v2
