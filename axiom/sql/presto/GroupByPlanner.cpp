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

#include "axiom/sql/presto/GroupByPlanner.h"
#include <set>
#include "axiom/sql/presto/ColumnsExpansion.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/SortProjection.h"
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "folly/container/F14Set.h"
#include "velox/common/base/BitUtil.h"
#include "velox/exec/Aggregate.h"

namespace axiom::sql::presto {

using namespace facebook::velox;

namespace lp = facebook::axiom::logical_plan;

namespace {

template <typename V>
using ExprMap =
    folly::F14FastMap<core::ExprPtr, V, core::IExprHash, core::IExprEqual>;

// Set of aggregate expressions for deduplication. Uses IExpr equality which
// includes AggregateCallExpr options (DISTINCT, FILTER, ORDER BY).
using AggregateExprSet =
    folly::F14FastSet<core::ExprPtr, core::IExprHash, core::IExprEqual>;

// Maps aggregate expressions to their rewritten output columns.
using AggregateExprMap = folly::
    F14FastMap<core::ExprPtr, core::ExprPtr, core::IExprHash, core::IExprEqual>;

// Rewrites a window function expression: replaces column references in the
// function arguments, PARTITION BY keys, and ORDER BY keys using the provided
// rewrite function. Constructs a new WindowCallExpr directly.
lp::ExprApi rewriteWindowExpr(
    const lp::ExprApi& item,
    const std::function<core::ExprPtr(const core::ExprPtr&)>& rewriteIExpr) {
  VELOX_CHECK(item.expr()->is(core::IExpr::Kind::kWindow));
  auto* window = item.expr()->as<core::WindowCallExpr>();

  // Rewrite function arguments.
  std::vector<core::ExprPtr> newInputs;
  for (const auto& input : window->inputs()) {
    newInputs.push_back(rewriteIExpr(input));
  }

  // Rewrite partition keys.
  std::vector<core::ExprPtr> newPartitionKeys;
  for (const auto& key : window->partitionKeys()) {
    newPartitionKeys.push_back(rewriteIExpr(key));
  }

  // Rewrite order by keys.
  std::vector<core::SortKey> newOrderByKeys;
  for (const auto& key : window->orderByKeys()) {
    newOrderByKeys.push_back(
        {rewriteIExpr(key.expr), key.ascending, key.nullsFirst});
  }

  return lp::ExprApi(
      std::make_shared<core::WindowCallExpr>(
          window->name(),
          std::move(newInputs),
          std::move(newPartitionKeys),
          std::move(newOrderByKeys),
          window->frame(),
          window->isIgnoreNulls()),
      item.name());
}

// Given an expression, and pairs of search-and-replace sub-expressions,
// produces a new expression with sub-expressions replaced. Uses keyReplacements
// for grouping keys and aggregateReplacements for aggregates. If
// 'onUnreplacedLeaf' is provided, invokes the callback for any
// FieldAccessExpr that is not in the replacement map and whose children were
// not replaced either. Used to detect invalid column references in HAVING.
core::ExprPtr replaceInputs(
    const core::ExprPtr& expr,
    const ExprMap<core::ExprPtr>& keyReplacements,
    const AggregateExprMap& aggregateReplacements,
    const std::function<void(const core::FieldAccessExpr&)>& onUnreplacedLeaf =
        nullptr) {
  // First try grouping keys.
  auto keyIt = keyReplacements.find(expr);
  if (keyIt != keyReplacements.end()) {
    return keyIt->second;
  }

  // Then try aggregates.
  auto aggIt = aggregateReplacements.find(expr);
  if (aggIt != aggregateReplacements.end()) {
    return aggIt->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool hasNewInput = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(
        input, keyReplacements, aggregateReplacements, onUnreplacedLeaf);
    if (newInput.get() != input.get()) {
      hasNewInput = true;
    }
    newInputs.push_back(newInput);
  }

  if (!hasNewInput && onUnreplacedLeaf &&
      expr->is(core::IExpr::Kind::kFieldAccess)) {
    onUnreplacedLeaf(*expr->as<core::FieldAccessExpr>());
  }

  if (hasNewInput) {
    return expr->replaceInputs(std::move(newInputs));
  }

  return expr;
}

// Walks the expression tree looking for aggregate function calls and appending
// these calls to 'aggregates' after deduplication through 'aggregateSet'.
// Window functions (kWindow) are not treated as aggregates — their children
// and window spec keys are recursed into to find nested aggregates.
void findAggregates(
    const core::ExprPtr& expr,
    std::vector<lp::ExprApi>& aggregates,
    AggregateExprSet& aggregateSet) {
  switch (expr->kind()) {
    case core::IExpr::Kind::kInput:
      return;
    case core::IExpr::Kind::kFieldAccess:
      return;
    case core::IExpr::Kind::kAggregate: {
      if (aggregateSet.emplace(expr).second) {
        aggregates.emplace_back(expr);
      }
      return;
    }
    case core::IExpr::Kind::kCall: {
      // Plain CallExpr that is registered as an aggregate (no DISTINCT/FILTER/
      // ORDER BY). This happens for simple aggregates like count(*).
      if (exec::getAggregateFunctionEntry(expr->as<core::CallExpr>()->name())) {
        if (aggregateSet.emplace(expr).second) {
          aggregates.emplace_back(expr);
        }
      } else {
        for (const auto& input : expr->inputs()) {
          findAggregates(input, aggregates, aggregateSet);
        }
      }
      return;
    }
    case core::IExpr::Kind::kCast:
      findAggregates(
          expr->as<core::CastExpr>()->input(), aggregates, aggregateSet);
      return;
    case core::IExpr::Kind::kConstant:
      return;
    case core::IExpr::Kind::kLambda:
      // TODO: Reject aggregates in lambda expressions.
      return;
    case core::IExpr::Kind::kSubquery:
      // TODO: Handle aggregates in subqueries.
      return;
    case core::IExpr::Kind::kWindow: {
      // Recurse into function inputs, partition keys, and order by keys
      // to find nested aggregates.
      auto* window = expr->as<core::WindowCallExpr>();
      for (const auto& input : window->inputs()) {
        findAggregates(input, aggregates, aggregateSet);
      }
      for (const auto& key : window->partitionKeys()) {
        findAggregates(key, aggregates, aggregateSet);
      }
      for (const auto& key : window->orderByKeys()) {
        findAggregates(key.expr, aggregates, aggregateSet);
      }
      return;
    }
    case core::IExpr::Kind::kConcat:
      for (const auto& input : expr->inputs()) {
        findAggregates(input, aggregates, aggregateSet);
      }
      return;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported expression kind in findAggregates: {}",
          expr->toString());
  }
}

// Analyzes the expression to find out whether there are any aggregate function
// calls and to verify that aggregate calls are not nested, e.g. sum(count(x))
// is not allowed.
class ExprAnalyzer : public DefaultTraversalVisitor {
 public:
  bool hasAggregate() const {
    return numAggregates_ > 0;
  }

 protected:
  void visitExistsPredicate(ExistsPredicate* node) override {
    // Aggregate function calls within a subquery do not count.
  }

  void visitFunctionCall(FunctionCall* node) override {
    const auto& name = node->name()->suffix();
    if (exec::getAggregateFunctionEntry(name) && node->window() == nullptr) {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          !aggregateName_.has_value(),
          node->location(),
          name,
          "Cannot nest aggregations inside aggregation: {}",
          aggregateName_.value());

      aggregateName_ = name;
      ++numAggregates_;
    }

    DefaultTraversalVisitor::visitFunctionCall(node);

    aggregateName_.reset();
  }

  void visitSubqueryExpression(SubqueryExpression* node) override {
    // Aggregate function calls within a subquery do not count.
  }

 private:
  size_t numAggregates_{0};
  std::optional<std::string> aggregateName_;
};

// ROLLUP(a,b,c) -> {(a,b,c), (a,b), (a), ()}
std::vector<std::vector<lp::ExprApi>> expandRollup(
    const std::vector<lp::ExprApi>& expressions) {
  std::vector<std::vector<lp::ExprApi>> groupingSets;
  groupingSets.reserve(expressions.size() + 1);

  for (size_t i = expressions.size(); i > 0; --i) {
    std::vector<lp::ExprApi> groupingSet(
        expressions.begin(), expressions.begin() + i);
    groupingSets.push_back(std::move(groupingSet));
  }
  groupingSets.emplace_back();
  return groupingSets;
}

// CUBE(a,b) -> {(a,b), (a), (b), ()}
std::vector<std::vector<lp::ExprApi>> expandCube(
    const std::vector<lp::ExprApi>& expressions,
    NodeLocation location) {
  const size_t n = expressions.size();
  // Presto limits CUBE to 30 columns (2^30 possible grouping sets).
  // https://github.com/prestodb/presto/issues/27096
  AXIOM_PRESTO_SEMANTIC_CHECK(
      n <= 30, location, "CUBE", "CUBE supports at most 30 columns, got {}", n);
  const size_t numGroupingSets = 1ULL << n;
  std::vector<std::vector<lp::ExprApi>> groupingSets;
  groupingSets.reserve(numGroupingSets);

  for (size_t mask = numGroupingSets; mask > 0; --mask) {
    size_t bits = mask - 1;
    std::vector<lp::ExprApi> groupingSet;
    for (size_t i = 0; i < n; ++i) {
      if (bits & (1ULL << (n - 1 - i))) {
        groupingSet.push_back(expressions[i]);
      }
    }
    groupingSets.push_back(std::move(groupingSet));
  }
  return groupingSets;
}

// Computes Cartesian product of accumulatedSets and newSets.
std::vector<std::vector<lp::ExprApi>> crossProductGroupingSets(
    const std::vector<std::vector<lp::ExprApi>>& accumulatedSets,
    std::vector<std::vector<lp::ExprApi>> newSets) {
  if (accumulatedSets.empty()) {
    return newSets;
  }
  if (newSets.empty()) {
    return accumulatedSets;
  }
  std::vector<std::vector<lp::ExprApi>> combined;
  combined.reserve(accumulatedSets.size() * newSets.size());
  for (const auto& base : accumulatedSets) {
    for (const auto& addition : newSets) {
      std::vector<lp::ExprApi> merged = base;
      merged.insert(merged.end(), addition.begin(), addition.end());
      combined.push_back(std::move(merged));
    }
  }
  return combined;
}

// Removes duplicate grouping sets from 'groupingSetsIndices'. Two sets are
// duplicates if they contain the same key indices (order-insensitive).
void deduplicateGroupingSets(
    std::vector<std::vector<int32_t>>& groupingSetsIndices) {
  std::set<std::vector<int32_t>> seen;
  size_t next = 0;
  for (size_t i = 0; i < groupingSetsIndices.size(); ++i) {
    std::sort(groupingSetsIndices[i].begin(), groupingSetsIndices[i].end());
    if (seen.insert(groupingSetsIndices[i]).second) {
      if (next != i) {
        groupingSetsIndices[next] = std::move(groupingSetsIndices[i]);
      }
      ++next;
    }
  }
  groupingSetsIndices.resize(next);
}

} // namespace

void GroupByPlanner::plan(
    const std::vector<GroupingElementPtr>& groupingElements,
    bool distinct,
    const std::vector<lp::ExprApi>& selectExprs,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) && {
  // Expand ROLLUP, CUBE, GROUPING SETS into a list of grouping sets, then
  // extract deduplicated grouping keys and per-set index vectors.
  // Populates: groupingSets_, groupingKeys_, groupingSetsIndices_.
  groupingSets_ = expandGroupingSets(groupingElements, selectExprs);
  deduplicateGroupingKeys();
  // GROUP BY DISTINCT removes duplicate grouping sets after expansion
  // (order-insensitive comparison).
  if (distinct) {
    deduplicateGroupingSets(groupingSetsIndices_);
  }

  // Walk SELECT, HAVING, and ORDER BY expressions to collect aggregate
  // function calls, then add the Aggregate plan node.
  // Populates: aggregates_, projections_, filter_,
  //   sortingKeyExprs_, outputNames_.
  collectAggregates(selectExprs, having, orderBy);

  addAggregate(groupingSetsIndices_.size() > 1);

  // Rewrite filter_, projections_, and sortingKeyExprs_ to reference the
  // aggregate output columns instead of the original input expressions.
  // Populates: flatInputs_.
  // Mutates: filter_, projections_, sortingKeyExprs_.
  rewritePostAggregateExprs();

  // Resolve sorting key ordinals before projecting: ORDER BY expressions
  // that are not in the SELECT list are appended to projections_ so they
  // are included in the project node. Must happen before builder_->project().
  auto sortingKeyOrdinals = resolveSortOrdinals(orderBy);

  // Apply HAVING filter, then project.
  if (filter_.has_value()) {
    builder_->filter(filter_.value());
  }

  if (!isIdentityProjection()) {
    builder_->project(projections_);
  }

  // Sort and drop any extra projections added for ORDER BY.
  if (orderBy != nullptr && !sortingKeyOrdinals.empty()) {
    SortProjection::sortAndTrim(
        *builder_,
        orderBy->sortItems(),
        sortingKeyOrdinals,
        selectExprs.size());
  }
}

bool GroupByPlanner::tryPlanGlobalAgg(
    const std::vector<SelectItemPtr>& selectItems,
    const ExpressionPtr& having) && {
  for (const auto& item : selectItems) {
    if (item->is(NodeType::kAllColumns) || item->is(NodeType::kSelectColumns)) {
      return false;
    }
  }

  bool hasAggregate = false;
  for (const auto& item : selectItems) {
    VELOX_CHECK(item->is(NodeType::kSingleColumn));
    auto* singleColumn = item->as<SingleColumn>();

    ExprAnalyzer exprAnalyzer;
    singleColumn->expression()->accept(&exprAnalyzer);

    if (exprAnalyzer.hasAggregate()) {
      hasAggregate = true;
      break;
    }
  }

  if (!hasAggregate) {
    return false;
  }

  // Resolve SELECT items into ExprApi.
  std::vector<lp::ExprApi> selectExprs;
  for (const auto& item : selectItems) {
    auto* singleColumn = item->as<SingleColumn>();
    auto expr = exprPlanner_.toExpr(singleColumn->expression());
    if (singleColumn->alias() != nullptr) {
      expr = expr.as(canonicalizeIdentifier(*singleColumn->alias()));
    }
    selectExprs.push_back(std::move(expr));
  }

  std::move(*this).plan(
      {}, /*distinct=*/false, selectExprs, having, /*orderBy=*/nullptr);
  return true;
}

std::vector<std::vector<lp::ExprApi>> GroupByPlanner::expandGroupingSets(
    const std::vector<GroupingElementPtr>& groupingElements,
    const std::vector<lp::ExprApi>& selectExprs) {
  std::vector<std::vector<lp::ExprApi>> groupingSets;
  for (const auto& element : groupingElements) {
    switch (element->type()) {
      case NodeType::kSimpleGroupBy: {
        const auto* simple = element->as<SimpleGroupBy>();
        auto exprs = resolveWithCache(simple->expressions(), selectExprs);
        groupingSets =
            crossProductGroupingSets(groupingSets, {std::move(exprs)});
        break;
      }

      case NodeType::kRollup: {
        const auto* rollup = element->as<Rollup>();
        auto rollupSets =
            expandRollup(resolveWithCache(rollup->expressions(), selectExprs));
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(rollupSets));
        break;
      }

      case NodeType::kCube: {
        const auto* cube = element->as<Cube>();
        auto cubeSets = expandCube(
            resolveWithCache(cube->expressions(), selectExprs),
            element->location());
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(cubeSets));
        break;
      }

      case NodeType::kGroupingSets: {
        const auto* groupingSetsNode = element->as<GroupingSets>();
        std::vector<std::vector<lp::ExprApi>> explicitSets;
        explicitSets.reserve(groupingSetsNode->sets().size());
        for (const auto& set : groupingSetsNode->sets()) {
          explicitSets.push_back(resolveWithCache(set, selectExprs));
        }
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(explicitSets));
        break;
      }

      default:
        AXIOM_PRESTO_SYNTAX_FAIL(
            element->location(),
            std::nullopt,
            "Grouping element type not supported: {}",
            NodeTypeName::toName(element->type()));
    }
  }
  return groupingSets;
}

void GroupByPlanner::deduplicateGroupingKeys() {
  core::ExprMap<int32_t> exprToIndex;
  for (const auto& groupingSet : groupingSets_) {
    std::vector<int32_t> indices;
    indices.reserve(groupingSet.size());
    for (const auto& expr : groupingSet) {
      int32_t idx = static_cast<int32_t>(groupingKeys_.size());
      auto [it, inserted] = exprToIndex.try_emplace(expr.expr(), idx);
      if (inserted) {
        groupingKeys_.push_back(expr);
      }
      // Deduplicate keys within a single grouping set: GROUP BY (a, a)
      // collapses to (a).
      if (std::find(indices.begin(), indices.end(), it->second) ==
          indices.end()) {
        indices.push_back(it->second);
      }
    }
    groupingSetsIndices_.push_back(std::move(indices));
  }
}

void GroupByPlanner::collectAggregates(
    const std::vector<lp::ExprApi>& selectExprs,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) {
  // Go over SELECT expressions and figure out for each: whether a grouping
  // key, a function of one or more grouping keys, a constant, an aggregate
  // or a function over one or more aggregates and possibly grouping keys.
  //
  // Collect all individual aggregates. A single select item 'sum(x) /
  // sum(y)' will produce 2 aggregates: sum(x), sum(y).

  AggregateExprSet aggregateSet;
  for (const auto& selectExpr : selectExprs) {
    findAggregates(selectExpr.expr(), aggregates_, aggregateSet);

    if (!aggregates_.empty() &&
        aggregates_.back().expr().get() == selectExpr.expr().get()) {
      // Preserve the alias.
      if (selectExpr.alias().has_value()) {
        aggregates_.back() = aggregates_.back().as(selectExpr.alias().value());
      }
    }

    projections_.emplace_back(selectExpr);
  }

  if (having != nullptr) {
    lp::ExprApi expr = exprPlanner_.toExpr(having);
    findAggregates(expr.expr(), aggregates_, aggregateSet);
    filter_ = expr;
  }

  if (orderBy != nullptr) {
    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = exprPlanner_.toExpr(item->sortKey());
      findAggregates(expr.expr(), aggregates_, aggregateSet);
      sortingKeyExprs_.emplace_back(expr);
    }
  }
}

void GroupByPlanner::addAggregate(bool useGroupingSets) {
  std::vector<lp::ExprApi> aggregateExprs;
  aggregateExprs.reserve(aggregates_.size());
  for (const auto& agg : aggregates_) {
    aggregateExprs.push_back(agg);
  }

  if (useGroupingSets) {
    builder_->aggregate(
        groupingKeys_,
        groupingSetsIndices_,
        aggregateExprs,
        "$grouping_set_id");
  } else {
    builder_->aggregate(groupingKeys_, aggregateExprs);
  }

  auto outputColumns = builder_->findOrAssignOutputNames();
  outputNames_.clear();
  outputNames_.reserve(outputColumns.size());
  for (const auto& column : outputColumns) {
    VELOX_CHECK(
        !column.alias.has_value(),
        "Unexpected ambiguous column after aggregate: {}",
        column.name);
    outputNames_.emplace_back(column.name);
  }
}

void GroupByPlanner::rewritePostAggregateExprs() {
  ExprMap<core::ExprPtr> keyInputs;
  AggregateExprMap aggregateInputs;

  size_t index = 0;
  for (const auto& key : groupingKeys_) {
    flatInputs_.emplace_back(lp::Col(outputNames_.at(index)));
    keyInputs.emplace(key.expr(), flatInputs_.back().expr());
    ++index;
  }

  for (const auto& agg : aggregates_) {
    flatInputs_.emplace_back(lp::Col(outputNames_.at(index)));
    aggregateInputs.emplace(agg.expr(), flatInputs_.back().expr());
    ++index;
  }

  if (filter_.has_value()) {
    filter_ = replaceInputs(
        filter_.value().expr(),
        keyInputs,
        aggregateInputs,
        [](const core::FieldAccessExpr& expr) {
          VELOX_USER_FAIL(
              "HAVING clause cannot reference column: {}", expr.name());
        });
  }

  // Rewrites an IExpr to reference post-aggregate columns.
  auto rewriteIExpr = [&](const core::ExprPtr& expr) {
    return replaceInputs(expr, keyInputs, aggregateInputs);
  };

  // Project nested window functions (e.g. sum(sum(a)) OVER () inside
  // sum(a) / sum(sum(a)) OVER ()) and add replacements to keyInputs.
  projectNestedWindows(rewriteIExpr, keyInputs);

  // Rewrite projections and sorting keys to reference post-aggregate columns.
  // Top-level windows need rewriteWindowExpr to rewrite args and
  // partition/order keys. Non-window expressions use replaceInputs which also
  // substitutes nested window references added to keyInputs by
  // projectNestedWindows above.

  // TODO: Verify that SELECT expressions don't depend on anything other
  // than grouping keys and aggregates.

  auto rewriteExpr = [&](lp::ExprApi& item) {
    if (item.expr()->is(core::IExpr::Kind::kWindow)) {
      item = rewriteWindowExpr(item, rewriteIExpr);
    } else {
      item = lp::ExprApi(rewriteIExpr(item.expr()), item.name());
    }
  };

  for (auto& item : projections_) {
    rewriteExpr(item);
  }

  for (auto& expr : sortingKeyExprs_) {
    rewriteExpr(expr);
  }
}

void GroupByPlanner::projectNestedWindows(
    const std::function<core::ExprPtr(const core::ExprPtr&)>& rewriteIExpr,
    ExprMap<core::ExprPtr>& keyInputs) {
  std::unordered_map<const core::IExpr*, core::ExprPtr> windowExprPtrs;
  std::vector<const core::IExpr*> windowOrder;
  ExpressionPlanner::findNestedWindowExprs(
      projections_, windowExprPtrs, windowOrder);

  if (windowOrder.empty()) {
    return;
  }

  std::vector<lp::ExprApi> windowExprs;
  windowExprs.reserve(windowOrder.size());
  for (const auto* exprPtr : windowOrder) {
    // Rewrite aggregate references in function arguments, partition keys,
    // and order by keys.
    windowExprs.push_back(rewriteWindowExpr(
        lp::ExprApi(windowExprPtrs.at(exprPtr)), rewriteIExpr));
  }

  builder_->with(windowExprs);

  auto outputColumns =
      builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

  auto numInputColumns = outputColumns.size() - windowOrder.size();
  for (size_t i = 0; i < windowOrder.size(); ++i) {
    const auto& column = outputColumns.at(numInputColumns + i);
    keyInputs.emplace(windowExprPtrs.at(windowOrder[i]), column.toCol().expr());
  }
}

std::vector<size_t> GroupByPlanner::resolveSortOrdinals(
    const OrderByPtr& orderBy) {
  if (sortingKeyExprs_.empty()) {
    return {};
  }

  VELOX_CHECK_EQ(orderBy->sortItems().size(), sortingKeyExprs_.size());
  std::vector<size_t> preResolved(sortingKeyExprs_.size(), 0);
  for (size_t i = 0; i < sortingKeyExprs_.size(); ++i) {
    const auto& sortKey = orderBy->sortItems().at(i)->sortKey();
    if (sortKey->is(NodeType::kLongLiteral)) {
      preResolved.at(i) = sortKey->as<LongLiteral>()->value();
    }
  }

  return SortProjection::widenProjections(
      sortingKeyExprs_, preResolved, projections_);
}

bool GroupByPlanner::isIdentityProjection() const {
  if (flatInputs_.size() != projections_.size()) {
    return false;
  }

  for (size_t i = 0; i < projections_.size(); ++i) {
    if (projections_.at(i).expr() != flatInputs_.at(i).expr()) {
      return false;
    }

    const auto& alias = projections_.at(i).alias();
    if (alias.has_value() && alias.value() != outputNames_.at(i)) {
      return false;
    }
  }

  return true;
}

lp::ExprApi GroupByPlanner::resolveGroupingExpression(
    const ExpressionPtr& expr,
    const std::vector<lp::ExprApi>& selectExprs) {
  if (expr->is(NodeType::kLongLiteral)) {
    const auto n = expr->as<LongLiteral>()->value();
    AXIOM_PRESTO_SEMANTIC_CHECK_GE(
        n,
        static_cast<int64_t>(1),
        expr->location(),
        std::to_string(n),
        "GROUP BY position is not in select list");
    AXIOM_PRESTO_SEMANTIC_CHECK_LE(
        n,
        static_cast<int64_t>(selectExprs.size()),
        expr->location(),
        std::to_string(n),
        "GROUP BY position is not in select list");
    return selectExprs.at(n - 1);
  }
  return exprPlanner_.toExpr(expr);
}

lp::ExprApi GroupByPlanner::resolveWithCache(
    const ExpressionPtr& expr,
    const std::vector<lp::ExprApi>& selectExprs) {
  const Expression* cacheKey = expr.get();
  auto it = exprCache_.find(cacheKey);
  if (it != exprCache_.end()) {
    return it->second;
  }
  auto resolved = resolveGroupingExpression(expr, selectExprs);
  exprCache_.emplace(cacheKey, resolved);
  return resolved;
}

std::vector<lp::ExprApi> GroupByPlanner::resolveWithCache(
    const std::vector<ExpressionPtr>& exprs,
    const std::vector<lp::ExprApi>& selectExprs) {
  std::vector<lp::ExprApi> result;
  result.reserve(exprs.size());
  for (const auto& expr : exprs) {
    auto resolved = resolveWithCache(expr, selectExprs);

    // Expand COLUMNS() calls to multiple grouping keys.
    auto expanded =
        ColumnsExpansion::expand(resolved, *builder_, expr->location());
    if (!expanded.empty()) {
      result.insert(
          result.end(),
          std::make_move_iterator(expanded.begin()),
          std::make_move_iterator(expanded.end()));
    } else {
      result.push_back(std::move(resolved));
    }
  }
  return result;
}

} // namespace axiom::sql::presto
