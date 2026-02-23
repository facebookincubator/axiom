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
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "velox/exec/Aggregate.h"

namespace axiom::sql::presto {

using namespace facebook::velox;

namespace lp = facebook::axiom::logical_plan;

namespace {

using ExprSet =
    folly::F14FastSet<core::ExprPtr, core::IExprHash, core::IExprEqual>;

template <typename V>
using ExprMap =
    folly::F14FastMap<core::ExprPtr, V, core::IExprHash, core::IExprEqual>;

// Given an expression, and pairs of search-and-replace sub-expressions,
// produces a new expression with sub-expressions replaced. If
// 'onUnreplacedLeaf' is provided, invokes the callback for any
// FieldAccessExpr that is not in the replacement map and whose children were
// not replaced either. Used to detect invalid column references in HAVING.
core::ExprPtr replaceInputs(
    const core::ExprPtr& expr,
    const ExprMap<core::ExprPtr>& replacements,
    const std::function<void(const core::FieldAccessExpr&)>& onUnreplacedLeaf =
        nullptr) {
  auto it = replacements.find(expr);
  if (it != replacements.end()) {
    return it->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool hasNewInput = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(input, replacements, onUnreplacedLeaf);
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
// these to 'aggregates'.
void findAggregates(
    const core::ExprPtr& expr,
    std::vector<lp::ExprApi>& aggregates,
    ExprSet& aggregateSet) {
  switch (expr->kind()) {
    case core::IExpr::Kind::kInput:
      return;
    case core::IExpr::Kind::kFieldAccess:
      return;
    case core::IExpr::Kind::kCall: {
      if (exec::getAggregateFunctionEntry(expr->as<core::CallExpr>()->name())) {
        if (aggregateSet.emplace(expr).second) {
          aggregates.emplace_back(lp::ExprApi(expr));
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
      VELOX_USER_CHECK(
          !aggregateName_.has_value(),
          "Cannot nest aggregations inside aggregation: {}({})",
          aggregateName_.value(),
          name);

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
    const std::vector<lp::ExprApi>& expressions) {
  const size_t n = expressions.size();
  // Presto limits CUBE to 30 columns (2^30 possible grouping sets).
  // https://github.com/prestodb/presto/issues/27096
  VELOX_USER_CHECK_LE(n, 30, "CUBE supports at most 30 columns");
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

bool hasGroupingSets(const std::vector<GroupingElementPtr>& groupingElements) {
  for (const auto& element : groupingElements) {
    switch (element->type()) {
      case NodeType::kRollup:
      case NodeType::kCube:
      case NodeType::kGroupingSets:
        return true;
      default:
        break;
    }
  }
  return false;
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

std::vector<ExpressionPtr> extractExpressionsFromGroupingElement(
    const GroupingElementPtr& element) {
  switch (element->type()) {
    case NodeType::kSimpleGroupBy:
      return element->as<SimpleGroupBy>()->expressions();
    case NodeType::kRollup:
      return element->as<Rollup>()->expressions();
    case NodeType::kCube:
      return element->as<Cube>()->expressions();
    case NodeType::kGroupingSets: {
      std::vector<ExpressionPtr> expressions;
      for (const auto& set : element->as<GroupingSets>()->sets()) {
        for (const auto& e : set) {
          expressions.push_back(e);
        }
      }
      return expressions;
    }
    default:
      return {};
  }
}

} // namespace

void GroupByPlanner::plan(
    const std::vector<SelectItemPtr>& selectItems,
    const std::vector<GroupingElementPtr>& groupingElements,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) && {
  // Expand ROLLUP, CUBE, GROUPING SETS into a list of grouping sets, then
  // extract deduplicated grouping keys and per-set index vectors.
  // Populates: groupingSets_, groupingKeys_, groupingSetsIndices_.
  groupingSets_ = expandGroupingSets(groupingElements, selectItems);
  deduplicateGroupingKeys();

  // Walk SELECT, HAVING, and ORDER BY expressions to collect aggregate
  // function calls, then add the Aggregate plan node.
  // Populates: aggregates_, projections_, filter_, sortingKeys_, outputNames_.
  auto aggregateOptionsMap = collectAggregates(selectItems, having, orderBy);
  addAggregate(aggregateOptionsMap, hasGroupingSets(groupingElements));

  // Rewrite filter_, projections_, and sortingKeys_ to reference the
  // aggregate output columns instead of the original input expressions.
  // Populates: flatInputs_.
  // Mutates: filter_, projections_, sortingKeys_.
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
  addSort(selectItems, sortingKeyOrdinals);
}

bool GroupByPlanner::tryPlanGlobalAgg(
    const std::vector<SelectItemPtr>& selectItems,
    const ExpressionPtr& having) && {
  for (const auto& item : selectItems) {
    if (item->is(NodeType::kAllColumns)) {
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

  std::move(*this).plan(selectItems, {}, having, /*orderBy=*/nullptr);
  return true;
}

std::vector<std::vector<lp::ExprApi>> GroupByPlanner::expandGroupingSets(
    const std::vector<GroupingElementPtr>& groupingElements,
    const std::vector<SelectItemPtr>& selectItems) {
  std::vector<std::vector<lp::ExprApi>> groupingSets;
  for (const auto& element : groupingElements) {
    switch (element->type()) {
      case NodeType::kSimpleGroupBy: {
        const auto* simple = element->as<SimpleGroupBy>();
        auto exprs = resolveWithCache(simple->expressions(), selectItems);
        groupingSets =
            crossProductGroupingSets(groupingSets, {std::move(exprs)});
        break;
      }

      case NodeType::kRollup: {
        const auto* rollup = element->as<Rollup>();
        auto rollupSets =
            expandRollup(resolveWithCache(rollup->expressions(), selectItems));
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(rollupSets));
        break;
      }

      case NodeType::kCube: {
        const auto* cube = element->as<Cube>();
        auto cubeSets =
            expandCube(resolveWithCache(cube->expressions(), selectItems));
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(cubeSets));
        break;
      }

      case NodeType::kGroupingSets: {
        const auto* groupingSetsNode = element->as<GroupingSets>();
        std::vector<std::vector<lp::ExprApi>> explicitSets;
        explicitSets.reserve(groupingSetsNode->sets().size());
        for (const auto& set : groupingSetsNode->sets()) {
          explicitSets.push_back(resolveWithCache(set, selectItems));
        }
        groupingSets =
            crossProductGroupingSets(groupingSets, std::move(explicitSets));
        break;
      }

      default:
        VELOX_NYI(
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
      indices.push_back(it->second);
    }
    groupingSetsIndices_.push_back(std::move(indices));
  }
}

GroupByPlanner::AggregateOptionsMap GroupByPlanner::collectAggregates(
    const std::vector<SelectItemPtr>& selectItems,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) {
  // Go over SELECT expressions and figure out for each: whether a grouping
  // key, a function of one or more grouping keys, a constant, an aggregate
  // or a function over one or more aggregates and possibly grouping keys.
  //
  // Collect all individual aggregates. A single select item 'sum(x) /
  // sum(y)' will produce 2 aggregates: sum(x), sum(y).

  AggregateOptionsMap aggregateOptionsMap;
  ExprSet aggregateSet;
  for (const auto& item : selectItems) {
    VELOX_CHECK(item->is(NodeType::kSingleColumn));
    auto* singleColumn = item->as<SingleColumn>();

    lp::ExprApi expr = [&]() {
      auto it = exprCache_.find(singleColumn->expression().get());
      if (it != exprCache_.end()) {
        return it->second;
      }
      return exprPlanner_.toExpr(
          singleColumn->expression(), &aggregateOptionsMap);
    }();

    findAggregates(expr.expr(), aggregates_, aggregateSet);

    if (!aggregates_.empty() &&
        aggregates_.back().expr().get() == expr.expr().get()) {
      // Preserve the alias.
      if (singleColumn->alias() != nullptr) {
        aggregates_.back() = aggregates_.back().as(
            canonicalizeIdentifier(*singleColumn->alias()));
      }
    }

    if (singleColumn->alias() != nullptr) {
      expr = expr.as(canonicalizeIdentifier(*singleColumn->alias()));
    }

    projections_.emplace_back(expr);
  }

  if (having != nullptr) {
    lp::ExprApi expr = exprPlanner_.toExpr(having, &aggregateOptionsMap);
    findAggregates(expr.expr(), aggregates_, aggregateSet);
    filter_ = expr;
  }

  if (orderBy != nullptr) {
    std::vector<lp::ExprApi> sortingAggregates;

    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = exprPlanner_.toExpr(item->sortKey(), &aggregateOptionsMap);
      findAggregates(expr.expr(), sortingAggregates, aggregateSet);
      sortingKeys_.emplace_back(
          expr, item->isAscending(), item->isNullsFirst());
    }

    for (const auto& aggregate : sortingAggregates) {
      aggregates_.emplace_back(aggregate);
    }
  }

  return aggregateOptionsMap;
}

void GroupByPlanner::addAggregate(
    const AggregateOptionsMap& aggregateOptionsMap,
    bool useGroupingSets) {
  std::vector<lp::PlanBuilder::AggregateOptions> aggregateOptions;
  for (const auto& agg : aggregates_) {
    auto it = aggregateOptionsMap.find(agg.expr().get());
    if (it != aggregateOptionsMap.end()) {
      aggregateOptions.emplace_back(it->second);
    } else {
      aggregateOptions.emplace_back();
    }
  }

  if (useGroupingSets) {
    builder_->aggregate(
        groupingKeys_,
        groupingSetsIndices_,
        aggregates_,
        aggregateOptions,
        "$grouping_set_id");
  } else {
    builder_->aggregate(groupingKeys_, aggregates_, aggregateOptions);
  }

  outputNames_ = builder_->findOrAssignOutputNames();
}

void GroupByPlanner::rewritePostAggregateExprs() {
  ExprMap<core::ExprPtr> inputs;

  size_t index = 0;
  for (const auto& key : groupingKeys_) {
    flatInputs_.emplace_back(lp::Col(outputNames_.at(index)));
    inputs.emplace(key.expr(), flatInputs_.back().expr());
    ++index;
  }

  for (const auto& agg : aggregates_) {
    flatInputs_.emplace_back(lp::Col(outputNames_.at(index)));
    inputs.emplace(agg.expr(), flatInputs_.back().expr());
    ++index;
  }

  if (filter_.has_value()) {
    filter_ = replaceInputs(
        filter_.value().expr(), inputs, [](const core::FieldAccessExpr& expr) {
          VELOX_USER_FAIL(
              "HAVING clause cannot reference column: {}", expr.name());
        });
  }

  // Replace sub-expressions in SELECT projections with column references to
  // the aggregate output.

  // TODO: Verify that SELECT expressions don't depend on anything other
  // than grouping keys and aggregates.

  for (auto& item : projections_) {
    auto newExpr = replaceInputs(item.expr(), inputs);
    item = lp::ExprApi(newExpr, item.name());
  }

  // Replace sorting key expressions too.
  for (auto& key : sortingKeys_) {
    auto newExpr = replaceInputs(key.expr.expr(), inputs);
    key = lp::SortKey(
        lp::ExprApi(newExpr, key.expr.name()), key.ascending, key.nullsFirst);
  }
}

std::vector<size_t> GroupByPlanner::resolveSortOrdinals(
    const OrderByPtr& orderBy) {
  std::vector<size_t> sortingKeyOrdinals;
  if (sortingKeys_.empty()) {
    return sortingKeyOrdinals;
  }

  ExprMap<size_t> projectionMap;
  for (size_t i = 0; i < projections_.size(); ++i) {
    projectionMap.emplace(projections_.at(i).expr(), i + 1);
  }

  for (size_t i = 0; i < sortingKeys_.size(); ++i) {
    const auto& sortKey = orderBy->sortItems().at(i)->sortKey();
    if (sortKey->is(NodeType::kLongLiteral)) {
      const auto n = sortKey->as<LongLiteral>()->value();
      sortingKeyOrdinals.emplace_back(n);
    } else {
      auto [it, inserted] = projectionMap.emplace(
          sortingKeys_.at(i).expr.expr(), projections_.size() + 1);
      if (inserted) {
        sortingKeyOrdinals.emplace_back(projections_.size() + 1);
        projections_.emplace_back(sortingKeys_.at(i).expr);
      } else {
        sortingKeyOrdinals.emplace_back(it->second);
      }
    }
  }

  return sortingKeyOrdinals;
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

void GroupByPlanner::addSort(
    const std::vector<SelectItemPtr>& selectItems,
    const std::vector<size_t>& sortingKeyOrdinals) {
  if (sortingKeys_.empty()) {
    return;
  }

  for (size_t i = 0; i < sortingKeys_.size(); ++i) {
    const auto name =
        builder_->findOrAssignOutputNameAt(sortingKeyOrdinals.at(i) - 1);

    auto& key = sortingKeys_.at(i);
    key = lp::SortKey(lp::Col(name), key.ascending, key.nullsFirst);
  }

  builder_->sort(sortingKeys_);

  // Drop projections used only for sorting.
  if (selectItems.size() < projections_.size()) {
    std::vector<lp::ExprApi> finalProjections;
    finalProjections.reserve(selectItems.size());
    for (size_t i = 0; i < selectItems.size(); ++i) {
      finalProjections.emplace_back(
          lp::Col(builder_->findOrAssignOutputNameAt(i)));
    }
    builder_->project(finalProjections);
  }
}

lp::ExprApi GroupByPlanner::resolveGroupingExpression(
    const ExpressionPtr& expr,
    const std::vector<SelectItemPtr>& selectItems) {
  if (expr->is(NodeType::kLongLiteral)) {
    const auto n = expr->as<LongLiteral>()->value();
    VELOX_CHECK_GE(n, 1);
    VELOX_CHECK_LE(n, selectItems.size());

    const auto& item = selectItems.at(n - 1);
    VELOX_CHECK(item->is(NodeType::kSingleColumn));

    const auto* singleColumn = item->as<SingleColumn>();
    return exprPlanner_.toExpr(singleColumn->expression());
  }
  return exprPlanner_.toExpr(expr);
}

lp::ExprApi GroupByPlanner::resolveWithCache(
    const ExpressionPtr& expr,
    const std::vector<SelectItemPtr>& selectItems) {
  const Expression* cacheKey = expr.get();
  if (expr->is(NodeType::kLongLiteral)) {
    const auto ordinal = expr->as<LongLiteral>()->value();
    const auto& selectItem = selectItems.at(ordinal - 1);
    if (selectItem->is(NodeType::kSingleColumn)) {
      cacheKey = selectItem->as<SingleColumn>()->expression().get();
    }
  }
  auto it = exprCache_.find(cacheKey);
  if (it != exprCache_.end()) {
    return it->second;
  }
  auto resolved = resolveGroupingExpression(expr, selectItems);
  exprCache_.emplace(cacheKey, resolved);
  return resolved;
}

std::vector<lp::ExprApi> GroupByPlanner::resolveWithCache(
    const std::vector<ExpressionPtr>& exprs,
    const std::vector<SelectItemPtr>& selectItems) {
  std::vector<lp::ExprApi> result;
  result.reserve(exprs.size());
  for (const auto& expr : exprs) {
    result.push_back(resolveWithCache(expr, selectItems));
  }
  return result;
}

} // namespace axiom::sql::presto
