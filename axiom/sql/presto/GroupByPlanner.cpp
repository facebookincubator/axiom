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

// Aggregate expression paired with its options pointer. Used as a key for
// deduplication of aggregation calls.
struct AggregateExprWithOptions {
  /// The aggregation expression.
  core::ExprPtr expr;
  /// The execution options of this aggregation. If this aggregation expression
  /// has the default options, i.e., no distinct, filter, or orderBy, this is
  /// nullptr.
  const lp::PlanBuilder::AggregateOptions* options;
};

// Hashes aggregate expression keys.
struct AggregateExprWithOptionsHash {
  size_t operator()(const AggregateExprWithOptions& key) const {
    size_t exprHash = key.expr->hash();
    size_t optionsHash =
        key.options ? key.options->hash() : kDefaultOptionsHash;
    return bits::hashMix(exprHash, optionsHash);
  }

  // Hash of a default-constructed AggregateOptions.
  static inline const size_t kDefaultOptionsHash =
      lp::PlanBuilder::AggregateOptions{}.hash();
};

// Compares aggregate expression keys for equality.
struct AggregateExprWithOptionsEqual {
  bool operator()(
      const AggregateExprWithOptions& lhs,
      const AggregateExprWithOptions& rhs) const {
    if (*lhs.expr != *rhs.expr) {
      return false;
    }
    if (static_cast<bool>(lhs.options) != static_cast<bool>(rhs.options)) {
      return false;
    }
    return (lhs.options == rhs.options) || (*lhs.options == *rhs.options);
  }
};

// Maps aggregation expression with options to their output.
using AggregateExprMap = folly::F14FastMap<
    AggregateExprWithOptions,
    core::ExprPtr,
    AggregateExprWithOptionsHash,
    AggregateExprWithOptionsEqual>;

// Looks up options for an expression in the options map, returning nullptr if
// not found.
const lp::PlanBuilder::AggregateOptions* findOptions(
    const core::ExprPtr& expr,
    const AggregateOptionsMap& optionsMap) {
  auto it = optionsMap.find(expr.get());
  return it != optionsMap.end() ? &it->second : nullptr;
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
    const AggregateOptionsMap& optionsMap,
    const std::function<void(const core::FieldAccessExpr&)>& onUnreplacedLeaf =
        nullptr) {
  // First try grouping keys.
  auto keyIt = keyReplacements.find(expr);
  if (keyIt != keyReplacements.end()) {
    return keyIt->second;
  }

  // Then try aggregates.
  AggregateExprWithOptions key{expr, findOptions(expr, optionsMap)};
  auto aggIt = aggregateReplacements.find(key);
  if (aggIt != aggregateReplacements.end()) {
    return aggIt->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool hasNewInput = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(
        input,
        keyReplacements,
        aggregateReplacements,
        optionsMap,
        onUnreplacedLeaf);
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

// Set of aggregation expression with options for deduplication.
using AggregateExprSet = folly::F14FastSet<
    AggregateExprWithOptions,
    AggregateExprWithOptionsHash,
    AggregateExprWithOptionsEqual>;

// Walks the expression tree looking for aggregate function calls and appending
// these calls to 'aggregates' after deduplication through 'aggregateSet'.
void findAggregates(
    const core::ExprPtr& expr,
    const AggregateOptionsMap& optionsMap,
    std::vector<AggregateWithOptions>& aggregates,
    AggregateExprSet& aggregateSet) {
  switch (expr->kind()) {
    case core::IExpr::Kind::kInput:
      return;
    case core::IExpr::Kind::kFieldAccess:
      return;
    case core::IExpr::Kind::kCall: {
      if (exec::getAggregateFunctionEntry(expr->as<core::CallExpr>()->name())) {
        auto* options = findOptions(expr, optionsMap);
        AggregateExprWithOptions key{expr, options};
        if (aggregateSet.emplace(key).second) {
          aggregates.emplace_back(lp::ExprApi(expr), options);
        }
      } else {
        for (const auto& input : expr->inputs()) {
          findAggregates(input, optionsMap, aggregates, aggregateSet);
        }
      }
      return;
    }
    case core::IExpr::Kind::kCast:
      findAggregates(
          expr->as<core::CastExpr>()->input(),
          optionsMap,
          aggregates,
          aggregateSet);
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
    const std::vector<SelectItemPtr>& selectItems,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) && {
  // Expand ROLLUP, CUBE, GROUPING SETS into a list of grouping sets, then
  // extract deduplicated grouping keys and per-set index vectors.
  // Populates: groupingSets_, groupingKeys_, groupingSetsIndices_.
  groupingSets_ = expandGroupingSets(groupingElements, selectItems);
  deduplicateGroupingKeys();
  // GROUP BY DISTINCT removes duplicate grouping sets after expansion
  // (order-insensitive comparison).
  if (distinct) {
    deduplicateGroupingSets(groupingSetsIndices_);
  }

  // Walk SELECT, HAVING, and ORDER BY expressions to collect aggregate
  // function calls, then add the Aggregate plan node.
  // Populates: aggregates_, aggregateOptionsMap_, projections_, filter_,
  //   sortingKeyExprs_, outputNames_.
  collectAggregates(selectItems, having, orderBy);
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
        selectItems.size());
  }
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

  std::move(*this).plan(
      {}, /*distinct=*/false, selectItems, having, /*orderBy=*/nullptr);
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
    const std::vector<SelectItemPtr>& selectItems,
    const ExpressionPtr& having,
    const OrderByPtr& orderBy) {
  // Go over SELECT expressions and figure out for each: whether a grouping
  // key, a function of one or more grouping keys, a constant, an aggregate
  // or a function over one or more aggregates and possibly grouping keys.
  //
  // Collect all individual aggregates. A single select item 'sum(x) /
  // sum(y)' will produce 2 aggregates: sum(x), sum(y).

  AggregateExprSet aggregateSet;
  for (const auto& item : selectItems) {
    VELOX_CHECK(item->is(NodeType::kSingleColumn));
    auto* singleColumn = item->as<SingleColumn>();

    lp::ExprApi expr = [&]() {
      auto it = exprCache_.find(singleColumn->expression().get());
      if (it != exprCache_.end()) {
        return it->second;
      }
      return exprPlanner_.toExpr(
          singleColumn->expression(), &aggregateOptionsMap_);
    }();

    findAggregates(
        expr.expr(), aggregateOptionsMap_, aggregates_, aggregateSet);

    if (!aggregates_.empty() &&
        aggregates_.back().expr.expr().get() == expr.expr().get()) {
      // Preserve the alias.
      if (singleColumn->alias() != nullptr) {
        aggregates_.back().expr = aggregates_.back().expr.as(
            canonicalizeIdentifier(*singleColumn->alias()));
      }
    }

    if (singleColumn->alias() != nullptr) {
      expr = expr.as(canonicalizeIdentifier(*singleColumn->alias()));
    }

    projections_.emplace_back(expr);
  }

  if (having != nullptr) {
    lp::ExprApi expr = exprPlanner_.toExpr(having, &aggregateOptionsMap_);
    findAggregates(
        expr.expr(), aggregateOptionsMap_, aggregates_, aggregateSet);
    filter_ = expr;
  }

  if (orderBy != nullptr) {
    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = exprPlanner_.toExpr(item->sortKey(), &aggregateOptionsMap_);
      findAggregates(
          expr.expr(), aggregateOptionsMap_, aggregates_, aggregateSet);
      sortingKeyExprs_.emplace_back(expr);
    }
  }
}

void GroupByPlanner::addAggregate(bool useGroupingSets) {
  std::vector<lp::ExprApi> aggregateExprs;
  std::vector<lp::PlanBuilder::AggregateOptions> aggregateOptions;
  aggregateExprs.reserve(aggregates_.size());
  aggregateOptions.reserve(aggregates_.size());

  for (const auto& [expr, options] : aggregates_) {
    aggregateExprs.push_back(expr);
    if (options != nullptr) {
      aggregateOptions.emplace_back(*options);
    } else {
      aggregateOptions.emplace_back();
    }
  }

  if (useGroupingSets) {
    builder_->aggregate(
        groupingKeys_,
        groupingSetsIndices_,
        aggregateExprs,
        aggregateOptions,
        "$grouping_set_id");
  } else {
    builder_->aggregate(groupingKeys_, aggregateExprs, aggregateOptions);
  }

  outputNames_ = builder_->findOrAssignOutputNames();
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

  for (const auto& [expr, options] : aggregates_) {
    flatInputs_.emplace_back(lp::Col(outputNames_.at(index)));
    AggregateExprWithOptions key{expr.expr(), options};
    aggregateInputs.emplace(key, flatInputs_.back().expr());
    ++index;
  }

  if (filter_.has_value()) {
    filter_ = replaceInputs(
        filter_.value().expr(),
        keyInputs,
        aggregateInputs,
        aggregateOptionsMap_,
        [](const core::FieldAccessExpr& expr) {
          VELOX_USER_FAIL(
              "HAVING clause cannot reference column: {}", expr.name());
        });
  }

  // Replace sub-expressions in SELECT projections with column references to
  // the aggregate output.

  // TODO: Verify that SELECT expressions don't depend on anything other
  // than grouping keys and aggregates.

  for (auto& item : projections_) {
    auto newExpr = replaceInputs(
        item.expr(), keyInputs, aggregateInputs, aggregateOptionsMap_);
    const auto windowSpec = item.windowSpec();
    item = lp::ExprApi(std::move(newExpr), item.name());
    if (windowSpec) {
      item = item.over(*windowSpec);
    }
  }

  // Replace sorting key expressions too.
  for (auto& expr : sortingKeyExprs_) {
    auto newExpr = replaceInputs(
        expr.expr(), keyInputs, aggregateInputs, aggregateOptionsMap_);
    const auto windowSpec = expr.windowSpec();
    expr = lp::ExprApi(newExpr, expr.name());
    if (windowSpec) {
      expr = expr.over(*windowSpec);
    }
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
    const std::vector<SelectItemPtr>& selectItems) {
  if (expr->is(NodeType::kLongLiteral)) {
    const auto n = expr->as<LongLiteral>()->value();
    VELOX_USER_CHECK_GE(n, 1, "GROUP BY position is not in select list: {}", n);
    VELOX_USER_CHECK_LE(
        n,
        selectItems.size(),
        "GROUP BY position is not in select list: {}",
        n);

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
    VELOX_USER_CHECK_GE(
        ordinal, 1, "GROUP BY position is not in select list: {}", ordinal);
    VELOX_USER_CHECK_LE(
        ordinal,
        selectItems.size(),
        "GROUP BY position is not in select list: {}",
        ordinal);
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
