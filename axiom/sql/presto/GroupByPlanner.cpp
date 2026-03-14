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
    const std::unordered_map<const core::IExpr*, lp::WindowSpec>*
        windowOptions = nullptr,
    const std::function<void(const core::FieldAccessExpr&)>& onUnreplacedLeaf =
        nullptr) {
  // First try grouping keys.
  auto keyIt = keyReplacements.find(expr);
  if (keyIt != keyReplacements.end()) {
    return keyIt->second;
  }

  // Skip window function expressions. They should not be replaced with
  // aggregate column references even if structurally equal to a plain
  // aggregate. Sub-expressions are still rewritten below.
  if (windowOptions == nullptr || windowOptions->count(expr.get()) == 0) {
    // Then try aggregates.
    AggregateExprWithOptions key{expr, findOptions(expr, optionsMap)};
    auto aggIt = aggregateReplacements.find(key);
    if (aggIt != aggregateReplacements.end()) {
      return aggIt->second;
    }
  }

  std::vector<core::ExprPtr> newInputs;
  bool hasNewInput = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(
        input,
        keyReplacements,
        aggregateReplacements,
        optionsMap,
        windowOptions,
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

// Finds sub-expressions in an IExpr tree using pointer-identity matching.
// Walks the expression tree and collects ExprPtrs whose raw pointers are
// found in targets. Also appends matches to order in traversal order for
// deterministic plan generation.
void findExprPtrs(
    const core::ExprPtr& expr,
    const std::unordered_map<const core::IExpr*, lp::WindowSpec>& targets,
    std::unordered_map<const core::IExpr*, core::ExprPtr>& found,
    std::vector<const core::IExpr*>& order) {
  if (targets.count(expr.get())) {
    if (found.emplace(expr.get(), expr).second) {
      order.push_back(expr.get());
    }
    return;
  }
  for (const auto& input : expr->inputs()) {
    findExprPtrs(input, targets, found, order);
  }
}

// Replaces sub-expressions by pointer identity. Used to swap window function
// call nodes with column references after the window projection is added.
core::ExprPtr replaceWindowInputs(
    const core::ExprPtr& expr,
    const std::unordered_map<const core::IExpr*, core::ExprPtr>& replacements) {
  auto it = replacements.find(expr.get());
  if (it != replacements.end()) {
    return it->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool changed = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceWindowInputs(input, replacements);
    if (newInput.get() != input.get()) {
      changed = true;
    }
    newInputs.push_back(std::move(newInput));
  }

  return changed ? expr->replaceInputs(std::move(newInputs)) : expr;
}

// Set of aggregation expression with options for deduplication.
using AggregateExprSet = folly::F14FastSet<
    AggregateExprWithOptions,
    AggregateExprWithOptionsHash,
    AggregateExprWithOptionsEqual>;

// Walks the expression tree looking for aggregate function calls and appending
// these calls to 'aggregates' after deduplication through 'aggregateSet'.
// Skips expressions found in windowOptions. These are window functions that
// share an aggregate function name (e.g. sum(...) OVER (...)) and should not be
// collected as aggregates. Nested arguments are still searched.
void findAggregates(
    const core::ExprPtr& expr,
    const AggregateOptionsMap& optionsMap,
    const std::unordered_map<const core::IExpr*, lp::WindowSpec>* windowOptions,
    std::vector<AggregateWithOptions>& aggregates,
    AggregateExprSet& aggregateSet) {
  switch (expr->kind()) {
    case core::IExpr::Kind::kInput:
      return;
    case core::IExpr::Kind::kFieldAccess:
      return;
    case core::IExpr::Kind::kCall: {
      // Skip window functions even if they share an aggregate function name.
      // Recurse into their arguments to find nested aggregates.
      if (windowOptions != nullptr && windowOptions->count(expr.get())) {
        for (const auto& input : expr->inputs()) {
          findAggregates(
              input, optionsMap, windowOptions, aggregates, aggregateSet);
        }
        return;
      }
      if (exec::getAggregateFunctionEntry(expr->as<core::CallExpr>()->name())) {
        auto* options = findOptions(expr, optionsMap);
        AggregateExprWithOptions key{expr, options};
        if (aggregateSet.emplace(key).second) {
          aggregates.emplace_back(lp::ExprApi(expr), options);
        }
      } else {
        for (const auto& input : expr->inputs()) {
          findAggregates(
              input, optionsMap, windowOptions, aggregates, aggregateSet);
        }
      }
      return;
    }
    case core::IExpr::Kind::kCast:
      findAggregates(
          expr->as<core::CastExpr>()->input(),
          optionsMap,
          windowOptions,
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
  // Populates: aggregates_, aggregateOptionsMap_, projections_, filter_,
  //   sortingKeys_, outputNames_.
  collectAggregates(selectItems, having, orderBy);
  addAggregate(hasGroupingSets(groupingElements));

  // Rewrite filter_, projections_, and sortingKeys_ to reference the
  // aggregate output columns instead of the original input expressions.
  // Populates: flatInputs_.
  // Mutates: filter_, projections_, sortingKeys_.
  rewritePostAggregateExprs();

  // Apply HAVING filter before window projection.
  if (filter_.has_value()) {
    builder_->filter(filter_.value());
  }

  // Materialize window functions as a separate projection, then replace
  // window sub-expressions in projections_ and sortingKeys_ with column
  // references to the window output.
  if (!windowOptions_.empty()) {
    addWindowProjection();
  }

  // Resolve sorting key ordinals before projecting: ORDER BY expressions
  // that are not in the SELECT list are appended to projections_ so they
  // are included in the project node. Must happen before builder_->project().
  auto sortingKeyOrdinals = resolveSortOrdinals(orderBy);

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
          singleColumn->expression(), &aggregateOptionsMap_, &windowOptions_);
    }();

    findAggregates(
        expr.expr(),
        aggregateOptionsMap_,
        &windowOptions_,
        aggregates_,
        aggregateSet);

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
    // Do not pass windowOptions_ for HAVING. Window functions in HAVING are
    // invalid SQL, so passing nullptr lets them be rejected with the default
    // "Window function cannot be an argument to a scalar function" error.
    lp::ExprApi expr = exprPlanner_.toExpr(
        having, &aggregateOptionsMap_, /*windowOptions=*/nullptr);
    findAggregates(
        expr.expr(),
        aggregateOptionsMap_,
        /*windowOptions=*/nullptr,
        aggregates_,
        aggregateSet);
    filter_ = expr;
  }

  if (orderBy != nullptr) {
    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = exprPlanner_.toExpr(
          item->sortKey(), &aggregateOptionsMap_, &windowOptions_);
      findAggregates(
          expr.expr(),
          aggregateOptionsMap_,
          &windowOptions_,
          aggregates_,
          aggregateSet);
      sortingKeys_.emplace_back(
          expr, item->isAscending(), item->isNullsFirst());
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
        /*windowOptions=*/nullptr,
        [](const core::FieldAccessExpr& expr) {
          VELOX_USER_FAIL(
              "HAVING clause cannot reference column: {}", expr.name());
        });
  }

  // Replace sub-expressions in SELECT projections with column references to
  // the aggregate output.

  // TODO: Verify that SELECT expressions don't depend on anything other
  // than grouping keys and aggregates.

  // Updates windowOptions_ when replaceInputs produces a new IExpr pointer
  // for a window function (e.g., aggregate sub-expressions inside the window
  // call were replaced with column references).
  auto updateWindowKey =
      [this](const core::IExpr* oldPtr, const core::IExpr* newPtr) {
        if (oldPtr != newPtr) {
          auto it = windowOptions_.find(oldPtr);
          if (it != windowOptions_.end()) {
            auto spec = std::move(it->second);
            windowOptions_.erase(it);
            windowOptions_.emplace(newPtr, std::move(spec));
          }
        }
      };

  for (auto& item : projections_) {
    auto* oldExprPtr = item.expr().get();
    auto newExpr = replaceInputs(
        item.expr(),
        keyInputs,
        aggregateInputs,
        aggregateOptionsMap_,
        &windowOptions_);
    auto windowSpec = item.windowSpec();
    item = lp::ExprApi(std::move(newExpr), item.name());
    if (windowSpec) {
      item = item.over(*windowSpec);
    }
    updateWindowKey(oldExprPtr, item.expr().get());
  }

  // Replace sorting key expressions too.
  for (auto& key : sortingKeys_) {
    auto* oldExprPtr = key.expr.expr().get();
    auto newExpr = replaceInputs(
        key.expr.expr(),
        keyInputs,
        aggregateInputs,
        aggregateOptionsMap_,
        &windowOptions_);
    auto newKeyExpr = lp::ExprApi(std::move(newExpr), key.expr.name());
    if (key.expr.windowSpec()) {
      newKeyExpr = newKeyExpr.over(*key.expr.windowSpec());
    }
    key = lp::SortKey(newKeyExpr, key.ascending, key.nullsFirst);
    updateWindowKey(oldExprPtr, key.expr.expr().get());
  }
}

void GroupByPlanner::addWindowProjection() {
  auto inputColumns =
      builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

  // Collect ExprPtrs for the window calls from the expression trees.
  // windowOrder captures matches in left-to-right traversal order for
  // deterministic plan generation.
  std::unordered_map<const core::IExpr*, core::ExprPtr> windowExprPtrs;
  std::vector<const core::IExpr*> windowOrder;
  for (const auto& expr : projections_) {
    findExprPtrs(expr.expr(), windowOptions_, windowExprPtrs, windowOrder);
  }
  for (const auto& key : sortingKeys_) {
    findExprPtrs(key.expr.expr(), windowOptions_, windowExprPtrs, windowOrder);
  }

  // Build window projection: all input columns + window function expressions.
  std::vector<lp::ExprApi> windowProjection;
  windowProjection.reserve(inputColumns.size() + windowOrder.size());
  for (const auto& name : inputColumns) {
    windowProjection.push_back(lp::Col(name));
  }
  for (const auto* exprPtr : windowOrder) {
    windowProjection.push_back(
        lp::ExprApi(windowExprPtrs.at(exprPtr))
            .over(windowOptions_.at(exprPtr)));
  }

  builder_->project(windowProjection);

  // Build replacement map: window expr pointer to column reference.
  auto outputNames =
      builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

  std::unordered_map<const core::IExpr*, core::ExprPtr> replacements;
  for (size_t i = 0; i < windowOrder.size(); ++i) {
    const auto& name = outputNames.at(inputColumns.size() + i);
    replacements.emplace(windowOrder[i], lp::Col(name).expr());
  }

  // Replace window sub-expressions in projections with column references.
  for (auto& item : projections_) {
    item = lp::ExprApi(
        replaceWindowInputs(item.expr(), replacements), item.name());
  }

  // Replace window sub-expressions in sorting keys with column references.
  for (auto& key : sortingKeys_) {
    auto newKeyExpr = lp::ExprApi(
        replaceWindowInputs(key.expr.expr(), replacements), key.expr.name());
    key = lp::SortKey(newKeyExpr, key.ascending, key.nullsFirst);
  }

  // Refresh flatInputs_ and outputNames_ to reflect the window projection
  // output, so isIdentityProjection() can detect when the final projection is
  // identity (e.g., for top-level window functions where no further computation
  // is needed beyond the window project).
  flatInputs_.clear();
  outputNames_ = outputNames;
  for (const auto& name : outputNames) {
    flatInputs_.push_back(lp::Col(name));
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

  core::IExprEqual exprEqual;
  for (size_t i = 0; i < projections_.size(); ++i) {
    if (!exprEqual(projections_.at(i).expr(), flatInputs_.at(i).expr())) {
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
