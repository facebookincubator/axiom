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

#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/RelationOpPrinter.h"

namespace facebook::axiom::optimizer {

namespace {

// True if single worker, i.e. do not plan remote exchanges
bool isSingleWorker() {
  return queryCtx()->optimization()->runnerOptions().numWorkers == 1;
}

} // namespace

PlanState::PlanState(
    Optimization& optimization,
    DerivedTableCP dt,
    bool syntacticJoinOrder)
    : optimization(optimization),
      dt(dt),
      syntacticJoinOrder_{syntacticJoinOrder} {}

PlanState::PlanState(Optimization& optimization, DerivedTableCP dt, PlanP plan)
    : optimization(optimization),
      dt(dt),
      cost(plan->cost),
      syntacticJoinOrder_{optimization.options().syntacticJoinOrder} {
  if (auto it = optimization.planGroupedLeaves().find(plan);
      it != optimization.planGroupedLeaves().end()) {
    // Copy: planGroupedLeaves_[plan] is reused if 'plan' is selected as input
    // to multiple parents; each consumer needs its own starting map.
    currentGroupedLeaves_ = GroupedLeaves{it->second};
  }
}

#ifndef NDEBUG
// NOLINTBEGIN
// The dt for which we set a breakpoint for plan candidate.
int32_t debugDt{-1};

// Number of tables in 'debugPlacedTables'
int32_t debugNumPlaced = 0;

// Tables for setting a breakpoint. Join order selection calls planBreakpoint()
// right before evaluating the cost for the tables in 'debugPlacedTables'.
int32_t debugPlaced[10];

void planBreakpoint() {
  // Set breakpoint here for looking at cost of join order in
  // 'debugPlacedTables'.
  LOG(INFO) << "Join order breakpoint";
}

void PlanState::debugSetFirstTable(int32_t id) {
  if (dt->id() == debugDt) {
    debugPlacedTables.resize(1);
    debugPlacedTables[0] = id;
  }
}
// NOLINTEND
#endif

void PlanState::save(PlanStateSaver& saver) const {
  saver.placed = placed_;
  saver.columns = columns_;
  saver.cost = cost;
  saver.exprToColumn = exprToColumn_;
  saver.currentGroupedLeaves = currentGroupedLeaves_;
  saver.numDebugPlacedTables = debugPlacedTables.size();
}

void PlanState::restore(PlanStateSaver& saver) {
  placed_ = std::move(saver.placed);
  columns_ = std::move(saver.columns);
  cost = saver.cost;
  exprToColumn_ = std::move(saver.exprToColumn);
  currentGroupedLeaves_ = std::move(saver.currentGroupedLeaves);
  debugPlacedTables.resize(saver.numDebugPlacedTables);
}

void PlanState::restore(const NextJoin& nextJoin) {
  placed_ = nextJoin.placed;
  columns_ = nextJoin.columns;
  cost = nextJoin.cost;
  exprToColumn_.clear();
  // Copy, not move: NextJoin may be revisited if multiple candidates remain
  // in toTry and the outer scope iterates them.
  currentGroupedLeaves_ = nextJoin.currentGroupedLeaves;
}

void PlanState::place(PlanObjectCP object) {
  placed_.add(object);
}

void PlanState::place(const PlanObjectSet& objects) {
  placed_.unionSet(objects);
}

void PlanState::place(const ExprVector& objects) {
  placed_.unionObjects(objects);
}

void PlanState::placeColumn(PlanObjectCP column) {
  columns_.add(column);
}

void PlanState::placeColumns(const PlanObjectSet& columns) {
  columns_.unionSet(columns);
}

void PlanState::placeColumns(const ColumnVector& columns) {
  columns_.unionObjects(columns);
}

void PlanState::replaceColumns(PlanObjectSet newColumns) {
  columns_ = std::move(newColumns);
}

PlanStateSaver::PlanStateSaver(PlanState& state) : state_(state) {
  state.save(*this);
}

PlanStateSaver::~PlanStateSaver() {
  state_.restore(*this);
}

PlanStateSaver::PlanStateSaver(PlanState& state, const JoinCandidate& candidate)
    : PlanStateSaver(state) {
#ifndef NDEBUG
  if (state.dt->id() != debugDt) {
    return;
  }
  state.debugPlacedTables.push_back(candidate.tables[0]->id());
  if (debugNumPlaced == 0) {
    return;
  }

  for (auto i = 0; i < debugNumPlaced; ++i) {
    if (debugPlaced[i] != state.debugPlacedTables[i]) {
      return;
    }
  }
  planBreakpoint();
#endif
}

namespace {
PlanObjectSet exprColumns(const PlanObjectSet& exprs) {
  PlanObjectSet columns;
  exprs.forEach<Expr>([&](ExprCP expr) { columns.unionSet(expr->columns()); });
  return columns;
}
} // namespace

Plan::Plan(RelationOpPtr op, const PlanState& state)
    : op(std::move(op)),
      cost(state.cost),
      tables(state.placed()),
      columns(exprColumns(state.targetExprs)),
      constraints(&this->op->constraints()) {
  // Park the per-leaf bucketed map on the Optimization side table keyed by
  // this Plan's address. Plans are sometimes allocated via the arena
  // (make<Plan>) where destructors don't run; storing the map externally
  // avoids leaking the F14FastMap's heap storage.
  state.optimization.planGroupedLeaves()[this] = state.currentGroupedLeaves();
}

bool Plan::isStateBetter(const PlanState& state, float margin) const {
  // Unknown costs are not comparable, so a plan with an unknown cost is never
  // declared better.
  return lessThan(add(state.cost.cost, margin), cost.cost);
}

std::string Plan::printCost() const {
  return cost.toString();
}

std::string Plan::toString(bool detail) const {
  queryCtx()->contextPlan() = const_cast<Plan*>(this);
  auto result = RelationOpPrinter::toText(*op, {.includeCost = detail});
  queryCtx()->contextPlan() = nullptr;
  return result;
}

bool PlanState::mayConsiderNext(PlanObjectCP table) const {
  if (!syntacticJoinOrder_) {
    return true;
  }

  const auto id = table->id();
  auto it = std::find(dt->joinOrder.begin(), dt->joinOrder.end(), id);
  if (it == dt->joinOrder.end()) {
    return true;
  }

  const auto end = it - dt->joinOrder.begin();
  for (auto i = 0; i < end; ++i) {
    if (!placed_.BitSet::contains(dt->joinOrder[i])) {
      return false;
    }
  }
  return true;
}

void PlanState::addNextJoin(
    const JoinCandidate* candidate,
    RelationOpPtr plan,
    std::vector<NextJoin>& toTry) const {
  VELOX_DCHECK(exprToColumn_.empty());
  if (!isOverBest()) {
    toTry.emplace_back(
        candidate,
        std::move(plan),
        cost,
        placed_,
        columns_,
        currentGroupedLeaves_);
  } else {
    optimization.trace(OptimizerOptions::kExceededBest, dt->id(), cost, *plan);
  }
}

void PlanState::setTargetExprsForDt(const PlanObjectSet& target) {
  for (auto i = 0; i < dt->columns.size(); ++i) {
    if (target.contains(dt->columns[i])) {
      targetExprs.add(dt->exprs[i]);
    }
  }
}

ExprCP PlanState::isDownstreamFilterOnly(ColumnCP column) const {
  ExprCP result = nullptr;

  for (const auto* conjunct : dt->conjuncts) {
    if (!placed_.contains(conjunct)) {
      const auto& columns = conjunct->columns();
      if (columns.size() == 1 && columns.onlyObject() == column) {
        if (result != nullptr) {
          // Found multiple conjuncts that use the column.
          return nullptr;
        }

        result = conjunct;
      }
    }
  }

  if (result == nullptr) {
    return nullptr;
  }

  if (computeDownstreamColumns(/*includeFilters=*/false).contains(column)) {
    // Column has non-filter usage.
    return nullptr;
  }

  return result;
}

const PlanObjectSet& PlanState::downstreamColumns() const {
  auto it = downstreamColumnsCache_.find(placed_);
  if (it != downstreamColumnsCache_.end()) {
    return it->second;
  }

  auto result = computeDownstreamColumns(/*includeFilters=*/true);

  return downstreamColumnsCache_[placed_] = std::move(result);
}

PlanObjectSet PlanState::computeDownstreamColumns(bool includeFilters) const {
  PlanObjectSet result;

  auto translateExpr = [&](ExprCP expr) {
    auto it = exprToColumn_.find(expr);
    if (it != exprToColumn_.end()) {
      return it->second;
    } else {
      return expr;
    }
  };

  auto addExpr = [&](ExprCP expr) { result.unionColumns(translateExpr(expr)); };

  auto addExprs = [&](const ExprVector& exprs) {
    for (auto expr : exprs) {
      addExpr(expr);
    }
  };

  // Joins.
  for (auto join : dt->joins) {
    if (join->isSemi() || join->isAnti()) {
      if (placed_.contains(join->rightTable())) {
        continue;
      }

      // For an unplaced exists/not exists downstream, we need the left side
      // columns but not the right side since nothing is projected out from the
      // right side.
      addExprs(join->leftKeys());

      if (!join->filter().empty()) {
        // If there is a filter, then the filter columns that do not come from
        // the right side are needed.
        for (auto& conjunct : join->filter()) {
          translateExpr(conjunct)->columns().forEach<Column>(
              [&](ColumnCP column) {
                if (column->relation() != join->rightTable()) {
                  result.add(column);
                }
              });
        }
      }
      continue;
    }

    bool addFilter = false;
    if (!placed_.contains(join->rightTable())) {
      addFilter = true;
      addExprs(join->leftKeys());
    }
    if (join->leftTable() && !placed_.contains(join->leftTable())) {
      addFilter = true;
      addExprs(join->rightKeys());
    }
    if (addFilter && !join->filter().empty()) {
      addExprs(join->filter());
    }

    if (addFilter) {
      addExprs(join->leftExprs());
      addExprs(join->rightExprs());
    }
  }

  // Filters.
  if (includeFilters) {
    for (const auto* conjunct : dt->conjuncts) {
      if (!placed_.contains(conjunct)) {
        addExpr(conjunct);
      }
    }
  }

  // Aggregations.
  if (dt->aggregation && !placed_.contains(dt->aggregation)) {
    auto aggToPlace = dt->aggregation;
    addExprs(aggToPlace->groupingKeys());
    for (auto& aggregate : aggToPlace->aggregates()) {
      addExpr(aggregate);
    }
  }

  // Window functions.
  if (dt->windowPlan) {
    for (const auto* func : dt->windowPlan->functions()) {
      if (!placed_.contains(func)) {
        addExpr(func);
      }
    }
  }

  // Filters after aggregation.
  for (const auto* conjunct : dt->having) {
    if (!placed_.contains(conjunct)) {
      addExpr(conjunct);
    }
  }

  // Order by.
  for (const auto* key : dt->orderKeys) {
    if (!placed_.contains(key)) {
      addExpr(key);
    }
  }

  // Write.
  if (dt->write) {
    VELOX_DCHECK(!placed_.contains(dt->write));
    addExprs(dt->write->columnExprs());
  }

  // Output expressions.
  targetExprs.forEach<Expr>([&](ExprCP expr) { addExpr(expr); });

  return result;
}

ExprCP PlanState::toColumn(ExprCP expr) const {
  auto it = exprToColumn_.find(expr);
  if (it != exprToColumn_.end()) {
    return it->second;
  } else {
    return expr;
  }
}

void PlanState::addExprToColumn(ExprCP expr, ExprCP column) {
  exprToColumn_[expr] = column;
}

std::string PlanState::printCost() const {
  return cost.toString();
}

std::string PlanState::printPlan(RelationOpPtr op, bool detail) const {
  auto plan = std::make_unique<Plan>(std::move(op), *this);
  return plan->toString(detail);
}

PlanP PlanSet::addPlan(RelationOpPtr plan, PlanState& state) {
  int32_t replaceIndex = -1;
  // Shuffle margin for cost comparisons. 0 when the cardinality is unknown: the
  // comparisons (isStateBetter) don't rank unknown costs anyway, so the margin
  // is moot.
  const float shuffle =
      shuffleCost(plan->columns()) * state.cost.cardinality.value_or(0);

  if (!plans.empty()) {
    // Compare with existing. If there is one with same distribution and new is
    // better, replace. If there is one with a different distribution and the
    // new one can produce the same distribution by repartition, for cheaper,
    // add the new one and delete the old one.
    for (auto i = 0; i < plans.size(); ++i) {
      auto old = plans[i].get();

      const bool newIsBetter = old->isStateBetter(state);
      const bool newIsBetterWithShuffle = old->isStateBetter(state, shuffle);
      const bool sameDist =
          old->op->distribution().isSamePartition(plan->distribution());
      const bool sameOrder =
          old->op->distribution().isSameOrder(plan->distribution());
      if (sameDist && sameOrder) {
        if (newIsBetter) {
          replaceIndex = i;
          continue;
        }
        // There's a better one with same dist and partition.
        return nullptr;
      }

      if (newIsBetterWithShuffle &&
          old->op->distribution().orderKeys().empty()) {
        // Old plan has no order and is worse than new plus shuffle. Can't win.
        // Erase.
        queryCtx()->optimization()->trace(
            OptimizerOptions::kExceededBest,
            state.dt->id(),
            old->cost,
            *old->op);
        plans.erase(plans.begin() + i);
        --i;
        continue;
      }

      if (plan->distribution().orderKeys().empty() &&
          !old->isStateBetter(state, -shuffle)) {
        // New has no order and old would beat it even after adding shuffle.
        return nullptr;
      }
    }
  }

  auto newPlan = std::make_unique<Plan>(std::move(plan), state);
  auto* result = newPlan.get();

  // bestCostWithShuffle tracks the cheapest known-cost plan; an unknown-cost
  // plan does not participate.
  if (result->cost.cost.has_value()) {
    bestCostWithShuffle =
        std::min(bestCostWithShuffle, *result->cost.cost + shuffle);
  }
  if (replaceIndex >= 0) {
    plans[replaceIndex] = std::move(newPlan);
  } else {
    plans.push_back(std::move(newPlan));
  }
  return result;
}

PlanP PlanSet::best(
    const std::optional<DesiredDistribution>& distribution,
    bool& needsShuffle) {
  VELOX_CHECK_GT(plans.size(), 0, "No plans to pick best from");

  PlanP best = nullptr;
  PlanP match = nullptr;
  float bestCost = -1;
  float matchCost = -1;

  const bool checkDistribution = !isSingleWorker() && distribution.has_value();

  for (const auto& plan : plans) {
    // Unknown-cost plans are not rankable by cost; skip them in cost-based
    // selection (handled by the fallback below).
    if (!plan->cost.cost.has_value()) {
      continue;
    }
    const float cost = *plan->cost.cost;

    auto update = [&](PlanP& current, float& currentCost) {
      if (!current || cost < currentCost) {
        current = plan.get();
        currentCost = cost;
      }
    };

    update(best, bestCost);
    if (checkDistribution &&
        plan->op->distribution().isCopartitionedWith(distribution.value())) {
      update(match, matchCost);
    }
  }

  // All plans have unknown cost (CBO bailed to syntactic order, which ranks
  // nothing). Cost-based selection is impossible, but distribution still
  // matters: prefer a plan already copartitioned with the requested
  // distribution; otherwise return one and flag that a shuffle is needed.
  if (best == nullptr) {
    if (checkDistribution) {
      for (const auto& plan : plans) {
        if (plan->op->distribution().isCopartitionedWith(
                distribution.value())) {
          return plan.get();
        }
      }
      needsShuffle = true;
    }
    return plans.front().get();
  }

  if (!checkDistribution || best == match) {
    return best;
  }

  if (match) {
    const float shuffle =
        shuffleCost(best->op->columns()) * best->cost.cardinality.value_or(0);
    if (matchCost <= bestCost + shuffle) {
      return match;
    }
  }

  needsShuffle = true;
  return best;
}

const JoinEdgeVector& joinedBy(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    return table->as<BaseTable>()->joinedBy;
  }

  if (table->is(PlanType::kValuesTableNode)) {
    return table->as<ValuesTable>()->joinedBy;
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    return table->as<UnnestTable>()->joinedBy;
  }

  VELOX_DCHECK(table->is(PlanType::kDerivedTableNode));
  return table->as<DerivedTable>()->joinedBy;
}

std::pair<JoinSide, JoinSide> JoinCandidate::joinSides() const {
  return {join->sideOf(tables[0], false), join->sideOf(tables[0], true)};
}

namespace {
bool hasEqual(ExprCP key, const ExprVector& keys) {
  if (key->isNot(PlanType::kColumnExpr) || !key->as<Column>()->equivalence()) {
    return false;
  }

  return std::ranges::any_of(
      keys, [&](ExprCP e) { return key->sameOrEqual(*e); });
}
} // namespace

void JoinCandidate::addEdge(
    PlanState& state,
    JoinEdgeP edge,
    PlanObjectCP joined) {
  auto newTableSide = edge->sideOf(joined);
  auto newPlacedSide = edge->sideOf(joined, true);
  VELOX_CHECK_NOT_NULL(newPlacedSide.table);
  if (!state.isPlaced(newPlacedSide.table)) {
    return;
  }

  const auto* joinSideKeys = compositeEdge ? &compositeEdge->rightKeys()
      : state.isPlaced(join->rightTable()) ? &join->leftKeys()
                                           : &join->rightKeys();

  bool newEdgeCounted = false;
  for (auto i = 0; i < newPlacedSide.keys.size(); ++i) {
    auto* key = newPlacedSide.keys[i];
    if (!hasEqual(key, *joinSideKeys)) {
      if (!compositeEdge) {
        // We make the coposite edge with the placed on the left and unplaced on
        // the right.
        compositeEdge = make<JoinEdge>(*join);
        if (state.isPlaced(join->rightTable())) {
          compositeEdge = JoinEdge::reverse(*compositeEdge);
        }
        join = compositeEdge;
      }
      if (!newEdgeCounted) {
        newEdgeCounted = true;
        const auto preFanout = join->lrFanout();
        auto [other, newFanout] = edge->otherTable(newPlacedSide.table);
        // We update the lr fanout. The rl fanout will not be used for an inner
        // join, so we set this to 1. The combined fanout is unknown if either
        // input fanout is unknown.
        join->setFanouts(
            minOf(mul(newFanout, preFanout), minOf(preFanout, newFanout)), 1);
        fanout = join->lrFanout();
      }
      join->addEquality(key, newTableSide.keys[i]);
    }
  }
}

bool JoinCandidate::isDominantEdge(PlanState& state, JoinEdgeP edge) {
  auto* joined = tables[0];
  auto newPlacedSide = edge->sideOf(joined, true);
  VELOX_CHECK_NOT_NULL(newPlacedSide.table);
  if (!state.isPlaced(newPlacedSide.table)) {
    return false;
  }
  auto tableSide = join->sideOf(joined);
  auto placedSide = join->sideOf(joined, true);
  for (auto i = 0; i < newPlacedSide.keys.size(); ++i) {
    auto* key = newPlacedSide.keys[i];
    if (!hasEqual(key, tableSide.keys)) {
      return false;
    }
  }
  return newPlacedSide.keys.size() > placedSide.keys.size();
}

std::string JoinCandidate::toString() const {
  std::stringstream out;
  if (join != nullptr) {
    out << join->toString() << " fanout ";
    if (fanout.has_value()) {
      out << *fanout;
    } else {
      out << "?";
    }
  } else {
    out << "x-join: " << tables[0]->toString();
  }

  for (auto i = 1; i < tables.size(); ++i) {
    out << " + " << tables[i]->toString();
  }

  if (!existences.empty()) {
    out << " exists " << existences[0].toString(false);
  }

  return out.str();
}

bool NextJoin::isWorse(const NextJoin& other) const {
  float shuffle = 0;
  if (!plan->distribution().isSamePartition(other.plan->distribution())) {
    // The shuffle margin only refines a comparison of known costs; an unknown
    // cardinality contributes 0.
    shuffle =
        other.cost.cardinality.value_or(0) * shuffleCost(other.plan->columns());
  }

  // Unknown costs are incomparable, so neither variant is declared worse.
  return lessThan(add(other.cost.cost, shuffle), cost.cost);
}

velox::core::JoinType reverseJoinType(velox::core::JoinType joinType) {
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return velox::core::JoinType::kRight;
    case velox::core::JoinType::kRight:
      return velox::core::JoinType::kLeft;
    case velox::core::JoinType::kLeftSemiFilter:
      return velox::core::JoinType::kRightSemiFilter;
    case velox::core::JoinType::kLeftSemiProject:
      return velox::core::JoinType::kRightSemiProject;
    default:
      return joinType;
  }
}

} // namespace facebook::axiom::optimizer
