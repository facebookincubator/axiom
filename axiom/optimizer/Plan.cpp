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

namespace facebook::axiom::optimizer {

namespace {

// True if single worker, i.e. do not plan remote exchanges
bool isSingleWorker() {
  return queryCtx()->optimization()->runnerOptions().numWorkers == 1;
}

} // namespace

PlanState::PlanState(Optimization& optimization, DerivedTableCP dt)
    : optimization(optimization),
      dt(dt),
      syntacticJoinOrder_{optimization.options().syntacticJoinOrder} {}

PlanState::PlanState(Optimization& optimization, DerivedTableCP dt, PlanP plan)
    : optimization(optimization),
      dt(dt),
      cost(plan->cost),
      syntacticJoinOrder_{optimization.options().syntacticJoinOrder} {}

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
      tables(state.placed),
      columns(exprColumns(state.targetExprs)),
      fullyImported(state.dt->fullyImported) {}

std::string Plan::printCost() const {
  return cost.toString(true, false);
}

std::string Plan::toString(bool detail) const {
  queryCtx()->contextPlan() = const_cast<Plan*>(this);
  auto result = op->toString(true, detail);
  queryCtx()->contextPlan() = nullptr;
  return result;
}

void PlanState::addCost(RelationOp& op) {
  VELOX_DCHECK_EQ(cost.inputCardinality, 1);
  cost.setupCost += op.cost().setupCost;
  cost.unitCost += op.cost().unitCost * op.cost().inputCardinality;
  cost.fanout = op.cost().resultCardinality();
  cost.totalBytes += op.cost().totalBytes;
  cost.transferBytes += op.cost().transferBytes;
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
    if (!placed.BitSet::contains(dt->joinOrder[i])) {
      return false;
    }
  }
  return true;
}

void PlanState::addNextJoin(
    const JoinCandidate* candidate,
    RelationOpPtr plan,
    HashBuildVector builds,
    std::vector<NextJoin>& toTry) const {
  if (!isOverBest()) {
    toTry.emplace_back(
        candidate, std::move(plan), cost, placed, columns, std::move(builds));
  } else {
    optimization.trace(OptimizerOptions::kExceededBest, dt->id(), cost, *plan);
  }
}

void PlanState::addBuilds(const HashBuildVector& added) {
  for (auto build : added) {
    if (std::find(builds.begin(), builds.end(), build) == builds.end()) {
      builds.push_back(build);
    }
  }
}

void PlanState::setTargetExprsForDt(const PlanObjectSet& target) {
  for (auto i = 0; i < dt->columns.size(); ++i) {
    if (target.contains(dt->columns[i])) {
      targetExprs.add(dt->exprs[i]);
    }
  }
}

const PlanObjectSet& PlanState::downstreamColumns() const {
  auto it = downstreamColumnsCache_.find(placed);
  if (it != downstreamColumnsCache_.end()) {
    return it->second;
  }

  PlanObjectSet result;

  auto addExpr = [&](ExprCP expr) {
    auto it = exprToColumn.find(expr);
    if (it != exprToColumn.end()) {
      result.unionColumns(it->second);
    } else {
      result.unionColumns(expr);
    }
  };

  auto addExprs = [&](ExprVector exprs) {
    for (auto expr : exprs) {
      addExpr(expr);
    }
  };

  // Joins.
  for (auto join : dt->joins) {
    bool addFilter = false;
    if (!placed.contains(join->rightTable())) {
      addFilter = true;
      addExprs(join->leftKeys());
    }
    if (join->leftTable() && !placed.contains(join->leftTable())) {
      addFilter = true;
      addExprs(join->rightKeys());
    }
    if (addFilter && !join->filter().empty()) {
      addExprs(join->filter());
    }
  }

  // Filters.
  for (const auto* conjunct : dt->conjuncts) {
    if (!placed.contains(conjunct)) {
      addExpr(conjunct);
    }
  }

  // Aggregations.
  if (dt->aggregation && !placed.contains(dt->aggregation)) {
    auto aggToPlace = dt->aggregation;
    addExprs(aggToPlace->groupingKeys());
    for (auto& aggregate : aggToPlace->aggregates()) {
      addExpr(aggregate);
    }
  }

  // Filters after aggregation.
  for (const auto* conjunct : dt->having) {
    if (!placed.contains(conjunct)) {
      addExpr(conjunct);
    }
  }

  // Order by.
  for (const auto* key : dt->orderKeys) {
    if (!placed.contains(key)) {
      addExpr(key);
    }
  }

  // Write.
  if (dt->write) {
    VELOX_DCHECK(!placed.contains(dt->write));
    addExprs(dt->write->columnExprs());
  }

  // Output expressions.
  targetExprs.forEach<Expr>([&](ExprCP expr) { addExpr(expr); });

  return downstreamColumnsCache_[placed] = std::move(result);
}

ExprCP PlanState::toColumn(ExprCP expr) const {
  auto it = exprToColumn.find(expr);
  if (it != exprToColumn.end()) {
    return it->second;
  } else {
    return expr;
  }
}

std::string PlanState::printCost() const {
  return cost.toString(true, true);
}

std::string PlanState::printPlan(RelationOpPtr op, bool detail) const {
  auto plan = std::make_unique<Plan>(std::move(op), *this);
  return plan->toString(detail);
}

PlanP PlanSet::addPlan(RelationOpPtr plan, PlanState& state) {
  const float shuffleCostPerRow = shuffleCost(plan->columns());

  // Determine is old plan worse the new one in all aspects.
  auto isWorse = [&](const Plan& old) {
    if (plan->distribution().needsSort(old.op->distribution())) {
      // New plan needs a sort to match the old one, so cannot compare.
      return false;
    }
    const bool needsShuffle =
        plan->distribution().needsShuffle(old.op->distribution());
    return old.cost.cost() >
        state.cost.cost(needsShuffle ? shuffleCostPerRow : 0);
  };

  // Determine is old plan better than the new one in all aspects.
  auto isBetter = [&](const Plan& old) {
    if (old.op->distribution().needsSort(plan->distribution())) {
      // Old plan needs a sort to match the new one, so cannot compare.
      return false;
    }
    const bool needsShuffle =
        old.op->distribution().needsShuffle(plan->distribution());
    return state.cost.cost() >
        old.cost.cost(needsShuffle ? shuffleCost(old.op->columns()) : 0);
  };

  // Compare with existing plans.
  const auto plansSize = plans.size();
  enum {
    kFoundWorse = -1,
    kNone = 0,
    kFoundBetter = 1,
  };
  auto found = kNone;
  for (size_t i = 0; i < plans.size(); ++i) {
    const auto& old = *plans[i];
    if (old.input != state.input) {
      // Different plans, cannot compare.
      continue;
    }
    if (isWorse(old)) {
      // Remove old plan, it is worse than the new one in all aspects.
      queryCtx()->optimization()->trace(
          OptimizerOptions::kExceededBest, state.dt->id(), old.cost, *old.op);
      std::swap(plans[i], plans.back());
      plans.pop_back();
      --i;
      found = kFoundWorse;
    } else if (found == kNone && isBetter(old)) {
      // Old plan is better than the new one in all aspects.
      found = kFoundBetter;
    }
  }
  if (found == kFoundBetter) {
    // No existing plan was worse than the new one in all aspects,
    // and at least one existing plan is better than the new one in all aspects.
    // So don't add the new plan.
    return nullptr;
  }

  auto newPlan = std::make_unique<Plan>(std::move(plan), state);
  auto* result = newPlan.get();
  const auto newPlanCost = result->cost.cost(shuffleCostPerRow);
  bestCostWithShuffle = std::min(bestCostWithShuffle, newPlanCost);
  plans.push_back(std::move(newPlan));
  return result;
}

PlanP PlanSet::best(const Distribution& desired, bool& needsShuffle) {
  // TODO: Consider desired order here too.
  PlanP best = nullptr;
  PlanP match = nullptr;
  float bestCost = -1;
  float matchCost = -1;

  const bool single = isSingleWorker();

  for (const auto& plan : plans) {
    const float cost = plan->cost.cost();

    auto update = [&](PlanP& current, float& currentCost) {
      if (!current || cost < currentCost) {
        current = plan.get();
        currentCost = cost;
      }
    };

    update(best, bestCost);
    if (!single && !plan->op->distribution().needsShuffle(desired)) {
      update(match, matchCost);
    }
  }

  VELOX_DCHECK_NOT_NULL(best);

  if (single || best == match) {
    return best;
  }

  if (match) {
    const float bestCostWithShuffle =
        best->cost.cost(shuffleCost(best->op->columns()));
    if (matchCost <= bestCostWithShuffle) {
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

void JoinCandidate::addEdge(PlanState& state, JoinEdgeP edge) {
  auto* joined = tables[0];
  auto newTableSide = edge->sideOf(joined);
  auto newPlacedSide = edge->sideOf(joined, true);
  VELOX_CHECK_NOT_NULL(newPlacedSide.table);
  if (!state.placed.contains(newPlacedSide.table)) {
    return;
  }

  auto tableSide = join->sideOf(joined);
  auto placedSide = join->sideOf(joined, true);
  for (auto i = 0; i < newPlacedSide.keys.size(); ++i) {
    auto* key = newPlacedSide.keys[i];
    if (!hasEqual(key, tableSide.keys)) {
      if (!compositeEdge) {
        compositeEdge = make<JoinEdge>(*join);
        join = compositeEdge;
      }
      auto [other, preFanout] = join->otherTable(placedSide.table);
      // do not recompute a fanout after adding more equalities. This makes the
      // join edge non-binary and it cannot be sampled.
      join->setFanouts(join->rlFanout(), join->lrFanout());
      if (joined == join->rightTable()) {
        join->addEquality(key, newTableSide.keys[i]);
      } else {
        join->addEquality(newTableSide.keys[i], key);
      }
      auto [other2, postFanout] = join->otherTable(placedSide.table);
      auto change = postFanout > 0 ? preFanout / postFanout : 0;
      fanout = change > 0 ? fanout / change : preFanout / 2;
    }
  }
}

bool JoinCandidate::isDominantEdge(PlanState& state, JoinEdgeP edge) {
  auto* joined = tables[0];
  auto newPlacedSide = edge->sideOf(joined, true);
  VELOX_CHECK_NOT_NULL(newPlacedSide.table);
  if (!state.placed.contains(newPlacedSide.table)) {
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
    out << join->toString() << " fanout " << fanout;
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
  if (other.plan->distribution().needsSort(plan->distribution())) {
    // 'other' needs a sort to match 'plan', so cannot compare.
    return false;
  }
  const auto needsShuffle =
      other.plan->distribution().needsShuffle(plan->distribution());
  return cost.cost() >
      other.cost.cost(needsShuffle ? shuffleCost(other.plan->columns()) : 0);
}

size_t MemoKey::hash() const {
  size_t hash = tables.hash();
  for (auto& exists : existences) {
    hash = velox::bits::commutativeHashMix(hash, exists.hash());
  }
  return hash;
}

bool MemoKey::operator==(const MemoKey& other) const {
  if (firstTable == other.firstTable && columns == other.columns &&
      tables == other.tables) {
    if (existences.size() != other.existences.size()) {
      return false;
    }
    for (auto& e : existences) {
      for (auto& e2 : other.existences) {
        if (e2 == e) {
          break;
        }
      }
    }
    return true;
  }
  return false;
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
