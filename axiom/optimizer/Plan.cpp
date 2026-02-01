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

PlanState::PlanState(Optimization& optimization, DerivedTableCP dt)
    : optimization(optimization),
      dt(dt),
      syntacticJoinOrder_{optimization.options().syntacticJoinOrder()} {}

PlanState::PlanState(Optimization& optimization, DerivedTableCP dt, PlanP plan)
    : optimization(optimization),
      dt(dt),
      cost(plan->cost),
      syntacticJoinOrder_{optimization.options().syntacticJoinOrder()} {
  // TODO Remove std::max after fixing that we miss PrecomputeProjection costs.
  // VELOX_DCHECK_EQ(
  //     std::max<float>(1, cost.cardinality), plan->op->resultCardinality());
}

void PlanState::save(PlanStateSaver& saver) const {
  saver.placed = placed_;
  saver.columns = columns_;
  saver.cost = cost;
  saver.exprToColumn = exprToColumn_;
}

void PlanState::restore(PlanStateSaver& saver) {
  placed_ = std::move(saver.placed);
  columns_ = std::move(saver.columns);
  cost = saver.cost;
  exprToColumn_ = std::move(saver.exprToColumn);
}

void PlanState::restore(NextJoin& nextJoin) {
  placed_ = std::move(nextJoin.placed);
  columns_ = std::move(nextJoin.columns);
  cost = nextJoin.cost;
  exprToColumn_.clear();
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
      columns(exprColumns(state.targetExprs)) {}

std::string Plan::printCost() const {
  return cost.toString();
}

std::string Plan::toString(bool detail) const {
  queryCtx()->contextPlan() = const_cast<Plan*>(this);
  auto result = op->toString(true, detail);
  queryCtx()->contextPlan() = nullptr;
  return result;
}

bool PlanState::mayConsiderNext(PlanObjectCP table) const {
  if (!syntacticJoinOrder_) {
    return true;
  }
  const auto tableId = table->id();
  for (const auto id : dt->joinOrder) {
    if (id == tableId) {
      return true;
    }
    if (!placed_.BitSet::contains(id)) {
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
    toTry.emplace_back(candidate, std::move(plan), cost, placed_, columns_);
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
  // Joins.
  for (auto join : dt->joins) {
    const bool rightPlaced = placed_.contains(join->rightTable());
    const bool leftPlaced = placed_.contains(join->leftTable());
    auto addFilter = [&](PlanObjectCP placedTable) {
      for (auto& conjunct : join->filter()) {
        translateExpr(conjunct)->columns().forEach<Column>(
            [&](ColumnCP column) {
              if (column->relation() == placedTable) {
                result.add(column);
              }
            });
      }
    };
    if (rightPlaced && leftPlaced) {
      continue;
    }
    if (rightPlaced) {
      addExprs(join->rightKeys());
      addFilter(join->rightTable());
      continue;
    }
    if (leftPlaced) {
      addExprs(join->leftKeys());
      addFilter(join->leftTable());
      continue;
    }
    addExprs(join->leftKeys());
    addExprs(join->rightKeys());
    addExprs(join->filter());
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
  const float shuffleCostPerRow = shuffleCost(plan->columns());

  // Determine is old plan worse the new one in all aspects.
  auto isWorse = [&](const Plan& old) {
    if (plan->distribution().needsSort(old.op->distribution())) {
      // New plan needs a sort to match the old one, so cannot compare.
      return false;
    }
    const bool needsShuffle =
        plan->distribution().needsShuffle(old.op->distribution());
    auto newCost = state.cost.totalCost(needsShuffle ? shuffleCostPerRow : 0);
    return old.cost.cost > newCost;
  };

  // Determine is old plan better than the new one in all aspects.
  auto isBetter = [&](const Plan& old) {
    if (old.op->distribution().needsSort(plan->distribution())) {
      // Old plan needs a sort to match the new one, so cannot compare.
      return false;
    }
    const bool needsShuffle =
        old.op->distribution().needsShuffle(plan->distribution());
    const auto oldCost =
        old.cost.totalCost(needsShuffle ? shuffleCost(old.op->columns()) : 0);
    return state.cost.cost >= oldCost;
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

  const auto newPlanCost = result->cost.totalCost(shuffleCostPerRow);
  bestCostWithShuffle = std::min(bestCostWithShuffle, newPlanCost);
  plans.push_back(std::move(newPlan));
  return result;
}

PlanP PlanSet::best(const Distribution* desired, bool& needsShuffle) {
  // TODO: Consider desired order here too.
  PlanP best = nullptr;
  PlanP match = nullptr;
  float bestCost = -1;
  float matchCost = -1;

  const bool checkDistribution = desired && !isSingleWorker();

  for (const auto& plan : plans) {
    const float cost = plan->cost.cost;

    auto update = [&](PlanP& current, float& currentCost) {
      if (!current || cost < currentCost) {
        current = plan.get();
        currentCost = cost;
      }
    };

    update(best, bestCost);
    if (checkDistribution && !plan->op->distribution().needsShuffle(*desired)) {
      update(match, matchCost);
    }
  }

  VELOX_DCHECK_NOT_NULL(best);

  if (!checkDistribution || best == match) {
    return best;
  }

  if (match) {
    const float bestCostWithShuffle =
        best->cost.totalCost(shuffleCost(best->op->columns()));
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
bool hasEqual(ExprCP key, CPSpan<Expr> keys) {
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
        auto preFanout = join->lrFanout();
        auto [other, newFanout] = edge->otherTable(newPlacedSide.table);
        // We update the lr fanout. The rl fanout will not be used for an inner
        // join, so we set this to 1.
        join->setFanouts(
            std::min({newFanout * preFanout, preFanout, newFanout}), 1);
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
  return cost.cost >=
      other.cost.totalCost(
          needsShuffle ? shuffleCost(other.plan->columns()) : 0);
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

std::string MemoKey::toString() const {
  return fmt::format(
      "MemoKey({}, columns: {}, tables: {})",
      cname(firstTable),
      columns.toString(true),
      tables.toString(true));
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
    case velox::core::JoinType::kRightSemiFilter:
      return velox::core::JoinType::kLeftSemiFilter;
    case velox::core::JoinType::kRightSemiProject:
      return velox::core::JoinType::kLeftSemiProject;
    default:
      return joinType;
  }
}

} // namespace facebook::axiom::optimizer
