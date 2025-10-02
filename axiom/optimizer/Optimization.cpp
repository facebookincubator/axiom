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

#include "axiom/optimizer/Optimization.h"
#include <algorithm>
#include <iostream>
#include <utility>
#include "axiom/optimizer/PrecomputeProjection.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer {

Optimization::Optimization(
    const logical_plan::LogicalPlanNode& logicalPlan,
    const Schema& schema,
    History& history,
    std::shared_ptr<velox::core::QueryCtx> veloxQueryCtx,
    velox::core::ExpressionEvaluator& evaluator,
    OptimizerOptions options,
    runner::MultiFragmentPlan::Options runnerOptions)
    : options_(std::move(options)),
      runnerOptions_(std::move(runnerOptions)),
      isSingleWorker_(runnerOptions_.numWorkers == 1),
      logicalPlan_(&logicalPlan),
      history_(history),
      veloxQueryCtx_(std::move(veloxQueryCtx)),
      toGraph_{schema, evaluator, options_},
      toVelox_{runnerOptions_, options_} {
  queryCtx()->optimization() = this;
  root_ = toGraph_.makeQueryGraph(*logicalPlan_);
  root_->distributeConjuncts();
  root_->addImpliedJoins();
  root_->linkTablesToJoins();
  for (auto* join : root_->joins) {
    join->guessFanout();
  }
  toGraph_.setDtOutput(root_, *logicalPlan_);
}

// static
PlanAndStats Optimization::toVeloxPlan(
    const logical_plan::LogicalPlanNode& logicalPlan,
    velox::memory::MemoryPool& pool,
    OptimizerOptions options,
    runner::MultiFragmentPlan::Options runnerOptions) {
  auto allocator = std::make_unique<velox::HashStringAllocator>(&pool);
  auto context = std::make_unique<QueryGraphContext>(*allocator);
  queryCtx() = context.get();
  SCOPE_EXIT {
    queryCtx() = nullptr;
  };

  auto veloxQueryCtx = velox::core::QueryCtx::create();
  velox::exec::SimpleExpressionEvaluator evaluator(veloxQueryCtx.get(), &pool);

  auto schemaResolver = std::make_shared<connector::SchemaResolver>();

  VeloxHistory history;

  Schema schema("default", schemaResolver.get(), /* locus */ nullptr);

  Optimization opt{
      logicalPlan,
      schema,
      history,
      veloxQueryCtx,
      evaluator,
      std::move(options),
      std::move(runnerOptions)};

  auto best = opt.bestPlan();
  return opt.toVeloxPlan(best->op);
}

void Optimization::trace(
    uint32_t event,
    int32_t id,
    const Cost& cost,
    RelationOp& plan) const {
  if (event & options_.traceFlags) {
    std::cout << (event == OptimizerOptions::kRetained ? "Retained: "
                                                       : "Abandoned: ")
              << id << ": " << cost.toString(true, true) << ": " << " "
              << plan.toString(true, false) << std::endl;
  }
}

PlanP Optimization::bestPlan() {
  PlanObjectSet targetColumns;
  targetColumns.unionObjects(root_->columns);

  topState_.dt = root_;
  topState_.setTargetExprsForDt(targetColumns);

  makeJoins(topState_);

  return topState_.plans.best();
}

namespace {
// Traverses joins from 'candidate'. Follows any join that goes to a table not
// in 'visited' with a fanout < 'maxFanout'. 'fanoutFromRoot' is the product of
// the fanouts between 'candidate' and the 'candidate' of the top level call to
// this. 'path' is the set of joined tables between this invocation and the top
// level. 'fanoutFromRoot' is thus the selectivity of the linear join sequence
// in 'path'. When a reducing join sequence is found, the tables on the path
// are added to 'result'. 'reduction' is the product of the fanouts of all the
// reducing join paths added to 'result'.
void reducingJoinsRecursive(
    const PlanState& state,
    PlanObjectCP candidate,
    float fanoutFromRoot,
    float maxFanout,
    std::vector<PlanObjectCP>& path,
    PlanObjectSet& visited,
    PlanObjectSet& result,
    float& reduction,
    const std::function<
        void(const std::vector<PlanObjectCP>& path, float reduction)>&
        resultFunc = {}) {
  bool isLeaf = true;
  for (auto join : joinedBy(candidate)) {
    if (join->leftOptional() || join->rightOptional()) {
      continue;
    }
    JoinSide other = join->sideOf(candidate, true);
    if (!state.dt->hasTable(other.table) || !state.dt->hasJoin(join)) {
      continue;
    }
    if (other.table->isNot(PlanType::kTableNode) &&
        other.table->isNot(PlanType::kValuesTableNode) &&
        other.table->isNot(PlanType::kUnnestTableNode)) {
      continue;
    }
    if (visited.contains(other.table)) {
      continue;
    }
    if (other.fanout > maxFanout) {
      continue;
    }
    visited.add(other.table);
    auto fanout = fanoutFromRoot * other.fanout;
    if (fanout < 0.9) {
      result.add(other.table);
      for (auto step : path) {
        result.add(step);
        maxFanout = 1;
      }
    }
    path.push_back(other.table);
    isLeaf = false;
    reducingJoinsRecursive(
        state,
        other.table,
        fanout,
        maxFanout,
        path,
        visited,
        result,
        reduction,
        resultFunc);
    path.pop_back();
  }
  if (fanoutFromRoot < 1 && isLeaf) {
    // We are at the end of a reducing sequence of joins. Update the total
    // fanout for the set of all reducing join paths from the top level
    // 'candidate'.
    reduction *= fanoutFromRoot;
    if (resultFunc) {
      resultFunc(path, fanoutFromRoot);
    }
  }
}

// For an inner join, see if can bundle reducing joins on the build.
std::optional<JoinCandidate> reducingJoins(
    const PlanState& state,
    const JoinCandidate& candidate) {
  std::vector<PlanObjectCP> tables;
  std::vector<PlanObjectSet> existences;
  float fanout = candidate.fanout;

  PlanObjectSet reducingSet;
  if (candidate.join->isInner()) {
    PlanObjectSet visited = state.placed;
    VELOX_DCHECK(!candidate.tables.empty());
    visited.add(candidate.tables[0]);
    reducingSet.add(candidate.tables[0]);
    std::vector<PlanObjectCP> path{candidate.tables[0]};
    float reduction = 1;
    reducingJoinsRecursive(
        state,
        candidate.tables[0],
        1,
        1.2,
        path,
        visited,
        reducingSet,
        reduction);
    if (reduction < 0.9) {
      // The only table in 'candidate' must be first in the bushy table list.
      tables = candidate.tables;
      reducingSet.forEach([&](auto object) {
        if (object != tables[0]) {
          tables.push_back(object);
        }
      });
      fanout = candidate.fanout * reduction;
    }
  }

  if (!state.dt->noImportOfExists) {
    PlanObjectSet exists;
    float reduction = 1;
    VELOX_DCHECK(!candidate.tables.empty());
    std::vector<PlanObjectCP> path{candidate.tables[0]};
    // Look for reducing joins that were not added before, also covering already
    // placed tables. This may copy reducing joins from a probe to the
    // corresponding build.
    reducingSet.add(candidate.tables[0]);
    reducingSet.unionSet(state.dt->importedExistences);
    reducingJoinsRecursive(
        state,
        candidate.tables[0],
        1,
        1.2,
        path,
        reducingSet,
        exists,
        reduction,
        [&](auto& path, float reduction) {
          if (reduction < 0.7) {
            // The original table is added to the reducing existences because
            // the path starts with it but it is not joined twice since it
            // already is the start of the main join.
            PlanObjectSet added;
            for (auto i = 1; i < path.size(); ++i) {
              added.add(path[i]);
            }
            existences.push_back(std::move(added));
          }
        });
  }

  if (tables.empty() && existences.empty()) {
    // No reduction.
    return std::nullopt;
  }

  if (tables.empty()) {
    // No reducing joins but reducing existences from probe side.
    tables = candidate.tables;
  }

  JoinCandidate reducing(candidate.join, tables[0], fanout);
  reducing.tables = std::move(tables);
  reducing.existences = std::move(existences);
  return reducing;
}

// Calls 'func' with join, joined table and fanout for the joinable tables.
template <typename Func>
void forJoinedTables(const PlanState& state, Func func) {
  folly::F14FastSet<JoinEdgeP> visited;
  state.placed.forEach([&](PlanObjectCP placedTable) {
    if (!placedTable->isTable()) {
      return;
    }

    for (auto join : joinedBy(placedTable)) {
      if (join->isNonCommutative()) {
        if (!visited.insert(join).second) {
          continue;
        }
        bool usable = true;
        for (auto key : join->leftKeys()) {
          if (!key->allTables().isSubset(state.placed)) {
            // All items that the left key depends on must be placed.
            usable = false;
            break;
          }
        }
        if (usable) {
          func(join, join->rightTable(), join->lrFanout());
        }
      } else {
        auto [table, fanout] = join->otherTable(placedTable);
        if (!state.dt->hasTable(table)) {
          continue;
        }
        func(join, table, fanout);
      }
    }
  });
}

bool addExtraEdges(PlanState& state, JoinCandidate& candidate) {
  // See if there are more join edges from the first of 'candidate' to already
  // placed tables. Fill in the non-redundant equalities into the join edge.
  // Make a new edge if the edge would be altered.
  auto* originalJoin = candidate.join;
  auto* table = candidate.tables[0];
  for (auto* otherJoin : joinedBy(table)) {
    if (otherJoin == originalJoin || !otherJoin->isInner()) {
      continue;
    }
    auto [otherTable, fanout] = otherJoin->otherTable(table);
    if (!state.dt->hasTable(otherTable)) {
      continue;
    }
    if (candidate.isDominantEdge(state, otherJoin)) {
      return false;
    }
    candidate.addEdge(state, otherJoin);
  }
  return true;
}
} // namespace

std::vector<JoinCandidate> Optimization::nextJoins(PlanState& state) {
  std::vector<JoinCandidate> candidates;
  candidates.reserve(state.dt->tables.size());
  forJoinedTables(
      state, [&](JoinEdgeP join, PlanObjectCP joined, float fanout) {
        if (!state.placed.contains(joined) && state.dt->hasJoin(join) &&
            state.dt->hasTable(joined)) {
          candidates.emplace_back(join, joined, fanout);
          if (join->isInner()) {
            if (!addExtraEdges(state, candidates.back())) {
              // Drop the candidate if the edge was a subsumed in some other
              // edge.
              candidates.pop_back();
            }
          }
        }
      });

  // Take the  first hand joined tables and bundle them with reducing joins that
  // can go on the build side.
  std::vector<JoinCandidate> bushes;
  for (auto& candidate : candidates) {
    if (auto bush = reducingJoins(state, candidate)) {
      bushes.push_back(std::move(bush.value()));
    }
  }

  candidates.insert(candidates.begin(), bushes.begin(), bushes.end());
  std::ranges::sort(
      candidates, [](const JoinCandidate& left, const JoinCandidate& right) {
        return left.fanout < right.fanout;
      });
  if (candidates.empty()) {
    // There are no join edges. There could still be cross joins.
    state.dt->startTables.forEach([&](PlanObjectCP object) {
      if (!state.placed.contains(object)) {
        candidates.emplace_back(nullptr, object, tableCardinality(object));
      }
    });
  }
  return candidates;
}

namespace {
constexpr uint32_t kNotFound = ~0U;

/// Returns index of 'expr' in collection 'exprs'. kNotFound if not found.
/// Compares with equivalence classes, so that equal columns are
/// interchangeable.
template <typename V>
uint32_t position(const V& exprs, const Expr& expr) {
  for (auto i = 0; i < exprs.size(); ++i) {
    if (exprs[i]->sameOrEqual(expr)) {
      return i;
    }
  }
  return kNotFound;
}

/// Returns index of 'expr' in collection 'exprs'. kNotFound if not found.
/// Compares with equivalence classes, so that equal columns are
/// interchangeable. Applies 'getter' to each element of 'exprs' before
/// comparison.
template <typename V, typename Getter>
uint32_t position(const V& exprs, Getter getter, const Expr& expr) {
  for (auto i = 0; i < exprs.size(); ++i) {
    if (getter(exprs[i])->sameOrEqual(expr)) {
      return i;
    }
  }
  return kNotFound;
}

// True if single worker, i.e. do not plan remote exchanges
bool isSingleWorker() {
  return queryCtx()->optimization()->runnerOptions().numWorkers == 1;
}

RelationOpPtr repartitionForAgg(const RelationOpPtr& plan, PlanState& state) {
  // No shuffle if all grouping keys are in partitioning.
  if (isSingleWorker() || plan->distribution().distributionType.isGather) {
    return plan;
  }

  const auto* agg = state.dt->aggregation;

  // If no grouping and not yet gathered on a single node, add a gather before
  // final agg.
  if (agg->groupingKeys().empty() &&
      !plan->distribution().distributionType.isGather) {
    auto* gather =
        make<Repartition>(plan, Distribution::gather(), plan->columns());
    state.addCost(*gather);
    return gather;
  }

  // 'intermediateColumns' contains grouping keys followed by partial agg
  // results.
  ExprVector keyValues;
  for (auto i = 0; i < agg->groupingKeys().size(); ++i) {
    keyValues.push_back(agg->intermediateColumns()[i]);
  }

  bool shuffle = false;
  for (auto& key : keyValues) {
    auto nthKey = position(plan->distribution().partition, *key);
    if (nthKey == kNotFound) {
      shuffle = true;
      break;
    }
  }
  if (!shuffle) {
    return plan;
  }

  Distribution distribution{
      plan->distribution().distributionType, std::move(keyValues)};
  auto* repartition =
      make<Repartition>(plan, std::move(distribution), plan->columns());
  state.addCost(*repartition);
  return repartition;
}

CPSpan<Column> leadingColumns(const ExprVector& exprs) {
  size_t i = 0;
  for (; i < exprs.size(); ++i) {
    if (exprs[i]->isNot(PlanType::kColumnExpr)) {
      break;
    }
  }
  return {reinterpret_cast<ColumnCP const*>(exprs.data()), i};
}

bool isIndexColocated(
    const IndexInfo& info,
    const ExprVector& lookupValues,
    const RelationOpPtr& input) {
  const auto& distribution = info.index->distribution;
  if (distribution.isBroadcast &&
      input->distribution().distributionType.locus ==
          distribution.distributionType.locus) {
    return true;
  }

  // True if 'input' is partitioned so that each partitioning key is joined to
  // the corresponding partition key in 'info'.
  if (input->distribution().distributionType != distribution.distributionType) {
    return false;
  }

  if (input->distribution().partition.empty()) {
    return false;
  }

  if (input->distribution().partition.size() != distribution.partition.size()) {
    return false;
  }

  for (auto i = 0; i < input->distribution().partition.size(); ++i) {
    auto nthKey = position(lookupValues, *input->distribution().partition[i]);
    if (nthKey != kNotFound) {
      if (info.schemaColumn(info.lookupKeys.at(nthKey)) !=
          distribution.partition.at(i)) {
        return false;
      }
    } else {
      return false;
    }
  }
  return true;
}

RelationOpPtr repartitionForIndex(
    const IndexInfo& info,
    const ExprVector& lookupValues,
    const RelationOpPtr& plan,
    PlanState& state) {
  if (isSingleWorker() || isIndexColocated(info, lookupValues, plan)) {
    return plan;
  }

  const auto& distribution = info.index->distribution;

  ExprVector keyExprs;
  auto& partition = distribution.partition;
  for (auto key : partition) {
    // partition is in schema columns, lookupKeys is in BaseTable columns. Use
    // the schema column of lookup key for matching.
    auto nthKey = position(
        info.lookupKeys,
        [](auto c) {
          return c->is(PlanType::kColumnExpr)
              ? c->template as<Column>()->schemaColumn()
              : c;
        },
        *key);
    if (nthKey == kNotFound) {
      return nullptr;
    }

    keyExprs.push_back(lookupValues[nthKey]);
  }

  auto* repartition = make<Repartition>(
      plan,
      Distribution{distribution.distributionType, std::move(keyExprs)},
      plan->columns());
  state.addCost(*repartition);
  return repartition;
}

float fanoutJoinTypeLimit(velox::core::JoinType joinType, float fanout) {
  switch (joinType) {
    case velox::core::JoinType::kLeft:
      return std::max<float>(1, fanout);
    case velox::core::JoinType::kLeftSemiFilter:
      return std::min<float>(1, fanout);
    case velox::core::JoinType::kAnti:
      return 1 - std::min<float>(1, fanout);
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kRightSemiProject:
      return 1;
    default:
      return fanout;
  }
}

// Returns the positions in 'keys' for the expressions that determine the
// partition. empty if the partition is not decided by 'keys'
std::vector<uint32_t> joinKeyPartition(
    const RelationOpPtr& op,
    const ExprVector& keys) {
  std::vector<uint32_t> positions;
  for (unsigned i = 0; i < op->distribution().partition.size(); ++i) {
    auto nthKey = position(keys, *op->distribution().partition[i]);
    if (nthKey == kNotFound) {
      return {};
    }
    positions.push_back(nthKey);
  }
  return positions;
}

PlanObjectSet availableColumns(PlanObjectCP object) {
  PlanObjectSet set;
  if (object->is(PlanType::kTableNode)) {
    set.unionObjects(object->as<BaseTable>()->columns);
  } else if (object->is(PlanType::kValuesTableNode)) {
    set.unionObjects(object->as<ValuesTable>()->columns);
  } else if (object->is(PlanType::kUnnestTableNode)) {
    set.unionObjects(object->as<UnnestTable>()->columns);
  } else if (object->is(PlanType::kDerivedTableNode)) {
    set.unionObjects(object->as<DerivedTable>()->columns);
  } else {
    VELOX_UNREACHABLE("Joinable must be a table or derived table");
  }
  return set;
}

PlanObjectSet availableColumns(BaseTableCP baseTable, ColumnGroupCP index) {
  // The columns of base table that exist in 'index'.
  PlanObjectSet result;
  for (auto column : index->columns) {
    for (auto baseColumn : baseTable->columns) {
      if (baseColumn->name() == column->name()) {
        result.add(baseColumn);
        break;
      }
    }
  }
  return result;
}

bool isBroadcastableSize(PlanP build, PlanState& /*state*/) {
  return build->cost.fanout < 100'000;
}

// The 'other' side gets shuffled to align with 'input'. If 'input' is not
// partitioned on its keys, shuffle the 'input' too.
void alignJoinSides(
    RelationOpPtr& input,
    const ExprVector& keys,
    PlanState& state,
    RelationOpPtr& otherInput,
    const ExprVector& otherKeys,
    PlanState& otherState) {
  auto part = joinKeyPartition(input, keys);
  if (part.empty()) {
    Distribution distribution{
        otherInput->distribution().distributionType, keys};
    auto* repartition =
        make<Repartition>(input, std::move(distribution), input->columns());
    state.addCost(*repartition);
    input = repartition;
  }

  ExprVector distColumns;
  for (size_t i = 0; i < keys.size(); ++i) {
    auto nthKey = position(input->distribution().partition, *keys[i]);
    if (nthKey != kNotFound) {
      if (distColumns.size() <= nthKey) {
        distColumns.resize(nthKey + 1);
      }
      distColumns[nthKey] = otherKeys[i];
    }
  }

  Distribution distribution{
      input->distribution().distributionType, std::move(distColumns)};
  auto* repartition = make<Repartition>(
      otherInput, std::move(distribution), otherInput->columns());
  otherState.addCost(*repartition);
  otherInput = repartition;
}

bool isRedundantProject(
    const RelationOpPtr& input,
    const ExprVector& exprs,
    const ColumnVector& columns) {
  const auto& inputColumns = input->columns();

  if (inputColumns.size() != exprs.size()) {
    return false;
  }

  for (auto i = 0; i < inputColumns.size(); ++i) {
    if (inputColumns[i] != exprs[i]) {
      return false;
    }

    if (inputColumns[i]->outputName() != columns[i]->outputName()) {
      return false;
    }
  }

  return true;
}

} // namespace

void Optimization::addPostprocess(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  if (dt->aggregation) {
    addAggregation(dt, plan, state);
  }

  if (!dt->having.empty()) {
    auto filter = make<Filter>(plan, dt->having);
    state.placed.unionObjects(dt->having);
    state.addCost(*filter);
    plan = filter;
  }

  // We probably want to make this decision based on cost.
  static constexpr int64_t kMaxLimitBeforeProject = 8'192;
  if (dt->hasOrderBy()) {
    addOrderBy(dt, plan, state);
  } else if (dt->hasLimit() && dt->limit <= kMaxLimitBeforeProject) {
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
  }

  if (!dt->columns.empty()) {
    ExprVector exprs = state.exprsToColumns(dt->exprs);
    plan = make<Project>(
        plan, exprs, dt->columns, isRedundantProject(plan, exprs, dt->columns));
  }

  if (!dt->hasOrderBy() && dt->limit > kMaxLimitBeforeProject) {
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
  }
}

void Optimization::addAggregation(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  const auto* aggPlan = dt->aggregation;

  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  auto groupingKeys =
      precompute.toColumns(aggPlan->groupingKeys(), &aggPlan->columns());

  AggregateVector aggregates;
  aggregates.reserve(aggPlan->aggregates().size());

  for (const auto& agg : aggPlan->aggregates()) {
    ExprCP condition = nullptr;
    if (agg->condition()) {
      condition = precompute.toColumn(agg->condition());
    }
    auto args = precompute.toColumns(
        agg->args(), /*aliases=*/nullptr, /*preserveLiterals=*/true);
    aggregates.emplace_back(make<Aggregate>(
        agg->name(),
        agg->value(),
        std::move(args),
        agg->functions(),
        agg->isDistinct(),
        condition,
        agg->intermediateType()));
  }

  plan = std::move(precompute).maybeProject();

  if (isSingleWorker_ && runnerOptions_.numDrivers == 1) {
    auto* singleAgg = make<Aggregation>(
        plan,
        std::move(groupingKeys),
        std::move(aggregates),
        velox::core::AggregationNode::Step::kSingle,
        aggPlan->columns());

    state.placed.add(aggPlan);
    state.addCost(*singleAgg);
    plan = singleAgg;
  } else {
    auto* partialAgg = make<Aggregation>(
        plan,
        std::move(groupingKeys),
        aggregates,
        velox::core::AggregationNode::Step::kPartial,
        aggPlan->intermediateColumns());

    state.placed.add(aggPlan);
    state.addCost(*partialAgg);
    plan = repartitionForAgg(partialAgg, state);

    const auto numKeys = aggPlan->groupingKeys().size();

    ExprVector finalGroupingKeys;
    finalGroupingKeys.reserve(numKeys);
    for (auto i = 0; i < numKeys; ++i) {
      finalGroupingKeys.push_back(aggPlan->intermediateColumns()[i]);
    }

    auto* finalAgg = make<Aggregation>(
        plan,
        std::move(finalGroupingKeys),
        std::move(aggregates),
        velox::core::AggregationNode::Step::kFinal,
        aggPlan->columns());

    state.addCost(*finalAgg);
    plan = finalAgg;
  }
}

void Optimization::addOrderBy(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  auto orderKeys = precompute.toColumns(dt->orderKeys);

  for (auto i = 0; i < orderKeys.size(); ++i) {
    state.exprToColumn[dt->orderKeys[i]] = orderKeys[i];
  }

  state.placed.unionObjects(dt->orderKeys);

  const auto& downstreamColumns = state.downstreamColumns();
  for (auto* column : plan->columns()) {
    if (downstreamColumns.contains(column)) {
      precompute.toColumn(column);
    }
  }

  auto* orderBy = make<OrderBy>(
      std::move(precompute).maybeProject(),
      std::move(orderKeys),
      dt->orderTypes,
      dt->limit,
      dt->offset);
  state.addCost(*orderBy);
  plan = orderBy;
}

void Optimization::joinByIndex(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  if (candidate.tables.size() != 1 ||
      candidate.tables[0]->isNot(PlanType::kTableNode) ||
      !candidate.existences.empty()) {
    // Index applies to single base tables.
    return;
  }

  auto [right, left] = candidate.joinSides();

  auto& keys = right.keys;
  auto keyColumns = leadingColumns(keys);
  if (keyColumns.empty()) {
    return;
  }

  auto rightTable = candidate.tables.at(0)->as<BaseTable>();
  for (auto& index : rightTable->schemaTable->columnGroups) {
    auto info = rightTable->schemaTable->indexInfo(index, keyColumns);
    if (info.lookupKeys.empty()) {
      continue;
    }
    PlanStateSaver save(state, candidate);
    auto newPartition = repartitionForIndex(info, left.keys, plan, state);
    if (!newPartition) {
      continue;
    }
    state.placed.add(candidate.tables.at(0));
    auto joinType = right.leftJoinType();
    if (joinType == velox::core::JoinType::kFull ||
        joinType == velox::core::JoinType::kRight) {
      // Not available by index.
      return;
    }
    auto fanout = fanoutJoinTypeLimit(
        joinType, info.scanCardinality * rightTable->filterSelectivity);

    auto lookupKeys = left.keys;
    // The number of keys is the prefix that matches index order.
    lookupKeys.resize(info.lookupKeys.size());
    state.columns.unionSet(availableColumns(rightTable, index));
    auto c = state.downstreamColumns();
    c.intersect(state.columns);
    for (auto& filter : rightTable->filter) {
      c.unionSet(filter->columns());
    }

    auto* scan = make<TableScan>(
        newPartition,
        newPartition->distribution(),
        rightTable,
        info.index,
        fanout,
        c.toObjects<Column>(),
        lookupKeys,
        joinType,
        candidate.join->filter());

    state.columns.unionSet(c);
    state.addCost(*scan);
    state.addNextJoin(&candidate, scan, {}, toTry);
  }
}

void Optimization::joinByHash(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  VELOX_DCHECK(!candidate.tables.empty());
  auto [build, probe] = candidate.joinSides();

  const auto partKeys = joinKeyPartition(plan, probe.keys);
  ExprVector copartition;
  if (partKeys.empty()) {
    // Prefer to make a build partitioned on join keys and shuffle probe to
    // align with build.
    copartition = build.keys;
  }

  PlanStateSaver save(state, candidate);

  PlanObjectSet buildFilterColumns;
  buildFilterColumns.unionColumns(candidate.join->filter());
  buildFilterColumns.intersect(availableColumns(candidate.tables[0]));

  PlanObjectSet buildTables;
  PlanObjectSet buildColumns;
  for (auto buildTable : candidate.tables) {
    buildColumns.unionSet(availableColumns(buildTable));
    buildTables.add(buildTable);
  }

  buildColumns.intersect(state.downstreamColumns());
  buildColumns.unionColumns(build.keys);
  buildColumns.unionSet(buildFilterColumns);
  state.columns.unionSet(buildColumns);

  MemoKey memoKey{
      candidate.tables[0], buildColumns, buildTables, candidate.existences};

  Distribution forBuild;
  if (plan->distribution().distributionType.isGather) {
    forBuild = Distribution::gather();
  } else {
    forBuild = {plan->distribution().distributionType, copartition};
  }

  PlanObjectSet empty;
  bool needsShuffle = false;
  auto buildPlan = makePlan(
      memoKey, forBuild, empty, candidate.existsFanout, state, needsShuffle);

  // The build side tables are all joined if the first build is a
  // table but if it is a derived table (most often with aggregation),
  // only some of the tables may be fully joined.
  if (candidate.tables[0]->is(PlanType::kDerivedTableNode)) {
    state.placed.add(candidate.tables[0]);
    state.placed.unionSet(buildPlan->fullyImported);
  } else {
    state.placed.unionSet(buildTables);
  }

  PlanState buildState(state.optimization, state.dt, buildPlan);
  RelationOpPtr buildInput = buildPlan->op;
  RelationOpPtr probeInput = plan;

  if (!isSingleWorker_) {
    if (!partKeys.empty()) {
      if (needsShuffle) {
        if (copartition.empty()) {
          for (auto i : partKeys) {
            copartition.push_back(build.keys[i]);
          }
        }
        Distribution distribution{
            plan->distribution().distributionType, copartition};
        auto* repartition = make<Repartition>(
            buildInput, std::move(distribution), buildInput->columns());
        buildState.addCost(*repartition);
        buildInput = repartition;
      }
    } else if (
        candidate.join->isBroadcastableType() &&
        isBroadcastableSize(buildPlan, state)) {
      auto* broadcast = make<Repartition>(
          buildInput,
          Distribution::broadcast(plan->distribution().distributionType),
          buildInput->columns());
      buildState.addCost(*broadcast);
      buildInput = broadcast;
    } else {
      // The probe gets shuffled to align with build. If build is not
      // partitioned on its keys, shuffle the build too.
      alignJoinSides(
          buildInput, build.keys, buildState, probeInput, probe.keys, state);
    }
  }

  PrecomputeProjection precomputeBuild(buildInput, state.dt);
  auto buildKeys = precomputeBuild.toColumns(build.keys);
  buildInput = std::move(precomputeBuild).maybeProject();

  auto* buildOp =
      make<HashBuild>(buildInput, ++buildCounter_, build.keys, buildPlan);
  buildState.addCost(*buildOp);

  const auto joinType = build.leftJoinType();
  const bool probeOnly = joinType == velox::core::JoinType::kLeftSemiFilter ||
      joinType == velox::core::JoinType::kLeftSemiProject ||
      joinType == velox::core::JoinType::kAnti;

  PlanObjectSet probeColumns;
  probeColumns.unionObjects(plan->columns());

  ColumnVector columns;
  PlanObjectSet columnSet;
  ColumnCP mark = nullptr;

  state.downstreamColumns().forEach<Column>([&](auto column) {
    if (column == build.markColumn) {
      mark = column;
      columnSet.add(column);
      return;
    }

    if ((probeOnly || !buildColumns.contains(column)) &&
        !probeColumns.contains(column)) {
      return;
    }

    columnSet.add(column);
    columns.push_back(column);
  });

  // If there is an existence flag, it is the rightmost result column.
  if (mark) {
    const_cast<Value*>(&mark->value())->trueFraction =
        std::min<float>(1, candidate.fanout);
    columns.push_back(mark);
  }
  state.columns = columnSet;
  const auto fanout = fanoutJoinTypeLimit(joinType, candidate.fanout);

  PrecomputeProjection precomputeProbe(probeInput, state.dt);
  auto probeKeys = precomputeProbe.toColumns(probe.keys);
  probeInput = std::move(precomputeProbe).maybeProject();

  auto* join = make<Join>(
      JoinMethod::kHash,
      joinType,
      probeInput,
      buildOp,
      std::move(probeKeys),
      std::move(buildKeys),
      candidate.join->filter(),
      fanout,
      std::move(columns));

  state.addCost(*join);
  state.cost.setupCost += buildState.cost.unitCost + buildState.cost.setupCost;
  state.cost.totalBytes += buildState.cost.totalBytes;
  state.cost.transferBytes += buildState.cost.transferBytes;
  join->buildCost = buildState.cost;
  state.addNextJoin(&candidate, join, {buildOp}, toTry);
}

void Optimization::joinByHashRight(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  VELOX_DCHECK(!candidate.tables.empty());
  auto [probe, build] = candidate.joinSides();

  PlanStateSaver save(state, candidate);

  PlanObjectSet probeFilterColumns;
  probeFilterColumns.unionColumns(candidate.join->filter());
  probeFilterColumns.intersect(availableColumns(candidate.tables[0]));

  PlanObjectSet probeTables;
  PlanObjectSet probeColumns;
  for (auto probeTable : candidate.tables) {
    probeColumns.unionSet(availableColumns(probeTable));
    state.placed.add(probeTable);
    probeTables.add(probeTable);
  }

  probeColumns.intersect(state.downstreamColumns());
  probeColumns.unionColumns(probe.keys);
  probeColumns.unionSet(probeFilterColumns);
  state.columns.unionSet(probeColumns);

  MemoKey memoKey{
      candidate.tables[0], probeColumns, probeTables, candidate.existences};

  PlanObjectSet empty;
  bool needsShuffle = false;
  auto probePlan = makePlan(
      memoKey,
      Distribution{plan->distribution().distributionType, {}},
      empty,
      candidate.existsFanout,
      state,
      needsShuffle);

  PlanState probeState(state.optimization, state.dt, probePlan);

  RelationOpPtr probeInput = probePlan->op;
  RelationOpPtr buildInput = plan;

  if (!isSingleWorker_) {
    // The build gets shuffled to align with probe. If probe is not partitioned
    // on its keys, shuffle the probe too.
    alignJoinSides(
        probeInput, probe.keys, probeState, buildInput, build.keys, state);
  }

  PrecomputeProjection precomputeBuild(buildInput, state.dt);
  auto buildKeys = precomputeBuild.toColumns(build.keys);
  buildInput = std::move(precomputeBuild).maybeProject();

  auto* buildOp =
      make<HashBuild>(buildInput, ++buildCounter_, build.keys, nullptr);
  state.addCost(*buildOp);

  PlanObjectSet buildColumns;
  buildColumns.unionObjects(buildInput->columns());

  const auto leftJoinType = probe.leftJoinType();
  const auto fanout = fanoutJoinTypeLimit(leftJoinType, candidate.fanout);

  // Change the join type to the right join variant.
  const auto rightJoinType = reverseJoinType(leftJoinType);
  VELOX_CHECK(
      leftJoinType != rightJoinType,
      "Join type does not have right hash join variant");

  const bool buildOnly =
      rightJoinType == velox::core::JoinType::kRightSemiFilter ||
      rightJoinType == velox::core::JoinType::kRightSemiProject;

  ColumnVector columns;
  PlanObjectSet columnSet;
  ColumnCP mark = nullptr;

  state.downstreamColumns().forEach<Column>([&](auto column) {
    if (column == probe.markColumn) {
      mark = column;
      return;
    }

    if (!buildColumns.contains(column) &&
        (buildOnly || !probeColumns.contains(column))) {
      return;
    }

    columnSet.add(column);
    columns.push_back(column);
  });

  if (mark) {
    const_cast<Value*>(&mark->value())->trueFraction =
        std::min<float>(1, candidate.fanout);
    columns.push_back(mark);
  }

  const auto buildCost = state.cost.unitCost;

  state.columns = columnSet;
  state.cost = probeState.cost;
  state.cost.setupCost += buildCost;

  PrecomputeProjection precomputeProbe(probeInput, state.dt);
  auto probeKeys = precomputeProbe.toColumns(probe.keys);
  probeInput = std::move(precomputeProbe).maybeProject();

  auto* join = make<Join>(
      JoinMethod::kHash,
      rightJoinType,
      probeInput,
      buildOp,
      std::move(probeKeys),
      std::move(buildKeys),
      candidate.join->filter(),
      fanout,
      std::move(columns));
  state.addCost(*join);

  state.addNextJoin(&candidate, join, {buildOp}, toTry);
}

void Optimization::crossJoin(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  VELOX_NYI("No cross joins");
}

void Optimization::crossJoinUnnest(
    RelationOpPtr plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  for (const auto* table : candidate.tables) {
    VELOX_CHECK(table->is(PlanType::kUnnestTableNode));
    // We add unnest table before compute downstream columns because
    // we're not interested in the replicating columns needed only for unnest.
    state.placed.add(table);

    PrecomputeProjection precompute(plan, state.dt, /*projectAllInputs=*/false);

    ExprVector replicateColumns;
    state.downstreamColumns().forEach<Column>([&](auto column) {
      if (state.columns.contains(column)) {
        replicateColumns.push_back(precompute.toColumn(column));
      }
    });

    // We don't use downstreamColumns() for unnestExprs/unnestedColumns.
    // Because 'unnest-column' should be unnested even when it isn't used.
    // Because it can change cardinality of the all output.
    const auto& unnestExprs = candidate.join->leftKeys();
    const auto& unnestedColumns = table->as<UnnestTable>()->columns;

    // Plan is updated here,
    // because we can have multiple unnest joins in single JoinCandidate.

    auto unnestColumns = precompute.toColumns(unnestExprs);
    plan = std::move(precompute).maybeProject();

    plan = make<Unnest>(
        std::move(plan),
        std::move(replicateColumns),
        std::move(unnestColumns),
        unnestedColumns);

    state.columns.unionObjects(unnestedColumns);
    state.addCost(*plan);
  }
  state.addNextJoin(&candidate, std::move(plan), {}, toTry);
}

void Optimization::addJoin(
    const JoinCandidate& candidate,
    const RelationOpPtr& plan,
    PlanState& state,
    std::vector<NextJoin>& result) {
  if (!candidate.join) {
    crossJoin(plan, candidate, state, result);
    return;
  }

  // If this candidate has multiple Unnest they all will be handled at once.
  if (candidate.tables.size() >= 1 &&
      candidate.tables[0]->is(PlanType::kUnnestTableNode)) {
    crossJoinUnnest(plan, candidate, state, result);
    return;
  }

  std::vector<NextJoin> toTry;

  joinByIndex(plan, candidate, state, toTry);

  const auto sizeAfterIndex = toTry.size();
  joinByHash(plan, candidate, state, toTry);
  if (toTry.size() > sizeAfterIndex && candidate.join->isNonCommutative() &&
      candidate.join->hasRightHashVariant()) {
    // There is a hash based candidate with a non-commutative join. Try a right
    // join variant.
    joinByHashRight(plan, candidate, state, toTry);
  }

  // If one is much better do not try the other.
  if (toTry.size() == 2 && candidate.tables.size() == 1) {
    if (toTry[0].isWorse(toTry[1])) {
      toTry.erase(toTry.begin());
    } else if (toTry[1].isWorse(toTry[0])) {
      toTry.erase(toTry.begin() + 1);
    }
  }
  result.insert(result.end(), toTry.begin(), toTry.end());
}

void Optimization::tryNextJoins(
    PlanState& state,
    const std::vector<NextJoin>& nextJoins) {
  for (auto& next : nextJoins) {
    PlanStateSaver save(state);
    state.placed = next.placed;
    state.columns = next.columns;
    state.cost = next.cost;
    state.addBuilds(next.newBuilds);
    makeJoins(next.plan, state);
  }
}

RelationOpPtr Optimization::placeSingleRowDt(
    RelationOpPtr plan,
    DerivedTableCP subquery,
    ExprCP filter,
    PlanState& state) {
  MemoKey memoKey;
  memoKey.firstTable = subquery;
  memoKey.tables.add(subquery);
  memoKey.columns.unionObjects(subquery->columns);

  const auto broadcast = Distribution::broadcast({});

  PlanObjectSet empty;
  bool needsShuffle = false;
  auto rightPlan = makePlan(memoKey, broadcast, empty, 1, state, needsShuffle);

  auto rightOp = rightPlan->op;
  if (needsShuffle) {
    rightOp = make<Repartition>(rightOp, broadcast, rightOp->columns());
  }

  auto resultColumns = plan->columns();
  resultColumns.insert(
      resultColumns.end(),
      rightOp->columns().begin(),
      rightOp->columns().end());
  auto* join = Join::makeCrossJoin(
      std::move(plan), std::move(rightOp), std::move(resultColumns));
  state.addCost(*join);
  return join;
}

void Optimization::placeDerivedTable(DerivedTableCP from, PlanState& state) {
  PlanStateSaver save(state);

  state.placed.add(from);

  PlanObjectSet dtColumns;
  dtColumns.unionObjects(from->columns);
  dtColumns.intersect(state.downstreamColumns());
  state.columns.unionSet(dtColumns);

  MemoKey key;
  key.columns = std::move(dtColumns);
  key.firstTable = from;
  key.tables.add(from);

  bool ignore = false;
  auto plan = makePlan(key, Distribution{}, PlanObjectSet{}, 1, state, ignore);

  // Make plans based on the dt alone as first.
  makeJoins(plan->op, state);

  // We see if there are reducing joins to import inside the dt.
  PlanObjectSet visited = state.placed;
  visited.add(from);
  visited.unionSet(state.dt->importedExistences);
  visited.unionSet(state.dt->fullyImported);

  PlanObjectSet reducingSet;
  reducingSet.add(from);

  std::vector<PlanObjectCP> path{from};

  float reduction = 1;
  reducingJoinsRecursive(
      state, from, 1, 1.2, path, visited, reducingSet, reduction);

  if (reduction < 0.9) {
    key.tables = reducingSet;
    key.columns = state.downstreamColumns();
    plan = makePlan(key, Distribution{}, PlanObjectSet{}, 1, state, ignore);
    // Not all reducing joins are necessarily retained in the plan. Only mark
    // the ones fully imported as placed.
    state.placed.unionSet(plan->fullyImported);
    makeJoins(plan->op, state);
  }
}

bool Optimization::placeConjuncts(
    RelationOpPtr plan,
    PlanState& state,
    bool allowNondeterministic) {
  PlanStateSaver save(state);

  PlanObjectSet columnsAndSingles = state.columns;
  state.dt->singleRowDts.forEach<DerivedTable>(
      [&](auto dt) { columnsAndSingles.unionObjects(dt->columns); });

  ExprVector filters;
  for (auto& conjunct : state.dt->conjuncts) {
    if (!allowNondeterministic && conjunct->containsNonDeterministic()) {
      continue;
    }
    if (state.placed.contains(conjunct)) {
      continue;
    }
    if (conjunct->columns().isSubset(state.columns)) {
      state.columns.add(conjunct);
      filters.push_back(conjunct);
      continue;
    }
    if (conjunct->columns().isSubset(columnsAndSingles)) {
      // The filter depends on placed tables and non-correlated single row
      // subqueries.
      std::vector<DerivedTableCP> placeable;
      auto subqColumns = conjunct->columns();
      subqColumns.except(state.columns);
      subqColumns.forEach([&](auto /*unused*/) {
        state.dt->singleRowDts.forEach<DerivedTable>([&](auto subquery) {
          // If the subquery provides columns for the filter, place it.
          const auto& conjunctColumns = conjunct->columns();
          for (auto subqColumn : subquery->columns) {
            if (conjunctColumns.contains(subqColumn)) {
              placeable.push_back(subquery);
              break;
            }
          }
        });
      });

      for (auto i = 0; i < placeable.size(); ++i) {
        state.placed.add(conjunct);
        plan = placeSingleRowDt(
            plan,
            placeable[i],
            (i == placeable.size() - 1 ? conjunct : nullptr),
            state);
        makeJoins(plan, state);
        return true;
      }
    }
  }

  if (!filters.empty()) {
    for (auto& filter : filters) {
      state.placed.add(filter);
    }
    auto* filter = make<Filter>(plan, std::move(filters));
    state.addCost(*filter);
    makeJoins(filter, state);
    return true;
  }
  return false;
}

namespace {

// Returns a subset of 'downstream' that exist in 'index' of 'table'.
ColumnVector indexColumns(
    const PlanObjectSet& downstream,
    BaseTableCP table,
    ColumnGroupCP index) {
  ColumnVector result;
  downstream.forEach<Column>([&](auto column) {
    if (!column->schemaColumn()) {
      return;
    }
    if (table != column->relation()) {
      return;
    }
    if (position(index->columns, *column->schemaColumn()) != kNotFound) {
      result.push_back(column);
    }
  });
  return result;
}

RelationOpPtr makeDistinct(const RelationOpPtr& input) {
  ExprVector groupingKeys;
  for (const auto& column : input->columns()) {
    groupingKeys.push_back(column);
  }

  return make<Aggregation>(
      input,
      groupingKeys,
      AggregateVector{},
      velox::core::AggregationNode::Step::kSingle,
      input->columns());
}

Distribution somePartition(const RelationOpPtrVector& inputs) {
  float card = 1;

  // A simple type and many values is a good partitioning key.
  auto score = [&](ColumnCP column) {
    const auto& value = column->value();
    const auto card = value.cardinality;
    return value.type->kind() >= velox::TypeKind::ARRAY ? card / 10000 : card;
  };

  const auto& firstInput = inputs[0];
  auto inputColumns = firstInput->columns();
  std::ranges::sort(inputColumns, [&](ColumnCP left, ColumnCP right) {
    return score(left) > score(right);
  });

  ExprVector columns;
  for (const auto* column : inputColumns) {
    card *= column->value().cardinality;
    columns.push_back(column);
    if (card > 100'000) {
      break;
    }
  }

  DistributionType distributionType;
  distributionType.numPartitions =
      queryCtx()->optimization()->runnerOptions().numWorkers;
  distributionType.locus = firstInput->distribution().distributionType.locus;

  return {distributionType, std::move(columns)};
}

// Adds the costs in the input states to the first state and if 'distinct' is
// not null adds the cost of that to the first state.
PlanP unionPlan(
    std::vector<PlanState>& states,
    const std::vector<PlanP>& inputPlans,
    const RelationOpPtr& result,
    Aggregation* distinct) {
  auto& firstState = states[0];

  PlanObjectSet fullyImported = inputPlans[0]->fullyImported;
  for (auto i = 1; i < states.size(); ++i) {
    const auto& otherCost = states[i].cost;
    fullyImported.intersect(inputPlans[i]->fullyImported);
    firstState.cost.add(otherCost);
    // The input cardinality is not additive, the fanout and other metrics are.
    firstState.cost.inputCardinality -= otherCost.inputCardinality;
  }
  if (distinct) {
    firstState.addCost(*distinct);
  }
  auto plan = make<Plan>(result, states[0]);
  plan->fullyImported = fullyImported;
  return plan;
}

float startingScore(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    return table->as<BaseTable>()->schemaTable->cardinality;
  }

  if (table->is(PlanType::kValuesTableNode)) {
    return table->as<ValuesTable>()->cardinality();
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    VELOX_FAIL("UnnestTable cannot be a starting table");
    // Because it's rigth side of directed inner (cross) join edge.
    // Directed edges are non-commutative, so right side cannot be starting.
  }

  return 10;
}

std::vector<int32_t> sortByStartingScore(const PlanObjectVector& tables) {
  std::vector<float> scores;
  scores.reserve(tables.size());
  for (auto table : tables) {
    scores.emplace_back(startingScore(table));
  }

  std::vector<int32_t> indices(tables.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::ranges::sort(indices, [&](int32_t left, int32_t right) {
    return scores[left] > scores[right];
  });

  return indices;
}
} // namespace

void Optimization::makeJoins(PlanState& state) {
  auto firstTables = state.dt->startTables.toObjects();

#ifndef NDEBUG
  for (auto table : firstTables) {
    state.debugSetFirstTable(table->id());
  }
#endif

  auto sortedIndices = sortByStartingScore(firstTables);

  for (auto index : sortedIndices) {
    auto from = firstTables.at(index);
    if (from->is(PlanType::kTableNode)) {
      auto table = from->as<BaseTable>();
      auto indices = table->chooseLeafIndex();
      // Make plan starting with each relevant index of the table.
      const auto downstream = state.downstreamColumns();
      for (auto index : indices) {
        auto columns = indexColumns(downstream, table, index);

        PlanStateSaver save(state);
        state.placed.add(table);
        state.columns.unionObjects(columns);

        auto distribution =
            TableScan::outputDistribution(table, index, columns);
        auto* scan = make<TableScan>(
            nullptr,
            std::move(distribution),
            table,
            index,
            index->table->cardinality * table->filterSelectivity,
            std::move(columns));
        state.addCost(*scan);
        makeJoins(scan, state);
      }
    } else if (from->is(PlanType::kValuesTableNode)) {
      const auto* valuesTable = from->as<ValuesTable>();
      ColumnVector columns;
      state.downstreamColumns().forEach<Column>([&](auto column) {
        if (valuesTable == column->relation()) {
          columns.push_back(column);
        }
      });

      PlanStateSaver save{state};
      state.placed.add(valuesTable);
      state.columns.unionObjects(columns);
      auto* scan = make<Values>(*valuesTable, std::move(columns));
      state.addCost(*scan);
      makeJoins(scan, state);
    } else if (from->is(PlanType::kUnnestTableNode)) {
      VELOX_FAIL("UnnestTable cannot be a starting table");
      // Because it's rigth side of directed inner (cross) join edge.
      // Directed edges are non-commutative, so right side cannot be starting.
    } else {
      // Start with a derived table.
      placeDerivedTable(from->as<const DerivedTable>(), state);
    }
  }
}

void Optimization::makeJoins(RelationOpPtr plan, PlanState& state) {
  VELOX_CHECK_NOT_NULL(plan);
  auto dt = state.dt;

  if (state.isOverBest()) {
    trace(OptimizerOptions::kExceededBest, dt->id(), state.cost, *plan);
    return;
  }

  // Add multitable filters not associated to a non-inner join.
  if (placeConjuncts(plan, state, false)) {
    return;
  }

  auto candidates = nextJoins(state);
  if (candidates.empty()) {
    if (placeConjuncts(plan, state, true)) {
      return;
    }

    addPostprocess(dt, plan, state);
    auto kept = state.plans.addPlan(plan, state);
    trace(
        kept ? OptimizerOptions::kRetained : OptimizerOptions::kExceededBest,
        dt->id(),
        state.cost,
        *plan);
    return;
  }

  std::vector<NextJoin> nextJoins;
  nextJoins.reserve(candidates.size());
  for (auto& candidate : candidates) {
    addJoin(candidate, plan, state, nextJoins);
  }
  tryNextJoins(state, nextJoins);
}

PlanP Optimization::makePlan(
    const MemoKey& key,
    const Distribution& distribution,
    const PlanObjectSet& boundColumns,
    float existsFanout,
    PlanState& state,
    bool& needsShuffle) {
  needsShuffle = false;
  if (key.firstTable->is(PlanType::kDerivedTableNode) &&
      key.firstTable->as<DerivedTable>()->setOp.has_value()) {
    return makeUnionPlan(
        key, distribution, boundColumns, existsFanout, state, needsShuffle);
  }
  return makeDtPlan(key, distribution, existsFanout, state, needsShuffle);
}

PlanP Optimization::makeUnionPlan(
    const MemoKey& key,
    const Distribution& distribution,
    const PlanObjectSet& boundColumns,
    float existsFanout,
    PlanState& state,
    bool& needsShuffle) {
  const auto* setDt = key.firstTable->as<DerivedTable>();

  RelationOpPtrVector inputs;
  std::vector<PlanP> inputPlans;
  std::vector<PlanState> inputStates;
  std::vector<bool> inputNeedsShuffle;

  for (auto* inputDt : setDt->children) {
    MemoKey inputKey = key;
    inputKey.firstTable = inputDt;
    inputKey.tables.erase(key.firstTable);
    inputKey.tables.add(inputDt);

    bool inputShuffle = false;
    auto inputPlan = makePlan(
        inputKey,
        distribution,
        boundColumns,
        existsFanout,
        state,
        inputShuffle);
    inputPlans.push_back(inputPlan);
    inputStates.emplace_back(*this, setDt, inputPlans.back());
    inputs.push_back(inputPlan->op);
    inputNeedsShuffle.push_back(inputShuffle);
  }

  const bool isDistinct =
      setDt->setOp.value() == logical_plan::SetOperation::kUnion;
  if (isSingleWorker_) {
    RelationOpPtr result = make<UnionAll>(inputs);
    Aggregation* distinct = nullptr;
    if (isDistinct) {
      result = makeDistinct(result);
      distinct = result->as<Aggregation>();
    }
    return unionPlan(inputStates, inputPlans, result, distinct);
  }

  if (distribution.partition.empty()) {
    if (isDistinct) {
      // Pick some partitioning key and shuffle on that and make distinct.
      Distribution someDistribution = somePartition(inputs);
      for (auto i = 0; i < inputs.size(); ++i) {
        inputs[i] = make<Repartition>(
            inputs[i], someDistribution, inputs[i]->columns());
        inputStates[i].addCost(*inputs[i]);
      }
    }
  } else {
    // Some need a shuffle. Add the shuffles, add an optional distinct and
    // return with no shuffle needed.
    for (auto i = 0; i < inputs.size(); ++i) {
      if (inputNeedsShuffle[i]) {
        inputs[i] =
            make<Repartition>(inputs[i], distribution, inputs[i]->columns());
        inputStates[i].addCost(*inputs[i]);
      }
    }
  }

  RelationOpPtr result = make<UnionAll>(inputs);
  Aggregation* distinct = nullptr;
  if (isDistinct) {
    result = makeDistinct(result);
    distinct = result->as<Aggregation>();
  }
  return unionPlan(inputStates, inputPlans, result, distinct);
}

PlanP Optimization::makeDtPlan(
    const MemoKey& key,
    const Distribution& distribution,
    float existsFanout,
    PlanState& state,
    bool& needsShuffle) {
  auto it = memo_.find(key);
  PlanSet* plans{};
  if (it == memo_.end()) {
    DerivedTable dt;
    dt.cname = newCName("tmp_dt");
    dt.import(
        *state.dt, key.firstTable, key.tables, key.existences, existsFanout);

    PlanState inner(*this, &dt);
    if (key.firstTable->is(PlanType::kDerivedTableNode)) {
      inner.setTargetExprsForDt(key.columns);
    } else {
      inner.targetExprs = key.columns;
    }

    makeJoins(inner);
    memo_[key] = std::move(inner.plans);
    plans = &memo_[key];
  } else {
    plans = &it->second;
  }
  return plans->best(distribution, needsShuffle);
}

ExprCP Optimization::combineLeftDeep(Name func, const ExprVector& exprs) {
  ExprVector copy = exprs;
  std::ranges::sort(copy, [&](ExprCP left, ExprCP right) {
    return left->id() < right->id();
  });
  ExprCP result = copy[0];
  for (auto i = 1; i < copy.size(); ++i) {
    result = toGraph_.deduppedCall(
        func,
        result->value(),
        ExprVector{result, copy[i]},
        result->functions() | copy[i]->functions());
  }
  return result;
}

} // namespace facebook::axiom::optimizer
