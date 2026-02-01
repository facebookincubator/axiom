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
#include <ranges>
#include <utility>
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/PrecomputeProjection.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer {

std::string_view toString(JoinOrder joinOrder) {
  switch (joinOrder) {
    case JoinOrder::kCost:
      return "cost";
    case JoinOrder::kSyntactic:
      return "syntactic";
    case JoinOrder::kGreedy:
      return "greedy";
  }
  return "unknown";
}

Optimization::Optimization(
    SessionPtr session,
    const logical_plan::LogicalPlanNode& logicalPlan,
    History& history,
    std::shared_ptr<velox::core::QueryCtx> veloxQueryCtx,
    velox::core::ExpressionEvaluator& evaluator,
    OptimizerOptions options,
    runner::MultiFragmentPlan::Options runnerOptions)
    : session_{std::move(session)},
      options_(std::move(options)),
      runnerOptions_(std::move(runnerOptions)),
      isSingleWorker_(runnerOptions_.numWorkers == 1),
      isSingleDriver_(runnerOptions_.numDrivers == 1),
      logicalPlan_(&logicalPlan),
      history_(history),
      veloxQueryCtx_(std::move(veloxQueryCtx)),
      topState_{*this, nullptr},
      negation_{toName(FunctionRegistry::instance()->negation())},
      toGraph_{evaluator, options_},
      toVelox_{session_, runnerOptions_, options_} {
  queryCtx()->optimization() = this;
  root_ = toGraph_.makeQueryGraph(*logicalPlan_);
  toGraph_.setDtOutput(root_, *logicalPlan_);

  if (!options_.lazyOptimizeGraph) {
    optimizeGraph();
  }
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

  VeloxHistory history;

  auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

  Optimization opt{
      session,
      logicalPlan,
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
    const PlanCost& cost,
    RelationOp& plan) const {
  if (event & options_.traceFlags) {
    std::cout << (event == OptimizerOptions::kRetained ? "Retained: "
                                                       : "Abandoned: ")
              << id << ": " << cost.toString() << ": " << " "
              << plan.toString(true, false) << std::endl;
  }
}

void Optimization::optimizeGraph() {
  root_->distributeConjuncts();
  root_->addImpliedJoins();
  root_->linkTablesToJoins();
  for (auto* join : root_->joins) {
    join->guessFanout();
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
    if (join->isLeftOuter() && candidate == join->rightTable() &&
        candidate->is(PlanType::kDerivedTableNode)) {
      // One can restrict the build of the optional side by a restriction on the
      // probe. This happens specially when value subqueries are represented as
      // optional sides of left join. These are often aggregations and there is
      // no point creating values for groups that can't be probed.
    } else if (join->leftOptional() || join->rightOptional()) {
      continue;
    }

    JoinSide other = join->sideOf(candidate, true);
    VELOX_DCHECK_NOT_NULL(other.table);

    if (!state.dt->hasTable(other.table) || !state.dt->hasJoin(join)) {
      continue;
    }

    if (other.table->is(PlanType::kDerivedTableNode)) {
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

bool allowReducingInnerJoins(const JoinCandidate& candidate) {
  if (!candidate.join->isInner()) {
    return false;
  }
  if (candidate.tables[0]->is(PlanType::kDerivedTableNode)) {
    return false;
  }
  return true;
}

// JoinCandidate.tables may contain a single derived table or one or more base
// tables.
void checkTables(const JoinCandidate& candidate) {
  VELOX_DCHECK(!candidate.tables.empty());
  if (candidate.tables[0]->is(PlanType::kDerivedTableNode)) {
    VELOX_DCHECK_EQ(1, candidate.tables.size());
  }
}

// For an inner join, see if can bundle reducing joins on the build.
std::optional<JoinCandidate> reducingJoins(
    const PlanState& state,
    const JoinCandidate& candidate,
    bool enableReducingExistences) {
  checkTables(candidate);

  std::vector<PlanObjectCP> tables;
  std::vector<PlanObjectSet> existences;
  float fanout = candidate.fanout;

  PlanObjectSet reducingSet;
  if (allowReducingInnerJoins(candidate)) {
    PlanObjectSet visited = state.placed();
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
      fanout *= reduction;
    }
  }

  if (enableReducingExistences && !state.dt->noImportOfExists) {
    std::vector<PlanObjectCP> path{candidate.tables[0]};
    // Look for reducing joins that were not added before, also covering already
    // placed tables. This may copy reducing joins from a probe to the
    // corresponding build.
    reducingSet.add(candidate.tables[0]);
    reducingSet.unionSet(state.dt->importedExistences);

    PlanObjectSet exists;
    float reduction = 1;
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
            fanout *= reduction;
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

void addExtraEdges(PlanState& state, JoinCandidate& candidate) {
  // See if there are more join edges from the first of 'candidate' to already
  // placed tables. Fill in the non-redundant equalities into the join edge.
  // Make a new edge if the edge would be altered.
  auto* originalJoin = candidate.join;
  for (auto* table : candidate.tables) {
    for (auto* otherJoin : joinedBy(table)) {
      if (otherJoin == originalJoin || !otherJoin->isInner()) {
        continue;
      }
      auto [otherTable, fanout] = otherJoin->otherTable(table);
      if (!state.dt->hasTable(otherTable)) {
        continue;
      }
      if (candidate.isDominantEdge(state, otherJoin)) {
        break;
      }
      candidate.addEdge(state, otherJoin, table);
    }
  }
}
} // namespace

std::vector<JoinCandidate> Optimization::nextJoins(PlanState& state) const {
  std::vector<JoinCandidate> candidates;
  candidates.reserve(state.dt->tables.size());
  state.placed().forEach([&](PlanObjectCP placedTable) {
    if (!placedTable->isTable()) {
      return;
    }
    for (auto* join : joinedBy(placedTable)) {
      PlanObjectCP joined{};
      float fanout{};
      if (placedTable == join->leftTable()) {
        joined = join->rightTable();
        fanout = join->lrFanout();
      } else if (join->isNonCommutative()) {
        continue;
      } else {
        joined = join->leftTable();
        fanout = join->rlFanout();
      }
      // TODO Think about what in what order to make these checks.
      if (state.isPlaced(joined) || !state.dt->hasTable(joined) ||
          !state.mayConsiderNext(joined) || !state.dt->hasJoin(join)) {
        continue;
      }
      auto& candidate = candidates.emplace_back(join, joined, fanout);
      if (join->isInner()) {
        addExtraEdges(state, candidate);
      }
    }
  });

  SCOPE_EXIT {
    if (!options_.costJoinOrder() && candidates.size() > 1) {
      candidates.erase(candidates.begin() + 1, candidates.end());
    }
  };

  if (candidates.empty()) {
    // There are no join edges. There could still be cross joins.
    state.dt->startTables.forEach([&](PlanObjectCP object) {
      if (!state.isPlaced(object) && state.mayConsiderNext(object)) {
        candidates.emplace_back(nullptr, object, tableCardinality(object));
      }
    });

    return candidates;
  }

  // Take the first hand joined tables and bundle them with reducing joins that
  // can go on the build side.

  if (!options_.syntacticJoinOrder()) {
    std::vector<JoinCandidate> bushes;
    for (auto& candidate : candidates) {
      if (auto bush = reducingJoins(
              state, candidate, options_.enableReducingExistences)) {
        bushes.push_back(std::move(*bush));
        addExtraEdges(state, bushes.back());
      }
    }
    candidates.insert(candidates.end(), bushes.begin(), bushes.end());
  }

  std::ranges::sort(
      candidates, [](const JoinCandidate& left, const JoinCandidate& right) {
        return left.fanout < right.fanout;
      });

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

RelationOpPtr repartitionForAgg(
    AggregationPlanCP const agg,
    const RelationOpPtr& plan,
    PlanCost& cost) {
  // No shuffle if all grouping keys are in partitioning.
  if (isSingleWorker() || plan->distribution().isGather()) {
    return plan;
  }

  // If no grouping and not yet gathered on a single node,
  // add a gather before final agg.
  if (agg->groupingKeys().empty()) {
    auto* gather = make<Repartition>(plan, Distribution::gather());
    cost.add(*gather);
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
    auto nthKey = position(plan->distribution().partitionKeys(), *key);
    if (nthKey == kNotFound) {
      shuffle = true;
      break;
    }
  }
  if (!shuffle) {
    return plan;
  }

  Distribution distribution{
      plan->distribution().distributionType(), std::move(keyValues)};
  auto* repartition = make<Repartition>(plan, std::move(distribution));
  cost.add(*repartition);
  return repartition;
}

CPSpan<Column> leadingColumns(CPSpan<Expr> exprs) {
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
    CPSpan<Expr> lookupValues,
    const RelationOpPtr& input) {
  const auto& current = input->distribution();
  const auto& desired = info.index->distribution;
  const auto needsShuffle = current.maybeNeedsShuffle(desired);
  if (needsShuffle != Distribution::NeedsShuffle::kMaybe) {
    return needsShuffle == Distribution::NeedsShuffle::kNo;
  }

  // TODO: Code in this function actually doesn't feel right.

  if (!hasCopartition(current.partitionType(), desired.partitionType())) {
    return false;
  }

  if (current.partitionKeys().empty()) {
    return false;
  }

  if (current.partitionKeys().size() != desired.partitionKeys().size()) {
    return false;
  }

  // We should check it when we will add indexes.
  // True if 'input' is partitioned so that each partitioning key is joined to
  // the corresponding partition key in 'info'.
  for (size_t i = 0; i < current.partitionKeys().size(); ++i) {
    auto nthKey = position(lookupValues, *current.partitionKeys()[i]);
    if (nthKey == kNotFound ||
        info.schemaColumn(info.lookupKeys[nthKey]) !=
            desired.partitionKeys()[i]) {
      return false;
    }
  }
  return true;
}

RelationOpPtr repartitionForIndex(
    const IndexInfo& info,
    CPSpan<Expr> lookupValues,
    const RelationOpPtr& plan,
    PlanState& state) {
  if (isSingleWorker() || isIndexColocated(info, lookupValues, plan)) {
    return plan;
  }

  const auto& distribution = info.index->distribution;

  ExprVector keyExprs;
  const auto& partitionKeys = distribution.partitionKeys();
  for (auto key : partitionKeys) {
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
      plan, Distribution{distribution.distributionType(), std::move(keyExprs)});
  state.addCost(*repartition);
  return repartition;
}

// Join edge: a -- b. Left is a. Right is b.
// @param lrFanout For each row in 'a' there are so many matches in 'b'.
// @param rlFanout For each row in 'b' there are so many matches in 'a'.
// @param rightToLeftRatio |b| / |a|
float fanoutJoinTypeLimit(
    velox::core::JoinType joinType,
    float lrFanout,
    float rlFanout,
    float rightToLeftRatio) {
  switch (joinType) {
    case velox::core::JoinType::kInner:
      return lrFanout;
    case velox::core::JoinType::kLeft:
      return std::max<float>(1, lrFanout);
    case velox::core::JoinType::kRight:
      return std::max<float>(1, rlFanout) * rightToLeftRatio;
    case velox::core::JoinType::kFull:
      return std::max<float>({1, lrFanout, rightToLeftRatio});
    case velox::core::JoinType::kLeftSemiProject:
      return 1;
    case velox::core::JoinType::kLeftSemiFilter:
      return std::min<float>(1, lrFanout);
    case velox::core::JoinType::kRightSemiProject:
      return rightToLeftRatio;
    case velox::core::JoinType::kRightSemiFilter:
      return std::min<float>(1, rlFanout) * rightToLeftRatio;
    case velox::core::JoinType::kAnti:
      return std::max<float>(0, 1 - lrFanout);
    default:
      VELOX_UNREACHABLE();
  }
}

// Returns the positions in 'keys' for the expressions that determine the
// partition. empty if the partition is not decided by 'keys'
std::vector<uint32_t> joinKeyPartition(
    const RelationOpPtr& op,
    CPSpan<Expr> keys) {
  std::vector<uint32_t> positions;
  for (const auto* partitionKey : op->distribution().partitionKeys()) {
    auto nthKey = position(keys, *partitionKey);
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

bool isSingleWorkerSize(const RelationOp& op) {
  return op.resultCardinality() < 100'000;
}

// The 'other' side gets shuffled to align with 'input'. If 'input' is not
// partitioned on its keys, shuffle the 'input' too.
void alignJoinSides(
    RelationOpPtr& input,
    CPSpan<Expr> keys,
    PlanState& state,
    RelationOpPtr& otherInput,
    CPSpan<Expr> otherKeys,
    PlanState& otherState) {
  VELOX_DCHECK(
      !input->distribution().isGather() ||
      !otherInput->distribution().isGather());

  auto part = joinKeyPartition(input, keys);
  if (part.empty()) {
    Distribution distribution{
        DistributionType{}, ExprVector{keys.begin(), keys.end()}};
    input = make<Repartition>(input, std::move(distribution));
    state.addCost(*input);
  }

  ExprVector distColumns;
  for (size_t i = 0; i < keys.size(); ++i) {
    auto nthKey = position(input->distribution().partitionKeys(), *keys[i]);
    if (nthKey != kNotFound) {
      if (distColumns.size() <= nthKey) {
        distColumns.resize(nthKey + 1);
      }
      distColumns[nthKey] = otherKeys[i];
    }
  }

  Distribution distribution{
      input->distribution().distributionType(), std::move(distColumns)};
  otherInput = make<Repartition>(otherInput, std::move(distribution));
  otherState.addCost(*otherInput);
}

// Check if 'plan' is an identity projection. If so, return its input.
// Otherwise, return 'plan'.
const RelationOpPtr& maybeDropProject(const RelationOpPtr& plan) {
  if (plan->is(RelType::kProject)) {
    bool redundant = true;

    const auto* project = plan->as<Project>();
    for (auto i = 0; i < project->columns().size(); ++i) {
      if (project->columns()[i] != project->exprs()[i]) {
        redundant = false;
        break;
      }
    }

    if (redundant) {
      return plan->input();
    }
  }

  return plan;
}

RelationOpPtr repartitionForWrite(const RelationOpPtr& plan, PlanState& state) {
  if (isSingleWorker() || plan->distribution().isGather()) {
    return plan;
  }

  const auto* write = state.dt->write;

  // TODO Introduce layout-for-write or primary layout to remove the assumption
  // that first layout is the right one.
  VELOX_CHECK_EQ(
      1,
      write->table().layouts().size(),
      "Writes to tables with multiple-layouts are not supported yet");

  const auto* layout = write->table().layouts().at(0);
  const auto& partitionColumns = layout->partitionColumns();
  if (partitionColumns.empty()) {
    // Unpartitioned write.
    return plan;
  }

  const auto& tableSchema = write->table().type();

  // Find values for all partition columns.
  ExprVector keyValues;
  keyValues.reserve(partitionColumns.size());
  for (const auto* column : partitionColumns) {
    const auto index = tableSchema->getChildIdx(column->name());
    keyValues.emplace_back(write->columnExprs().at(index));
  }

  const auto* planPartitionType = plan->distribution().partitionType();

  // Copartitioning is possible if PartitionTypes are compatible and the table
  // has no fewer partitions than the plan.
  bool shuffle = !hasCopartition(planPartitionType, layout->partitionType());
  if (!shuffle) {
    // Check that the partition keys of the plan are assigned pairwise to the
    // partition columns of the layout.
    for (auto i = 0; i < keyValues.size(); ++i) {
      if (!plan->distribution().partitionKeys()[i]->sameOrEqual(
              *keyValues[i])) {
        shuffle = true;
        break;
      }
    }

    if (!shuffle) {
      return plan;
    }
  }

  Distribution distribution(layout->partitionType(), std::move(keyValues));
  auto* repartition = make<Repartition>(plan, std::move(distribution));
  state.addCost(*repartition);
  return repartition;
}

} // namespace

void Optimization::addPostprocess(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
#ifndef NDEBUG
  // Sanity check that all tables and conjuncts have been placed.
  for (const auto& table : state.dt->tables) {
    VELOX_DCHECK(
        state.isPlaced(table),
        "Failed to place a table: {}",
        table->toString());
  }

  for (const auto* conjunct : state.dt->conjuncts) {
    VELOX_DCHECK(
        state.isPlaced(conjunct),
        "Failed to place a conjunct: {}",
        conjunct->toString());
  }
#endif

  if (dt->write) {
    VELOX_DCHECK(!dt->hasAggregation());
    VELOX_DCHECK(!dt->hasOrderBy());
    VELOX_DCHECK(!dt->hasLimit());
    PrecomputeProjection precompute{plan, dt, /*projectAllInputs=*/false};
    auto writeColumns = precompute.toColumns(dt->write->columnExprs());
    plan = std::move(precompute).maybeProject();
    state.addCost(*plan);

    plan = repartitionForWrite(plan, state);
    plan = make<TableWrite>(plan, std::move(writeColumns), dt->write);

    // Table write is present in every candidate plan and it is the root node.
    // Hence, it doesn't affect the choice of candidate plan. Hence, no need to
    // track the cost.
    return;
  }

  if (dt->aggregation) {
    addAggregation(dt, plan, state);
  }

  if (!dt->having.empty()) {
    auto filter = make<Filter>(plan, dt->having);
    state.place(dt->having);
    state.addCost(*filter);
    plan = filter;
  }

  const bool needsLimit = [&] {
    if (dt->limit == 0) {
      return true;
    }
    if (dt->hasOrderBy()) {
      addOrderBy(dt, plan, state);
      return false;
    }
    if (!dt->hasLimit()) {
      return false;
    }
    auto pushdownLimit = [&] {
      if ((isSingleWorker_ && isSingleDriver_) ||
          options_.alwaysPushdownLimit) {
        // Limit doesn't affect parallelism,
        // so we can apply it as early as possible.
        return true;
      }
      const auto parallelism =
          runnerOptions_.numWorkers * runnerOptions_.numDrivers;
      // Instead of 1024, we want to use preferred_output_batch_rows?
      const auto willCompute =
          options_.planBestThroughput ? 1024 / parallelism : 1024 * parallelism;
      return dt->limit <= std::max<int64_t>(1, willCompute);
    };
    if (options_.alwaysPullupLimit || !pushdownLimit()) {
      return true;
    }
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
    return false;
  }();

  if (!dt->columns.empty()) {
    ColumnVector usedColumns;
    ExprVector usedExprs;
    for (auto i = 0; i < dt->exprs.size(); ++i) {
      const auto* expr = dt->exprs[i];
      if (state.targetExprs.contains(expr)) {
        usedColumns.emplace_back(dt->columns[i]);
        usedExprs.emplace_back(state.toColumn(expr));
      }
    }

    if (dt->exprs.hasWindows()) {
      plan = addWindowOps(std::move(plan), usedExprs);
    }
    plan = make<Project>(
        maybeDropProject(plan),
        usedExprs,
        usedColumns,
        Project::isRedundant(plan, usedExprs, usedColumns));
  }

  if (needsLimit) {
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
  }
}

namespace {

AggregateVector flattenAggregates(
    const AggregateVector& aggregates,
    PrecomputeProjection& precompute) {
  AggregateVector flatAggregates;
  flatAggregates.reserve(aggregates.size());

  for (const auto& agg : aggregates) {
    ExprCP condition = nullptr;
    if (agg->condition()) {
      condition = precompute.toColumn(agg->condition());
    }
    auto args = precompute.toColumns(
        agg->args(), /*aliases=*/nullptr, /*preserveLiterals=*/true);
    auto orderKeys = precompute.toColumns(agg->orderKeys());
    flatAggregates.emplace_back(
        make<Aggregate>(
            agg->name(),
            agg->value(),
            std::move(args),
            agg->functions(),
            agg->isDistinct(),
            condition,
            agg->intermediateType(),
            std::move(orderKeys),
            agg->orderTypes()));
  }

  return flatAggregates;
}

} // namespace

// static
RelationOpPtr Optimization::planSingleAggregation(
    DerivedTableCP dt,
    RelationOpPtr& input) {
  const auto* aggPlan = dt->aggregation;

  PrecomputeProjection precompute(input, dt, /*projectAllInputs=*/false);
  auto groupingKeys =
      precompute.toColumns(aggPlan->groupingKeys(), &aggPlan->columns());
  auto aggregates = flattenAggregates(aggPlan->aggregates(), precompute);

  return make<Aggregation>(
      std::move(precompute).maybeProject(),
      std::move(groupingKeys),
      std::move(aggregates),
      velox::core::AggregationNode::Step::kSingle,
      aggPlan->columns());
}

void Optimization::addAggregation(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  const auto* aggPlan = dt->aggregation;

  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  auto groupingKeys =
      precompute.toColumns(aggPlan->groupingKeys(), &aggPlan->columns());
  auto aggregates = flattenAggregates(aggPlan->aggregates(), precompute);

  plan = std::move(precompute).maybeProject();
  state.place(aggPlan);

  if ((isSingleWorker_ && isSingleDriver_) ||
      options_.alwaysPlanSingleAggregation) {
    auto* singleAgg = make<Aggregation>(
        plan,
        std::move(groupingKeys),
        std::move(aggregates),
        velox::core::AggregationNode::Step::kSingle,
        aggPlan->columns());

    state.addCost(*singleAgg);
    plan = singleAgg;
    return;
  }

  // We make a plan with partial agg and one without and pick the better
  // according to cost model. We use the cost functions of the RelationOps to
  // get details of the width of intermediate results, shuffles and so forth. A
  // simpler but less precise way would be to simply not make a partial agg if
  // expected total cardinality is more than so much. But the capacity of
  // partial agg also depends on the width of the data and configs so instead of
  // unbundling the cost functions we make different kinds of plans and use the
  // plan's functions.
  const auto planBeforeAgg = plan;
  auto* partialAgg = make<Aggregation>(
      plan,
      groupingKeys,
      aggregates,
      velox::core::AggregationNode::Step::kPartial,
      aggPlan->intermediateColumns());

  PlanCost splitAggCost;
  splitAggCost.add(*partialAgg);
  plan = repartitionForAgg(aggPlan, partialAgg, splitAggCost);

  const auto numKeys = aggPlan->groupingKeys().size();

  ExprVector finalGroupingKeys;
  finalGroupingKeys.reserve(numKeys);
  for (auto i = 0; i < numKeys; ++i) {
    finalGroupingKeys.push_back(aggPlan->intermediateColumns()[i]);
  }

  auto* splitAggPlan = make<Aggregation>(
      plan,
      std::move(finalGroupingKeys),
      aggregates,
      velox::core::AggregationNode::Step::kFinal,
      aggPlan->columns());
  splitAggCost.add(*splitAggPlan);

  if (numKeys == 0 || options_.alwaysPlanPartialAggregation) {
    // If there is no grouping, we always make partial + final.
    plan = splitAggPlan;
    state.cost.add(splitAggCost);
    return;
  }

  // Now we make a plan without partial aggregation.
  PlanCost singleAggCost;
  plan = repartitionForAgg(aggPlan, planBeforeAgg, singleAggCost);
  auto* singleAgg = make<Aggregation>(
      plan,
      groupingKeys,
      aggregates,
      velox::core::AggregationNode::Step::kSingle,
      aggPlan->columns());
  singleAggCost.add(*singleAgg);

  if (singleAggCost.cost < splitAggCost.cost) {
    plan = singleAgg;
    state.cost.add(singleAggCost);
    return;
  }

  state.cost.add(splitAggCost);
  plan = splitAggPlan;
}

void Optimization::addOrderBy(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  ExprVector orderKeys;
  if (dt->orderKeys.hasWindows()) {
    orderKeys = dt->orderKeys;
    plan = addWindowOps(std::move(plan), orderKeys);
    orderKeys = precompute.toColumns(orderKeys);
  } else {
    orderKeys = precompute.toColumns(dt->orderKeys);
  }
  VELOX_CHECK_EQ(orderKeys.size(), dt->orderKeys.size());

  for (size_t i = 0; i < orderKeys.size(); ++i) {
    state.addExprToColumn(dt->orderKeys[i], orderKeys[i]);
  }

  state.place(dt->orderKeys);

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
  if (!options_.enableIndexLookupJoin) {
    return;
  }
  if (candidate.tables.size() != 1 ||
      candidate.tables[0]->isNot(PlanType::kTableNode) ||
      !candidate.existences.empty()) {
    // Index applies to single base tables.
    return;
  }

  auto [right, left] = candidate.joinSides();

  const auto joinType = right.leftJoinType;
  if (!velox::core::IndexLookupJoinNode::isSupported(joinType)) {
    return;
  }

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
    PlanStateSaver save{state};
    auto newPartition = repartitionForIndex(info, left.keys, plan, state);
    if (!newPartition) {
      continue;
    }
    state.place(candidate.tables.at(0));

    auto fanout = info.scanCardinality * rightTable->filterSelectivity;
    if (joinType == velox::core::JoinType::kLeft) {
      fanout = std::max<float>(1, fanout);
    }

    ExprVector lookupKeys{left.keys.begin(), left.keys.end()};
    // The number of keys is the prefix that matches index order.
    lookupKeys.resize(info.lookupKeys.size());
    state.placeColumns(availableColumns(rightTable, index));

    auto c = state.downstreamColumns();
    c.intersect(state.columns());
    c.unionColumns(rightTable->filter);

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

    state.placeColumns(c);
    state.addCost(*scan);
    state.addNextJoin(&candidate, scan, toTry);
  }
}

namespace {
// Check if 'mark' column produced by a SemiProject join is used only to
// filter the results using 'mark' or 'not(mark)' condition. If so, replace
// the join with a SemiFilter and remove the filter.
void tryOptimizeSemiProject(
    velox::core::JoinType& joinType,
    ColumnCP& mark,
    PlanState& state,
    Name negation) {
  if (!mark) {
    return;
  }
  const bool leftProject = joinType == velox::core::JoinType::kLeftSemiProject;
  const bool rightProject =
      joinType == velox::core::JoinType::kRightSemiProject;
  if (!leftProject && !rightProject) {
    return;
  }
  const auto* markFilter = state.isDownstreamFilterOnly(mark);
  if (!markFilter) {
    return;
  }
  if (markFilter == mark) {
    joinType = leftProject ? velox::core::JoinType::kLeftSemiFilter
                           : velox::core::JoinType::kRightSemiFilter;
    mark = nullptr;
    state.place(markFilter);
  } else if (
      leftProject && isCallExpr(markFilter, negation) &&
      markFilter->as<Call>()->argAt(0) == mark) {
    joinType = velox::core::JoinType::kAnti;
    mark = nullptr;
    state.place(markFilter);
  }
}
} // namespace

void Optimization::tryMergeJoin(
    const JoinCandidate& candidate,
    PlanState& state,
    velox::core::JoinType joinType,
    const RelationOpPtr& probeInput,
    const ExprVector& probeKeys,
    const RelationOpPtr& buildInput,
    const ExprVector& buildKeys,
    float fanout,
    const ColumnVector& columns,
    std::vector<NextJoin>& toTry) const {
  if (!velox::core::MergeJoinNode::isSupported(joinType)) {
    return;
  }
  if (!isSingleWorker_ &&
      (!probeInput->distribution().isGather() ||
       !buildInput->distribution().isGather())) {
    return;
  }

  const auto& probeOrderTypes = probeInput->distribution().orderTypes();
  const auto& buildOrderTypes = buildInput->distribution().orderTypes();
  const auto& probeOrderKeys = probeInput->distribution().orderKeys();
  const auto& buildOrderKeys = buildInput->distribution().orderKeys();
  const uint32_t minSize =
      std::min(probeOrderTypes.size(), buildOrderTypes.size());
  ExprVector newProbeKeys;
  ExprVector newBuildKeys;
  newProbeKeys.resize(minSize);
  newBuildKeys.resize(minSize);
  uint32_t maxIndex = 0;
  for (uint32_t i = 0; i < probeKeys.size(); ++i) {
    uint32_t index = kNotFound;
    for (uint32_t j = 0; j < minSize; ++j) {
      if (probeOrderTypes[j] != buildOrderTypes[j]) {
        continue;
      }
      if (!probeOrderKeys[j]->sameOrEqual(*probeKeys[i])) {
        continue;
      }
      if (!buildOrderKeys[j]->sameOrEqual(*buildKeys[i])) {
        continue;
      }
      index = j;
      break;
    }
    if (index == kNotFound) {
      return;
    }
    newProbeKeys[index] = probeKeys[i];
    newBuildKeys[index] = buildKeys[i];
    maxIndex = std::max(maxIndex, index);
  }
  for (uint32_t i = 0; i <= maxIndex; ++i) {
    if (newProbeKeys[i]) {
      continue;
    }
    if (probeOrderTypes[i] != buildOrderTypes[i]) {
      return;
    }
    if (!probeOrderKeys[i]->sameOrEqual(*buildOrderKeys[i])) {
      return;
    }
  }
  std::erase(newProbeKeys, nullptr);
  std::erase(newBuildKeys, nullptr);

  PlanStateSaver save{state};
  RelationOp* join = make<Join>(
      JoinMethod::kMerge,
      joinType,
      false,
      probeInput,
      buildInput,
      std::move(newProbeKeys),
      std::move(newBuildKeys),
      candidate.join->filter(),
      fanout,
      columns);
  state.addCost(*join);
  state.addNextJoin(&candidate, join, toTry);
}

template <typename Reshuffle>
void Optimization::joinByKeys(
    const JoinCandidate& candidate,
    const JoinSide& probe,
    RelationOpPtr probeInput,
    PlanState& state,
    const JoinSide& build,
    RelationOpPtr buildInput,
    PlanState& memoState,
    const Reshuffle& reshuffle,
    float lrFanout,
    float rlFanout,
    std::vector<NextJoin>& toTry) {
  VELOX_DCHECK(probe.markColumn == build.markColumn);

  auto joinType = build.leftJoinType;

  const bool probeOnly = joinType == velox::core::JoinType::kLeftSemiFilter ||
      joinType == velox::core::JoinType::kLeftSemiProject ||
      joinType == velox::core::JoinType::kAnti;

  const bool buildOnly = joinType == velox::core::JoinType::kRightSemiFilter ||
      joinType == velox::core::JoinType::kRightSemiProject;

  PlanObjectSet probeColumns;
  if (!buildOnly) {
    probeColumns.unionObjects(probeInput->columns());
  }

  PlanObjectSet buildColumns;
  if (!probeOnly) {
    buildColumns.unionObjects(buildInput->columns());
  }

  ColumnVector columns;
  PlanObjectSet columnSet;
  ColumnCP mark = nullptr;

  state.downstreamColumns().forEach<Column>([&](auto column) {
    if (column == build.markColumn) {
      mark = column;
      return;
    }

    if ((probeOnly || !buildColumns.contains(column)) &&
        (buildOnly || !probeColumns.contains(column))) {
      return;
    }

    columnSet.add(column);
    columns.push_back(column);
  });

  tryOptimizeSemiProject(joinType, mark, state, negation_);

  // If there is an existence flag, it is the rightmost result column.
  if (mark) {
    columnSet.add(mark);
    columns.push_back(mark);
  }

  PrecomputeProjection precomputeProbe(probeInput, state.dt);
  auto probeKeys = precomputeProbe.toColumns(probe.keys);
  probeInput = std::move(precomputeProbe).maybeProject();

  PrecomputeProjection precomputeBuild(buildInput, state.dt);
  auto buildKeys = precomputeBuild.toColumns(build.keys);
  buildInput = std::move(precomputeBuild).maybeProject();

  VELOX_DCHECK_EQ(probeKeys.size(), buildKeys.size());

  reshuffle(probeInput, probeKeys, state, buildInput, buildKeys, memoState);

  const auto fanout = fanoutJoinTypeLimit(
      joinType,
      lrFanout,
      rlFanout,
      memoState.cost.cardinality / state.cost.cardinality);

  state.replaceColumns(std::move(columnSet));
  state.cost.cost += memoState.cost.cost;

  tryMergeJoin(
      candidate,
      state,
      joinType,
      probeInput,
      probeKeys,
      buildInput,
      buildKeys,
      fanout,
      columns,
      toTry);

  RelationOp* join = make<Join>(
      JoinMethod::kHash,
      joinType,
      candidate.join->isNullAwareIn(),
      probeInput,
      buildInput,
      std::move(probeKeys),
      std::move(buildKeys),
      candidate.join->filter(),
      fanout,
      std::move(columns));
  state.addCost(*join);
  state.addNextJoin(&candidate, join, toTry);
}

void Optimization::probeJoin(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  checkTables(candidate);

  auto [build, probe] = candidate.joinSides();
  const auto joinType = build.leftJoinType;

  const auto partKeys = joinKeyPartition(plan, probe.keys);
  ExprVector copartition;
  if (partKeys.empty()) {
    // Prefer to make a build partitioned on join keys and shuffle probe to
    // align with build.
    copartition = {build.keys.begin(), build.keys.end()};
  }

  PlanObjectSet buildFilterColumns;
  buildFilterColumns.unionColumns(candidate.join->filter());
  buildFilterColumns.intersect(availableColumns(candidate.tables[0]));

  PlanObjectSet buildTables;
  PlanObjectSet buildColumns;
  for (auto buildTable : candidate.tables) {
    buildColumns.unionSet(availableColumns(buildTable));
    buildTables.add(buildTable);
  }

  PlanStateSaver save{state};
  state.place(buildTables);
  state.placeColumns(buildColumns);

  buildColumns.intersect(state.downstreamColumns());

  buildColumns.unionColumns(build.keys);
  buildColumns.unionSet(buildFilterColumns);

  MemoKey memoKey = MemoKey::create(
      candidate.tables[0], buildColumns, buildTables, candidate.existences);

  Distribution distributionForBuild;
  const Distribution* forBuild = nullptr;
  if (!copartition.empty()) {
    distributionForBuild = {nullptr, copartition};
    forBuild = &distributionForBuild;
  }

  bool needsShuffle = false;
  auto buildPlan = makePlan(
      *state.dt, memoKey, forBuild, {}, candidate.existsFanout, needsShuffle);

  PlanState memoState(state.optimization, state.dt, buildPlan);
  RelationOpPtr buildInput = buildPlan->op;
  RelationOpPtr probeInput = plan;

  auto reshuffle = [&](auto& probeInput,
                       const auto& probeKeys,
                       auto& state,
                       auto& buildInput,
                       const auto& buildKeys,
                       auto& memoState) {
    if (!isSingleWorker_ &&
        !(probeInput->distribution().isGather() &&
          buildInput->distribution().isGather())) {
      if (!partKeys.empty()) {
        if (needsShuffle) {
          if (copartition.empty()) {
            for (auto i : partKeys) {
              copartition.push_back(build.keys[i]);
            }
          }
          Distribution distribution{
              probeInput->distribution().distributionType(), copartition};
          buildInput = make<Repartition>(buildInput, std::move(distribution));
          memoState.addCost(*buildInput);
        }
      } else if (
          joinType != velox::core::JoinType::kRight &&
          joinType != velox::core::JoinType::kFull &&
          isSingleWorkerSize(*buildInput)) {
        buildInput = make<Repartition>(buildInput, Distribution::broadcast());
        memoState.addCost(*buildInput);
      } else {
        // The probe gets shuffled to align with build. If build is not
        // partitioned on its keys, shuffle the build too.
        alignJoinSides(
            buildInput, buildKeys, memoState, probeInput, probeKeys, state);
      }
    }
  };

  joinByKeys(
      candidate,
      probe,
      probeInput,
      state,
      build,
      buildInput,
      memoState,
      reshuffle,
      candidate.fanout,
      candidate.join->rlFanout(),
      toTry);
}

void Optimization::buildJoin(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  checkTables(candidate);

  auto [probe, build] = candidate.joinSides();
  const auto joinType = build.leftJoinType;

  PlanObjectSet probeFilterColumns;
  probeFilterColumns.unionColumns(candidate.join->filter());
  probeFilterColumns.intersect(availableColumns(candidate.tables[0]));

  PlanObjectSet probeTables;
  PlanObjectSet probeColumns;
  for (auto probeTable : candidate.tables) {
    probeColumns.unionSet(availableColumns(probeTable));
    probeTables.add(probeTable);
  }

  PlanStateSaver save{state};
  state.place(probeTables);
  state.placeColumns(probeColumns);

  probeColumns.intersect(state.downstreamColumns());

  probeColumns.unionColumns(probe.keys);
  probeColumns.unionSet(probeFilterColumns);

  MemoKey memoKey = MemoKey::create(
      candidate.tables[0], probeColumns, probeTables, candidate.existences);

  bool needsShuffle = false;
  auto probePlan = makePlan(
      *state.dt, memoKey, {}, {}, candidate.existsFanout, needsShuffle);

  PlanState memoState(state.optimization, state.dt, probePlan);
  RelationOpPtr probeInput = probePlan->op;
  RelationOpPtr buildInput = plan;

  auto reshuffle = [&](auto& probeInput,
                       const auto& probeKeys,
                       auto& state,
                       auto& buildInput,
                       const auto& buildKeys,
                       auto& memoState) {
    if (!isSingleWorker_ &&
        !(probeInput->distribution().isGather() &&
          buildInput->distribution().isGather())) {
      // The build gets shuffled to align with probe. If probe is not
      // partitioned on its keys, shuffle the probe too.
      alignJoinSides(
          probeInput, probeKeys, memoState, buildInput, buildKeys, state);
    }
    std::swap(state.cost, memoState.cost);
  };

  joinByKeys(
      candidate,
      probe,
      probeInput,
      state,
      build,
      buildInput,
      memoState,
      reshuffle,
      candidate.join->rlFanout(),
      candidate.fanout,
      toTry);
}

void Optimization::crossJoin(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  const auto build = candidate.join
      ? candidate.join->sideOf(candidate.tables[0], false)
      : JoinSide{};
  const auto joinType = build.leftJoinType;
  if (!velox::core::NestedLoopJoinNode::isSupported(joinType)) {
    return;
  }
  auto filterExprs = candidate.join ? candidate.join->filter() : ExprVector{};

  PlanObjectSet buildFilterColumns;
  buildFilterColumns.unionColumns(filterExprs);
  buildFilterColumns.intersect(availableColumns(candidate.tables[0]));

  PlanObjectSet buildTables;
  PlanObjectSet buildColumns;
  for (auto buildTable : candidate.tables) {
    buildColumns.unionSet(availableColumns(buildTable));
    buildTables.add(buildTable);
  }

  PlanStateSaver save{state};
  state.place(buildTables);
  state.placeColumns(buildColumns);

  buildColumns.intersect(state.downstreamColumns());

  buildColumns.unionSet(buildFilterColumns);

  MemoKey memoKey = MemoKey::create(
      candidate.tables[0], buildColumns, buildTables, candidate.existences);

  const auto forBuild = plan->distribution().isGather() ||
          joinType == velox::core::JoinType::kRight ||
          joinType == velox::core::JoinType::kFull
      ? Distribution::gather()
      : Distribution::broadcast();

  bool needsShuffle = false;
  auto buildPlan = makePlan(
      *state.dt, memoKey, &forBuild, {}, candidate.existsFanout, needsShuffle);

  PlanState memoState(state.optimization, state.dt, buildPlan);
  RelationOpPtr buildInput = buildPlan->op;
  if (needsShuffle) {
    buildInput = make<Repartition>(buildInput, forBuild);
    memoState.addCost(*buildInput);
  }

  const bool probeOnly = joinType == velox::core::JoinType::kLeftSemiProject;

  PlanObjectSet probeColumns;
  probeColumns.unionObjects(plan->columns());

  if (!probeOnly) {
    buildColumns = {};
    buildColumns.unionObjects(buildInput->columns());
  }

  ColumnVector columns;
  PlanObjectSet columnSet;
  ColumnCP mark = nullptr;

  state.downstreamColumns().forEach<Column>([&](auto column) {
    if (column == build.markColumn) {
      mark = column;
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
    columnSet.add(mark);
    columns.push_back(mark);
  }

  state.replaceColumns(std::move(columnSet));
  state.cost.cost += memoState.cost.cost;

  RelationOp* join = Join::makeCrossJoin(
      plan,
      std::move(buildInput),
      joinType,
      std::move(filterExprs),
      std::move(columns));
  state.addCost(*join);

  state.addNextJoin(&candidate, join, toTry);
}

void Optimization::crossJoinUnnest(
    RelationOpPtr plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  PlanStateSaver save{state};
  for (const auto* table : candidate.tables) {
    VELOX_CHECK(table->is(PlanType::kUnnestTableNode));
    // We add unnest table before compute downstream columns because
    // we're not interested in the replicating columns needed only for unnest.
    state.place(table);

    PrecomputeProjection precompute(plan, state.dt, /*projectAllInputs=*/false);

    ExprVector replicateColumns;
    const auto& downstreamColumns = state.downstreamColumns();
    downstreamColumns.forEach<Column>([&](auto column) {
      if (state.columns().contains(column)) {
        replicateColumns.push_back(precompute.toColumn(column));
      }
    });

    const auto* unnestTable = table->as<UnnestTable>();
    auto ordinalityColumn = unnestTable->ordinalityColumn;

    // TODO: Revisit here if there is pruning of ordinality
    // column in optimizer.
    if (ordinalityColumn) {
      VELOX_CHECK(
          downstreamColumns.contains(ordinalityColumn),
          "Ordinality column must be present");
    }

    // We don't use downstreamColumns() for unnestExprs/unnestedColumns.
    // Because 'unnest-column' should be unnested even when it isn't used.
    // Because it can change cardinality of the all output.
    const auto& unnestExprs = candidate.join->leftKeys();
    const auto& unnestedColumns = unnestTable->columns;

    // Plan is updated here,
    // because we can have multiple unnest joins in single JoinCandidate.

    auto unnestColumns = precompute.toColumns(unnestExprs);
    plan = std::move(precompute).maybeProject();

    plan = make<Unnest>(
        std::move(plan),
        std::move(replicateColumns),
        std::move(unnestColumns),
        unnestedColumns,
        unnestTable->ordinalityColumn);

    state.replaceColumns(PlanObjectSet::fromObjects(plan->columns()));
    state.addCost(*plan);
  }
  state.addNextJoin(&candidate, std::move(plan), toTry);
}

void Optimization::addJoin(
    const JoinCandidate& candidate,
    const RelationOpPtr& plan,
    PlanState& state,
    std::vector<NextJoin>& result) {
  // If either no join or join without equalities, make cross
  // join. Revisit for index joins when adding index capable
  // connectors.
  if (!candidate.join || candidate.join->leftKeys().empty()) {
    crossJoin(plan, candidate, state, result);
    return;
  }

  // If this candidate has multiple Unnest they all will be handled at once.
  if (candidate.join->isUnnest()) {
    crossJoinUnnest(plan, candidate, state, result);
    return;
  }

  std::vector<NextJoin> toTry;
  joinByIndex(plan, candidate, state, toTry);

  probeJoin(plan, candidate, state, toTry);

  if (!options_.syntacticJoinOrder() && candidate.join->hasRightHashVariant()) {
    // There is a hash based candidate with a non-commutative join.
    // Try a right join variant.
    buildJoin(plan, candidate, state, toTry);
  }

  // If one is much better do not try the other.
  for (auto& next : toTry | std::views::reverse) {
    for (const auto& prev : toTry) {
      if (&prev == &next || !next.candidate || !prev.candidate) {
        continue;
      }
      if (next.isWorse(prev)) {
        next.candidate = nullptr;
      }
    }
  }
  for (auto& next : toTry) {
    if (next.candidate) {
      result.push_back(std::move(next));
    }
  }
}

void Optimization::tryNextJoins(
    PlanState& state,
    std::vector<NextJoin>& nextJoins) {
  for (auto& next : nextJoins) {
    PlanStateSaver save(state);
    state.restore(next);
    makeJoins(std::move(next.plan), state);
  }
}

RelationOpPtr Optimization::placeSingleRowDt(
    RelationOpPtr plan,
    DerivedTableCP subquery,
    PlanState& state) {
  MemoKey memoKey = MemoKey::create(
      subquery,
      PlanObjectSet::fromObjects(subquery->columns),
      PlanObjectSet::single(subquery));

  const auto forBuild = plan->distribution().isGather()
      ? Distribution::gather()
      : Distribution::broadcast();
  bool needsShuffle = false;
  auto rightPlan = makePlan(*state.dt, memoKey, &forBuild, {}, 1, needsShuffle);

  auto rightOp = rightPlan->op;

  if (needsShuffle) {
    rightOp = make<Repartition>(rightOp, forBuild);
  }

  auto resultColumns = plan->columns();
  resultColumns.insert(
      resultColumns.end(),
      rightOp->columns().begin(),
      rightOp->columns().end());
  auto* join = Join::makeCrossJoin(
      std::move(plan),
      std::move(rightOp),
      velox::core::JoinType::kInner,
      ExprVector{},
      std::move(resultColumns));

  state.place(subquery);
  state.addCost(*join);
  return join;
}

void Optimization::placeDerivedTable(DerivedTableCP from, PlanState& state) {
  PlanStateSaver save(state);

  state.place(from);

  PlanObjectSet dtColumns;
  dtColumns.unionObjects(from->columns);
  dtColumns.intersect(state.downstreamColumns());
  state.placeColumns(dtColumns);

  MemoKey key =
      MemoKey::create(from, std::move(dtColumns), PlanObjectSet::single(from));

  bool ignore = false;
  auto plan = makePlan(*state.dt, key, {}, {}, 1, ignore);
  state.cost = plan->cost;

  // Make plans based on the dt alone as first.
  makeJoins(plan->op, state);

  // We see if there are reducing joins to import inside the dt.
  PlanObjectSet visited = state.placed();
  visited.add(from);
  visited.unionSet(state.dt->importedExistences);

  PlanObjectSet reducingSet = PlanObjectSet::single(from);

  std::vector<PlanObjectCP> path{from};

  float reduction = 1;
  reducingJoinsRecursive(
      state, from, 1, 1.2, path, visited, reducingSet, reduction);

  if (reduction < 0.9) {
    MemoKey reducingKey = MemoKey::create(
        from, state.downstreamColumns(), std::move(reducingSet));
    plan = makePlan(*state.dt, reducingKey, {}, {}, 1, ignore);
    state.cost = plan->cost;
    makeJoins(plan->op, state);
  }
}

bool Optimization::placeConjuncts(
    RelationOpPtr plan,
    PlanState& state,
    bool joinsPlaced) {
  PlanStateSaver save(state);

  PlanObjectSet noPushdownTables;
  if (!joinsPlaced) {
    for (const auto* join : state.dt->joins) {
      if (join->leftOptional()) {
        // No pushdown to the left side of a RIGHT or FULL join.
        noPushdownTables.add(join->leftTable());
      }
      if (join->rightOptional()) {
        // No pushdown to the right side of a LEFT or FULL join.
        noPushdownTables.add(join->rightTable());
      }
    }
  }

  PlanObjectSet columnsAndSingles = state.columns();
  state.dt->singleRowDts.forEach<DerivedTable>(
      [&](auto dt) { columnsAndSingles.unionObjects(dt->columns); });

  ExprVector filters;
  for (auto& conjunct : state.dt->conjuncts) {
    if (!joinsPlaced && conjunct->containsNonDeterministic()) {
      continue;
    }
    if (state.isPlaced(conjunct)) {
      continue;
    }
    if (!joinsPlaced) {
      const auto* singleTable = conjunct->singleTable();
      if (singleTable && noPushdownTables.contains(singleTable)) {
        continue;
      }
    }
    if (conjunct->columns().isSubset(state.columns())) {
      state.placeColumn(conjunct);
      filters.push_back(conjunct);
      continue;
    }
    if (conjunct->columns().isSubset(columnsAndSingles)) {
      // The filter depends on placed tables and non-correlated single row
      // subqueries.
      std::vector<DerivedTableCP> placeable;
      auto subqColumns = conjunct->columns();
      subqColumns.except(state.columns());
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
        state.place(conjunct);
        plan = placeSingleRowDt(plan, placeable[i], state);

        plan = make<Filter>(plan, ExprVector{conjunct});
        state.addCost(*plan);

        makeJoins(plan, state);
        return true;
      }
    }
  }

  if (!filters.empty()) {
    for (auto& filter : filters) {
      state.place(filter);
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

  return {DistributionType{}, std::move(columns)};
}

// Adds the costs in the input states to the first state and if 'distinct' is
// not null adds the cost of that to the first state.
PlanP unionPlan(
    std::vector<PlanState>& states,
    const RelationOpPtr& result,
    Aggregation* distinct) {
  auto& firstState = states[0];

  for (auto i = 1; i < states.size(); ++i) {
    const auto& otherCost = states[i].cost;

    firstState.cost.cost += otherCost.cost;
    firstState.cost.cardinality += otherCost.cardinality;
  }
  if (distinct) {
    firstState.addCost(*distinct);
  }
  return make<Plan>(result, states[0]);
}

} // namespace

void Optimization::makeJoins(PlanState& state) {
  QGVector<std::tuple<bool, float, PlanObjectCP>> firstTables;

  VELOX_DCHECK(!state.dt->startTables.empty());
  if (options_.syntacticJoinOrder()) {
    VELOX_DCHECK(!state.dt->joinOrder.empty());
    const auto firstTableId = state.dt->joinOrder[0];
    VELOX_CHECK(state.dt->startTables.BitSet::contains(firstTableId));

    firstTables.emplace_back(false, 0, queryCtx()->objectAt(firstTableId));
  } else {
    firstTables.reserve(state.dt->startTables.size());
    state.dt->startTables.forEach([&](PlanObjectCP table) {
      const float cardinality = tableCardinality(table);
      const bool maybeJoins = table->is(PlanType::kDerivedTableNode) &&
          table->as<DerivedTable>()->tables.size() > 1;
      firstTables.emplace_back(maybeJoins, -cardinality, table);
    });
    // First tables that don't have joins and with larger cardinality.
    std::ranges::sort(firstTables);
    if (!options_.costJoinOrder()) {
      VELOX_DCHECK(!firstTables.empty());
      firstTables.resize(1);
    }
  }

  for (auto [maybeJoins, score, from] : firstTables) {
    if (from->is(PlanType::kTableNode)) {
      auto table = from->as<BaseTable>();
      // Make plan starting with each relevant index of the table.
      VELOX_DCHECK(!table->schemaTable->columnGroups.empty());
      const auto& downstream = state.downstreamColumns();
      for (const auto* index : table->schemaTable->columnGroups) {
        auto columns = indexColumns(downstream, table, index);

        PlanStateSaver save(state);
        state.place(table);
        state.placeColumns(columns);

        auto* scan = make<TableScan>(table, index, columns);
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
      state.place(valuesTable);
      state.placeColumns(columns);
      auto* scan = make<Values>(*valuesTable, std::move(columns));
      state.addCost(*scan);
      makeJoins(scan, state);
    } else if (from->is(PlanType::kUnnestTableNode)) {
      VELOX_FAIL("UnnestTable cannot be a starting table");
      // Because it's right side of unnest join edge
      // and they are non-commutative.
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

    // Go over single-row DTs that haven't been placed yet. If unused, just mark
    // as "placed". Otherwise, place these. Look for any unused single-row DTs.
    // Mark them as "placed".
    const auto& downstream = state.downstreamColumns();
    std::vector<DerivedTableCP> singleRowDtsToPlace;
    state.dt->singleRowDts.forEach<DerivedTable>([&](DerivedTableCP subquery) {
      if (!state.isPlaced(subquery)) {
        if (!downstream.containsAny(subquery->columns)) {
          state.place(subquery);
        } else {
          singleRowDtsToPlace.push_back(subquery);
        }
      }
    });

    for (const auto* singleRowDt : singleRowDtsToPlace) {
      plan = placeSingleRowDt(plan, singleRowDt, state);
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
    const DerivedTable& dt,
    const MemoKey& key,
    const Distribution* desired,
    const PlanObjectSet& boundColumns,
    float existsFanout,
    bool& needsShuffle) {
  needsShuffle = false;
  if (key.firstTable->is(PlanType::kDerivedTableNode) &&
      key.firstTable->as<DerivedTable>()->setOp.has_value()) {
    return makeUnionPlan(dt, key, desired, boundColumns, existsFanout);
  }
  return makeDtPlan(dt, key, desired, existsFanout, needsShuffle);
}

PlanP Optimization::makeUnionPlan(
    const DerivedTable& dt,
    const MemoKey& key,
    const Distribution* desired,
    const PlanObjectSet& boundColumns,
    float existsFanout) {
  const auto* setDt = key.firstTable->as<DerivedTable>();

  RelationOpPtrVector inputs;
  std::vector<PlanP> inputPlans;
  std::vector<PlanState> inputStates;
  std::vector<bool> inputNeedsShuffle;

  for (auto* inputDt : setDt->children) {
    MemoKey inputKey = [&]() {
      PlanObjectSet inputTables = key.tables;
      inputTables.erase(key.firstTable);
      inputTables.add(inputDt);
      return MemoKey::create(
          inputDt,
          PlanObjectSet{key.columns},
          std::move(inputTables),
          std::vector<PlanObjectSet>{key.existences});
    }();

    bool inputShuffle = false;
    auto inputPlan = makePlan(
        dt, inputKey, desired, boundColumns, existsFanout, inputShuffle);
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
    return unionPlan(inputStates, result, distinct);
  }

  if (!desired) {
    if (isDistinct) {
      // Pick some partitioning key and shuffle on that and make distinct.
      Distribution someDistribution = somePartition(inputs);
      for (auto i = 0; i < inputs.size(); ++i) {
        inputs[i] = make<Repartition>(inputs[i], someDistribution);
        inputStates[i].addCost(*inputs[i]);
      }
    }
  } else {
    // Some need a shuffle. Add the shuffles, add an optional distinct and
    // return with no shuffle needed.
    for (auto i = 0; i < inputs.size(); ++i) {
      if (inputNeedsShuffle[i]) {
        inputs[i] = make<Repartition>(inputs[i], *desired);
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
  return unionPlan(inputStates, result, distinct);
}

PlanP Optimization::makeDtPlan(
    const DerivedTable& dt,
    const MemoKey& key,
    const Distribution* desired,
    float existsFanout,
    bool& needsShuffle) {
  PlanSet* plans = memo_.find(key);
  if (plans == nullptr) {
    // Allocate temp DT in the arena. The DT may get flattened and then
    // PrecomputeProjection may create columns that reference that DT. Hence,
    // the DT's lifetime must extend to the lifetime of the optimization.
    auto tmpDt = make<DerivedTable>();
    tmpDt->cname = newCName("tmp_dt");
    tmpDt->import(dt, key.firstTable, key.tables, key.existences, existsFanout);

    PlanState inner(*this, tmpDt);
    if (key.firstTable->is(PlanType::kDerivedTableNode)) {
      inner.setTargetExprsForDt(key.columns);
    } else {
      inner.targetExprs = key.columns;
    }

    makeJoins(inner);
    plans = memo_.insert(key, std::move(inner.plans));
  }
  return plans->best(desired, needsShuffle);
}

ExprCP Optimization::combineLeftDeep(Name func, const ExprVector& exprs) {
  ExprVector copy = exprs;
  std::ranges::sort(copy, [&](ExprCP left, ExprCP right) {
    return left->id() < right->id();
  });
  VELOX_DCHECK(!copy.empty());
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
