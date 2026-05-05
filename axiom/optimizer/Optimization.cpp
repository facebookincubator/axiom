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
#include "axiom/optimizer/AggregationPlanner.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/PrecomputeProjection.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"
#include "folly/coro/Task.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {

Optimization::Optimization(
    SessionPtr session,
    const logical_plan::LogicalPlanNode& logicalPlan,
    const connector::SchemaResolver& schema,
    History& history,
    std::shared_ptr<velox::core::QueryCtx> veloxQueryCtx,
    velox::core::ExpressionEvaluator& evaluator,
    OptimizerOptions options,
    MultiFragmentPlan::Options runnerOptions,
    std::shared_ptr<QueryRuntimeStats> runtimeStats)
    : session_{std::move(session)},
      options_(std::move(options)),
      runnerOptions_(std::move(runnerOptions)),
      isSingleWorker_(runnerOptions_.numWorkers == 1),
      isSingleDriver_(runnerOptions_.numDrivers == 1),
      logicalPlan_(&logicalPlan),
      history_(history),
      veloxQueryCtx_(std::move(veloxQueryCtx)),
      topState_{*this, nullptr},
      aggregationPlanner_{
          isSingleWorker_,
          isSingleDriver_,
          options_.alwaysPlanPartialAggregation},
      toGraph_{schema, evaluator, options_},
      toVelox_{session_, runnerOptions_, options_},
      runtimeStats_{std::move(runtimeStats)} {
  queryCtx()->optimization() = this;

  const auto* planRoot = logicalPlan_;
  if (logicalPlan_->is(logical_plan::NodeKind::kOutput)) {
    outputNames_ = logicalPlan_->as<logical_plan::OutputNode>()->entries();
    planRoot = logicalPlan_->onlyInput().get();
  }

  root_ = toGraph_.makeQueryGraph(*planRoot);
  root_->initializePlans();
}

void Optimization::estimateLeafSelectivity(BaseTable& baseTable) {
  if (!estimatedBaseTables_.insert(baseTable.id()).second) {
    return;
  }
  filterUpdated(&baseTable);
  auto tableHandle = toVelox_.leafData(baseTable.id())->handle;
  history_.estimateLeafSelectivity(baseTable, tableHandle);
}

namespace {
void collectBaseTables(DerivedTable* dt, std::vector<BaseTable*>& baseTables) {
  if (dt->setOp.has_value()) {
    for (auto* child : dt->children) {
      collectBaseTables(child, baseTables);
    }
  } else {
    for (auto* table : dt->tables) {
      if (table->is(PlanType::kTableNode)) {
        baseTables.push_back(const_cast<PlanObject*>(table)->as<BaseTable>());
      } else if (table->is(PlanType::kDerivedTableNode)) {
        collectBaseTables(
            const_cast<PlanObject*>(table)->as<DerivedTable>(), baseTables);
      }
    }
  }
}
} // namespace

void Optimization::estimateAllBaseTableSelectivity(DerivedTable& dt) {
  if (!options_.useFilteredTableStats) {
    return;
  }

  std::vector<BaseTable*> baseTables;
  collectBaseTables(&dt, baseTables);

  if (baseTables.empty()) {
    return;
  }

  // Prepare table handles and launch async stats requests.
  struct TableTask {
    BaseTable* baseTable;
    size_t taskIndex;
    // Indices into baseTable->columns for the top-level columns we requested
    // stats for. columnStats[i] corresponds to
    // baseTable->columns[columnIndices[i]].
    std::vector<size_t> columnIndices;
  };
  std::vector<TableTask> tableTasks;
  std::vector<folly::coro::Task<std::optional<connector::FilteredTableStats>>>
      tasks;

  for (auto* baseTable : baseTables) {
    if (estimatedBaseTables_.contains(baseTable->id())) {
      continue;
    }
    filterUpdated(baseTable);
    const auto* data = toVelox_.leafData(baseTable->id());

    // Collect top-level table column names referenced by the query. Subfield
    // columns (e.g. m.[1]) don't have connector-level stats.
    std::vector<std::string> columnNames;
    std::vector<size_t> columnIndices;
    for (size_t i = 0; i < baseTable->columns.size(); ++i) {
      if (baseTable->columns[i]->topColumn() == nullptr) {
        columnNames.emplace_back(baseTable->columns[i]->name());
        columnIndices.push_back(i);
      }
    }

    auto* layout = baseTable->schemaTable->connectorTable->layouts()[0];
    auto connectorSession =
        session_->toConnectorSession(layout->connector()->connectorId());
    tableTasks.push_back({baseTable, tasks.size(), std::move(columnIndices)});
    tasks.push_back(layout->co_estimateStats(
        std::move(connectorSession),
        data->handle,
        std::move(columnNames),
        data->filterConjuncts));
  }

  if (tasks.empty()) {
    return;
  }

  // Wait for all connector stats requests concurrently. Schedule on the
  // query context executor if available, otherwise run inline.
  auto collectTask = folly::coro::collectAllRange(std::move(tasks));
  auto* executor = veloxQueryCtx_->executor();
  auto estimateStart = std::chrono::steady_clock::now();
  auto results = executor ? folly::coro::blockingWait(co_withExecutor(
                                executor, std::move(collectTask)))
                          : folly::coro::blockingWait(std::move(collectTask));
  if (runtimeStats_) {
    runtimeStats_->recordTiming(
        QueryRuntimeStats::kEstimateStatsWallNanos,
        std::chrono::steady_clock::now() - estimateStart);
  }

  // Apply results.
  for (auto& [baseTable, taskIndex, columnIndices] : tableTasks) {
    applyFilteredStats(*baseTable, results[taskIndex], columnIndices);
  }
}

void Optimization::applyFilteredStats(
    BaseTable& baseTable,
    const std::optional<connector::FilteredTableStats>& stats,
    const std::vector<size_t>& columnIndices) {
  VELOX_CHECK(
      estimatedBaseTables_.insert(baseTable.id()).second,
      "Duplicate estimation for BaseTable: {}",
      baseTable.cname);

  // Fall back to the history-based estimation path if the connector does not
  // support stats estimation.
  if (!stats.has_value()) {
    auto tableHandle = toVelox_.leafData(baseTable.id())->handle;
    history_.estimateLeafSelectivity(baseTable, tableHandle);
    return;
  }

  // Apply connector-provided column statistics first so that
  // conjunctsSelectivity uses connector-refined values for rejected filters.
  if (!stats->columnStats.empty()) {
    VELOX_CHECK_EQ(stats->columnStats.size(), columnIndices.size());
    for (size_t i = 0; i < stats->columnStats.size(); ++i) {
      const auto& columnStats = stats->columnStats[i];
      auto& existing = baseTable.columns[columnIndices[i]]->value();
      Value value(
          existing.type,
          std::max<float>(1, columnStats.numDistinct.value_or(1)));
      value.min = columnStats.min.has_value()
          ? registerVariant(columnStats.min.value())
          : nullptr;
      value.max = columnStats.max.has_value()
          ? registerVariant(columnStats.max.value())
          : nullptr;
      value.nullFraction = columnStats.nullPct / 100.0f;
      value.nullable = !columnStats.nonNull;
      const_cast<Value&>(existing) = value;
    }
  }

  if (stats->rejectedFilterIndices.empty()) {
    // Connector estimated all filters.
    baseTable.filteredCardinality = stats->numRows;
    return;
  }

  // Collect the ExprCP filters the connector could not estimate.
  // Indices refer to columnFilters followed by filter.
  ExprVector rejectedFilters;
  rejectedFilters.reserve(stats->rejectedFilterIndices.size());
  const auto numColumnFilters = baseTable.columnFilters.size();
  const auto totalFilters = numColumnFilters + baseTable.filter.size();
  for (auto idx : stats->rejectedFilterIndices) {
    VELOX_CHECK_LT(
        idx,
        totalFilters,
        "rejectedFilterIndices out of range for BaseTable: {}",
        baseTable.cname);
    rejectedFilters.push_back(
        idx < numColumnFilters ? baseTable.columnFilters[idx]
                               : baseTable.filter[idx - numColumnFilters]);
  }

  // Compute selectivity for the rejected filters and apply constraints.
  ConstraintMap constraints;
  auto selectivity = conjunctsSelectivity(constraints, rejectedFilters, true);

  baseTable.filteredCardinality = stats->numRows * selectivity.trueFraction;

  // Update column values with refined constraints from rejected filters.
  for (const auto& [columnId, constrainedValue] : constraints) {
    for (auto* column : baseTable.columns) {
      if (column->id() == columnId) {
        auto& value = const_cast<Value&>(column->value());
        value.cardinality = constrainedValue.cardinality;
        value.min = constrainedValue.min;
        value.max = constrainedValue.max;
        value.trueFraction = constrainedValue.trueFraction;
        value.nullFraction = constrainedValue.nullFraction;
        value.nullable = constrainedValue.nullable;
        break;
      }
    }
  }
}

// static
PlanAndStats Optimization::toVeloxPlan(
    const logical_plan::LogicalPlanNode& logicalPlan,
    velox::memory::MemoryPool& pool,
    OptimizerOptions options,
    MultiFragmentPlan::Options runnerOptions) {
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

  auto session = std::make_shared<Session>(veloxQueryCtx->queryId());

  Optimization opt{
      session,
      logicalPlan,
      *schemaResolver,
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
              << id << ": " << cost.toString() << ": " << " " << plan.toString()
              << std::endl;
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
// Thresholds used in reducing-joins logic.
//
// These constants control when the optimizer introduces "reducing" semi-joins
// that pre-filter a table before joining with the main query. The goal is to
// reduce intermediate result sizes when small dimension tables can filter
// large fact tables early.
//
// The values are empirically tuned based on TPC-H and production workloads:
// - Too aggressive (low thresholds): Creates unnecessary semi-joins that add
//   overhead without sufficient row reduction.
// - Too conservative (high thresholds): Misses opportunities to reduce
//   intermediate result sizes.
struct ReducingJoinsMagic {
  // Maximum single-edge fanout to follow during DFS traversal.
  // Edges with fanout > 1.2 are unlikely to reduce rows enough to justify
  // the overhead of a semi-join. The 1.2 threshold allows for slight row
  // expansion while exploring paths that ultimately reduce overall cardinality.
  static constexpr float kMaxEdgeFanout = 1.2;

  // Cumulative fanout below which a path is declared "reducing"
  // and its tables are added to the result.
  // A path with cumulative fanout < 0.9 reduces total rows by at least 10%,
  // which typically justifies the cost of an additional semi-join operation.
  static constexpr float kReducingPathThreshold = 0.9;

  // Minimum reduction required to create a semi-join existence.
  // Existence checks (semi-joins) have lower overhead than full joins, so we
  // require at least 30% row reduction (fanout < 0.7) to justify adding them.
  // This is more conservative than kReducingPathThreshold because existence
  // semi-joins add an extra table scan and hash build.
  static constexpr float kExistenceReductionThreshold = 0.7;
};

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
    if (join->isLeftOuter() && join->leftTable() != nullptr &&
        candidate == join->rightTable() &&
        candidate->is(PlanType::kDerivedTableNode)) {
      // One can restrict the build of the optional side by a restriction on the
      // probe. This happens specially when value subqueries are represented as
      // optional sides of left join. These are often aggregations and there is
      // no point creating values for groups that can't be probed.
      ;
    } else if (join->leftOptional() || join->rightOptional()) {
      continue;
    }

    JoinSide other = join->sideOf(candidate, true);
    if (other.table == nullptr) {
      continue;
    }

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
    auto nextMaxFanout = maxFanout;
    if (fanout < ReducingJoinsMagic::kReducingPathThreshold) {
      result.add(other.table);
      for (auto step : path) {
        result.add(step);
        nextMaxFanout = 1;
      }
    }

    path.push_back(other.table);
    isLeaf = false;
    reducingJoinsRecursive(
        state,
        other.table,
        fanout,
        nextMaxFanout,
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

// Result of searching for bushy reducing joins from a start table.
struct BushyJoins {
  PlanObjectSet tables;
  float reduction;
};

// Traverses join edges from 'startTable', skipping tables in
// 'excludedTables', looking for paths whose cumulative fanout is
// below kReducingPathThreshold. Returns the set of tables (including
// 'startTable') and the cumulative reduction, or std::nullopt if no
// reducing path is found.
std::optional<BushyJoins> findReducingBushyJoins(
    const PlanState& state,
    PlanObjectCP startTable,
    const PlanObjectSet& excludedTables = {}) {
  PlanObjectSet reducingTables;
  PlanObjectSet visited = state.placed();
  visited.add(startTable);
  visited.unionSet(excludedTables);
  reducingTables.add(startTable);
  std::vector<PlanObjectCP> path{startTable};
  float reduction = 1;
  reducingJoinsRecursive(
      state,
      startTable,
      /*fanoutFromRoot=*/1,
      ReducingJoinsMagic::kMaxEdgeFanout,
      path,
      visited,
      reducingTables,
      reduction);
  if (reduction < ReducingJoinsMagic::kReducingPathThreshold) {
    return BushyJoins{std::move(reducingTables), reduction};
  }
  return std::nullopt;
}

// Traverses join edges from 'startTable', skipping tables in
// 'excludedTables', looking for paths whose cumulative fanout is
// below kExistenceReductionThreshold. Each qualifying path becomes
// a semi-join existence appended to 'existences'. Returns the
// cumulative existence reduction, or 1 if none found.
float findReducingExistences(
    const PlanState& state,
    PlanObjectCP startTable,
    const PlanObjectSet& excludedTables,
    std::vector<PlanObjectSet>& existences) {
  std::vector<PlanObjectCP> path{startTable};
  PlanObjectSet exists;
  auto visitedCopy = excludedTables;
  // Required by reducingJoinsRecursive but unused; only
  // existenceReduction matters.
  float reduction = 1;
  float existenceReduction = 1;
  reducingJoinsRecursive(
      state,
      startTable,
      /*fanoutFromRoot=*/1,
      ReducingJoinsMagic::kMaxEdgeFanout,
      path,
      visitedCopy,
      exists,
      reduction,
      [&](auto& path, float reduction) {
        if (reduction < ReducingJoinsMagic::kExistenceReductionThreshold) {
          // The original table is added to the reducing existences because
          // the path starts with it but it is not joined twice since it
          // already is the start of the main join.
          PlanObjectSet added;
          for (auto i = 1; i < path.size(); ++i) {
            added.add(path[i]);
          }
          existences.push_back(std::move(added));
          existenceReduction *= reduction;
        }
      });
  return existenceReduction;
}

// Result of the two-pass reducing-join discovery: bushy tables and
// existences, with their respective reductions.
struct ReducingJoinsResult {
  std::vector<PlanObjectCP> bushyTables;
  std::vector<PlanObjectSet> existences;
  float bushyReduction{1};
  float existenceReduction{1};
};

// Runs both reducing-join passes (bushy inner joins + existences) from
// 'startTable'. Tables in 'excludedTables' are skipped. Bushy joins are
// only discovered when 'allowBushyJoins' is true.
ReducingJoinsResult findReducingJoins(
    const PlanState& state,
    PlanObjectCP startTable,
    const PlanObjectSet& excludedTables,
    bool allowBushyJoins) {
  ReducingJoinsResult result;

  // Pass 1: bushy reducing inner joins.
  PlanObjectSet reducingTables;
  if (allowBushyJoins) {
    if (auto bushy =
            findReducingBushyJoins(state, startTable, excludedTables)) {
      // startTable must be first in bushyTables (see JoinCandidate::tables).
      result.bushyTables.push_back(startTable);
      bushy->tables.forEach([&](auto object) {
        if (object != startTable) {
          result.bushyTables.push_back(object);
        }
      });
      result.bushyReduction = bushy->reduction;
      reducingTables = std::move(bushy->tables);
    }
  }

  // Pass 2: reducing existences (semi-joins from probe side).
  if (state.optimization.options().enableReducingExistences &&
      !state.dt->noImportOfExists) {
    reducingTables.add(startTable);
    reducingTables.unionSet(excludedTables);
    result.existenceReduction = findReducingExistences(
        state, startTable, reducingTables, result.existences);
  }

  return result;
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

// For an inner join candidate, looks for co-located tables whose joins reduce
// cardinality and bundles them into a bushy build side. Also looks for reducing
// existences (semi-joins) that can be imported from the probe side to shrink
// the build.
//
// Returns a new JoinCandidate with the expanded table list, accumulated
// existences, and adjusted fanout. Returns std::nullopt if no reduction is
// found.
std::optional<JoinCandidate> reducingJoins(
    const PlanState& state,
    const JoinCandidate& candidate) {
  checkTables(candidate);

  VELOX_DCHECK_EQ(candidate.tables.size(), 1);
  auto startTable = candidate.tables[0];

  auto result = findReducingJoins(
      state,
      startTable,
      state.dt->importedExistences,
      /*allowBushyJoins=*/allowReducingInnerJoins(candidate));

  if (result.bushyTables.empty() && result.existences.empty()) {
    return std::nullopt;
  }

  auto fanout = candidate.fanout;
  if (!result.bushyTables.empty()) {
    fanout *= result.bushyReduction;
  }

  JoinCandidate reducing(candidate.join, startTable, fanout);
  if (result.bushyTables.empty()) {
    reducing.tables = candidate.tables;
  } else {
    reducing.tables = std::move(result.bushyTables);
  }
  reducing.existences = std::move(result.existences);
  reducing.existsFanout = result.existenceReduction;
  return reducing;
}

// Calls 'func' with join, joined table and fanout for the joinable tables.
template <typename Func>
void forJoinedTables(const PlanState& state, Func func) {
  folly::F14FastSet<JoinEdgeP> visited;
  state.placed().forEach([&](PlanObjectCP placedTable) {
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
          if (!key->allTables().isSubset(state.placed())) {
            // All items that the left key depends on must be placed.
            usable = false;
            break;
          }
        }
        if (usable &&
            (state.mayConsiderNext(join->rightTable()) || join->markColumn() ||
             join->rowNumberColumn())) {
          func(join, join->rightTable(), join->lrFanout());
        }
      } else {
        auto [table, fanout] = join->otherTable(placedTable);
        if (!state.dt->hasTable(table) || !state.mayConsiderNext(table)) {
          continue;
        }
        func(join, table, fanout);
      }
    }
  });
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

std::vector<JoinCandidate> Optimization::nextJoins(PlanState& state) {
  std::vector<JoinCandidate> candidates;
  candidates.reserve(state.dt->tables.size());
  forJoinedTables(
      state, [&](JoinEdgeP join, PlanObjectCP joined, float fanout) {
        if (!state.isPlaced(joined) && state.dt->hasJoin(join) &&
            state.dt->hasTable(joined)) {
          candidates.emplace_back(join, joined, fanout);
          if (join->isInner()) {
            addExtraEdges(state, candidates.back());
          }
        }
      });

  if (candidates.empty()) {
    // There are no join edges. There could still be cross joins.
    state.dt->startTables.forEach([&](PlanObjectCP object) {
      if (!state.isPlaced(object) && state.mayConsiderNext(object)) {
        auto fanout = tableCardinality(object);
        if (object->is(PlanType::kTableNode)) {
          fanout = object->as<BaseTable>()->filteredCardinality;
        }
        candidates.emplace_back(nullptr, object, fanout);
      }
    });

    return candidates;
  }

  // Take the first hand joined tables and bundle them with reducing joins that
  // can go on the build side.

  if (!options_.syntacticJoinOrder && !candidates.empty()) {
    std::vector<JoinCandidate> bushes;
    for (auto& candidate : candidates) {
      if (auto bush = reducingJoins(state, candidate)) {
        bushes.push_back(std::move(bush.value()));
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
  if (distribution.isBroadcast()) {
    return true;
  }

  // True if 'input' is partitioned so that each partitioning key is joined to
  // the corresponding partition key in 'info'.
  if (input->distribution().distributionType() !=
      distribution.distributionType()) {
    return false;
  }

  const auto& inputPartitionKeys = input->distribution().partitionKeys();

  if (inputPartitionKeys.empty()) {
    return false;
  }

  const auto& indexPartitionKeys = distribution.partitionKeys();
  if (inputPartitionKeys.size() != indexPartitionKeys.size()) {
    return false;
  }

  for (auto i = 0; i < inputPartitionKeys.size(); ++i) {
    auto nthKey = position(lookupValues, *inputPartitionKeys[i]);
    if (nthKey != kNotFound) {
      if (info.schemaColumn(info.lookupKeys.at(nthKey)) !=
          indexPartitionKeys.at(i)) {
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
      plan,
      Distribution{distribution.distributionType(), std::move(keyExprs)},
      plan->columns());
  state.addCost(*repartition);
  return repartition;
}

// Returns the positions in 'keys' for the expressions that determine the
// partition. empty if the partition is not decided by 'keys'
std::vector<uint32_t> joinKeyPartition(
    const RelationOpPtr& op,
    const ExprVector& keys) {
  const auto& partitionKeys = op->distribution().partitionKeys();
  std::vector<uint32_t> positions;
  for (unsigned i = 0; i < partitionKeys.size(); ++i) {
    auto nthKey = position(keys, *partitionKeys[i]);
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

bool isBroadcastableSize(PlanP build) {
  return build->cost.cardinality < 100'000;
}

// The 'other' side gets shuffled to align with 'input'. If 'input' is not
// partitioned on its keys, shuffle the 'input' too.
void alignJoinSides(
    RelationOpPtr& input,
    const ExprVector& keys,
    PlanState& state,
    RelationOpPtr& otherInput,
    const ExprVector& otherKeys,
    PlanState& otherState,
    bool replicateNullsAndAny) {
  if (input->distribution().isGather() &&
      otherInput->distribution().isGather()) {
    // No alignment needed.
    return;
  }

  auto part = joinKeyPartition(input, keys);
  if (part.empty()) {
    Distribution distribution{
        DistributionType{},
        keys,
        /*orderKeys=*/{},
        /*orderTypes=*/{},
        /*numKeysUnique=*/0,
        /*clusterKeys=*/{},
        replicateNullsAndAny};
    auto* repartition =
        make<Repartition>(input, std::move(distribution), input->columns());
    state.addCost(*repartition);
    input = repartition;
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
  auto* repartition = make<Repartition>(
      otherInput, std::move(distribution), otherInput->columns());
  otherState.addCost(*repartition);
  otherInput = repartition;
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

const connector::PartitionType* copartitionType(
    const connector::PartitionType* first,
    const connector::PartitionType* second) {
  if (first != nullptr && second != nullptr) {
    return first->copartition(*second);
  }

  return nullptr;
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

  const auto* planPartitionType =
      plan->distribution().distributionType().partitionType();

  auto copartition =
      copartitionType(planPartitionType, layout->partitionType());

  // Copartitioning is possible if PartitionTypes are compatible and the table
  // has no fewer partitions than the plan.
  bool shuffle = !copartition || copartition != planPartitionType;
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
  auto* repartition =
      make<Repartition>(plan, std::move(distribution), plan->columns());
  state.addCost(*repartition);
  return repartition;
}

} // namespace

void Optimization::addPostprocess(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  // Sanity check that all tables and conjuncts have been placed.
  for (const auto& table : state.dt->tables) {
    VELOX_CHECK(
        state.isPlaced(table),
        "Failed to place a table: {}",
        table->toString());
  }

  for (const auto* conjunct : state.dt->conjuncts) {
    VELOX_CHECK(
        state.isPlaced(conjunct),
        "Failed to place a conjunct: {}",
        conjunct->toString());
  }

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

  const bool limitConsumedByWindow = addWindow(dt, plan, state);

  // We probably want to make this decision based on cost.
  static constexpr int64_t kMaxLimitBeforeProject = 8'192;
  if (dt->hasOrderBy()) {
    addOrderBy(dt, plan, state);
  } else if (
      !limitConsumedByWindow && dt->hasLimit() &&
      dt->limit <= kMaxLimitBeforeProject) {
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
  }

  if (dt->outputColumns.has_value()) {
    ExprVector outputExprs;
    for (auto* column : *dt->outputColumns) {
      bool found = false;
      for (size_t i = 0; i < dt->columns.size(); ++i) {
        if (dt->columns[i] == column) {
          outputExprs.emplace_back(state.toColumn(dt->exprs[i]));
          found = true;
          break;
        }
      }
      VELOX_CHECK(found, "outputColumn not in columns: {}", column->toString());
    }

    plan = make<Project>(
        maybeDropProject(plan),
        outputExprs,
        *dt->outputColumns,
        Project::isRedundant(plan, outputExprs, *dt->outputColumns));
  }

  if (!limitConsumedByWindow && !dt->hasOrderBy() &&
      dt->limit > kMaxLimitBeforeProject) {
    auto limit = make<Limit>(plan, dt->limit, dt->offset);
    state.addCost(*limit);
    plan = limit;
  }

  if (dt->enforceSingleRow) {
    auto enforceSingleRow = make<EnforceSingleRow>(plan);
    state.addCost(*enforceSingleRow);
    plan = enforceSingleRow;
  }
}

void Optimization::addAggregation(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  aggregationPlanner_.plan(dt, plan, state);
}

namespace {

// Appends all elements of 'src' to the end of 'dst'.
template <typename T>
void append(QGVector<T>& dst, const QGVector<T>& src) {
  dst.insert(dst.end(), src.begin(), src.end());
}

// Returns true if 'lhs' is a prefix of 'rhs'.
template <typename T>
bool isPrefix(const T& lhs, const T& rhs) {
  if (lhs.size() > rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}

// Returns true if two expression vectors are equal (same pointers, same
// order).
bool sameKeys(const ExprVector& lhs, const ExprVector& rhs) {
  return lhs.size() == rhs.size() && isPrefix(lhs, rhs);
}

// Returns true if two order type vectors are equal.
bool sameOrderTypes(const OrderTypeVector& lhs, const OrderTypeVector& rhs) {
  return lhs.size() == rhs.size() && isPrefix(lhs, rhs);
}

bool isRowNumber(std::string_view name) {
  const auto* registry = FunctionRegistry::instance();
  return registry->rowNumber().has_value() && name == *registry->rowNumber();
}

// Returns the RankFunction if the function name is a ranking function,
// std::nullopt otherwise.
std::optional<velox::core::TopNRowNumberNode::RankFunction> toRankFunction(
    std::string_view name) {
  using RankFunction = velox::core::TopNRowNumberNode::RankFunction;
  if (isRowNumber(name)) {
    return RankFunction::kRowNumber;
  }

  const auto* registry = FunctionRegistry::instance();
  if (registry->rank().has_value() && name == *registry->rank()) {
    return RankFunction::kRank;
  }
  if (registry->denseRank().has_value() && name == *registry->denseRank()) {
    return RankFunction::kDenseRank;
  }
  return std::nullopt;
}

// Groups window functions by partition keys and order keys specification.
struct WindowGroup {
  ExprVector partitionKeys;
  ExprVector orderKeys;
  OrderTypeVector orderTypes;
  WindowFunctionVector functions;
  ColumnVector outputColumns;

  // Creates a new group from a single window function and its output column.
  static WindowGroup create(
      WindowFunctionCP windowFunc,
      ColumnCP outputColumn) {
    WindowGroup group;
    group.partitionKeys = windowFunc->partitionKeys();
    group.orderKeys = windowFunc->orderKeys();
    group.orderTypes = windowFunc->orderTypes();
    group.functions.push_back(windowFunc);
    group.outputColumns.push_back(outputColumn);
    return group;
  }

  // Tries to add a window function to this group. Returns true if the function
  // has matching specification, no dependency on existing output columns, and
  // was added. Window functions whose arguments reference another function's
  // output cannot share a Window node — the output isn't available until the
  // Window runs.
  bool tryAdd(WindowFunctionCP windowFunc, ColumnCP outputColumn) {
    if (!sameKeys(partitionKeys, windowFunc->partitionKeys()) ||
        !sameKeys(orderKeys, windowFunc->orderKeys()) ||
        !sameOrderTypes(orderTypes, windowFunc->orderTypes())) {
      return false;
    }

    auto outputColumnSet = PlanObjectSet::fromObjects(outputColumns);
    for (const auto* arg : windowFunc->args()) {
      if (arg->columns().hasIntersection(outputColumnSet)) {
        return false;
      }
    }

    functions.push_back(windowFunc);
    outputColumns.push_back(outputColumn);
    return true;
  }

  void appendAll(const WindowGroup& other) {
    append(functions, other.functions);
    append(outputColumns, other.outputColumns);
  }
};

// Returns true if all window functions in the group use ROWS frames.
bool allRowsFrames(const WindowFunctionVector& functions) {
  return std::all_of(
      functions.begin(), functions.end(), [](WindowFunctionCP func) {
        return func->frame().type == lp::WindowExpr::WindowType::kRows;
      });
}

// Groups window functions by specification (partition keys + order keys),
// merges compatible ROWS-frame groups, and sorts for optimal execution order.
std::vector<WindowGroup> groupWindowFunctions(WindowPlanCP windowPlan) {
  // Group by specification.
  std::vector<WindowGroup> groups;
  for (size_t i = 0; i < windowPlan->functions().size(); ++i) {
    auto windowFunc = windowPlan->functions()[i];
    auto outputColumn = windowPlan->columns()[i];
    bool found = false;
    for (auto& group : groups) {
      if (group.tryAdd(windowFunc, outputColumn)) {
        found = true;
        break;
      }
    }

    if (!found) {
      groups.push_back(WindowGroup::create(windowFunc, outputColumn));
    }
  }

  // ROWS-only merge: merge shorter-ORDER-BY group into longer when all
  // functions in the shorter group use ROWS frames and the shorter ORDER BY
  // is a prefix of the longer, and partition keys match.
  for (size_t i = 0; i < groups.size(); ++i) {
    for (size_t j = i + 1; j < groups.size();) {
      auto& target = groups[i];
      auto& candidate = groups[j];

      if (!sameKeys(target.partitionKeys, candidate.partitionKeys)) {
        ++j;
        continue;
      }

      // Try merging candidate into target or target into candidate.
      if (isPrefix(candidate.orderKeys, target.orderKeys) &&
          isPrefix(candidate.orderTypes, target.orderTypes) &&
          allRowsFrames(candidate.functions)) {
        target.appendAll(candidate);
        groups.erase(groups.begin() + j);
      } else if (
          isPrefix(target.orderKeys, candidate.orderKeys) &&
          isPrefix(target.orderTypes, candidate.orderTypes) &&
          allRowsFrames(target.functions)) {
        candidate.appendAll(target);
        groups.erase(groups.begin() + i);
        // Don't increment i since the element at i has changed.
        j = i + 1;
      } else {
        ++j;
      }
    }
  }

  // Order groups: same partition keys adjacent, longer ORDER BY first,
  // empty partition keys last.
  std::sort(groups.begin(), groups.end(), [](const auto& lhs, const auto& rhs) {
    // Empty partition keys go last.
    if (lhs.partitionKeys.empty() != rhs.partitionKeys.empty()) {
      return !lhs.partitionKeys.empty();
    }
    // Same partition keys adjacent: compare by first partition key id.
    if (!lhs.partitionKeys.empty() && !rhs.partitionKeys.empty()) {
      auto lhsId = lhs.partitionKeys[0]->id();
      auto rhsId = rhs.partitionKeys[0]->id();
      if (lhsId != rhsId) {
        return lhsId < rhsId;
      }
    }
    // Longer ORDER BY first within same partition.
    return lhs.orderKeys.size() > rhs.orderKeys.size();
  });

  return groups;
}

// Creates the appropriate operator for a window function group: RowNumber for
// row_number() without ORDER BY, TopNRowNumber for a ranking function with
// ORDER BY + LIMIT, or Window for the general case.
RelationOpPtr makeWindowOp(
    const RelationOpPtr& plan,
    DerivedTableCP dt,
    WindowGroup& group,
    ExprVector partitionKeys,
    ExprVector orderKeys,
    WindowFunctionVector windowFunctions,
    bool inputsSorted,
    ColumnVector columns,
    std::optional<int32_t> rankingFilterLimit) {
  if (group.functions.size() == 1) {
    auto rankFunction =
        toRankFunction(std::string_view(group.functions[0]->name()));

    if (rankFunction.has_value() && group.orderKeys.empty() &&
        *rankFunction ==
            velox::core::TopNRowNumberNode::RankFunction::kRowNumber) {
      return make<RowNumber>(
          plan,
          std::move(partitionKeys),
          rankingFilterLimit,
          group.outputColumns[0],
          std::move(columns));
    }

    // Use the minimum of DT limit and ranking filter limit.
    auto topNLimit = rankingFilterLimit;
    if (dt->hasLimit() && !dt->hasOrderBy()) {
      topNLimit = topNLimit.has_value()
          ? std::min(*topNLimit, static_cast<int32_t>(dt->limit))
          : std::optional<int32_t>(dt->limit);
    }
    if (rankFunction.has_value() && !group.orderKeys.empty() &&
        topNLimit.has_value()) {
      return make<TopNRowNumber>(
          plan,
          std::move(partitionKeys),
          std::move(orderKeys),
          std::move(group.orderTypes),
          *rankFunction,
          *topNLimit,
          group.outputColumns[0],
          std::move(columns));
    }
  }

  VELOX_CHECK(
      !rankingFilterLimit.has_value(),
      "Ranking filter limit not consumed by RowNumber or TopNRowNumber.");

  return make<Window>(
      plan,
      std::move(partitionKeys),
      std::move(orderKeys),
      std::move(group.orderTypes),
      std::move(windowFunctions),
      inputsSorted,
      std::move(columns));
}

// Adds a Project to drop columns from 'plan' that are not in 'neededColumns'.
// No-op if all columns are needed.
void maybeDropColumns(RelationOpPtr& plan, const PlanObjectSet& neededColumns) {
  ExprVector exprs;
  ColumnVector cols;
  for (auto* column : plan->columns()) {
    if (neededColumns.contains(column)) {
      exprs.emplace_back(column);
      cols.emplace_back(column);
    }
  }

  if (cols.size() < plan->columns().size()) {
    plan = make<Project>(
        plan, std::move(exprs), std::move(cols), /*redundant=*/false);
  }
}

// Precomputes window function args and frame bound values, returning new
// WindowFunction objects with column references in place of expressions.
WindowFunctionVector flattenWindowFunctions(
    const WindowFunctionVector& functions,
    PrecomputeProjection& precompute) {
  WindowFunctionVector result;
  result.reserve(functions.size());
  for (const auto& func : functions) {
    auto args = precompute.toColumns(
        func->args(), /*aliases=*/nullptr, /*preserveLiterals=*/true);
    Frame frame = func->frame();
    if (frame.startValue) {
      frame.startValue = precompute.toColumn(frame.startValue);
    }
    if (frame.endValue) {
      frame.endValue = precompute.toColumn(frame.endValue);
    }
    result.emplace_back(
        make<WindowFunction>(
            func->name(),
            func->value(),
            std::move(args),
            func->functions(),
            func->partitionKeys(),
            func->orderKeys(),
            func->orderTypes(),
            frame,
            func->ignoreNulls()));
  }
  return result;
}

bool isSingleWindowWithLimit(
    DerivedTableCP dt,
    const std::vector<WindowGroup>& groups) {
  return dt->hasLimit() && !dt->hasOrderBy() && groups.size() == 1 &&
      groups[0].functions.size() == 1;
}

} // namespace

bool Optimization::addWindow(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  if (!dt->windowPlan) {
    return false;
  }

  state.place(dt->windowPlan);

  auto groups = groupWindowFunctions(dt->windowPlan);

  // Check for ranking function + LIMIT optimizations. Only applies when there
  // is exactly one window function group with a single ranking function.
  bool limitConsumed = false;
  bool addLimitAfterTopN = false;
  if (isSingleWindowWithLimit(dt, groups)) {
    const auto& group = groups[0];
    const auto functionName = group.functions[0]->name();

    if (group.orderKeys.empty() && isRowNumber(functionName)) {
      // row_number() without ORDER BY + LIMIT. Push LIMIT below RowNumber.
      // The row numbering is non-deterministic (no ORDER BY), so computing
      // row numbers on a smaller set produces a valid result.
      plan = make<Limit>(plan, dt->limit, dt->offset);
      state.addCost(*plan);
      limitConsumed = true;
    } else if (
        !group.orderKeys.empty() && group.partitionKeys.empty() &&
        toRankFunction(functionName).has_value()) {
      // Ranking function with ORDER BY + LIMIT, no partition keys. After
      // gather, TopNRowNumber runs on a single node — no distributed limit
      // needed. row_number never produces ties, so the limit is fully
      // absorbed. rank() and dense_rank() may produce ties, so a Limit is
      // added after TopNRowNumber.
      limitConsumed = true;
      addLimitAfterTopN = !isRowNumber(functionName);
    }
  }

  // Emit Window operators.
  for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
    auto& group = groups[groupIndex];

    // Precompute projection for window function arguments, partition keys,
    // order keys, frame bound expressions, and downstream columns.
    PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);

    // Add input columns needed downstream (by operators above all window
    // groups). Uses downstreamColumns which accounts for placed expressions.
    const auto downstream = state.downstreamColumns();
    for (auto* column : plan->columns()) {
      if (downstream.contains(column)) {
        precompute.toColumn(column);
      }
    }

    // Precompute window function args and frame bound values.
    auto windowFunctions = flattenWindowFunctions(group.functions, precompute);

    auto partitionKeys = precompute.toColumns(group.partitionKeys);
    auto orderKeys = precompute.toColumns(group.orderKeys);
    auto planBeforeProject = plan;
    plan = std::move(precompute).maybeProject();

    // Velox WindowNode passes through all input columns. When precompute
    // didn't add a Project (all needed columns are simple pass-throughs of
    // input columns), drop columns not needed by this or subsequent window
    // groups or by downstream operators.
    if (plan == planBeforeProject) {
      maybeDropColumns(plan, downstream);
    }

    if (!isSingleWorker_) {
      PlanCost repartitionCost;
      std::tie(plan, repartitionCost) =
          maybeRepartition(plan, ExprVector(partitionKeys));
      state.cost.add(repartitionCost);
    }

    // Build output columns: all input columns + window function result columns.
    ColumnVector columns = plan->columns();
    append(columns, group.outputColumns);

    // Check inputsSorted: true if the previous group had the same partition
    // keys and this group's ORDER BY is a prefix of the previous.
    bool inputsSorted = groupIndex > 0 &&
        sameKeys(groups[groupIndex - 1].partitionKeys, group.partitionKeys) &&
        isPrefix(group.orderKeys, groups[groupIndex - 1].orderKeys);

    plan = makeWindowOp(
        plan,
        dt,
        group,
        std::move(partitionKeys),
        std::move(orderKeys),
        std::move(windowFunctions),
        inputsSorted,
        std::move(columns),
        dt->windowPlan->rankingLimit());
    state.addCost(*plan);

    if (addLimitAfterTopN) {
      plan = make<Limit>(plan, dt->limit, dt->offset);
      state.addCost(*plan);
    }

    // Map original window function expressions to their output columns and
    // mark them as placed so that downstreamColumns() is recomputed for the
    // next group.
    for (size_t i = 0; i < group.outputColumns.size(); ++i) {
      state.addExprToColumn(group.functions[i], group.outputColumns[i]);
      state.place(group.functions[i]);
    }
  }

  return limitConsumed;
}

void Optimization::addOrderBy(
    DerivedTableCP dt,
    RelationOpPtr& plan,
    PlanState& state) const {
  PrecomputeProjection precompute(plan, dt, /*projectAllInputs=*/false);
  auto orderKeys = precompute.toColumns(dt->orderKeys);

  for (auto i = 0; i < orderKeys.size(); ++i) {
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
    state.place(candidate.tables.at(0));
    auto joinType = right.leftJoinType();
    if (joinType == velox::core::JoinType::kFull ||
        joinType == velox::core::JoinType::kRight) {
      // Not available by index.
      return;
    }

    auto fanout = info.scanCardinality * rightTable->filteredCardinality /
        rightTable->schemaTable->cardinality;
    if (joinType == velox::core::JoinType::kLeft) {
      fanout = std::max<float>(1, fanout);
    } else if (joinType == velox::core::JoinType::kLeftSemiProject) {
      fanout = 1;
    } else if (joinType == velox::core::JoinType::kLeftSemiFilter) {
      fanout = std::min<float>(1, fanout);
    }

    auto lookupKeys = left.keys;
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

struct ProjectionBuilder {
  ColumnVector columns;
  ExprVector exprs;

  // Project 'expr' as 'column'.
  void add(ColumnCP column, ExprCP expr) {
    VELOX_DCHECK(
        expr->isColumn(), "Expr is not a column: {}", expr->toString());
    columns.emplace_back(column);
    exprs.emplace_back(expr);
  }

  RelationOp* build(RelationOp* input) {
    return make<Project>(
        input, exprs, columns, Project::isRedundant(input, exprs, columns));
  }

  ColumnVector inputColumns() const {
    ColumnVector columns;
    columns.reserve(exprs.size());
    for (const auto* expr : exprs) {
      VELOX_DCHECK(
          expr->isColumn(), "Expr is not a column: {}", expr->toString());
      columns.emplace_back(expr->as<Column>());
    }

    return columns;
  }

  PlanObjectSet outputColumns() const {
    PlanObjectSet columnSet;
    columnSet.unionObjects(columns);
    return columnSet;
  }
};

folly::F14FastMap<PlanObjectCP, ExprCP> makeJoinColumnMapping(
    JoinEdgeP joinEdge) {
  folly::F14FastMap<PlanObjectCP, ExprCP> mapping;
  for (auto i = 0; i < joinEdge->leftColumns().size(); ++i) {
    mapping.emplace(joinEdge->leftColumns()[i], joinEdge->leftExprs()[i]);
  }

  for (auto i = 0; i < joinEdge->rightColumns().size(); ++i) {
    mapping.emplace(joinEdge->rightColumns()[i], joinEdge->rightExprs()[i]);
  }

  return mapping;
}

// Computes columns from the join filter that belong to each side of the join.
//
// @param filter The join filter expressions.
// @param sideAColumns Columns available from side A.
// @param sideBColumns Columns available from side B.
// @return A pair of filter columns for {sideA, sideB}.
std::pair<PlanObjectSet, PlanObjectSet> computeJoinFilterColumns(
    const ExprVector& filter,
    const PlanObjectSet& sideAColumns,
    const PlanObjectSet& sideBColumns) {
  PlanObjectSet allFilterColumns;
  allFilterColumns.unionColumns(filter);

  auto a = allFilterColumns;
  a.intersect(sideAColumns);

  auto b = allFilterColumns;
  b.intersect(sideBColumns);

  return {a, b};
}

// Computes the minimal set of columns needed from one side of a join,
// enabling column pruning by projecting only what's required downstream.
// Includes filter columns for filter evaluation.
//
// Note: Keys are handled separately by precomputeJoinInputs, which precomputes
// key expressions and adds only the result columns to the output.
//
// @param availableColumns Columns available from the input relation.
// @param side Join side containing join edge columns.
// @param downstreamColumns Columns needed by operators above the join.
// @param filterColumns Columns used in the join filter from this side.
PlanObjectSet computeJoinSideOutputColumns(
    const PlanObjectSet& availableColumns,
    const JoinSide& side,
    const PlanObjectSet& downstreamColumns,
    const PlanObjectSet& filterColumns) {
  auto outputColumns = availableColumns;
  outputColumns.unionObjects(side.columns);
  outputColumns.intersect(downstreamColumns);
  outputColumns.unionSet(filterColumns);
  return outputColumns;
}

// Result of precomputing join column expressions and keys on both sides of
// a join.
struct PrecomputedJoinInputs {
  RelationOpPtr probeInput;
  RelationOpPtr buildInput;
  ExprVector probeKeys;
  ExprVector buildKeys;
  folly::F14FastMap<PlanObjectCP, ExprCP> joinColumnMapping;
};

// Precomputes join column expressions and keys, projecting only the columns
// in probeOutputColumns and buildOutputColumns.
PrecomputedJoinInputs precomputeJoinInputs(
    RelationOpPtr probeInput,
    RelationOpPtr buildInput,
    const JoinSide& probe,
    const JoinSide& build,
    const DerivedTable* dt,
    const PlanObjectSet& probeOutputColumns,
    const PlanObjectSet& buildOutputColumns,
    const folly::F14FastMap<PlanObjectCP, ExprCP>& columnMapping) {
  folly::F14FastMap<PlanObjectCP, ExprCP> updatedMapping;

  // Adds columns from outputColumns to the projection.
  // Uses columnMapping for lookup of non-identity projections.
  // Adds projected non-identity columns to updatedMapping.
  auto addOutputColumns = [&](PrecomputeProjection& precompute,
                              const PlanObjectSet& outputColumns) {
    outputColumns.forEach<Column>([&](ColumnCP column) {
      auto it = columnMapping.find(column);
      if (it != columnMapping.end()) {
        // Non-identity: project expression as column, track result.
        auto projected = precompute.toColumn(it->second, column);
        updatedMapping.emplace(column, projected);
      } else {
        // Pass-through: column passes through as-is.
        precompute.toColumn(column);
      }
    });
  };

  // Probe side: project output columns (includes keys).
  PrecomputeProjection precomputeProbe(
      probeInput, dt, /*projectAllInputs=*/false);
  addOutputColumns(precomputeProbe, probeOutputColumns);
  // Compute keys in case they are expressions (e.g., a + 1).
  auto probeKeys = precomputeProbe.toColumns(probe.keys);
  probeInput = std::move(precomputeProbe).maybeProject();

  // Build side: project output columns (includes keys).
  PrecomputeProjection precomputeBuild(
      buildInput, dt, /*projectAllInputs=*/false);
  addOutputColumns(precomputeBuild, buildOutputColumns);
  // Compute keys in case they are expressions (e.g., b + 2).
  auto buildKeys = precomputeBuild.toColumns(build.keys);
  buildInput = std::move(precomputeBuild).maybeProject();

  return {
      std::move(probeInput),
      std::move(buildInput),
      std::move(probeKeys),
      std::move(buildKeys),
      std::move(updatedMapping)};
}

// Translates columns from outside the join to columns below the join if there
// is a rename, as in the case of optional sides of outer joins.
PlanObjectSet translateToJoinInput(
    const PlanObjectSet& columns,
    const folly::F14FastMap<PlanObjectCP, ExprCP>& mapping) {
  PlanObjectSet result;
  columns.forEach([&](PlanObjectCP object) {
    auto it = mapping.find(object);
    if (it != mapping.end()) {
      result.unionColumns(it->second);
    } else {
      result.add(object);
    }
  });
  return result;
}

// Check if 'mark' column produced by a SemiProject join is used only to
// filter the results using 'mark' or 'not(mark)' condition. If so, replace
// the join with a SemiFilter and remove the filter.
void tryOptimizeSemiProject(
    velox::core::JoinType& joinType,
    ColumnCP& mark,
    PlanState& state) {
  if (mark) {
    const bool leftProject =
        joinType == velox::core::JoinType::kLeftSemiProject;
    const bool rightProject =
        joinType == velox::core::JoinType::kRightSemiProject;

    if (leftProject || rightProject) {
      if (auto markFilter = state.isDownstreamFilterOnly(mark)) {
        if (markFilter == mark) {
          joinType = leftProject ? velox::core::JoinType::kLeftSemiFilter
                                 : velox::core::JoinType::kRightSemiFilter;
          mark = nullptr;
          state.place(markFilter);
          return;
        }

        if (leftProject &&
            isCallExpr(markFilter, queryCtx()->functionNames().negation) &&
            markFilter->as<Call>()->argAt(0) == mark) {
          joinType = velox::core::JoinType::kAnti;
          mark = nullptr;
          state.place(markFilter);
          return;
        }
      }
    }
  }
}
} // namespace

void Optimization::joinByHash(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  checkTables(candidate);

  auto [build, probe] = candidate.joinSides();

  const auto partKeys = joinKeyPartition(plan, probe.keys);
  ExprVector copartition;
  if (partKeys.empty()) {
    // Prefer to make a build partitioned on join keys and shuffle probe to
    // align with build.
    copartition = build.keys;
  }

  PlanStateSaver save(state, candidate);

  auto probeColumns = PlanObjectSet::fromObjects(plan->columns());

  auto [probeFilterColumns, buildFilterColumns] = computeJoinFilterColumns(
      candidate.join->filter(),
      probeColumns,
      availableColumns(candidate.tables[0]));

  PlanObjectSet buildTables;
  PlanObjectSet buildColumns;
  for (auto buildTable : candidate.tables) {
    buildColumns.unionSet(availableColumns(buildTable));
    buildTables.add(buildTable);
  }

  state.place(buildTables);

  const auto downstreamColumns = state.downstreamColumns();

  // Mapping from join output column to probe or build side input.
  auto joinColumnMapping = makeJoinColumnMapping(candidate.join);
  buildColumns.intersect(
      translateToJoinInput(downstreamColumns, joinColumnMapping));

  buildColumns.unionColumns(build.keys);
  buildColumns.unionSet(buildFilterColumns);

  state.placeColumns(buildColumns);

  MemoKey memoKey = MemoKey::create(
      candidate.tables[0], buildColumns, buildTables, candidate.existences);

  std::optional<DesiredDistribution> forBuild;
  if (!copartition.empty()) {
    forBuild = {nullptr, copartition};
  } else if (!partKeys.empty()) {
    ExprVector buildPartKeys;
    for (auto i : partKeys) {
      buildPartKeys.push_back(build.keys[i]);
    }
    forBuild = {
        plan->distribution().distributionType().partitionType(),
        std::move(buildPartKeys)};
  }

  bool needsShuffle = false;
  auto buildPlan = makePlan(
      *state.dt, memoKey, forBuild, candidate.existsFanout, needsShuffle);

  PlanState buildState(state.optimization, state.dt, buildPlan);

  auto precomputed = [&]() {
    auto probeOutputColumns = computeJoinSideOutputColumns(
        probeColumns, probe, downstreamColumns, probeFilterColumns);
    auto buildOutputColumns = computeJoinSideOutputColumns(
        buildColumns, build, downstreamColumns, buildFilterColumns);

    return precomputeJoinInputs(
        plan,
        buildPlan->op,
        probe,
        build,
        state.dt,
        probeOutputColumns,
        buildOutputColumns,
        joinColumnMapping);
  }();

  auto probeInput = std::move(precomputed.probeInput);
  auto buildInput = std::move(precomputed.buildInput);
  auto probeKeys = std::move(precomputed.probeKeys);
  auto buildKeys = std::move(precomputed.buildKeys);
  joinColumnMapping = std::move(precomputed.joinColumnMapping);

  // Add AssignUniqueId for correlated scalar subquery decorrelation.
  // For non-equi correlation with aggregation: rowNumberColumn is set,
  // multipleMatchesError is null. For equi-correlation without aggregation:
  // rowNumberColumn is set and multipleMatchesError is set.
  ColumnCP enforceDistinctColumn = nullptr;
  if (auto* rowNumColumn = candidate.join->rowNumberColumn()) {
    probeInput = make<AssignUniqueId>(probeInput, rowNumColumn);
    probeColumns.add(rowNumColumn);

    if (candidate.join->multipleMatchesError()) {
      enforceDistinctColumn = rowNumColumn;
    }
  }

  if (!isSingleWorker_ &&
      !(probeInput->distribution().isGather() &&
        buildInput->distribution().isGather())) {
    // For null-aware IN/NOT IN, the build side's shuffle must replicate
    // rows with NULLs in partition keys (and one arbitrary non-null row) to
    // all workers. Both IN and NOT IN need this:
    //   NOT IN: each worker must know if the build side contains a NULL
    //     (return no rows) or is empty (return all probe rows).
    //   IN: each worker must correctly produce NULL (not false) for the mark
    //     column when the build side contains a NULL that could match.
    // Without replication, NULL rows are hash-partitioned to one worker and
    // other workers produce wrong results.
    // See https://facebookincubator.github.io/velox/develop/anti-join.html
    const bool replicateNullsAndAny = candidate.join->isNullAwareIn();

    if (!partKeys.empty()) {
      if (needsShuffle) {
        if (copartition.empty()) {
          for (auto i : partKeys) {
            copartition.push_back(buildKeys[i]);
          }
        }
        Distribution distribution{
            plan->distribution().distributionType(),
            copartition,
            /*orderKeys=*/{},
            /*orderTypes=*/{},
            /*numKeysUnique=*/0,
            /*clusterKeys=*/{},
            replicateNullsAndAny};
        auto* repartition = make<Repartition>(
            buildInput, std::move(distribution), buildInput->columns());
        buildState.addCost(*repartition);
        buildInput = repartition;
      }
    } else if (
        candidate.join->isBroadcastableType() &&
        isBroadcastableSize(buildPlan)) {
      auto* broadcast = make<Repartition>(
          buildInput, Distribution::broadcast(), buildInput->columns());
      buildState.addCost(*broadcast);
      buildInput = broadcast;
    } else {
      // The probe gets shuffled to align with build. If build is not
      // partitioned on its keys, shuffle the build too.
      alignJoinSides(
          buildInput,
          buildKeys,
          buildState,
          probeInput,
          probeKeys,
          state,
          replicateNullsAndAny);
    }
  }

  auto* buildOp = make<HashBuild>(buildInput, buildKeys, buildPlan);
  buildState.addCost(*buildOp);

  auto joinType = build.leftJoinType();
  const bool probeOnly = velox::core::isProbeOnlyJoin(joinType);

  ProjectionBuilder projectionBuilder;
  bool needsProjection = false;

  ColumnCP mark = nullptr;

  downstreamColumns.forEach<Column>([&](auto column) {
    if (column == build.markColumn) {
      mark = column;
      return;
    }

    if (auto it = joinColumnMapping.find(column);
        it != joinColumnMapping.end()) {
      projectionBuilder.add(column, it->second);
      needsProjection = true;
      return;
    }

    if ((probeOnly || !buildColumns.contains(column)) &&
        !probeColumns.contains(column)) {
      return;
    }

    projectionBuilder.add(column, column);
  });

  tryOptimizeSemiProject(joinType, mark, state);

  // If there is an existence flag, it is the rightmost result column.
  if (mark) {
    projectionBuilder.add(mark, mark);
  }

  // Add the row-number column for EnforceDistinct. It is not part of
  // downstreamColumns, so we need to add it explicitly.
  if (enforceDistinctColumn &&
      !downstreamColumns.contains(enforceDistinctColumn)) {
    projectionBuilder.add(enforceDistinctColumn, enforceDistinctColumn);
  }

  state.replaceColumns(projectionBuilder.outputColumns());

  RelationOp* join = make<Join>(
      JoinMethod::kHash,
      joinType,
      candidate.join->isNullAwareIn(),
      candidate.join->isNullAsValue(),
      probeInput,
      buildOp,
      std::move(probeKeys),
      std::move(buildKeys),
      candidate.join->filter(),
      candidate.fanout,
      candidate.join->rlFanout(),
      projectionBuilder.inputColumns());

  state.addCost(*join);
  state.cost.cost += buildState.cost.cost;

  if (enforceDistinctColumn != nullptr) {
    join = make<EnforceDistinct>(
        join,
        ExprVector{enforceDistinctColumn},
        ExprVector{enforceDistinctColumn},
        candidate.join->multipleMatchesError());
    state.addCost(*join);
  }

  if (needsProjection) {
    join = projectionBuilder.build(join);
  }

  state.addNextJoin(&candidate, join, toTry);
}

void Optimization::joinByHashRight(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  checkTables(candidate);

  VELOX_CHECK_NULL(candidate.join->rowNumberColumn());

  auto [probe, build] = candidate.joinSides();

  PlanStateSaver save(state, candidate);

  PlanObjectSet probeTables;
  PlanObjectSet probeColumns;
  for (auto probeTable : candidate.tables) {
    probeColumns.unionSet(availableColumns(probeTable));
    probeTables.add(probeTable);
  }

  auto buildColumns = PlanObjectSet::fromObjects(plan->columns());

  auto [probeFilterColumns, buildFilterColumns] = computeJoinFilterColumns(
      candidate.join->filter(), probeColumns, buildColumns);

  state.place(probeTables);
  const auto downstreamColumns = state.downstreamColumns();

  auto joinColumnMapping = makeJoinColumnMapping(candidate.join);
  probeColumns.intersect(
      translateToJoinInput(downstreamColumns, joinColumnMapping));
  probeColumns.unionColumns(probe.keys);
  probeColumns.unionSet(probeFilterColumns);

  state.placeColumns(probeColumns);

  MemoKey memoKey = MemoKey::create(
      candidate.tables[0], probeColumns, probeTables, candidate.existences);

  bool needsShuffle = false;
  auto probePlan = makePlan(
      *state.dt, memoKey, std::nullopt, candidate.existsFanout, needsShuffle);

  PlanState probeState(state.optimization, state.dt, probePlan);

  auto precomputed = [&]() {
    auto probeOutputColumns = computeJoinSideOutputColumns(
        probeColumns, probe, downstreamColumns, probeFilterColumns);
    auto buildOutputColumns = computeJoinSideOutputColumns(
        buildColumns, build, downstreamColumns, buildFilterColumns);

    return precomputeJoinInputs(
        probePlan->op,
        plan,
        probe,
        build,
        state.dt,
        probeOutputColumns,
        buildOutputColumns,
        joinColumnMapping);
  }();

  auto probeInput = std::move(precomputed.probeInput);
  auto buildInput = std::move(precomputed.buildInput);
  auto probeKeys = std::move(precomputed.probeKeys);
  auto buildKeys = std::move(precomputed.buildKeys);
  joinColumnMapping = std::move(precomputed.joinColumnMapping);

  buildColumns = PlanObjectSet::fromObjects(buildInput->columns());

  const auto leftJoinType = probe.leftJoinType();
  // Change the join type to the right join variant.
  auto rightJoinType = reverseJoinType(leftJoinType);

  VELOX_CHECK(
      leftJoinType != rightJoinType ||
          velox::core::isCountingLeftSemiFilterJoin(leftJoinType),
      "Join type does not have right hash join variant");

  // For counting joins (INTERSECT ALL), downstream columns reference
  // build-side keys (the original left side of the set operation). Map them
  // to probe-side keys through join equalities so the output references the
  // correct (probe) side.
  if (velox::core::isCountingJoin(rightJoinType)) {
    VELOX_DCHECK(
        joinColumnMapping.empty(),
        "Counting joins should not have column mappings from outer join "
        "columns or precomputed expressions");
    const auto& leftKeys = candidate.join->leftKeys();
    const auto& rightKeys = candidate.join->rightKeys();
    for (size_t i = 0; i < candidate.join->numKeys(); ++i) {
      joinColumnMapping.emplace(leftKeys[i], rightKeys[i]);
    }
  }

  const bool buildOnly =
      rightJoinType == velox::core::JoinType::kRightSemiFilter ||
      rightJoinType == velox::core::JoinType::kRightSemiProject;

  ColumnCP mark = nullptr;

  ProjectionBuilder projectionBuilder;
  bool needsProjection = false;

  downstreamColumns.forEach<Column>([&](auto column) {
    if (column == probe.markColumn) {
      mark = column;
      return;
    }

    if (auto it = joinColumnMapping.find(column);
        it != joinColumnMapping.end()) {
      projectionBuilder.add(column, it->second);
      needsProjection = true;
      return;
    }

    if (!buildColumns.contains(column) &&
        (buildOnly || !probeColumns.contains(column))) {
      return;
    }

    projectionBuilder.add(column, column);
  });

  tryOptimizeSemiProject(rightJoinType, mark, state);

  if (mark) {
    projectionBuilder.add(mark, mark);
  }

  const auto buildCost = state.cost;

  state.replaceColumns(projectionBuilder.outputColumns());
  state.cost = probeState.cost;
  state.cost.cost += buildCost.cost;

  if (!isSingleWorker_) {
    // The build gets shuffled to align with probe. If probe is not
    // partitioned on its keys, shuffle the probe too.
    // In the right-join variant, probeInput is the subquery side (originally
    // the right/build side). For null-aware IN/NOT IN, it needs
    // replicateNullsAndAny.
    alignJoinSides(
        probeInput,
        probeKeys,
        probeState,
        buildInput,
        buildKeys,
        state,
        candidate.join->isNullAwareIn());
  }

  auto* buildOp = make<HashBuild>(buildInput, buildKeys, nullptr);
  state.addCost(*buildOp);

  RelationOp* join = make<Join>(
      JoinMethod::kHash,
      rightJoinType,
      candidate.join->isNullAwareIn(),
      candidate.join->isNullAsValue(),
      probeInput,
      buildOp,
      std::move(probeKeys),
      std::move(buildKeys),
      candidate.join->filter(),
      candidate.join->rlFanout(),
      candidate.fanout,
      projectionBuilder.inputColumns());
  state.addCost(*join);

  if (needsProjection) {
    join = projectionBuilder.build(join);
  }

  state.addNextJoin(&candidate, join, toTry);
}

void Optimization::crossJoin(
    const RelationOpPtr& plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  VELOX_CHECK_EQ(candidate.tables.size(), 1);
  VELOX_CHECK_EQ(candidate.existences.size(), 0);

  PlanStateSaver save(state, candidate);

  const auto* table = candidate.tables[0];
  state.place(table);

  const auto downstreamColumns = state.downstreamColumns();

  auto buildColumns = availableColumns(table);
  auto probeColumns = PlanObjectSet::fromObjects(plan->columns());

  // Columns from the filter that belong to each side.
  PlanObjectSet probeFilterColumns;
  PlanObjectSet buildFilterColumns;

  folly::F14FastMap<PlanObjectCP, ExprCP> joinColumnMapping;
  if (candidate.join != nullptr) {
    joinColumnMapping = makeJoinColumnMapping(candidate.join);

    std::tie(probeFilterColumns, buildFilterColumns) = computeJoinFilterColumns(
        candidate.join->filter(), probeColumns, buildColumns);

    buildColumns.intersect(
        translateToJoinInput(downstreamColumns, joinColumnMapping));
    buildColumns.unionSet(buildFilterColumns);
  }

  PlanObjectSet buildTables = PlanObjectSet::single(table);
  MemoKey memoKey =
      MemoKey::create(table, buildColumns, buildTables, candidate.existences);

  bool needsShuffle = false;
  auto buildPlan = makePlan(
      *state.dt, memoKey, std::nullopt, candidate.existsFanout, needsShuffle);

  PlanState buildState(state.optimization, state.dt, buildPlan);

  RelationOpPtr buildInput = buildPlan->op;

  if (!isSingleWorker_) {
    buildInput = make<Repartition>(
        buildInput, Distribution::broadcast(), buildInput->columns());
  }

  buildState.addCost(*buildInput);

  state.cost.cost += buildState.cost.cost;

  if (candidate.join) {
    auto [build, probe] = candidate.joinSides();

    auto mark = build.markColumn;
    if (mark != nullptr) {
      VELOX_CHECK(downstreamColumns.contains(mark));
    }

    auto precomputed = [&]() {
      auto probeOutputColumns = computeJoinSideOutputColumns(
          probeColumns, probe, downstreamColumns, probeFilterColumns);
      auto buildOutputColumns = computeJoinSideOutputColumns(
          buildColumns, build, downstreamColumns, buildFilterColumns);

      return precomputeJoinInputs(
          plan,
          buildInput,
          probe,
          build,
          state.dt,
          probeOutputColumns,
          buildOutputColumns,
          joinColumnMapping);
    }();

    auto probeInput = std::move(precomputed.probeInput);
    buildInput = std::move(precomputed.buildInput);
    joinColumnMapping = std::move(precomputed.joinColumnMapping);

    // Add AssignUniqueId for correlated scalar subquery decorrelation.
    // For non-equi correlation with aggregation: rowNumberColumn is set,
    // multipleMatchesError is null. For equi-correlation without aggregation:
    // rowNumberColumn is set and multipleMatchesError is set.
    ColumnCP enforceDistinctColumn = nullptr;
    if (auto* rowNumColumn = candidate.join->rowNumberColumn()) {
      probeInput = make<AssignUniqueId>(probeInput, rowNumColumn);
      probeColumns.add(rowNumColumn);

      if (candidate.join->multipleMatchesError()) {
        enforceDistinctColumn = rowNumColumn;
      }
    }

    ProjectionBuilder projectionBuilder;
    bool needsProjection = false;

    downstreamColumns.forEach<Column>([&](auto column) {
      if (column == mark) {
        projectionBuilder.add(mark, mark);
        return;
      }

      if (auto it = joinColumnMapping.find(column);
          it != joinColumnMapping.end()) {
        projectionBuilder.add(column, it->second);
        needsProjection = true;
        return;
      }

      if (!buildColumns.contains(column) && !probeColumns.contains(column)) {
        return;
      }

      projectionBuilder.add(column, column);
    });

    // Add the row-number column for EnforceDistinct. It is not part of
    // downstreamColumns, so we need to add it explicitly.
    if (enforceDistinctColumn &&
        !downstreamColumns.contains(enforceDistinctColumn)) {
      projectionBuilder.add(enforceDistinctColumn, enforceDistinctColumn);
    }

    auto joinType = build.leftJoinType();

    VELOX_CHECK(
        joinType == velox::core::JoinType::kInner ||
            joinType == velox::core::JoinType::kLeft ||
            joinType == velox::core::JoinType::kRight ||
            joinType == velox::core::JoinType::kFull ||
            joinType == velox::core::JoinType::kLeftSemiProject,
        "Unsupported cross join type: {}",
        velox::core::JoinTypeName::toName(joinType));

    RelationOp* join = Join::makeCrossJoin(
        probeInput,
        buildInput,
        joinType,
        candidate.join->filter(),
        projectionBuilder.inputColumns());

    state.replaceColumns(projectionBuilder.outputColumns());
    state.addCost(*join);

    if (enforceDistinctColumn != nullptr) {
      join = make<EnforceDistinct>(
          join,
          ExprVector{enforceDistinctColumn},
          ExprVector{enforceDistinctColumn},
          candidate.join->multipleMatchesError());
      state.addCost(*join);
    }

    if (needsProjection) {
      join = projectionBuilder.build(join);
    }

    state.addNextJoin(&candidate, std::move(join), toTry);
  } else {
    // No join edge - simple cross join.
    PlanObjectSet resultColumns;
    resultColumns.unionObjects(plan->columns());
    resultColumns.unionObjects(buildInput->columns());
    resultColumns.intersect(downstreamColumns);

    RelationOp* join = Join::makeCrossJoin(
        plan,
        buildInput,
        velox::core::JoinType::kInner,
        {},
        resultColumns.toObjects<Column>());

    state.replaceColumns(std::move(resultColumns));
    state.addCost(*join);

    state.addNextJoin(&candidate, std::move(join), toTry);
  }
}

void Optimization::crossJoinUnnest(
    RelationOpPtr plan,
    const JoinCandidate& candidate,
    PlanState& state,
    std::vector<NextJoin>& toTry) {
  PlanStateSaver save(state, candidate);
  for (const auto* table : candidate.tables) {
    VELOX_CHECK(table->is(PlanType::kUnnestTableNode));
    // We add unnest table before compute downstream columns because
    // we're not interested in the replicating columns needed only for unnest.
    state.place(table);

    PrecomputeProjection precompute(plan, state.dt, /*projectAllInputs=*/false);

    ExprVector replicateColumns;
    auto downstreamColumns = state.downstreamColumns();
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
  if (candidate.tables.size() >= 1 &&
      candidate.tables[0]->is(PlanType::kUnnestTableNode)) {
    crossJoinUnnest(plan, candidate, state, result);
    return;
  }

  std::vector<NextJoin> toTry;

  if (options_.syntacticJoinOrder &&
      candidate.join->originalJoinType().has_value() &&
      candidate.join->originalJoinType().value() ==
          logical_plan::JoinType::kRight) {
    joinByHashRight(plan, candidate, state, toTry);
  } else {
    joinByIndex(plan, candidate, state, toTry);

    const auto sizeAfterIndex = toTry.size();
    joinByHash(plan, candidate, state, toTry);

    if (!options_.syntacticJoinOrder && toTry.size() > sizeAfterIndex &&
        candidate.join->isNonCommutative() &&
        candidate.join->hasRightHashVariant()) {
      // There is a hash based candidate with a non-commutative join. Try a
      // right join variant.
      joinByHashRight(plan, candidate, state, toTry);
    }
  }

  // If one is much better do not try the other.
  // TODO: NextJoin::isWorse accounts for probe-side shuffle cost but not for
  // build-side distribution changes. Flipping a LEFT SEMI to RIGHT SEMI can
  // turn a cheap broadcast into an expensive shuffle (e.g., when the existence
  // table is small enough to broadcast as LEFT SEMI build, but RIGHT SEMI
  // forces both sides to be shuffled). Consider build-side distribution cost
  // in isWorse() or skip the right variant when the left build is
  // broadcastable.
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
    state.restore(next);
    makeJoins(next.plan, state);
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

  bool needsShuffle = false;
  auto rightPlan = makePlan(*state.dt, memoKey, std::nullopt, 1, needsShuffle);

  auto rightOp = rightPlan->op;

  if (!isSingleWorker_) {
    rightOp = make<Repartition>(
        rightOp, Distribution::broadcast(), rightOp->columns());
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

  auto dtColumns = PlanObjectSet::fromObjects(from->columns);
  dtColumns.intersect(state.downstreamColumns());
  state.placeColumns(dtColumns);

  MemoKey key =
      MemoKey::create(from, std::move(dtColumns), PlanObjectSet::single(from));

  bool ignore = false;
  auto plan = makePlan(*state.dt, key, std::nullopt, 1, ignore);
  state.cost = plan->cost;

  // Make plans based on the dt alone as first.
  makeJoins(plan->op, state);

  // Look for reducing joins to import inside the DT. Bushy joins are
  // only allowed when the primary table is a base table — when it is a
  // subquery DT, bushy tables would be placed inside the MemoKey alongside
  // the subquery, but they need to join above the subquery's
  // aggregation/window boundary. The >= 3 threshold requires at least 2
  // unplaced tables besides 'from' to form a meaningful bushy build side.
  auto result = findReducingJoins(
      state,
      from,
      state.dt->importedExistences,
      /*allowBushyJoins=*/!from->is(PlanType::kDerivedTableNode) &&
          state.dt->tables.size() >= 3);

  if (result.bushyTables.empty() && result.existences.empty()) {
    return;
  }

  auto reducingTables = result.bushyTables.empty()
      ? PlanObjectSet::single(from)
      : PlanObjectSet::fromObjects(result.bushyTables);
  MemoKey reducingKey = MemoKey::create(
      from,
      state.downstreamColumns(),
      std::move(reducingTables),
      std::move(result.existences));
  plan = makePlan(
      *state.dt,
      reducingKey,
      /*distribution=*/std::nullopt,
      /*existsFanout=*/result.existenceReduction,
      /*needsShuffle=*/ignore);
  state.cost = plan->cost;
  makeJoins(plan->op, state);
}

bool Optimization::placeConjuncts(
    RelationOpPtr plan,
    PlanState& state,
    bool allowNondeterministic) {
  PlanStateSaver save(state);

  PlanObjectSet columnsAndSingles = state.columns();
  state.dt->singleRowDts.forEach<DerivedTable>(
      [&](auto dt) { columnsAndSingles.unionObjects(dt->columns); });

  ExprVector filters;
  for (auto& conjunct : state.dt->conjuncts) {
    if (!allowNondeterministic && conjunct->containsNonDeterministic()) {
      continue;
    }
    if (state.isPlaced(conjunct)) {
      continue;
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

      if (!placeable.empty()) {
        state.place(conjunct);
        for (auto* subquery : placeable) {
          plan = placeSingleRowDt(plan, subquery, state);
        }

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

// Restricts dt->outputColumns to only those present in 'needed'.
void pruneOutputColumns(DerivedTableP dt, const PlanObjectSet& needed) {
  if (!dt->outputColumns.has_value()) {
    return;
  }
  ColumnVector output;
  for (auto* column : *dt->outputColumns) {
    if (needed.contains(column)) {
      output.push_back(column);
    }
  }
  dt->outputColumns = std::move(output);
}

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
      /*preGroupedKeys*/ ExprVector{},
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

float startingScore(PlanObjectCP table) {
  if (table->is(PlanType::kTableNode)) {
    auto* baseTable = table->as<BaseTable>();
    return baseTable->filteredCardinality;
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
  // Sanity check that there are no RIGHT joins.
  for (auto join : state.dt->joins) {
    if (join->leftOptional()) {
      VELOX_CHECK(
          join->rightOptional(), "Unexpected RIGHT join: {}", join->toString());
    }
  }

  PlanObjectVector firstTables;

  if (options_.syntacticJoinOrder) {
    const auto firstTableId = state.dt->joinOrder[0];
    VELOX_CHECK(state.dt->startTables.BitSet::contains(firstTableId));

    firstTables.push_back(queryCtx()->objectAt(firstTableId));
  } else {
    firstTables = state.dt->startTables.toObjects();

#ifndef NDEBUG
    for (auto table : firstTables) {
      state.debugSetFirstTable(table->id());
    }
#endif
  }

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

    // Go over single-row DTs that haven't been placed yet. If unused, just mark
    // as "placed". Otherwise, place these. Look for any unused single-row DTs.
    // Mark them as "placed".
    const auto downstream = state.downstreamColumns();
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

RelationOpPtr Optimization::makeInitialPlan(const DerivedTable& dt) {
  const auto key = dt.memoKey();
  PlanState state(*this, &dt);
  RelationOpPtr result;

  if (dt.setOp.has_value()) {
    // Union: assemble from already-planned children.
    RelationOpPtrVector childOps;
    for (auto* childDt : dt.children) {
      auto* plans = memo_.find(childDt->memoKey());
      VELOX_CHECK(
          plans != nullptr, "Expecting to find a plan for union branch");
      childOps.push_back(plans->best()->op);
    }

    result = make<UnionAll>(std::move(childOps));
    if (dt.setOp.value() == logical_plan::SetOperation::kUnion) {
      result = makeDistinct(result);
    }

    state.plans.addPlan(result, state);
  } else {
    // Non-union.
    state.targetExprs.unionObjects(dt.exprs);
    makeJoins(state);
    result = state.plans.best()->op;
  }

  memo_.insert(key, std::move(state.plans));
  return result;
}

PlanP Optimization::makePlan(
    const DerivedTable& dt,
    const MemoKey& key,
    const std::optional<DesiredDistribution>& distribution,
    float existsFanout,
    bool& needsShuffle) {
  needsShuffle = false;
  if (key.firstTable->is(PlanType::kDerivedTableNode) &&
      key.firstTable->as<DerivedTable>()->setOp.has_value()) {
    return makeUnionPlan(key, distribution);
  }
  return makeDtPlan(dt, key, distribution, existsFanout, needsShuffle);
}

PlanP Optimization::makeUnionPlan(
    const MemoKey& key,
    const std::optional<DesiredDistribution>& distribution) {
  const auto* setDt = key.firstTable->as<DerivedTable>();

  RelationOpPtrVector inputs;
  std::vector<PlanP> inputPlans;
  std::vector<PlanState> inputStates;
  std::vector<bool> inputNeedsShuffle;

  // For UNION DISTINCT, the deduplication (GROUP BY) requires all the UNION's
  // output columns. Ensure the needed columns include at least these, even if
  // the consumer doesn't reference them (e.g., COUNT(*) needs no specific
  // columns). Without this, pruneOutputColumns can prune children
  // inconsistently, causing UnionAll::initConstraints to crash.
  const bool isDistinct = *setDt->setOp == lp::SetOperation::kUnion;
  PlanObjectSet childColumns{key.columns};
  if (isDistinct) {
    childColumns.unionObjects(setDt->columns);
  }

  for (auto* inputDt : setDt->children) {
    // Only include the child DT in the input tables. Extra tables from
    // the parent key (e.g., reducing bushy join tables) reference joins
    // against the UNION ALL DT which don't exist for individual children.
    auto inputKey =
        MemoKey::create(inputDt, childColumns, PlanObjectSet::single(inputDt));

    PlanP inputPlan;
    bool inputShuffle = false;
    if (inputDt->setOp.has_value()) {
      // Nested union (e.g., UNION ALL of UNION ALL).
      // TODO: Flatten nested unions in ToGraph.
      inputPlan = makeUnionPlan(inputKey, distribution);
    } else {
      PlanSet* plans = memo_.find(inputKey);
      if (plans == nullptr) {
        auto tmpDt = make<DerivedTable>();
        tmpDt->cname = newCName("tmp_dt");
        tmpDt->importUnionChild(inputDt);

        // Restrict outputColumns to only the columns needed by the
        // parent. The child DT inherits all union columns in
        // outputColumns, but the parent may need only a subset.
        // Without this, addPostprocess creates a Project that
        // references columns pruned from the scan.
        pruneOutputColumns(tmpDt, inputKey.columns);

        PlanState inner(*this, tmpDt);
        inner.setTargetExprsForDt(inputKey.columns);

        makeJoins(inner);
        memo_.insert(inputKey, std::move(inner.plans));
        plans = memo_.find(inputKey);
      }
      inputPlan = plans->best(distribution, inputShuffle);
    }
    inputPlans.push_back(inputPlan);
    inputStates.emplace_back(*this, setDt, inputPlans.back());
    inputs.push_back(inputPlan->op);
    inputNeedsShuffle.push_back(inputShuffle);
  }

  if (isSingleWorker_) {
    RelationOpPtr result = make<UnionAll>(inputs);
    Aggregation* distinct = nullptr;
    if (isDistinct) {
      result = makeDistinct(result);
      distinct = result->as<Aggregation>();
    }
    return unionPlan(inputStates, result, distinct);
  }

  if (!distribution.has_value()) {
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
        inputs[i] = make<Repartition>(
            inputs[i],
            Distribution{
                DistributionType(distribution->partitionType),
                distribution->partitionKeys},
            inputs[i]->columns());
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
    const std::optional<DesiredDistribution>& distribution,
    float existsFanout,
    bool& needsShuffle) {
  PlanSet* plans = memo_.find(key);
  if (plans == nullptr) {
    // Allocate temp DT in the arena. The DT may get flattened and then
    // PrecomputeProjection may create columns that reference that DT. Hence,
    // the DT's lifetime must extend to the lifetime of the optimization.
    auto tmpDt = make<DerivedTable>();
    tmpDt->cname = newCName("tmp_dt");
    tmpDt->import(dt, key.tables, key.firstTable, key.existences, existsFanout);

    PlanState inner(*this, tmpDt);
    if (key.firstTable->is(PlanType::kDerivedTableNode) &&
        !tmpDt->columns.empty()) {
      // tmpDt wraps a subquery DT and has output columns from flattening.
      // Prune output columns and translate target expressions through the
      // tmpDt's column-to-expression mapping.
      pruneOutputColumns(tmpDt, key.columns);
      inner.setTargetExprsForDt(key.columns);
    } else {
      inner.targetExprs = key.columns;
    }

    makeJoins(inner);
    memo_.insert(key, std::move(inner.plans));
    plans = memo_.find(key);
  }
  return plans->best(distribution, needsShuffle);
}

ExprCP Optimization::combineLeftDeep(Name func, const ExprVector& exprs) {
  VELOX_CHECK(!exprs.empty());
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

ExprCP Optimization::tryFoldConstant(ExprCP expr) {
  if (expr->is(PlanType::kLiteralExpr)) {
    return expr;
  }

  if (!expr->columns().empty()) {
    return nullptr;
  }

  try {
    auto typedExpr = toTypedExpr(expr);
    auto exprSet = evaluator()->compile(typedExpr);
    const auto& first = *exprSet->exprs().front();
    if (!first.isConstant()) {
      return nullptr;
    }
    const auto& constantExpr =
        static_cast<const velox::exec::ConstantExpr&>(first);
    auto variant = constantExpr.value()->variantAt(0);
    Value value(toType(constantExpr.type()), 1);
    auto* registered = registerVariant(std::move(variant));
    if (constantExpr.type()->isPrimitiveType()) {
      value.min = registered;
      value.max = registered;
    }
    return make<Literal>(value, registered);
  } catch (const std::exception&) {
    return nullptr;
  }
}

} // namespace facebook::axiom::optimizer
