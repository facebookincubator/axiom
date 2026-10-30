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

#include "axiom/optimizer/v2/EstimateLeafStatsPass.h"

#include <algorithm>
#include <cmath>

#include <fmt/format.h>

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/NodeSimplifier.h"
#include "axiom/optimizer/v2/TableAccessHandle.h"

#include <folly/container/F14Set.h>
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/OptimizerMetrics.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/StatsFilterSelectivityEstimator.h"
#include "velox/expression/Expr.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Collects the Scan nodes reachable from 'node'.
void collectScans(NodeCP node, std::vector<ScanCP>& scans) {
  if (node->is(NodeType::kScan)) {
    scans.push_back(node->as<Scan>());
    return;
  }

  for (NodeCP input : node->inputs()) {
    collectScans(input, scans);
  }
}

// Overwrites 'column''s Value with connector-provided per-column statistics.
void applyColumnStats(
    ColumnCP column,
    const connector::ColumnStatistics& stats) {
  const auto& existing = column->value();
  const_cast<Value&>(existing) =
      Value::fromColumnStatistics(existing.type, stats);
}

// Scales 'count', a number of rows or splits a scan of 'baseTable' produces
// without sampling, by its TABLESAMPLE SYSTEM rate, which keeps each split
// with that probability.
std::optional<uint64_t> sampled(
    const BaseTable& baseTable,
    std::optional<uint64_t> count) {
  if (!count.has_value() || !baseTable.sampledPercentage.has_value()) {
    return count;
  }
  return static_cast<uint64_t>(
      std::llround(*count * (*baseTable.sampledPercentage / 100.0)));
}

// Applies one base table's connector stats result. Sets filteredCardinality to
// the connector's post-filter row count. A nullopt result (the connector does
// not support stats) leaves filteredCardinality at 0 so downstream estimation
// falls back to constraint-based selectivity.
bool applyFilteredStats(
    const Scan& scan,
    const std::vector<ColumnCP>& statColumns,
    const std::optional<connector::FilteredTableStats>& stats) {
  if (!stats.has_value()) {
    return false;
  }

  auto* baseTable = const_cast<BaseTable*>(scan.baseTable());

  baseTable->numRawInputRows = sampled(*baseTable, stats->numRawInputRows);
  baseTable->numRawInputBytesPerRow = stats->numRawInputBytesPerRow;
  baseTable->numSplits = sampled(*baseTable, stats->numSplits);

  if (!stats->columnStats.empty()) {
    VELOX_CHECK_EQ(stats->columnStats.size(), statColumns.size());
    for (size_t i = 0; i < stats->columnStats.size(); ++i) {
      applyColumnStats(statColumns[i], stats->columnStats[i]);
    }
  }

  // numRows is post-filter for the filters the connector took; the refused
  // ones are estimated at the Filter above the scan.
  baseTable->filteredCardinality = std::max<float>(1, stats->numRows);
  return stats->isKnownEmpty;
}

// Carries a simplified result from a child to its parent.
struct SimplifiedNodeContext {
  NodeSimplifier::SimplifiedNode result;
};

// Applies the common node simplification rules bottom-up, seeding the walk with
// scans that connector metadata proved empty.
class EmptyScanSimplifier : public NodeRewriter<SimplifiedNodeContext> {
 public:
  using NodeRewriter::rewrite;

  EmptyScanSimplifier(
      Builder& builder,
      NodeSimplifier& simplifier,
      folly::F14FastSet<ScanCP> knownEmptyScans)
      : NodeRewriter(builder),
        simplifier_(simplifier),
        knownEmptyScans_(std::move(knownEmptyScans)) {}

  NodeSimplifier::SimplifiedNode rewrite(NodeCP node) {
    SimplifiedNodeContext context;
    NodeRewriter::rewrite(node, context);
    return std::move(context.result);
  }

 protected:
  NodeCP rewriteScan(const Scan* node, SimplifiedNodeContext& context)
      override {
    return setResult(
        node,
        knownEmptyScans_.contains(node)
            ? NodeSimplifier::SimplifiedNode{nullptr, {}, node->outputColumns()}
            : NodeSimplifier::SimplifiedNode{node, {}},
        context);
  }

  NodeCP rewriteValues(const Values* node, SimplifiedNodeContext& context)
      override {
    return setResult(
        node,
        simplifier_.make(
            {node->source(),
             node->rows(),
             node->outputColumns(),
             node->channels()}),
        context);
  }

  NodeCP rewriteFilter(const Filter* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Filter::Key{node->input(), node->predicates()}, std::move(input));
    });
  }

  NodeCP rewriteProject(const Project* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Project::Key{node->input(), node->exprs(), node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteLimit(const Limit* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Limit::Key{node->input(), node->offset(), node->count()},
          std::move(input));
    });
  }

  NodeCP rewriteSort(const Sort* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Sort::Key{node->input(), node->orderKeys(), node->orderTypes()},
          std::move(input));
    });
  }

  NodeCP rewriteTopN(const TopN* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          TopN::Key{
              node->input(),
              node->orderKeys(),
              node->orderTypes(),
              node->offset(),
              node->count()},
          std::move(input));
    });
  }

  NodeCP rewriteAggregate(const Aggregate* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Aggregate::Key{
              .input = node->input(),
              .groupingKeys = node->groupingKeys(),
              .aggregates = node->aggregates(),
              .outputColumns = node->outputColumns(),
              .step = node->step(),
              .groupId = node->groupId(),
              .globalGroupingSets = node->globalGroupingSets()},
          std::move(input));
    });
  }

  NodeCP rewriteGroupId(const GroupId* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          GroupId::Key{
              node->input(),
              node->groupingKeys(),
              node->aggregationInputs(),
              node->groupingSets(),
              node->groupingKeyColumns(),
              node->groupId(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteMarkDistinct(
      const MarkDistinct* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          MarkDistinct::Key{
              node->input(),
              node->markers(),
              node->distinctKeys(),
              node->masks(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteUnnest(const Unnest* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Unnest::Key{
              node->input(),
              node->unnestExpressions(),
              node->replicatedColumns(),
              node->unnestColumns(),
              node->ordinalityColumn(),
              node->markerColumn(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteJoin(const Join* node, SimplifiedNodeContext& context)
      override {
    auto left = rewriteChild(node->left());
    auto right = rewriteChild(node->right());
    return setResult(
        node,
        simplifier_.make(
            Join::Key{
                node->left(),
                node->right(),
                node->joinType(),
                node->leftKeys(),
                node->rightKeys(),
                node->filter(),
                node->nullAware(),
                node->nullAsValue(),
                node->outputColumns(),
                node->sourceColumns()},
            std::move(left),
            std::move(right)),
        context);
  }

  NodeCP rewriteIndexLookupJoin(
      const IndexLookupJoin* node,
      SimplifiedNodeContext& context) override {
    auto probe = rewriteChild(node->probe());
    return setResult(
        node,
        simplifier_.make(
            IndexLookupJoin::Key{
                node->probe(),
                node->lookupTable(),
                node->index(),
                node->lookupOutputColumns(),
                node->lookupHandle(),
                node->joinType(),
                node->probeKeys(),
                node->lookupKeys(),
                node->filter(),
                node->outputColumns(),
                node->sourceColumns()},
            std::move(probe)),
        context);
  }

  NodeCP rewriteUnionAll(const UnionAll* node, SimplifiedNodeContext& context)
      override {
    std::vector<NodeSimplifier::SimplifiedNode> inputs;
    inputs.reserve(node->inputs().size());
    for (NodeCP input : node->inputs()) {
      inputs.push_back(rewriteChild(input));
    }
    return setResult(
        node,
        simplifier_.make(
            UnionAll::Key{
                NodeVector(node->inputs().begin(), node->inputs().end()),
                node->legColumns(),
                node->outputColumns()},
            std::move(inputs)),
        context);
  }

  NodeCP rewriteWindow(const Window* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Window::Key{
              node->input(),
              node->functions(),
              node->partitionKeys(),
              node->orderKeys(),
              node->orderTypes(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteInference(const Inference* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          Inference::Key{
              node->input(),
              node->call(),
              node->result(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteRowNumber(const RowNumber* node, SimplifiedNodeContext& context)
      override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          RowNumber::Key{
              node->input(),
              node->partitionKeys(),
              node->limit(),
              node->rankColumn(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteTopNRowNumber(
      const TopNRowNumber* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          TopNRowNumber::Key{
              node->input(),
              node->rankFunction(),
              node->partitionKeys(),
              node->orderKeys(),
              node->orderTypes(),
              node->limit(),
              node->rankColumn(),
              node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteApply(const Apply* node, SimplifiedNodeContext& context)
      override {
    auto input = rewriteChild(node->input());
    auto body = rewriteChild(node->body());
    return setResult(
        node,
        simplifier_.make(
            Apply::Key{
                node->input(),
                node->body(),
                node->correlationColumns(),
                node->kind(),
                node->filter(),
                node->enforceSingleRow(),
                node->markColumn(),
                node->inLhs(),
                node->inBodyKey(),
                node->includeMarker(),
                node->outputColumns(),
                node->sourceColumns()},
            std::move(input),
            std::move(body)),
        context);
  }

  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      input.originalColumns = node->input()->outputColumns();
      return simplifier_.make(
          EnforceSingleRow::Key{node->input(), node->outputColumns()},
          std::move(input));
    });
  }

  NodeCP rewriteTableWrite(
      const TableWrite* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          TableWrite::Key{
              node->input(),
              node->table(),
              node->kind(),
              node->columnExprs(),
              node->rowIdColumns(),
              node->deleteTarget(),
              node->deleteLayout()},
          std::move(input),
          node->outputColumns());
    });
  }

  NodeCP rewriteAssignUniqueId(
      const AssignUniqueId* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          AssignUniqueId::Key{node->input(), node->idColumn()},
          std::move(input));
    });
  }

  NodeCP rewriteEnforceDistinct(
      const EnforceDistinct* node,
      SimplifiedNodeContext& context) override {
    return unary(node, context, [&](auto input) {
      return simplifier_.make(
          EnforceDistinct::Key{
              node->input(), node->distinctKeys(), node->errorMessage()},
          std::move(input));
    });
  }

  NodeCP rewriteExchange(
      const Exchange* /*node*/,
      SimplifiedNodeContext& /*context*/) override {
    VELOX_UNREACHABLE("EstimateLeafStats runs before physical planning");
  }

  NodeCP rewriteWorkingTable(
      const WorkingTable* node,
      SimplifiedNodeContext& context) override {
    return setResult(node, NodeSimplifier::SimplifiedNode{node, {}}, context);
  }

  NodeCP rewriteRepeat(const Repeat* node, SimplifiedNodeContext& context)
      override {
    auto start = rewriteChild(node->start());
    auto hop = rewriteChild(node->hop());
    std::optional<NodeSimplifier::SimplifiedNode> end;
    if (node->end() != nullptr) {
      end = rewriteChild(node->end());
    }
    return setResult(
        node,
        simplifier_.make(
            Repeat::Key{
                .start = node->start(),
                .hop = node->hop(),
                .end = node->end(),
                .startKeys = node->startKeys(),
                .hopSourceKeys = node->hopSourceKeys(),
                .hopDestinationKeys = node->hopDestinationKeys(),
                .endKeys = node->endKeys(),
                .minHops = node->minHops(),
                .maxHops = node->maxHops(),
                .outputColumns = node->outputColumns(),
                .sourceColumns = node->sourceColumns(),
                .hopCount = node->hopCount(),
            },
            std::move(start),
            std::move(hop),
            std::move(end)),
        context);
  }

  NodeCP rewriteFixedPoint(
      const FixedPoint* node,
      SimplifiedNodeContext& context) override {
    auto anchor = rewriteChild(node->anchor());
    auto step = rewriteChild(node->step());
    NodeSimplifier::SimplifiedNode convergence;
    if (node->convergence() != nullptr) {
      convergence = rewriteChild(node->convergence());
    }
    return setResult(
        node,
        simplifier_.make(
            FixedPoint::Key{
                .anchor = node->anchor(),
                .step = node->step(),
                .name = node->name(),
                .outputColumns = node->outputColumns(),
                .sourceColumns = node->sourceColumns(),
                .maxIterations = node->maxIterations(),
                .failAtBound = node->failAtBound(),
                .mode = node->mode(),
                .recursiveNumDrivers = node->recursiveNumDrivers(),
                .convergence = node->convergence(),
            },
            std::move(anchor),
            std::move(step),
            std::move(convergence)),
        context);
  }

 private:
  template <typename TNode, typename TMake>
  NodeCP unary(const TNode* node, SimplifiedNodeContext& context, TMake make) {
    return setResult(node, make(rewriteChild(node->input())), context);
  }

  NodeSimplifier::SimplifiedNode rewriteChild(NodeCP node) {
    SimplifiedNodeContext context;
    NodeRewriter::rewrite(node, context);
    return std::move(context.result);
  }

  NodeCP setResult(
      NodeCP original,
      NodeSimplifier::SimplifiedNode result,
      SimplifiedNodeContext& context) {
    NodeCP rewritten = result.empty() ? original : result.node;
    context.result = std::move(result);
    return rewritten;
  }

  NodeSimplifier& simplifier_;
  // Scans whose accepted filters were proven to match no rows.
  const folly::F14FastSet<ScanCP> knownEmptyScans_;
};

} // namespace

NodeCP EstimateLeafStatsPass::run(
    NodeCP root,
    ColumnVector& outputColumns,
    Builder& builder,
    velox::core::ExpressionEvaluator& evaluator,
    const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
    const OptimizerSession& session) {
  std::vector<ScanCP> scans;
  collectScans(root, scans);

  // One stats request per scan.
  struct TableTask {
    ScanCP scan;
    std::vector<ColumnCP> statColumns;
    std::shared_ptr<velox::memory::MemoryPool> evaluatorPool;
    std::unique_ptr<velox::core::ExpressionEvaluator> evaluator;
  };
  std::vector<TableTask> tasks;
  std::vector<folly::coro::Task<std::optional<connector::FilteredTableStats>>>
      requests;

  // Shared helper offered to each connector's co_estimateStats. Outlives the
  // coroutines below, which run under blockingWait before this returns.
  StatsFilterSelectivityEstimator estimator;
  for (ScanCP scan : scans) {
    const auto* baseTable = scan->baseTable();
    const TableAccessHandle* handle = scan->scanHandle();
    VELOX_CHECK_NOT_NULL(
        handle, "Filtered-table stats need the connector's read handle");

    const auto* layout = baseTable->schemaTable->columnGroups[0]->layout;
    auto connectorSession =
        session.context()->sessionFor(layout->connectorId());

    // Subfield columns have no connector-level statistics.
    std::vector<ColumnCP> statColumns;
    std::vector<std::string> columnNames;
    for (ColumnCP column : scan->outputColumns()) {
      if (column->topColumn() == nullptr) {
        statColumns.push_back(column);
        columnNames.emplace_back(column->schemaName());
      }
    }

    auto evaluatorPool = queryCtx->pool()->addLeafChild(
        fmt::format("estimateStats{}", tasks.size()));
    auto evaluator = std::make_unique<velox::exec::SimpleExpressionEvaluator>(
        queryCtx.get(), evaluatorPool.get());
    auto* evaluatorPtr = evaluator.get();
    tasks.push_back(
        TableTask{
            scan,
            std::move(statColumns),
            std::move(evaluatorPool),
            std::move(evaluator)});
    requests.push_back(layout->co_estimateStats(
        std::move(connectorSession),
        handle->tableHandle,
        *evaluatorPtr,
        std::move(columnNames),
        estimator));
  }

  if (requests.empty()) {
    return root;
  }

  // No optimizer-time executor is available, so the requests run inline. They
  // are still launched together so a connector that suspends on I/O can
  // overlap them.
  auto estimateStart = std::chrono::steady_clock::now();
  auto results = folly::coro::blockingWait(
      folly::coro::collectAllRange(std::move(requests)));
  session.statsWriter().addTiming(
      OptimizerMetrics::kEstimateStatsWallNanos,
      std::chrono::steady_clock::now() - estimateStart);

  folly::F14FastSet<ScanCP> knownEmptyScans;
  for (size_t i = 0; i < tasks.size(); ++i) {
    if (applyFilteredStats(*tasks[i].scan, tasks[i].statColumns, results[i])) {
      knownEmptyScans.insert(tasks[i].scan);
    }
  }
  if (knownEmptyScans.empty()) {
    return root;
  }

  ExprSimplifier expressionSimplifier{builder, evaluator};
  NodeSimplifier nodeSimplifier{builder, expressionSimplifier};
  auto simplified =
      EmptyScanSimplifier{builder, nodeSimplifier, std::move(knownEmptyScans)}
          .rewrite(root);
  if (simplified.empty()) {
    return builder.makeEmptyValues(outputColumns);
  }
  nodeSimplifier.restoreOutputLayout(
      simplified.node, outputColumns, simplified.substitutions);
  return simplified.node;
}

} // namespace facebook::axiom::optimizer::v2
