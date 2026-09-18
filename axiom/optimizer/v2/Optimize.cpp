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

#include "axiom/optimizer/v2/Optimize.h"

#include "axiom/optimizer/ConstantFold.h"
#include "axiom/optimizer/ExplainIo.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ConnectorPushdownPass.h"
#include "axiom/optimizer/v2/DecorrelatePass.h"
#include "axiom/optimizer/v2/EmitPass.h"
#include "axiom/optimizer/v2/EstimateLeafStatsPass.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/FoldMetadataAggregatePass.h"
#include "axiom/optimizer/v2/LimitAndOrderPass.h"
#include "axiom/optimizer/v2/PlanPhysicalPass.h"
#include "axiom/optimizer/v2/PushdownAndPrunePass.h"
#include "axiom/optimizer/v2/TranslatePass.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Collects each Scan's base table and the conjuncts that reach it. Reads them
// off the `Filter` above the `Scan`, so the caller must have run the pushdown
// pass with `ConnectorPushdown::kSkip`, which leaves every conjunct there.
void collectScans(
    NodeCP node,
    std::vector<std::pair<BaseTableCP, ExprVector>>& tableFilters) {
  if (node->is(NodeType::kScan)) {
    tableFilters.emplace_back(node->as<Scan>()->baseTable(), ExprVector{});
    return;
  }

  if (node->is(NodeType::kFilter)) {
    const auto* filter = node->as<Filter>();
    if (filter->input()->is(NodeType::kScan)) {
      tableFilters.emplace_back(
          filter->input()->as<Scan>()->baseTable(), filter->predicates());
      return;
    }
  }

  for (auto* input : node->inputs()) {
    collectScans(input, tableFilters);
  }
}

// Sums the rows the scans under 'node' are estimated to read. One unknown
// makes the total unknown: summing the rest would under-size the query by
// however much the unknown table reads. Each use of a table in a query has its
// own BaseTable, so no scan is counted twice.
std::optional<uint64_t> totalRawInputRows(NodeCP node) {
  if (node->is(NodeType::kScan)) {
    return node->as<Scan>()->baseTable()->numRawInputRows;
  }

  uint64_t total{0};
  for (NodeCP input : node->inputs()) {
    const auto rows = totalRawInputRows(input);
    if (!rows.has_value()) {
      return std::nullopt;
    }
    total += *rows;
  }

  return total;
}

// Returns the workers to run the query rooted at 'node' on:
// 'smallQueryNumWorkers' when its scans are estimated to read at most
// 'smallQueryMaxScanRows', and 'maxWorkers' otherwise. Never returns more than
// 'maxWorkers'.
int32_t chooseNumWorkers(
    NodeCP node,
    const OptimizerOptions& options,
    int32_t maxWorkers) {
  if (options.smallQueryMaxScanRows <= 0) {
    return maxWorkers;
  }

  const auto numRawInputRows = totalRawInputRows(node);
  if (!numRawInputRows.has_value() ||
      *numRawInputRows > static_cast<uint64_t>(options.smallQueryMaxScanRows)) {
    return maxWorkers;
  }

  return std::min(options.smallQueryNumWorkers, maxWorkers);
}

} // namespace

namespace {
const auto& passNames() {
  static const folly::F14FastMap<Optimizer::Pass, std::string_view> kNames = {
      {Optimizer::Pass::kTranslate, "TRANSLATE"},
      {Optimizer::Pass::kDecorrelate, "DECORRELATE"},
      {Optimizer::Pass::kLimitAndOrder, "LIMIT_AND_ORDER"},
      {Optimizer::Pass::kPushdownAndPrune, "PUSHDOWN_AND_PRUNE"},
      {Optimizer::Pass::kFoldMetadataAggregate, "FOLD_METADATA_AGGREGATE"},
      {Optimizer::Pass::kConnectorPushdown, "CONNECTOR_PUSHDOWN"},
      {Optimizer::Pass::kEstimateLeafStats, "ESTIMATE_LEAF_STATS"},
      {Optimizer::Pass::kPlanPhysical, "PLAN_PHYSICAL"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Optimizer, Pass, passNames);

NodeCP Optimizer::planTo(
    std::optional<Pass> pass,
    PushdownAndPrunePass::ConnectorPushdown connectorPushdown,
    const MultiFragmentPlan::Options* options) {
  ConstantPlanRunner constantPlanRunner{queryCtx_};
  auto translated = TranslatePass::run(
      plan_,
      schema_,
      schemaResolver_,
      evaluator_,
      builder_,
      session_,
      constantPlanRunner);
  outputColumns_ = translated.outputColumns;
  outputNames_ = translated.outputNames;
  if (pass == Pass::kTranslate) {
    return translated.root;
  }

  NodeCP node = DecorrelatePass::run(translated.root, builder_);
  if (pass == Pass::kDecorrelate) {
    return node;
  }

  node = LimitAndOrderPass::run(node, builder_);
  if (pass == Pass::kLimitAndOrder) {
    return node;
  }

  node = PushdownAndPrunePass::run(
      node,
      translated.outputColumns,
      builder_,
      evaluator_,
      session_,
      connectorPushdown);
  if (pass == Pass::kPushdownAndPrune) {
    return node;
  }

  node = FoldMetadataAggregatePass::run(node, builder_, session_);
  if (pass == Pass::kFoldMetadataAggregate) {
    return node;
  }

  if (translated.connectorPushdownSupported) {
    node = ConnectorPushdownPass::run(
        node, builder_, schema_, schemaResolver_, session_, evaluator_);
  }
  if (pass == Pass::kConnectorPushdown) {
    return node;
  }

  if (session_.options().useFilteredTableStats) {
    EstimateLeafStatsPass::run(node, session_);
  }
  if (pass == Pass::kEstimateLeafStats) {
    return node;
  }

  VELOX_CHECK_NOT_NULL(options, "Physical planning needs plan options");

  // Decide the width before physical planning, which reads maxRemotePartitions
  // to shape exchanges and to cost broadcasts.
  planOptions_ = *options;
  planOptions_.maxRemotePartitions =
      chooseNumWorkers(node, session_.options(), options->maxRemotePartitions);

  node = PlanPhysicalPass::run(
      node,
      builder_,
      session_.options(),
      planOptions_.maxRemotePartitions,
      planOptions_.maxLocalPartitions);
  return node;
}

Optimizer::DebugPlan Optimizer::debugPlanTo(
    const MultiFragmentPlan::Options& options,
    std::optional<Pass> pass) {
  markUsed();
  NodeCP root =
      planTo(pass, PushdownAndPrunePass::ConnectorPushdown::kOffer, &options);

  // Before leaf statistics are read the numbers are not the ones planning goes
  // on, so no estimate is offered at all.
  if (pass.has_value() && *pass < Pass::kEstimateLeafStats) {
    return {root, nullptr};
  }

  debugEstimates_.emplace();
  return {
      root, [this](NodeCP node) { return debugEstimates_->estimate(node); }};
}

PlanAndStats Optimizer::optimize(const MultiFragmentPlan::Options& options) {
  markUsed();

  NodeCP planned = planTo(
      /*pass=*/std::nullopt,
      PushdownAndPrunePass::ConnectorPushdown::kOffer,
      &options);

  EmitPass::Result emitted = EmitPass::run(
      planned,
      outputColumns_,
      outputNames_,
      session_,
      evaluator_,
      planOptions_);

  PlanAndStats result;
  result.plan = std::make_shared<MultiFragmentPlan>(
      std::move(emitted.fragments), planOptions_);
  result.plan->checkConsistency(
      /*mayBeEmpty=*/plan_.is(logical_plan::NodeKind::kTableWrite));
  result.finishWrite = std::move(emitted.finishWrite);
  result.prediction = std::move(emitted.prediction);

  // The plan's output must have one column per logical-plan output column. A
  // TableWrite root emits write-stats rows instead of the query columns, so it
  // is exempt.
  if (!plan_.is(logical_plan::NodeKind::kTableWrite)) {
    const auto& veloxOutput =
        result.plan->fragments().back().fragment.planNode->outputType();
    VELOX_CHECK(
        veloxOutput->equivalent(*plan_.outputType()),
        "Plan output type does not match the logical plan output type: {} vs {}",
        veloxOutput->toString(),
        plan_.outputType()->toString());
  }

  return result;
}

std::string Optimizer::explainIo(
    std::optional<CatalogSchemaTableName> outputTable) {
  markUsed();

  // The filters are not offered to the connector: the report is what the query
  // applies to each table, not how some connector would read it.
  NodeCP pushed = planTo(
      Pass::kPushdownAndPrune,
      PushdownAndPrunePass::ConnectorPushdown::kSkip,
      nullptr);

  std::vector<std::pair<BaseTableCP, ExprVector>> tableFilters;
  collectScans(pushed, tableFilters);
  return optimizer::explainIo(tableFilters, std::move(outputTable));
}

QueryStats Optimizer::estimateQueryStats() {
  markUsed();

  NodeCP root = planTo(
      Pass::kEstimateLeafStats,
      PushdownAndPrunePass::ConnectorPushdown::kOffer,
      nullptr);

  EstimateProvider estimateProvider;
  const Estimate& estimate = estimateProvider.estimate(root);

  const auto& columns = outputColumns_;
  const auto& names = outputNames_;
  VELOX_CHECK_EQ(columns.size(), names.size());

  QueryStats result;
  result.cardinality = estimate.cardinality;
  result.columns.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    // `value` returns the estimator's post-filter refined constraint, falling
    // back to the column's own Value for columns it did not refine.
    const Value& columnValue = value(estimate.constraints, columns[i]);
    result.columns.push_back(
        {.name = names[i],
         .type = columnValue.type,
         .nullFraction = columnValue.nullFraction,
         .distinctCount = columnValue.cardinality,
         .min = columnValue.min,
         .max = columnValue.max});
  }
  return result;
}

} // namespace facebook::axiom::optimizer::v2
