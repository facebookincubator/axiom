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

#include "axiom/optimizer/ExplainIo.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/DecorrelatePass.h"
#include "axiom/optimizer/v2/EmitPass.h"
#include "axiom/optimizer/v2/EstimateLeafStatsPass.h"
#include "axiom/optimizer/v2/ExpandAggregatePass.h"
#include "axiom/optimizer/v2/FoldMetadataAggregatePass.h"
#include "axiom/optimizer/v2/LimitAndOrderPass.h"
#include "axiom/optimizer/v2/PlanPhysicalPass.h"
#include "axiom/optimizer/v2/PrecomputeProjectionsPass.h"
#include "axiom/optimizer/v2/PushdownAndPrunePass.h"
#include "axiom/optimizer/v2/ScanHandle.h"
#include "axiom/optimizer/v2/TranslatePass.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Bundles the outputs of the front-end passes shared by full optimization and
// EXPLAIN (TYPE IO).
struct FrontendResult {
  TranslatePass::Result translated;
  NodeCP pushed;
};

// Runs the front-end passes shared by `optimize` and `explainIo`. 'schema' and
// 'builder' must outlive the returned IR.
FrontendResult translateAndPushdown(
    const logical_plan::LogicalPlanNode& plan,
    Schema& schema,
    velox::core::ExpressionEvaluator& evaluator,
    Builder& builder) {
  auto translated = TranslatePass::run(plan, schema, evaluator, builder);
  NodeCP decorrelated = DecorrelatePass::run(translated.root, builder);
  NodeCP limited = LimitAndOrderPass::run(decorrelated, builder);
  NodeCP pushed = PushdownAndPrunePass::run(limited, builder, evaluator);
  return {std::move(translated), pushed};
}

// Collects each Scan's base table and its pushed-down filter conjuncts.
void collectScans(
    NodeCP node,
    std::vector<std::pair<BaseTableCP, ExprVector>>& tableFilters) {
  if (node->is(NodeType::kScan)) {
    const auto* scan = node->as<Scan>();
    tableFilters.emplace_back(scan->baseTable(), scan->filters());
  }
  for (auto* input : node->inputs()) {
    collectScans(input, tableFilters);
  }
}

} // namespace

PlanAndStats optimize(
    const logical_plan::LogicalPlanNode& plan,
    const connector::SchemaResolver& schemaResolver,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options) {
  VELOX_USER_CHECK_GE(options.numWorkers, 1, "numWorkers must be at least 1");
  VELOX_USER_CHECK_GE(options.numDrivers, 1, "numDrivers must be at least 1");

  // Schema is owned here so its `connector::TablePtr`s — and the
  // `TableLayout`s the IR's `BaseTable` nodes hold raw pointers to —
  // stay alive through translate, precompute, and emit.
  // v2 does not surface optimizer metrics yet; Schema records into this sink.
  QueryRuntimeStats runtimeStats;
  Schema schema(schemaResolver, runtimeStats);

  // Connector table handles are built once here and reused at emit.
  ScanHandleCache scanHandles;

  Builder builder;
  auto frontend = translateAndPushdown(plan, schema, evaluator, builder);
  NodeCP folded = FoldMetadataAggregatePass::run(
      frontend.pushed, builder, session, evaluator, scanHandles);
  if (session.options().useFilteredTableStats) {
    EstimateLeafStatsPass::run(folded, session, evaluator, scanHandles);
  }
  NodeCP physicalPlanned = PlanPhysicalPass::run(
      folded,
      builder,
      session.options(),
      options.numWorkers,
      options.numDrivers);
  NodeCP precomputed = PrecomputeProjectionsPass::run(physicalPlanned, builder);
  // Distinct aggregates lower to MarkDistinct here, after physical planning
  // (grouping sets were already lowered to GroupId in translate).
  NodeCP root = ExpandAggregatePass::run(precomputed, builder);

  EmitPass::Result emitted = EmitPass::run(
      root,
      frontend.translated.outputColumns,
      frontend.translated.outputNames,
      session,
      evaluator,
      scanHandles,
      options);

  PlanAndStats result;
  result.plan = std::make_shared<MultiFragmentPlan>(
      std::move(emitted.fragments), options);
  result.finishWrite = std::move(emitted.finishWrite);
  return result;
}

std::string explainIo(
    const logical_plan::LogicalPlanNode& plan,
    const connector::SchemaResolver& schemaResolver,
    velox::core::ExpressionEvaluator& evaluator,
    std::optional<CatalogSchemaTableName> outputTable) {
  // Schema is owned here so its `connector::TablePtr`s — and the raw pointers
  // the IR's `BaseTable` nodes hold into them — stay alive for the duration.
  Schema schema(schemaResolver);

  // Run only the passes that push predicates into scans; join ordering and Emit
  // are not needed to report IO.
  Builder builder;
  auto frontend = translateAndPushdown(plan, schema, evaluator, builder);

  std::vector<std::pair<BaseTableCP, ExprVector>> tableFilters;
  collectScans(frontend.pushed, tableFilters);
  return optimizer::explainIo(tableFilters, std::move(outputTable));
}

} // namespace facebook::axiom::optimizer::v2
