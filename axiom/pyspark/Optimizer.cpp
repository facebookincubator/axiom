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

#include "axiom/pyspark/Optimizer.h"

#include <glog/logging.h>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "velox/expression/Expr.h"

using namespace facebook;

namespace axiom::collagen {
namespace {

// Helper to check if the root node of a plan is a CREATE TABLE operation.
const facebook::axiom::logical_plan::TableWriteNode* FOLLY_NULLABLE
isCreateTableNode(
    const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan) {
  if (!plan) {
    return nullptr;
  }

  // We only check the top node of the plan for CREATE TABLE operations.
  if (auto* writeNode =
          dynamic_cast<const facebook::axiom::logical_plan::TableWriteNode*>(
              plan.get())) {
    if (writeNode->writeKind() ==
        facebook::axiom::logical_plan::WriteKind::kCreate) {
      return writeNode;
    }
  }

  return nullptr;
}

// Creates a table with connector metadata.
facebook::axiom::connector::TablePtr createTable(
    const facebook::axiom::logical_plan::TableWriteNode& writeNode,
    const std::string& connectorId) {
  auto* metadata =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId);

  // Convert string options to velox::Variant options
  folly::F14FastMap<std::string, velox::Variant> options;
  for (const auto& [key, value] : writeNode.options()) {
    options[key] = velox::Variant(value);
  }

  // Build the table schema from column names and column expressions
  const auto& columnNames = writeNode.columnNames();
  const auto& columnExpressions = writeNode.columnExpressions();

  std::vector<std::string> schemaNames;
  std::vector<velox::TypePtr> schemaTypes;
  schemaNames.reserve(columnNames.size());
  schemaTypes.reserve(columnExpressions.size());

  for (size_t i = 0; i < columnNames.size(); ++i) {
    schemaNames.push_back(columnNames[i]);
    schemaTypes.push_back(columnExpressions[i]->type());
  }

  auto tableSchema = velox::ROW(std::move(schemaNames), std::move(schemaTypes));

  auto session = std::make_shared<facebook::axiom::connector::ConnectorSession>(
      "pyspark_session");
  return metadata->createTable(
      session, writeNode.tableName(), tableSchema, options, /*explain=*/false);
}

} // namespace

facebook::axiom::optimizer::PlanAndStats optimize(
    const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
    const std::string& connectorId,
    velox::memory::MemoryPool* pool) {
  // Set up thread local structures.
  auto allocator = std::make_unique<velox::HashStringAllocator>(pool);
  auto context =
      std::make_unique<facebook::axiom::optimizer::QueryGraphContext>(
          *allocator);
  facebook::axiom::optimizer::queryCtx() = context.get();

  // Fetch connector and set up schema resolver.
  auto connector = velox::connector::getConnector(connectorId);
  auto schemaResolver =
      std::make_shared<facebook::axiom::connector::SchemaResolver>();

  // Check if this is a CREATE TABLE operation and set up schema resolver.
  if (auto* createTableNode = isCreateTableNode(logicalPlan)) {
    auto table = createTable(*createTableNode, connectorId);
    schemaResolver->setTargetTable(
        connectorId, createTableNode->tableName(), std::move(table));
  }

  auto history = std::make_unique<facebook::axiom::optimizer::VeloxHistory>();
  facebook::axiom::optimizer::OptimizerOptions optimizerOptions;
  optimizerOptions.sampleJoins = false;

  facebook::axiom::optimizer::MultiFragmentPlan::Options runnerOpts{
      .numWorkers = 1,
      .numDrivers = 1,
  };

  auto queryCtx = velox::core::QueryCtx::create();
  velox::exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), pool);

  auto session =
      std::make_shared<facebook::axiom::Session>(queryCtx->queryId());

  facebook::axiom::optimizer::Optimization opt(
      session,
      *logicalPlan,
      *schemaResolver,
      *history,
      queryCtx,
      evaluator,
      optimizerOptions,
      runnerOpts);
  auto best = opt.bestPlan();
  LOG(INFO) << "Axiom best plan:\n" << best->toString(false);

  return opt.toVeloxPlan(best->op);
}

} // namespace axiom::collagen
