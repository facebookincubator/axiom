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
#pragma once

#include "axiom/logical_plan/GraphMatchBuilder.h"
#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Plans graph queries: builds the property graph a WITH GRAPH clause
/// defines, and adds the GraphMatch node a GRAPH_TABLE relation describes.
///
///   GraphTablePlanner planner(exprPlanner);
///   auto graph = planner.buildGraph(*withGraph, context, planTable);
///   planner.planGraphTable(*graphTable, graph, builder);
class GraphTablePlanner {
 public:
  /// Returns a PlanBuilder that produces the rows of an element table, and
  /// calls 'translate' while expressions bind to the table's columns.
  using TablePlanner = std::function<lp::PlanBuilder(
      const GraphElementTable& table,
      const std::function<void()>& translate)>;

  explicit GraphTablePlanner(ExpressionPlanner& exprPlanner)
      : exprPlanner_{exprPlanner} {}

  /// Builds the property graph 'graph' defines. 'planTable' plans each element
  /// table's rows.
  lp::PropertyGraphPtr buildGraph(
      const WithGraph& graph,
      const lp::PlanBuilder::Context& context,
      const TablePlanner& planTable);

  /// Adds the GraphMatch node 'graphTable' describes, over 'graph', to
  /// 'builder', which has no nodes yet.
  void planGraphTable(
      const GraphTable& graphTable,
      lp::PropertyGraphPtr graph,
      lp::PlanBuilder& builder);

 private:
  // Adds the vertex, edge and subpath patterns in 'elements' to 'match', in
  // order.
  void addElements(
      const std::vector<PathElementPtr>& elements,
      lp::PlanBuilder::GraphMatchBuilder& match);

  // Returns 'expr' as an ExprApi, or nullopt if 'expr' is null.
  std::optional<lp::ExprApi> toOptionalExpr(const ExpressionPtr& expr);

  ExpressionPlanner& exprPlanner_;
};

} // namespace axiom::sql::presto
