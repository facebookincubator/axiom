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

#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/ToVelox.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::v2 {

/// End-to-end optimizer entry point: lowers a logical plan to a distributed
/// Velox execution plan (a `MultiFragmentPlan` of one or more fragments).
///
/// Stages over a tree IR:
///   - Translate — build the tree IR from the logical plan;
///   - Decorrelate — rewrite correlated subqueries as joins;
///   - LimitAndOrder — fold limits into ordering operators;
///   - PushdownAndPrune — push predicates down, prune unused columns;
///   - EstimateLeafStats — populate base-table cardinalities from the
///   connector;
///   - PlanPhysical — cost-based join order and distribution;
///   - PrecomputeProjections — lift compound expressions into `Project`s where
///     Velox needs a column or literal;
///   - ExpandAggregate — lower distinct aggregates to `MarkDistinct`;
///   - Emit — lower to Velox `PlanNode`s.
///
/// `options.numWorkers` / `options.numDrivers` (each >= 1) are the target task
/// and per-task driver counts; at `numWorkers > 1` the plan is distributed
/// across fragments with remote exchanges.
PlanAndStats optimize(
    const logical_plan::LogicalPlanNode& plan,
    const connector::SchemaResolver& schemaResolver,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options);

} // namespace facebook::axiom::optimizer::v2
