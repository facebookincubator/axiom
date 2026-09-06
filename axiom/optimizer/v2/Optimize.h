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

#include <optional>
#include <string>
#include <vector>

#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/ToVelox.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::optimizer::v2 {

/// Estimated statistics for one output column of the query estimate behind
/// SHOW STATS FOR (<query>). `type`, `min`, and `max` reference optimizer-owned
/// memory that is valid only while the QueryGraphContext the `Optimizer` ran
/// under is alive.
struct QueryColumnStats {
  std::string name;
  const velox::Type* type;
  /// Fraction of null values; nullopt when unknown.
  std::optional<float> nullFraction;
  /// Estimated number of distinct values; nullopt when unknown.
  std::optional<float> distinctCount;
  /// Min/max bounds, or nullptr when no bound is known.
  const velox::Variant* min;
  const velox::Variant* max;
};

/// Estimated output and resource statistics of a query under optimizer v2.
struct QueryStats {
  /// Estimated number of output rows, or nullopt when unknown.
  std::optional<float> cardinality;

  /// Statistics aligned 1:1 with the query's output columns.
  std::vector<QueryColumnStats> columns;

  /// Estimated rows read across all scans, or nullopt when any scan is
  /// unknown. TABLESAMPLE SYSTEM scales this count by its sampling rate.
  std::optional<float> numInputRows;

  /// Estimated logical payload bytes read across all scans using
  /// `Value::byteSize()` for column widths, or nullopt when any scan's row
  /// count is unknown. This does not estimate compressed storage I/O.
  std::optional<float> inputBytes;

  /// Estimated logical payload bytes in the query result, or nullopt when
  /// output cardinality is unknown.
  std::optional<float> outputBytes;
};

/// Runs the v2 optimizer over a single logical plan. Construct with the plan
/// and its resolution context, then call one entry point. Must be constructed
/// and used with an active QueryGraphContext.
class Optimizer {
 public:
  Optimizer(
      const logical_plan::LogicalPlanNode& plan,
      const connector::SchemaResolver& schemaResolver,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      std::shared_ptr<velox::core::QueryCtx> queryCtx)
      : plan_{plan},
        schemaResolver_{schemaResolver},
        session_{session},
        evaluator_{evaluator},
        queryCtx_{std::move(queryCtx)} {}

  /// Lowers the plan to a distributed Velox execution plan (a
  /// `MultiFragmentPlan` of one or more fragments).
  ///
  /// Stages over a tree IR:
  ///   - Translate — build the tree IR from the logical plan;
  ///   - Decorrelate — rewrite correlated subqueries as joins;
  ///   - LimitAndOrder — fold limits into ordering operators;
  ///   - PushdownAndPrune — push predicates down, prune unused columns;
  ///   - EstimateLeafStats — populate base-table cardinalities from the
  ///   connector;
  ///   - PlanPhysical — cost-based join order and distribution;
  ///   - PrecomputeProjections — lift compound expressions into `Project`s
  ///   where
  ///     Velox needs a column or literal;
  ///   - ExpandAggregate — lower distinct aggregates to `MarkDistinct`;
  ///   - Emit — lower to Velox `PlanNode`s.
  ///
  /// `options.numWorkers` / `options.numDrivers` (each >= 1) are the target
  /// task and per-task driver counts; at `numWorkers > 1` the plan is
  /// distributed across fragments with remote exchanges.
  ///
  /// Output column names are guaranteed to match the query only when the plan
  /// is rooted in an `OutputNode`, whose field names pin the result names. For
  /// a bare plan (no OutputNode) the optimizer may elide pure-rename
  /// projections and carry a column under a source or disambiguated name, so
  /// the result column names are unspecified — do not depend on them.
  PlanAndStats optimize(const MultiFragmentPlan::Options& options);

  /// Produces the EXPLAIN (TYPE IO) JSON. Runs the pipeline only through
  /// PushdownAndPrunePass (predicate pushdown into scans) — join ordering and
  /// Emit are skipped — then extracts per-table column domains from the scan
  /// filters. 'outputTable' is included for INSERT/CTAS.
  std::string explainIo(
      std::optional<CatalogSchemaTableName> outputTable = std::nullopt);

  /// Estimates output and resource statistics for the plan. Runs the shared
  /// front end (translate + pushdown) and, when the `useFilteredTableStats`
  /// option is set, EstimateLeafStatsPass. The returned pointers are valid
  /// only for the enclosing QueryGraphContext's lifetime.
  QueryStats estimateQueryStats();

 private:
  const logical_plan::LogicalPlanNode& plan_;
  const connector::SchemaResolver& schemaResolver_;
  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
  const std::shared_ptr<velox::core::QueryCtx> queryCtx_;
};

} // namespace facebook::axiom::optimizer::v2
