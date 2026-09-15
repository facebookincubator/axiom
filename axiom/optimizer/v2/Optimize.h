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

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/ToVelox.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/PushdownAndPrunePass.h"
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

/// Estimated result statistics of a query under the v2 optimizer.
/// `cardinality` is the estimated output row count (nullopt when unknown);
/// `columns` is aligned 1:1 with the query's output columns.
struct QueryStats {
  std::optional<float> cardinality;
  std::vector<QueryColumnStats> columns;
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
        queryCtx_{std::move(queryCtx)},
        schema_{schemaResolver_, session_.statsWriter()} {}

  /// Lowers the plan to a distributed Velox execution plan (a
  /// `MultiFragmentPlan` of one or more fragments).
  ///
  /// Passes over a tree IR:
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
  ///   - Emit — lower to Velox `PlanNode`s.
  ///
  /// `options.maxRemotePartitions` / `options.maxLocalPartitions` (each >= 1)
  /// are the target task and per-task driver counts; at
  /// `maxRemotePartitions > 1` the plan is distributed across fragments with
  /// remote exchanges.
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

  /// Estimates the result statistics of the plan (the estimate behind
  /// SHOW STATS FOR (<query>)). Runs the pipeline through EstimateLeafStats —
  /// join ordering and Emit are skipped — then reads the root estimate via
  /// EstimateProvider, the v2 analogue of v1 reading the root DerivedTable
  /// after logical optimization. The returned pointers are valid only for the
  /// enclosing QueryGraphContext's lifetime.
  QueryStats estimateQueryStats();

  /// The passes run over the tree IR, in order. `Emit` is absent because it
  /// produces Velox nodes rather than IR.
  enum class Pass {
    kTranslate,
    kDecorrelate,
    kLimitAndOrder,
    kPushdownAndPrune,
    kFoldMetadataAggregate,

    /// Annotates base tables with connector statistics rather than rewriting
    /// the tree, so the root it returns is the one the pass before produced.
    /// Skipped when `useFilteredTableStats` is unset.
    kEstimateLeafStats,

    kPlanPhysical,
    kPrecomputeProjections,
  };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Pass);

  /// Cardinality and column constraints for the nodes of a `DebugPlan`,
  /// computed the way physical planning computes them — the same
  /// `EstimateProvider` over the same base statistics, so a leaf reproduces
  /// exactly and a join subtree is estimated as planning would estimate it.
  ///
  /// Empty for a pass before `kEstimateLeafStats`, where the base statistics
  /// have not been read yet. That is a position in the pipeline, not a
  /// statement that the pass ran: it is skipped when `useFilteredTableStats`
  /// is unset, and the estimates then fall back to the table's own statistics.
  using Estimates = std::function<Estimate(NodeCP)>;

  /// The IR at a pass boundary, and what is known about it.
  struct DebugPlan {
    NodeCP root;
    Estimates estimates;
  };

  /// Returns the IR as the pipeline leaves it: after `pass` when one is given,
  /// after the last pass otherwise, so a caller wanting the fully optimized IR
  /// does not name whichever pass currently comes last.
  ///
  /// The result is owned by this `Optimizer` and by the `QueryGraphContext` it
  /// runs under, and is valid while both are alive. That includes `estimates`,
  /// which reads through state this `Optimizer` holds.
  ///
  /// For debugging only, and not a stable API: the pass sequence is internal
  /// and `Pass` changes with it. Production code calls `optimize`.
  DebugPlan debugPlanTo(
      const MultiFragmentPlan::Options& options,
      std::optional<Pass> pass = std::nullopt);

 private:
  // Runs the pipeline up to and including `pass`, or all of it when `pass` is
  // unset, and returns the IR as the last pass run left it.
  // `connectorPushdown` is what PushdownAndPrunePass does with the filters it
  // lands on a scan. `options` may be null only for a `pass` earlier than
  // `kPlanPhysical`, the first one that reads them.
  NodeCP planTo(
      std::optional<Pass> pass,
      PushdownAndPrunePass::ConnectorPushdown connectorPushdown,
      const MultiFragmentPlan::Options* options);

  // One entry point per instance: the passes annotate shared IR objects and
  // the front end runs once, so a second call would plan over what the first
  // left behind.
  void markUsed() {
    VELOX_CHECK(!used_, "Optimizer is single use: construct one per query");
    used_ = true;
  }

  const logical_plan::LogicalPlanNode& plan_;
  const connector::SchemaResolver& schemaResolver_;
  const OptimizerSession& session_;
  velox::core::ExpressionEvaluator& evaluator_;
  const std::shared_ptr<velox::core::QueryCtx> queryCtx_;

  // Own the schema and the node factory: the IR points into the schema's
  // table layouts, and callers of `debugPlanTo` read the IR after the call.
  Schema schema_;
  Builder builder_;

  bool used_{false};

  // Estimates for `debugPlanTo`, computed over the IR it returns. Held here so
  // the lookup it hands back stays valid for this Optimizer's lifetime.
  std::optional<EstimateProvider> debugEstimates_;

  // The query's output columns and the names to emit them under, as the
  // translate pass resolved them.
  ColumnVector outputColumns_;
  std::vector<std::string> outputNames_;

  // The options physical planning ran under, with the worker count the
  // optimizer chose. `optimize` emits with these, so the plan and the fragment
  // count it was made for cannot disagree.
  MultiFragmentPlan::Options planOptions_;
};

} // namespace facebook::axiom::optimizer::v2
