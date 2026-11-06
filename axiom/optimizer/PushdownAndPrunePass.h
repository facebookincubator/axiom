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

#include "axiom/optimizer/Builder.h"
#include "axiom/optimizer/Node.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "velox/core/ExpressionEvaluator.h"

namespace facebook::axiom::optimizer {

/// Top-down pass over the tree IR that combines filter pushdown with column
/// pruning:
///
///  - Filter pushdown. Each `Filter`'s conjuncts are flattened into a `pending`
///    set carried down the tree. At every node, conjuncts the node's rule
///    allows continue downward; a conjunct that cannot pass is re-emitted as a
///    `Filter` immediately above that node. Input `Filter` nodes thus dissolve
///    and reappear as low as each conjunct can legally go.
///  - Column pruning. A `required` column set is carried down from `root`'s
///    output schema. Each node narrows its `outputColumns` to what consumers
///    above still read, and drops the projections, aggregates, window
///    functions, and union legs that produce only unused columns.
///
/// Both jobs share one rewrite state (the internal `PushdownContext`):
/// `pending` conjuncts, the `required` / `requiredAbove` column sets, and
/// `nonNullColumns` (columns an ancestor inner join proves non-NULL).
/// On the way back up, an inner join may replace a right key column with its
/// equal left key and remove the right key from its output. Every ancestor must
/// apply these replacements to the expressions and column lists it retains.
/// Boundaries with a fixed internal schema restore the original column
/// identities with a `Project`; these include `TableWrite` inputs and each
/// `FixedPoint` branch. Root substitutions are returned to the pipeline as its
/// new output schema.
/// Before rewriting a join, the pass makes a read-only walk over its inputs to
/// collect the facts they guarantee. A join's facts are cached per join type:
/// conditions on its rows can make an outer join left, right or inner, or a
/// semi join that projects a mark a filtering one, so a join has at most four
/// types. Only joins read those conditions, and a node passes its input only
/// its own conditions or none, so the facts of any other node depend only on
/// the node and the columns requested from it. Every walk requests the same
/// columns from a node, so these facts are cached too and computed once.
/// Collection neither constructs plan nodes nor consults connectors.
///
/// Notable per-node behavior:
///  - Join places each conjunct on the left input, the right input, or the
///    join itself, and reduces an outer join to inner (or full to left/right)
///    when a pending conjunct or a proven-non-NULL column rejects nulls on a
///    padded side. It also fuses a consuming mark filter into a
///    `kLeftSemiProject` join. A cross join also moves the single-side parts of
///    its filter into the inputs, where they are evaluated once per row instead
///    of once per pair.
///  - Window pushes conjuncts that reference only partition keys below the
///    window (the rest stay above), prunes window functions no consumer reads,
///    and specializes a single ranking function (row_number / rank /
///    dense_rank) into the cheapest node that computes it: an unordered
///    `row_number` becomes a `RowNumber`, and an ordered ranking becomes a
///    `TopNRowNumber` when a bound caps each partition. That bound comes from a
///    `rankColumn <op> n` predicate, which is consumed, or from a `Limit` /
///    `TopN` above the window, which stays.
///  - `Apply` must already be gone: decorrelate removes every `Apply` before
///    this pass, so encountering one is a logic error.
///
/// Post-condition: every conjunct that reaches a `Scan` is offered to the
/// connector. The ones it takes go into the `TableAccessHandle` the `Scan`
/// points at. The ones it rejects stay in a single `Filter` directly above the
/// `Scan`, which then also outputs the columns they read, narrowed by a
/// `Project` when nothing above wants them and the consumer is not itself a
/// `Project`. A lookup-only `Scan` paired with a supported join becomes an
/// `IndexLookupJoin` whose handle carries its lookup keys and whose filter
/// holds the lookup filters the connector rejects. A rewrite running after this
/// pass sees the nodes the query will run.
class PushdownAndPrunePass {
 public:
  /// Rewritten root and its user-visible output columns.
  struct Result {
    /// Rewritten plan root.
    NodeCP root;
    /// Output layout after applying substitutions made by the pass.
    ColumnVector outputColumns;
  };

  /// Whether the conjuncts that reach a `Scan` are offered to the connector.
  enum class ConnectorPushdown {
    /// Offer them; the `Scan` comes out pointing at the resulting handle.
    kOffer,
    /// Do not call the connector. Every conjunct stays in a `Filter` over the
    /// `Scan` and the `Scan` has no handle, so the result says which
    /// predicates the query applies to each table without committing to a way
    /// of reading it. This is what EXPLAIN (TYPE IO) reports, and a plan
    /// produced this way cannot be emitted.
    kSkip,
  };

  /// Returns `root` rewritten as described in the class doc. `evaluator` backs
  /// the expression simplifier used to fold conjuncts after substitution (and
  /// to detect ones that reduce to constant `false`).
  /// @param outputColumns The user-visible output layout. `root` may produce
  /// more columns than this -- Emit trims them -- and pruning uses this, not
  /// `root`'s schema, to decide what the query actually needs.
  /// @param schema Resolves the storage tables a search through a vector index
  /// reads (see connector::Table::vectorIndexes()).
  static Result run(
      NodeCP root,
      const ColumnVector& outputColumns,
      Builder& builder,
      const Schema& schema,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerSession& session,
      ConnectorPushdown connectorPushdown);
};

} // namespace facebook::axiom::optimizer
