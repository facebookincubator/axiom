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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/core/ExpressionEvaluator.h"

namespace facebook::axiom::optimizer::v2 {

/// Single top-down pass over the tree IR that does two intertwined jobs in one
/// traversal:
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
/// Both jobs share one traversal state (the internal `PushdownContext`):
/// `pending` conjuncts, the `required` / `requiredAbove` column sets, and
/// `nonNullColumns` (columns an ancestor inner join proves non-NULL).
///
/// Notable per-node behavior:
///  - Join places each conjunct on the left input, the right input, or the
///    join itself, and demotes an outer join to inner (or full to left/right)
///    when a pending conjunct or a proven-non-NULL column rejects nulls on a
///    padded side. It also fuses a consuming mark filter into a
///    `kLeftSemiProject` join.
///  - Window pushes conjuncts that reference only partition keys below the
///    window (the rest stay above), prunes window functions no consumer reads,
///    and fuses a `rankColumn <op> n` bound over a single ranking function
///    (row_number / rank / dense_rank) into a `TopNRowNumber`, consuming that
///    predicate.
///  - `Apply` must already be gone: decorrelate removes every `Apply` before
///    this pass, so encountering one is a logic error.
///
/// Post-condition: no `Filter` node sits directly above a `Scan`. Every
/// conjunct that reaches a `Scan` is absorbed into `Scan.filters`; which of
/// those the connector can actually evaluate is decided later (at Emit), and
/// connector-rejected filters reappear as a `Filter` above the `TableScan` only
/// in the final Velox plan. A rewrite running after this pass can therefore
/// treat a `Scan` as carrying all of its predicates.
class PushdownAndPrunePass {
 public:
  /// Returns `root` rewritten as described in the class doc. `evaluator` backs
  /// the expression simplifier used to fold conjuncts after substitution (and
  /// to detect ones that reduce to constant `false`).
  static NodeCP run(
      NodeCP root,
      Builder& builder,
      velox::core::ExpressionEvaluator& evaluator);
};

} // namespace facebook::axiom::optimizer::v2
