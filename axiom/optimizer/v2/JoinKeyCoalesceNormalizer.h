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

namespace facebook::axiom::optimizer::v2 {

/// Replaces a binary coalesce of eligible equijoin keys with the key preserved
/// by the join, before required columns are calculated. For example,
/// `coalesce(leftKey, rightKey)` becomes `leftKey` above a left join.
///
/// Matched rows satisfy `leftKey = rightKey`, so either key has the same SQL
/// value. On an unmatched outer-join row, eligibility requires the
/// non-preserved key to evaluate to NULL after its side is null-padded, so the
/// preserved key has the coalesce value. This permits these choices: inner and
/// left joins use the left key, right joins use the right key, and full joins
/// have no single representative.
///
/// The rewrite relies on these invariants:
///  - Keys are deterministic expressions over columns from their respective
///    join inputs, have the same exact type, and have no floating-point or
///    custom-comparison semantics.
///  - A key on a null-padded side references that side and has default null
///    behavior, so it evaluates to NULL on an unmatched row.
///  - A substitution propagates through a node only while that node preserves
///    the participating column bindings. Rebinding a participating column to
///    another value invalidates the substitution. Row- and binding-preserving
///    nodes use the rewriter's pass-through behavior.
///  - Each DAG node is rewritten once and caches both its node and synthesized
///    substitutions, preventing facts from leaking between input branches.
///  - The input is the logical v2 plan after decorrelation and limit/order
///    rewriting. `Apply` is gone; `RowNumber`, `TopNRowNumber`, `MarkDistinct`,
///    and `Exchange` are introduced by later passes.
class JoinKeyCoalesceNormalizer {
 public:
  /// Returns `root` with eligible coalesces replaced by their representative
  /// join keys.
  static NodeCP normalize(NodeCP root, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
