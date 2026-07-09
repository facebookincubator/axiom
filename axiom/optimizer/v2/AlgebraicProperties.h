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

#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

/// Algebraic equivalences between a lower (child) join `x1` and an
/// upper (parent) join `x2` over operands (A, B, C), where `x1`
/// joins A and B, and `x2` joins their result with C. Each field
/// is true iff the corresponding rewrite produces the same result
/// as the original tree.
///
/// Used by `HypergraphBuilder` to derive TES expansions per
/// Moerkotte/Fender/Eich, "On the Correct and Complete Enumeration
/// of the Core Search Space" (SIGMOD 2013).
struct AlgebraicProperties {
  /// (A x1 B) x2 C  ==  A x1 (B x2 C)
  bool associative{false};
  /// (A x1 B) x2 C  ==  (A x2 C) x1 B
  bool leftAsscom{false};
  /// A x1 (B x2 C)  ==  B x2 (A x1 C)
  bool rightAsscom{false};

  /// Returns the algebraic properties for the (child, parent) join
  /// type pair. Encodes M/N Tables 2 and 3 for the left-form join
  /// types the enumerator reorders: `kInner`, `kLeft`, `kFull`,
  /// `kLeftSemiFilter`/`kLeftSemiProject` (semijoin ⋉, identical
  /// reorderability), and `kAnti` (antijoin ▷, sharing ⋉'s rows).
  /// Callers normalize `kRight`/`kRightSemi*` to their left forms
  /// (operand swap) before lookup. Throws for other types via
  /// `VELOX_NYI`, including the counting variants
  /// (`kCountingLeftSemiFilter`/`kCountingAnti`), which stay opaque.
  ///
  /// Conditional entries (those the paper marks "+ if predicate
  /// rejects nulls") are collapsed to false. The precondition that
  /// makes this sound is that `PushdownAndPrune` already demotes
  /// any outer join whose ancestor predicates reject nulls on its
  /// null-padded side, so the conditional rows never apply within
  /// a cluster.
  static AlgebraicProperties derive(
      velox::core::JoinType child,
      velox::core::JoinType parent);
};

} // namespace facebook::axiom::optimizer::v2
