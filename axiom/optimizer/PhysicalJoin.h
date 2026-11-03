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

namespace facebook::axiom::optimizer {

class ExprSimplifier;

/// Builds the operators of a join cluster in the form Velox executes: every
/// key position is a column of the operator's input, computed by a `Project`
/// below it.
///
/// The projection is placed once the join order is fixed: it decides which
/// input the expression is evaluated over, and which input that is follows
/// from the order.
class PhysicalJoin {
 public:
  /// Returns a `Join` reading each key as a column: an expression key is
  /// computed by a `Project` on the side that produces it, and 'key.filter' is
  /// rewritten to read that column rather than evaluate the same expression
  /// per pair. Each side's projection outputs only what the join reads, so an
  /// input column kept solely to feed a key stops below it. In a loop's step,
  /// marks the build loop-invariant when it can be (see
  /// `Join::Key::buildIsLoopInvariant`).
  static NodeCP
  makeJoin(Join::Key key, Builder& builder, ExprSimplifier& simplifier);

  /// Returns an `Unnest` reading each unnested value as a column, computed by
  /// a `Project` below it. That projection outputs only the replicated columns
  /// and the unnested values.
  static NodeCP
  makeUnnest(Unnest::Key key, Builder& builder, ExprSimplifier& simplifier);
};

} // namespace facebook::axiom::optimizer
