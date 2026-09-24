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

/// Per-consumer builder that lifts compound sub-expressions into a Project
/// inserted between the consumer and its existing input.
class PrecomputeProjections {
 public:
  /// Returns a `Project` computing 'exprs' as 'outColumns' over 'input'. When
  /// 'input' is itself a deterministic `Project`, its expressions are folded
  /// into 'exprs' and it is dropped, rather than stacking a second `Project`.
  /// A non-deterministic input folds only where 'exprs' read each of its
  /// outputs at most once, since a fold copies an expression per reference.
  ///
  /// TODO: still inline the deterministic outputs when only some are
  /// non-deterministic -- isolate the non-deterministic ones in a separate
  /// `Project` below and fold the rest.
  static NodeCP makeProject(
      NodeCP input,
      ExprVector exprs,
      ColumnVector outColumns,
      Builder& builder);

  /// Computes any key in 'keys' that is not already a column, returning
  /// 'input' wrapped in a `Project` that adds those columns alongside its own,
  /// and 'keys' with each such key replaced by its column. An `Exchange`
  /// requires column keys; materializing before the shuffle is placed lets the
  /// consumer above read the same column instead of computing the value a
  /// second time. Without 'aliases', returns 'input' and 'keys' unchanged when
  /// every key is already a column.
  ///
  /// 'aliases' names the materialized columns positionally; a null entry, or an
  /// empty vector, mints a fresh name. A caller whose node already publishes
  /// the key under a column of its own -- an `Aggregate` grouping key, which
  /// its `outputColumns` names -- must pass that column, since consumers
  /// reference the key by it.
  static std::pair<NodeCP, ExprVector> materializeKeys(
      NodeCP input,
      const ExprVector& keys,
      Builder& builder,
      const ColumnVector& aliases = {});

  // When `projectAllInputs` is true (the default, for pass-through consumers
  // like Window/Sort/TopN), the project preserves every input column
  // alongside the lifted ones. When false (for narrowing consumers like
  // Aggregate/Unnest/Join), the project outputs only the columns passed to
  // `toColumn` — so an input column kept solely to feed a lifted expression is
  // dropped instead of passed through. The caller must `toColumn` every column
  // the consumer reads.
  PrecomputeProjections(
      NodeCP input,
      Builder& builder,
      bool projectAllInputs = true);

  // Returns the ExprCP that the consumer should reference in place of 'expr'.
  // Lambdas always pass through because a Project cannot evaluate them. For
  // other expressions, a non-null 'alias' is the required output column,
  // including when 'expr' is already a column. Without an alias, columns pass
  // through, allowed literals remain literals, and other expressions are
  // lifted into fresh `__pXX` columns. A later request for an expression
  // already emitted under an alias returns that alias.
  ExprCP
  toColumn(ExprCP expr, ColumnCP alias = nullptr, bool allowConstant = false);

  // Returns the input unchanged if no projections were added, otherwise the
  // input wrapped in a fresh `Project`. With `projectAllInputs` the project
  // adds the lifted columns alongside all input columns; without it the
  // project outputs only the columns passed to `toColumn`.
  NodeCP node() &&;

 private:
  void addToProject(ExprCP expr, ColumnCP column);

  NodeCP input_;
  Builder& builder_;
  const bool projectAllInputs_;
  ColumnVector outColumns_;
  ExprVector outExprs_;
  folly::F14FastMap<ExprCP, ColumnCP> seen_;
  bool needsProject_{false};
};

} // namespace facebook::axiom::optimizer::v2
