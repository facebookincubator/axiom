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
  /// A non-deterministic input is left alone, since a fold could evaluate one
  /// of its outputs more than once.
  ///
  /// TODO: still inline the deterministic outputs when only some are
  /// non-deterministic -- isolate the non-deterministic ones in a separate
  /// `Project` below and fold the rest.
  static NodeCP makeProject(
      NodeCP input,
      ExprVector exprs,
      ColumnVector outColumns,
      Builder& builder);

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

  // Returns the ExprCP that the consumer should reference in place of
  // 'expr'. Pass-throughs:
  //   - 'expr' is a Column: returned unchanged.
  //   - 'expr' is a Literal and 'allowConstant' is true: returned unchanged.
  // Otherwise lifts 'expr' into a projected column. If 'alias' is
  // non-null, that exact Column is used as the projection's output;
  // otherwise a fresh `__pXX` column is synthesized.
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
