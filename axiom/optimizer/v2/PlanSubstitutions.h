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

#include <folly/container/HeterogeneousAccess.h>

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

class ExprSimplifier;

/// Records expressions that a rewritten plan replaces with equivalent ones.
/// A parent applies these substitutions to expressions that read the rewritten
/// child. Before passing them farther upward, it removes substitutions whose
/// replacement reads columns the rebuilt node no longer outputs.
///
/// For example:
///
///   InnerJoin(coalesce(t.a, u.a) = v.a)       InnerJoin(t.a = v.a)
///     InnerJoin(t.a = u.a)              ->      InnerJoin(t.a = u.a)
///       Scan(t)                                   Scan(t)
///       Scan(u)                                   Scan(u)
///     Scan(v)                                   Scan(v)
///
/// The inner join records `u.a -> t.a` and
/// `coalesce(t.a, u.a) -> t.a`. Applying these substitutions changes the
/// parent join key from `coalesce(t.a, u.a)` to `t.a`.
///
/// A constant substitution can expose further simplification and narrow a
/// scan's output:
///
///   Filter(t.a = 1)                         Project(t.b AS x)
///     Project(                              -> Scan(t.b, filter: t.a = 1)
///         t.a,
///         if(t.a = 1, t.b, t.c) AS x
///       )
///       Scan(t.a, t.b, t.c)
///
/// Pushing the filter down records `t.a -> 1`. Applying the substitution
/// produces `if(1 = 1, t.b, t.c)`, which simplifies to `t.b`. This removes the
/// now-unused `t.a` and `t.c` outputs. The scan still reads `t.a` to enforce
/// the filter, but outputs only `t.b`.
///
/// Substitutions continue upward only while their replacements can be
/// evaluated from the current node's output.
///
/// Applying substitutions rebuilds expressions but does not simplify them;
/// callers invoke `ExprSimplifier` when replacement can expose folding.
///
/// For example, applying `{a -> b, b -> 1}` to `a` produces `1`.
class PlanSubstitutions {
 public:
  PlanSubstitutions() = default;

  /// Returns true when there are no expressions to rewrite. Callers use this
  /// to avoid rebuilding expressions and repairing plan boundaries.
  bool empty() const {
    return substitutions_.empty();
  }

  /// Removes all substitutions. Callers use this at plan boundaries that do
  /// not preserve expression equivalence, such as grouping sets and unions.
  void clear() {
    substitutions_.clear();
  }

  /// Adds `source -> replacement`. Fails if `source` already maps to a
  /// different replacement.
  void add(ExprCP source, ExprCP replacement);

  /// Sets a substitution, replacing an existing replacement for `source`.
  void set(ExprCP source, ExprCP replacement);

  /// Removes the substitution for `source`.
  void erase(ExprCP source) {
    substitutions_.erase(source);
  }

  /// Sets all substitutions from `other`, replacing existing replacements.
  void setAll(const PlanSubstitutions& other);

  /// Adds a substitution only when `source` has no existing replacement.
  void addIfAbsent(ExprCP source, ExprCP replacement);

  /// Adds all substitutions from `other`. Fails if a source maps to different
  /// replacements in the two sets.
  void merge(const PlanSubstitutions& other);

  /// Applies substitutions repeatedly until the expression stops changing.
  /// Returns the rebuilt expression without simplifying it.
  ExprCP apply(ExprCP expression, ExprFactory& exprs) const;

  /// Applies substitutions to every expression.
  ExprVector apply(const ExprVector& expressions, ExprFactory& exprs) const;

  /// Rebuilds an aggregate call after applying substitutions to its inputs.
  optimizer::AggregateCP apply(
      optimizer::AggregateCP aggregate,
      ExprFactory& exprs,
      Builder& builder) const;

  /// Rebuilds aggregate calls after applying substitutions to their inputs.
  AggregateCallVector apply(
      const AggregateCallVector& aggregates,
      ExprFactory& exprs,
      Builder& builder) const;

  /// Drops substitutions whose targets cannot be evaluated from the output.
  void retainVisible(const ColumnVector& outputColumns, ExprFactory& exprs);

  /// Restores a fixed internal boundary's expressions, column identities and
  /// order after rewriting its input.
  NodeCP restore(
      NodeCP input,
      const ColumnVector& outputColumns,
      ExprFactory& exprs,
      Builder& builder,
      ExprSimplifier& simplifier) const;

 private:
  // Maps an expression identity to the expression that replaces it.
  ExprFactory::ExprSubstitution substitutions_;
};

} // namespace facebook::axiom::optimizer::v2
