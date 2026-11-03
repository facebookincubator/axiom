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

#include "axiom/optimizer/Node.h"
#include "axiom/optimizer/PathSet.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

/// Records which parts of each column a query reads, so that a scan can ask
/// its connector for those parts alone.
///
/// `PushdownAndPrunePass` fills one of these as it descends and reads it at
/// each Scan:
///
///   NodeCP rewrite(NodeCP node, PushdownContext& context) override {
///     access_.add(*node);
///     return NodeRewriter::rewrite(node, context);
///   }
///
///   // In rewriteProject, once the pass knows which projections it keeps:
///   access_.addProducing(survivingExprs[i], survivingOutputs[i]);
///
///   // At the Scan, negotiating with the connector:
///   toSubfields(column->name(), access_.subfieldsOf(column), ...);
///
/// An expression is decomposed into a column and a path when it is a chain of
/// struct dereferences and constant subscripts: `m[1].x` reads `[1].x` of `m`.
/// A chain rooted at anything else contributes the whole of every column it
/// reaches, which is the current behavior of every read and therefore always
/// safe. Note that an unrecognized call gives up only its own result: its
/// arguments are still decomposed, so `f(m[1].x)` still reads one key and one
/// field.
///
/// Absence of a column is not "reads nothing" — a column never passed to
/// `add` has no entry, and `subfieldsOf` reports the whole column for it.
///
/// A node that produces a column from an expression -- a projection, a union
/// leg -- composes the paths its consumers read with that expression.
/// Elsewhere a path is recorded against the column the expression reads rather
/// than the one it produces, which yields a prefix of the true path. A prefix
/// covers the paths below it, so an uncomposed read is wider than needed and
/// never narrower — which is what lets the nodes that rename columns leave
/// composition alone.
///
/// The caller must hold to all of:
///  - `add(const Node&)` runs on each node from the root down, so a node
///    producing a column is reached after the consumers whose paths it
///    extends.
///  - The expressions a pass may prune — a Project's, an Aggregate's — are
///    recorded by that pass from what it keeps, not by `add(const Node&)`.
///  - A subtree the descent has yet to reach, but whose reads a Scan above it
///    will serve, is recorded first with `addSubtree`.
class ColumnAccess {
 public:
  ColumnAccess();

  /// Records the paths 'expr' reads.
  void add(ExprCP expr);

  /// Records the paths 'expr' reads, given that its result is itself read
  /// through 'resultColumn'. Composes across a projection: an output column
  /// read as `[1]`, produced by `m[200800]`, reads `[200800][1]` of `m`.
  void addProducing(ExprCP expr, ColumnCP resultColumn);

  /// Records the paths each of 'exprs' reads.
  void addAll(const ExprVector& exprs);

  /// Records the paths every expression 'node' evaluates reads. Call on each
  /// node from the root down, so that a node producing a column is reached
  /// after the consumers whose paths it extends. The pass records the
  /// expressions it may prune -- a Project's and an Aggregate's -- itself,
  /// from what survives.
  void add(const Node& node);

  /// Records 'node' and everything below it, including the expressions a
  /// Project or an Aggregate holds. For a subtree read by a node the pass has
  /// yet to reach: recording every expression is wider than the pass would
  /// choose, which is the safe direction.
  void addSubtree(const Node& node);

  /// The paths read from 'column', reduced to the shortest set that covers
  /// them. Empty means the whole column is read, which is also what an
  /// unrecorded column reports.
  PathSet subfieldsOf(ColumnCP column) const;

 private:
  // Records the paths reaching 'expr', where 'steps' holds the path from the
  // reader down to it, outermost first, and 'tail' holds the paths its reader
  // takes from the result. An empty 'tail' means the result is read whole.
  void addSteps(ExprCP expr, std::vector<Step>& steps, const PathSet& tail);

  // Records the path reaching 'column', which is 'steps' reversed, extended by
  // each path in 'tail'.
  void addColumn(
      ColumnCP column,
      const std::vector<Step>& steps,
      const PathSet& tail);

  // Walks the argument of 'call' that supplies the field being read. Returns
  // false if no field of the constructed row is read, leaving 'steps'
  // unchanged.
  bool addRowConstructor(
      const Call* call,
      std::vector<Step>& steps,
      const PathSet& tail);

  // Walks the operand of 'call' with the step it contributes appended to
  // 'steps'. Returns false if 'call' is not a subscript or field dereference,
  // leaving 'steps' unchanged.
  bool
  addPathStep(const Call* call, std::vector<Step>& steps, const PathSet& tail);

  // Records that 'call' reads each of its inputs in full, decomposing the
  // inputs themselves so a path inside one still prunes.
  void addCallInputs(const Call* call);

  // Records that every column 'expr' reads is read whole.
  void addWhole(ExprCP expr);

  // Interned names of the calls that decompose into a path step. Null when the
  // dialect in force registers no such function.
  const Name subscript_;
  const Name elementAt_;

  // Paths read from each column, before reduction. A column absent here is
  // read whole.
  folly::F14FastMap<ColumnCP, PathSet> byColumn_;

  // Calls whose inputs have been added; see `addCallInputs`.
  folly::F14FastSet<const Call*> callInputsAdded_;
};

} // namespace facebook::axiom::optimizer
