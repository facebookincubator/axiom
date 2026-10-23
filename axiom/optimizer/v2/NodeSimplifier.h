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

#include <stddef.h>
#include <optional>
#include <vector>

#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/ExprSimplifier.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

/// Builds a plan node after applying facts reported by its simplified inputs.
/// Node-specific rules decide whether a literal key is dropped or restored as
/// a column. The returned substitutions describe the original output layout
/// to consumers above the node.
///
/// A bottom-up caller passes each simplified child into the overload for its
/// parent:
///
///   NodeSimplifier::SimplifiedNode input{rewrittenInput, substitutions};
///   auto output = simplifier.make(sortKey, std::move(input));
///
/// Invariants:
/// - A non-empty result has a node.
/// - Every non-literal substitution reads columns produced by that node.
/// - Every column-only node position contains a column.
class NodeSimplifier {
 public:
  struct SimplifiedNode {
    /// Simplified node, or nullptr when the result is known empty.
    NodeCP node;
    /// Replacements for columns in the node's original output layout.
    PlanSubstitutions substitutions;
    /// Original positional layout when the parent cannot derive it from its
    /// key. Empty when the rewritten node retains that layout. A result with a
    /// null node always sets it.
    ColumnVector originalColumns;

    /// True when no rows can come out: a null node, or a Values with no rows.
    bool empty() const {
      return node == nullptr || Values::isEmpty(node);
    }

    /// Columns a consumer can read: the node's output columns, or the original
    /// layout when the node is null.
    const ColumnVector& outputColumns() const {
      return node == nullptr ? originalColumns : node->outputColumns();
    }
  };

  /// Uses `builder` for node identity and `simplifier` for constant folding.
  NodeSimplifier(Builder& builder, ExprSimplifier& simplifier)
      : builder_(builder), exprs_(builder), simplifier_(simplifier) {}

  /// Returns the node of 'result', or an empty Values over its columns when the
  /// node is null.
  NodeCP materialize(const SimplifiedNode& result) const {
    return result.node != nullptr
        ? result.node
        : builder_.makeEmptyValues(result.originalColumns);
  }

  /// Builds Values, returning empty for zero rows and reporting every column
  /// of a single-row input as a literal.
  SimplifiedNode make(Values::Key key);

  /// Builds a Scan after its caller has negotiated connector pushdown.
  SimplifiedNode make(Scan::Key key);

  /// Builds a WorkingTable state read.
  SimplifiedNode make(WorkingTable::Key key);

  /// Builds a Filter after folding input substitutions into its predicates.
  SimplifiedNode make(Filter::Key key, SimplifiedNode input);

  /// Builds a Project, reporting literal outputs and removing a pure rename
  /// only when it preserves every input column.
  SimplifiedNode make(Project::Key key, SimplifiedNode input);

  /// Builds a Sort or returns its input when no order key remains.
  SimplifiedNode make(Sort::Key key, SimplifiedNode input);

  /// Builds a Limit, or returns empty when its count is zero.
  SimplifiedNode make(Limit::Key key, SimplifiedNode input);

  /// Builds a TopN or a Limit when no order key remains.
  SimplifiedNode make(TopN::Key key, SimplifiedNode input);

  /// Builds an Aggregate after removing redundant grouping keys and folding
  /// constant FILTER conditions.
  SimplifiedNode make(Aggregate::Key key, SimplifiedNode input);

  /// Returns whether an Aggregate grouping by 'groupingKeys' keeps the key at
  /// 'index' or replaces it with its literal value. A literal key has one value
  /// in every row, so the Aggregate keeps a literal only when every key is a
  /// literal, and then only the first: that key still makes the Aggregate
  /// return no row for no input.
  static bool keepsGroupingKey(const ExprVector& groupingKeys, size_t index);

  /// Builds a GroupId after simplifying its grouping-key layout.
  SimplifiedNode make(GroupId::Key key, SimplifiedNode input);

  /// Builds a Window after simplifying its functions and keys.
  SimplifiedNode make(Window::Key key, SimplifiedNode input);

  /// Builds an Inference after substituting its call arguments.
  SimplifiedNode make(Inference::Key key, SimplifiedNode input);

  /// Builds a RowNumber after removing redundant partition keys.
  SimplifiedNode make(RowNumber::Key key, SimplifiedNode input);

  /// Builds a TopNRowNumber after simplifying partition and order keys.
  SimplifiedNode make(TopNRowNumber::Key key, SimplifiedNode input);

  /// Builds an Unnest and reports constants among replicated columns.
  SimplifiedNode make(Unnest::Key key, SimplifiedNode input);

  /// Builds an EnforceDistinct after removing redundant distinct keys.
  SimplifiedNode make(EnforceDistinct::Key key, SimplifiedNode input);

  /// Builds a MarkDistinct after dropping constant and duplicate keys while
  /// retaining one key when all keys are constant.
  SimplifiedNode make(MarkDistinct::Key key, SimplifiedNode input);

  /// Builds an AssignUniqueId and propagates visible input substitutions.
  SimplifiedNode make(AssignUniqueId::Key key, SimplifiedNode input);

  /// Builds an EnforceSingleRow over its original positional sources.
  SimplifiedNode make(EnforceSingleRow::Key key, SimplifiedNode input);

  /// Builds a TableWrite after restoring its fixed positional inputs. A
  /// TableWrite creates a fresh row-count column; when rebuilding one,
  /// 'originalOutputs' names the replaced node's columns, which the result
  /// reports as renamed to the new ones.
  SimplifiedNode make(
      TableWrite::Key key,
      SimplifiedNode input,
      const ColumnVector& originalOutputs);

  /// Builds a FixedPoint after restoring the anchor, step and convergence
  /// layouts. An empty step leaves the anchor's rows in kAppend and no rows in
  /// kReplace. Fails when the
  /// simplified step or convergence no longer reads the state. 'convergence'
  /// is ignored when the key has none.
  SimplifiedNode make(
      FixedPoint::Key key,
      SimplifiedNode anchor,
      SimplifiedNode step,
      SimplifiedNode convergence);

  /// Builds a Repeat over the simplified inputs. Its keys read them through
  /// their substitutions, and each side outputs the columns the Repeat
  /// passes on. An empty input makes the result empty.
  SimplifiedNode make(
      Repeat::Key key,
      SimplifiedNode start,
      SimplifiedNode hop,
      std::optional<SimplifiedNode> end);

  /// Builds a UnionAll after restoring each leg's positional columns.
  /// Substitutes outputs that are one literal in every non-empty leg, and
  /// returns a single non-empty leg as is.
  SimplifiedNode make(UnionAll::Key key, std::vector<SimplifiedNode> inputs);

  /// Builds an inner, outer, or semi Join after simplifying constant equi-key
  /// pairs and inputs.
  SimplifiedNode make(Join::Key key, SimplifiedNode left, SimplifiedNode right);

  /// Builds an IndexLookupJoin after restoring the probe's positional layout.
  SimplifiedNode make(IndexLookupJoin::Key key, SimplifiedNode probe);

  /// Builds an Apply after simplifying its residual filter and fixed body
  /// positions.
  SimplifiedNode
  make(Apply::Key key, SimplifiedNode input, SimplifiedNode body);

  /// Rewrites a plan-root output layout to renamed columns when possible, or
  /// restores literals in a Project when the boundary must keep its columns.
  void restoreOutputLayout(
      NodeCP& node,
      ColumnVector& outputColumns,
      const PlanSubstitutions& substitutions);

  /// Adds to 'substitutions' the expression identities a join's equi-key pairs
  /// establish over the join's output:
  /// - Inner: when both keys of a pair are columns, the right key reads as the
  ///   left key. Coalesce of a key pair reads as the left key.
  /// - Left or right: coalesce of a key pair reads as the preserved side's key.
  /// - Full: coalesce of a key pair, in either order, reads as one canonical
  ///   coalesce.
  /// For an outer join, a pair whose padded key can be non-NULL over a NULL
  /// input, such as coalesce, establishes none. Other join types establish
  /// none.
  void addJoinKeySubstitutions(
      velox::core::JoinType joinType,
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      PlanSubstitutions& substitutions);

 private:
  // Simplifies a join; defined in JoinSimplifier.cpp.
  class JoinSimplifier;

  // Precomputes each side-local part of a keyless join filter on the input
  // that supplies it.
  void precomputeCrossJoinFilter(
      NodeCP& left,
      NodeCP& right,
      ExprVector& filter,
      const ColumnVector& sourceColumns);

  // Applies input substitutions and simplifies each expression.
  ExprVector simplifyExpressions(
      const PlanSubstitutions& substitutions,
      const ExprVector& expressions);

  // Applies input substitutions, simplifies keys, and removes literal and
  // duplicate positions while keeping order types aligned.
  void simplifyOrderKeys(
      const PlanSubstitutions& substitutions,
      ExprVector& keys,
      OrderTypeVector& types);

  // Returns input substitutions restricted to columns visible above output.
  PlanSubstitutions visibleSubstitutions(
      PlanSubstitutions substitutions,
      NodeCP output);

  // Restores non-column replacements in `positions` immediately above input
  // and returns the column expression for each position. Existing input
  // columns remain in their original order and each restored identity is
  // projected at most once.
  ExprVector restoreColumnPositions(
      NodeCP& input,
      const ExprVector& positions,
      const ExprVector& replacements);

  // Column-list form of restoreColumnPositions.
  ColumnVector restoreColumnPositions(
      NodeCP& input,
      const ColumnVector& positions,
      const PlanSubstitutions& substitutions);

  // Restores an exact positional layout, rebuilding a folded one-row Values
  // directly when possible.
  NodeCP restoreExactLayout(
      NodeCP input,
      const ColumnVector& outputColumns,
      const PlanSubstitutions& substitutions);

  // Owns the nodes and expressions allocated during simplification.
  Builder& builder_;
  // Applies input substitutions and checks output visibility.
  ExprFactory exprs_;
  // Folds expressions exposed by input substitutions.
  ExprSimplifier& simplifier_;
};

} // namespace facebook::axiom::optimizer::v2
