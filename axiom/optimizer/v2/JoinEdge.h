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

#include <utility>

#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/RelationSet.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

/// Whether the build (right) side of a join with `joinType` may be broadcast.
///
/// A side may be broadcast only if it is not *preserved* by the join — i.e. its
/// rows are emitted only on a match, never on their own account. Broadcasting
/// replicates a side to every task while the other side stays partitioned, so a
/// preserved side must be the partitioned one or its rows would be emitted on
/// every task. The right side is preserved by RIGHT / FULL outer joins and by
/// right semi joins; for those the build cannot be broadcast, and the mirror
/// orientation broadcasts the other side instead.
///
/// A counting join is excluded because its result depends on a per-key count
/// over the whole build input: a broadcast copy would give every task the full
/// count, so a key could be emitted once per task.
inline bool canBroadcastBuild(velox::core::JoinType joinType) {
  using velox::core::JoinType;
  return joinType == JoinType::kInner || joinType == JoinType::kLeft ||
      joinType == JoinType::kLeftSemiFilter ||
      joinType == JoinType::kLeftSemiProject || joinType == JoinType::kAnti;
}

/// A hyperedge in a join cluster's hypergraph. For join edges,
/// `leftEndpoints()` and `rightEndpoints()` are the relation sets directly
/// referenced by the corresponding key expressions. `leftEligibility()` and
/// `rightEligibility()` are the disjoint hypernodes that must lie on opposite
/// sides of an enumerated partition.
///
///   - Inner equi-join. `filter` is empty; non-equi conjuncts live in
///     `JoinHypergraph::filterConjuncts` so DPhyp can place them at
///     the lowest eligible join. `nullAware` and `nullAsValue` are
///     unused.
///   - Non-inner equi-join (outer, semi, or anti). `filter` carries conjuncts
///     bound to this edge because moving them can change null-padding or
///     existence semantics. `nullAware` and `nullAsValue` carry Velox null
///     semantics.
///   - Unnest. Built by `JoinEdge::unnest`. Its endpoint and eligibility sides
///     are identical: `leftEndpoints()` covers the relations whose subtree
///     feeds the Unnest and `rightEndpoints()` is the single Unnest relation.
///     No equi-keys, no filter; `isUnnest()` discriminates. Unlike the other
///     flavors this edge never becomes a join: an Unnest expands the rows of
///     the relations on `leftEndpoints()` rather than pairing two inputs, so
///     DPhyp lowers it to a unary `UnnestOp` over whichever plan covers those
///     relations. The edge states
///     connectivity and ordering — the Unnest relation is reachable only
///     through it, so no plan holds that relation without the relations it
///     expands.
///
/// Invariants:
///   - Both pairs of sides are non-empty and disjoint.
///   - `leftEndpoints()` is a subset of `leftEligibility()`; likewise on the
///     right.
///   - Equi-join flavors: `leftKeys.size() == rightKeys.size()` and
///     is non-empty.
///   - Unnest flavor: `leftKeys` / `rightKeys` / `filter` empty;
///     `joinType == kInner`.
class JoinEdge {
 public:
  JoinEdge(
      RelationSet leftEndpoints,
      RelationSet rightEndpoints,
      RelationSet leftEligibility,
      RelationSet rightEligibility,
      ExprVector leftKeys,
      ExprVector rightKeys,
      ExprVector filter,
      velox::core::JoinType joinType,
      bool nullAware,
      bool nullAsValue,
      ColumnCP markColumn = nullptr)
      : leftEndpoints_{std::move(leftEndpoints)},
        rightEndpoints_{std::move(rightEndpoints)},
        leftEligibility_{std::move(leftEligibility)},
        rightEligibility_{std::move(rightEligibility)},
        leftKeys_{std::move(leftKeys)},
        rightKeys_{std::move(rightKeys)},
        filter_{std::move(filter)},
        joinType_{joinType},
        nullAware_{nullAware},
        nullAsValue_{nullAsValue},
        markColumn_{markColumn} {
    VELOX_CHECK(!leftEndpoints_.empty());
    VELOX_CHECK(!rightEndpoints_.empty());
    VELOX_CHECK(!leftEndpoints_.hasIntersection(rightEndpoints_));
    VELOX_CHECK(!leftEligibility_.empty());
    VELOX_CHECK(!rightEligibility_.empty());
    VELOX_CHECK(!leftEligibility_.hasIntersection(rightEligibility_));
    VELOX_CHECK(leftEndpoints_.isSubset(leftEligibility_));
    VELOX_CHECK(rightEndpoints_.isSubset(rightEligibility_));
    VELOX_CHECK_EQ(leftKeys_.size(), rightKeys_.size());
    VELOX_CHECK(
        !leftKeys_.empty(), "Equi-join edge must carry at least one key pair");
    VELOX_CHECK(
        filter_.empty() || joinType_ != velox::core::JoinType::kInner,
        "Inner-join edges must have empty filter; conjuncts live in the hypergraph's filter pool: {}",
        velox::core::JoinTypeName::toName(joinType_));
    VELOX_CHECK(
        (!nullAware_ && !nullAsValue_) ||
            joinType_ != velox::core::JoinType::kInner,
        "Inner-join edges must not carry nullAware or nullAsValue: {}",
        velox::core::JoinTypeName::toName(joinType_));
  }

  /// Constructs an unnest edge: the relations whose subtree feeds the
  /// Unnest on `left`, the Unnest relation on `right`. The expressions to
  /// unnest live on that relation's IR node; the edge only states where the
  /// expansion happens. Equi-key invariants don't apply; see the class doc.
  static JoinEdge unnest(RelationSet left, RelationSet right) {
    return JoinEdge{std::move(left), std::move(right)};
  }

  /// Relations in the left endpoint set.
  const RelationSet& leftEndpoints() const {
    return leftEndpoints_;
  }

  /// Relations in the right endpoint set.
  const RelationSet& rightEndpoints() const {
    return rightEndpoints_;
  }

  /// Relations on the left eligibility side of this hyperedge.
  const RelationSet& leftEligibility() const {
    return leftEligibility_;
  }

  /// Relations on the right eligibility side of this hyperedge.
  const RelationSet& rightEligibility() const {
    return rightEligibility_;
  }

  /// Returns the Total Eligibility Set of this edge.
  RelationSet totalEligibility() const {
    return RelationSet{leftEligibility_.bits() | rightEligibility_.bits()};
  }

  /// Equi-join keys aligned by position with `rightKeys()`.
  const ExprVector& leftKeys() const {
    return leftKeys_;
  }

  const ExprVector& rightKeys() const {
    return rightKeys_;
  }

  /// Bound non-equi conjuncts for outer-join edges. Empty for inner.
  const ExprVector& filter() const {
    return filter_;
  }

  /// True iff this is an unnest edge.
  bool isUnnest() const {
    return isUnnest_;
  }

  velox::core::JoinType joinType() const {
    return joinType_;
  }

  std::string_view joinTypeName() const {
    return velox::core::JoinTypeName::toName(joinType_);
  }

  bool nullAware() const {
    return nullAware_;
  }

  bool nullAsValue() const {
    return nullAsValue_;
  }

  /// The mark/exists column produced by a `kLeftSemiProject` join;
  /// `nullptr` for all other join types. Populated in Phase 2 when
  /// semi-project joins are admitted to clusters.
  ColumnCP markColumn() const {
    return markColumn_;
  }

 private:
  // Builds an unnest edge; see the `unnest` factory.
  JoinEdge(RelationSet left, RelationSet right)
      : leftEndpoints_{std::move(left)},
        rightEndpoints_{std::move(right)},
        leftEligibility_{leftEndpoints_},
        rightEligibility_{rightEndpoints_},
        joinType_{velox::core::JoinType::kInner},
        isUnnest_{true} {
    VELOX_CHECK(!leftEndpoints_.empty());
    VELOX_CHECK(!rightEndpoints_.empty());
    VELOX_CHECK(!leftEndpoints_.hasIntersection(rightEndpoints_));
  }

  RelationSet leftEndpoints_;
  RelationSet rightEndpoints_;
  RelationSet leftEligibility_;
  RelationSet rightEligibility_;
  ExprVector leftKeys_;
  ExprVector rightKeys_;
  ExprVector filter_;
  velox::core::JoinType joinType_;
  bool nullAware_{false};
  bool nullAsValue_{false};
  // The mark/exists column produced by a kLeftSemiProject join;
  // nullptr otherwise. Populated in Phase 2.
  ColumnCP markColumn_{nullptr};
  bool isUnnest_{false};
};

} // namespace facebook::axiom::optimizer::v2
