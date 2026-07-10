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
inline bool canBroadcastBuild(velox::core::JoinType joinType) {
  using velox::core::JoinType;
  return joinType == JoinType::kInner || joinType == JoinType::kLeft ||
      joinType == JoinType::kLeftSemiFilter ||
      joinType == JoinType::kLeftSemiProject || joinType == JoinType::kAnti;
}

/// A hyperedge in a join cluster's hypergraph. Three flavors:
///
///   - Inner equi-join. `filter` is empty; non-equi conjuncts live in
///     `JoinHypergraph::filterConjuncts` so DPhyp can place them at
///     the lowest eligible join. `nullAware` and `nullAsValue` are
///     unused.
///   - Outer equi-join (LEFT / RIGHT / FULL). `filter` carries the
///     ON-clause non-equi conjuncts bound to this edge — moving them
///     above would let null-padded rows pass through, changing
///     semantics. `nullAware` and `nullAsValue` carry Velox null
///     semantics.
///   - Directed cross-join-unnest. Built by the unnest constructor
///     overload. `left` covers the relations whose subtree feeds the
///     Unnest; `right` is the single Unnest relation. `unnestExprs`
///     carries the ARRAY/MAP expressions to unnest. No equi-keys, no
///     filter. `isUnnest()` discriminates.
///
/// Invariants:
///   - `left` and `right` are non-empty and disjoint.
///   - Equi-join flavors: `leftKeys.size() == rightKeys.size()` and
///     is non-empty.
///   - Unnest flavor: `unnestExprs` non-empty; `leftKeys` /
///     `rightKeys` / `filter` empty; `joinType == kInner`.
class JoinEdge {
 public:
  JoinEdge(
      RelationSet left,
      RelationSet right,
      ExprVector leftKeys,
      ExprVector rightKeys,
      ExprVector filter,
      velox::core::JoinType joinType,
      bool nullAware,
      bool nullAsValue,
      ColumnCP markColumn = nullptr)
      : left_{std::move(left)},
        right_{std::move(right)},
        leftKeys_{std::move(leftKeys)},
        rightKeys_{std::move(rightKeys)},
        filter_{std::move(filter)},
        joinType_{joinType},
        nullAware_{nullAware},
        nullAsValue_{nullAsValue},
        markColumn_{markColumn} {
    VELOX_CHECK(!left_.empty());
    VELOX_CHECK(!right_.empty());
    VELOX_CHECK(!left_.hasIntersection(right_));
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

  /// Constructs a directed cross-join-unnest edge: input relations on
  /// `left`, the Unnest relation on `right`, the ARRAY/MAP expressions
  /// to unnest in `unnestExprs`. Equi-key invariants don't apply; see
  /// the class doc.
  JoinEdge(RelationSet left, RelationSet right, ExprVector unnestExprs)
      : left_{std::move(left)},
        right_{std::move(right)},
        joinType_{velox::core::JoinType::kInner},
        unnestExprs_{std::move(unnestExprs)} {
    VELOX_CHECK(!left_.empty());
    VELOX_CHECK(!right_.empty());
    VELOX_CHECK(!left_.hasIntersection(right_));
    VELOX_CHECK(
        !unnestExprs_.empty(),
        "Cross-join-unnest edge must carry at least one unnest expression");
  }

  const RelationSet& left() const {
    return left_;
  }

  const RelationSet& right() const {
    return right_;
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

  /// ARRAY/MAP expressions to unnest. Non-empty iff this is a
  /// cross-join-unnest edge.
  const ExprVector& unnestExprs() const {
    return unnestExprs_;
  }

  /// True iff this is a directed cross-join-unnest edge.
  bool isUnnest() const {
    return !unnestExprs_.empty();
  }

  velox::core::JoinType joinType() const {
    return joinType_;
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
  RelationSet left_;
  RelationSet right_;
  ExprVector leftKeys_;
  ExprVector rightKeys_;
  ExprVector filter_;
  velox::core::JoinType joinType_;
  bool nullAware_{false};
  bool nullAsValue_{false};
  // The mark/exists column produced by a kLeftSemiProject join;
  // nullptr otherwise. Populated in Phase 2.
  ColumnCP markColumn_{nullptr};
  ExprVector unnestExprs_;
};

} // namespace facebook::axiom::optimizer::v2
