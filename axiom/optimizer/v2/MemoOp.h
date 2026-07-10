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

#include <cstdint>
#include <vector>

#include "axiom/common/Enums.h"
#include "axiom/optimizer/v2/Cost.h"
#include "axiom/optimizer/v2/PhysicalProperties.h"
#include "axiom/optimizer/v2/RelationSet.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

enum class MemoOpKind : uint8_t {
  kLeaf,
  kJoin,
  kExchange,
};

AXIOM_DECLARE_ENUM_NAME(MemoOpKind);

/// Base class for DPhyp's slim memo entries. Each entry records
/// the choices DPhyp made for one subplan — kind, children, edge,
/// join type — not full IR data, which lives on the hypergraph and
/// the original tree IR.
class MemoOp {
 public:
  /// Cost of this subplan. Set once by the cost model immediately after
  /// construction and treated as immutable thereafter.
  Cost cost;

  virtual ~MemoOp() = default;

  MemoOpKind kind() const {
    return kind_;
  }

  bool is(MemoOpKind kind) const {
    return kind_ == kind;
  }

  /// Downcast to a known subclass. Caller must have checked `is(...)`.
  template <typename T>
  const T* as() const {
    return static_cast<const T*>(this);
  }

  /// Set of relations covered by this subplan.
  virtual RelationSet cover() const = 0;

  /// Global (across-tasks) partitioning of this subplan's output — the
  /// dimension the property-aware memo keeps non-dominated alternatives on.
  /// `kUnspecified` unless an `ExchangeOp` establishes a partitioning, or a
  /// `JoinOp` over inputs co-partitioned on its keys inherits one. Set at
  /// construction; every op leaves it unspecified at `numWorkers=1`, so the
  /// memo behaves as single-best.
  const Partitioning& outputPartitioning() const {
    return outputPartitioning_;
  }

 protected:
  MemoOp(Cost cost, MemoOpKind kind, Partitioning outputPartitioning = {})
      : cost{cost},
        kind_{kind},
        outputPartitioning_{std::move(outputPartitioning)} {}

 private:
  const MemoOpKind kind_;
  const Partitioning outputPartitioning_;
};

using MemoOpCP = const MemoOp*;

/// Leaf entry: a single relation. The IR node lives at
/// `graph.relation(relationId).node()`.
class LeafOp : public MemoOp {
 public:
  const int8_t relationId;

  LeafOp(Cost cost, int8_t id, Partitioning outputPartitioning = {})
      : MemoOp{cost, MemoOpKind::kLeaf, std::move(outputPartitioning)},
        relationId{id} {}

  RelationSet cover() const override {
    return RelationSet::singleton(relationId);
  }
};

/// Join entry: combines two child subplans via the predicate at
/// `graph.edges()[edgeIndex]`. `joinType` records the join
/// semantic chosen.
class JoinOp : public MemoOp {
 public:
  MemoOpCP const left;
  MemoOpCP const right;
  const size_t edgeIndex;
  const velox::core::JoinType joinType;

  /// True when this candidate is an antijoin played in its reversed
  /// (build-on-the-preserved-side) orientation, lowered at emit to a
  /// kRightSemiProject + Filter(not mark) + Project. A null-aware antijoin
  /// carrying an extra filter has no valid reversed form (Velox forbids a
  /// null-aware kRightSemiProject with an internal filter) and stays native.
  const bool reversedAnti;

  /// Additional inner equi-join edges that also cross this join's partition
  /// and are applied together with `edgeIndex`. Standard DPhyp conjoins all
  /// predicates connecting the two subgraphs; with a cyclic join graph more
  /// than one edge can cross a single partition. Empty for the common
  /// single-edge join; every entry is a `kInner` equi-edge.
  const std::vector<size_t> extraEdges;

  JoinOp(
      Cost cost,
      MemoOpCP leftChild,
      MemoOpCP rightChild,
      size_t edge,
      velox::core::JoinType type,
      bool reversedAnti = false,
      std::vector<size_t> extraEdges = {},
      Partitioning outputPartitioning = {});

  RelationSet cover() const override {
    return cover_;
  }

 private:
  const RelationSet cover_;
};

/// Memo entry for a remote exchange (repartition) wrapping `input`, lowered to
/// an `ir.Exchange` by `JoinTreeEmitter`. Its `outputPartitioning()` is the
/// target partitioning; its cost is the child's plus the shuffle, computed by
/// the candidate generator and passed in at construction.
class ExchangeOp : public MemoOp {
 public:
  MemoOpCP const input;

  ExchangeOp(Cost cost, MemoOpCP inputChild, Partitioning partitioning)
      : MemoOp{cost, MemoOpKind::kExchange, std::move(partitioning)},
        input{inputChild} {}

  RelationSet cover() const override {
    return input->cover();
  }
};

using ExchangeOpCP = const ExchangeOp*;

} // namespace facebook::axiom::optimizer::v2
