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
#include <optional>
#include <utility>

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/RelationSet.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

/// An atomic join input identified by `id`, wrapping a tree IR node
/// whose rows feed the join.
///
/// Kinds of nodes:
///   - Scan, Values: leaf nodes with no inputs.
///   - Unnest: a deferred operator registered as its own relation;
///     the cross-join-unnest edge built by `HypergraphBuilder`
///     supplies connectivity to the relations whose subtree feeds
///     the Unnest's input.
///   - Root of an already-planned subtree past a barrier (Sort,
///     Limit, Window, ...).
///
/// Invariants:
///   - `id` in `[0, RelationSet::kMaxRelations)`.
///   - `node` is non-null.
class Relation {
 public:
  Relation(
      int8_t id,
      NodeCP node,
      std::optional<float> cardinality,
      PlanObjectSet columns)
      : id_{id},
        node_{node},
        cardinality_{cardinality},
        columns_{std::move(columns)} {
    VELOX_CHECK_GE(id_, 0);
    VELOX_CHECK_LT(id_, RelationSet::kMaxRelations);

    VELOX_CHECK_NOT_NULL(node_);
  }

  int8_t id() const {
    return id_;
  }

  NodeCP node() const {
    return node_;
  }

  /// Estimated row count, or nullopt when unknown.
  std::optional<float> cardinality() const {
    return cardinality_;
  }

  const PlanObjectSet& columns() const {
    return columns_;
  }

 private:
  int8_t id_;
  NodeCP node_;
  std::optional<float> cardinality_;
  PlanObjectSet columns_;
};

} // namespace facebook::axiom::optimizer::v2
