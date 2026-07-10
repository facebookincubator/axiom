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

/// Fuses and pushes limits and orders down the tree IR. In one top-down
/// traversal it threads a pending row bound and an order-preservation flag,
/// applying these rewrites:
///
///  - fuses a `Limit` over a `Sort` into a `TopN`;
///  - collapses a `Limit` over a `Limit` or `TopN` into a single bound;
///  - pushes a `Limit` through row-preserving nodes (`Project`,
///    `AssignUniqueId`, `MarkDistinct`) and copies it into each `UnionAll` leg;
///  - drops a bare `Sort` whose order no consumer observes.
class LimitAndOrderPass {
 public:
  /// Returns `root` with the rewrites above applied. Runs after decorrelate.
  static NodeCP run(NodeCP root, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
