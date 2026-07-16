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
#include "folly/coro/Task.h"

namespace facebook::axiom::optimizer {
class Schema;
} // namespace facebook::axiom::optimizer

namespace facebook::axiom::optimizer::v2 {

/// For each opted-in connector, offers it each maximal subtree whose leaf
/// `Scan`s all belong to it and replaces the returned root with a synthetic
/// `Scan` over the connector's virtual `Table`. A connector opts in by
/// additional inheritance from `ConnectorPushdown`, which the pass finds
/// via `dynamic_cast` on the `ConnectorMetadata` pointer.
///
/// Example: given `Filter(x > 0) -> Scan(t)`, if `t`'s connector returns
/// the `Filter` as a `PushdownRoot` backed by virtual table `virt_t`, the
/// pass rewrites the plan to `Scan(virt_t)`.
///
/// Runs after `PushdownAndPrunePass` and before `EstimateLeafStatsPass`.
/// See `PushdownRoot` for the column-identity contract on the virtual
/// `Table`.
class ConnectorPushdownPass {
 public:
  static folly::coro::Task<NodeCP>
  run(NodeCP root, Builder& builder, const Schema& schema);
};

} // namespace facebook::axiom::optimizer::v2
