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

#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Reads the scans of bucketed tables under a subtree one bucket-group at a
/// time, so the subtree's rows arrive partitioned by the tables' bucketing
/// without a shuffle. Each table's bucketing is coarsened to the worker count,
/// so the plan carries the group count it runs with.
///
/// A caller asks first and rebuilds only when the answer helps:
///
///     const Partitioning grouped =
///         GroupedRead::partitioning(input, numWorkers, builder);
///     if (grouped.coLocates(keys)) {
///       input = GroupedRead::rewrite(input, numWorkers, builder);
///     }
///
/// Each node decides whether it preserves a scan's bucketing. A join may keep
/// its probe input's placement, a union keeps placement shared by every input,
/// and a global exchange ends propagation. A driver exchange preserves task
/// placement and is rebuilt over the grouped input.
class GroupedRead {
 public:
  /// Global partitioning 'node' has when read grouped. Builds no nodes.
  static Partitioning
  partitioning(NodeCP node, int32_t numWorkers, Builder& builder);

  /// 'node' rebuilt with every scan of a bucketed table read grouped. Its
  /// partitioning is `partitioning(node, numWorkers, builder)`. Nodes are
  /// interned, so only the regrouped scans and their ancestors are new.
  static NodeCP rewrite(NodeCP node, int32_t numWorkers, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
