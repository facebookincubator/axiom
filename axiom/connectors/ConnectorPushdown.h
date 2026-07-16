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

#include "axiom/connectors/ConnectorMetadata.h"
#include "folly/coro/Task.h"

#include <vector>

namespace facebook::axiom::optimizer::v2 {
class Node;
using NodeCP = const Node*;
} // namespace facebook::axiom::optimizer::v2

namespace facebook::axiom::connector {

/// One disjoint root the connector has agreed to execute natively.
/// The optimizer materialises a scan from `table` in place of
/// evaluating `root`; the optimized plan tree is rewritten to swap
/// the subtree for a synthetic `Scan`. `table` carries the connector's
/// handle, column statistics, and row-count estimate via the standard
/// `connector::Table` channel.
///
/// EXPERIMENTAL. Consumer-column contract: the pass reuses the
/// subtree's `outputColumns` as the synthetic `Scan`'s outputs, so
/// consumers above the pushdown keep their existing `ColumnCP`
/// references and don't need to be rewritten. For that to work,
/// `table` must expose, for every column in `root->outputColumns()`,
/// a column with the same name and an equivalent type; column order
/// is not significant and extra columns in `table` are ignored.
struct PushdownRoot {
  /// Requires both fields non-null; a `PushdownRoot` value always names a
  /// concrete subtree replacement.
  PushdownRoot(optimizer::v2::NodeCP root, TablePtr table)
      : root{root}, table{std::move(table)} {
    VELOX_CHECK_NOT_NULL(this->root, "PushdownRoot::root must be non-null");
    VELOX_CHECK_NOT_NULL(this->table, "PushdownRoot::table must be non-null");
  }

  /// Non-owning pointer into the optimized plan tree being processed.
  /// Valid for the duration of optimization.
  optimizer::v2::NodeCP root;

  /// Virtual table representing the pushed-down subtree's output.
  /// Owned by the optimizer for the duration of optimization.
  TablePtr table;
};

/// EXPERIMENTAL. Opts a `ConnectorMetadata` implementation into
/// optimizer pushdown. The optimizer offers each maximal subtree whose
/// leaf `Scan`s all belong to this connector, and the connector returns
/// the disjoint roots it will execute natively — each rooted at a
/// virtual `Table` it constructs on the spot to describe the pushed
/// computation. The optimizer then rewrites the plan to replace every
/// named subtree with a synthetic `Scan` over that virtual table (see
/// `PushdownRoot` for the consumer-column contract the virtual table
/// must uphold).
///
/// Implement this when the connector can execute more than raw table
/// scans — pre-aggregations, filters, joins pushed to a remote engine,
/// materialised views over the offered subtree, etc. Leave unimplemented
/// if the connector only vends raw scans; the optimizer discovers
/// implementers by `dynamic_cast<const ConnectorPushdown*>` on the
/// `ConnectorMetadata` pointer and silently skips those that aren't.
///
/// Invariant: `ConnectorPushdown` must be inherited on the SAME object
/// as `ConnectorMetadata`. Putting it on a sibling helper makes the
/// `dynamic_cast` return null and the connector silently opts out.
///
/// Example:
///
///     class MyMetadata : public ConnectorMetadata,
///                        public ConnectorPushdown {
///      public:
///       folly::coro::Task<std::vector<PushdownRoot>> co_pushdown(
///           const optimizer::v2::Node& subtree) const override;
///     };
class ConnectorPushdown {
 public:
  virtual ~ConnectorPushdown() = default;

  /// Returns the disjoint roots this connector will execute natively.
  /// Each returned root must be within `subtree` and must not be a bare
  /// `Scan` (the scan already runs on the connector). Within one response,
  /// roots must be pairwise disjoint: no root may be an ancestor or
  /// descendant of another, and no root may appear twice.
  virtual folly::coro::Task<std::vector<PushdownRoot>> co_pushdown(
      const optimizer::v2::Node& subtree) const = 0;
};

} // namespace facebook::axiom::connector
