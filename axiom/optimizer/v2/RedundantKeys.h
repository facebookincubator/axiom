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

#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer::v2 {

/// Drops the partition and sort keys that change no result.
///
/// A literal partition key puts every row in one partition, and a repeated one
/// partitions no further. A sort key that is literal or repeats a partition key
/// or an earlier sort key takes one value among the rows the earlier keys tie,
/// so it orders nothing.
///
/// The rule covers partition and sort keys only. A grouping key cannot always
/// be dropped: `GROUP BY 1` over empty input returns no rows, while a global
/// aggregate returns one. A RANGE frame with a boundary value compares it
/// against the ORDER BY key, so a window with such a frame keeps that key.
class RedundantKeys {
 public:
  /// Drops the literal and repeated keys of 'partitionKeys'.
  static void dropFromPartitionKeys(ExprVector& partitionKeys);

  /// Drops the keys of 'sortKeys' that are literal or repeat a key of
  /// 'partitionKeys' or an earlier sort key, keeping 'sortTypes' aligned.
  static void dropFromSortKeys(
      const ExprVector& partitionKeys,
      ExprVector& sortKeys,
      OrderTypeVector& sortTypes);
};

} // namespace facebook::axiom::optimizer::v2
