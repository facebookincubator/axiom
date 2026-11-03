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

#include <folly/container/F14Map.h>

#include "axiom/optimizer/ExprSimplifier.h"
#include "axiom/optimizer/Node.h"

namespace facebook::axiom::optimizer {

/// Derives the output columns of each node that are proven never NULL,
/// caching the answer per node. The derivation is conservative: a column can
/// be non-NULL without being in the set. A column is in the set when:
///
/// - Project: ExprSimplifier::isKnownNonNull proves its expression non-NULL
///   given the input's set.
/// - Aggregate: it is a grouping key that isKnownNonNull proves non-NULL given
///   the input's set.
/// - UnionAll: its column is in the set of every leg.
/// - Join: it is in the set of a side the join preserves, which an outer join
///   does not pad with NULLs. Under an inner join that does not match NULL
///   keys, it is also any column a key reads, unless the key can be non-NULL
///   over a NULL input, such as coalesce.
/// - A node that passes its input's rows through or only adds columns, such as
///   Filter, Limit, Window or Exchange: it is in the input's set.
/// - Any other node: never.
class NonNullOutput {
 public:
  explicit NonNullOutput(const ExprSimplifier& simplifier)
      : simplifier_(simplifier) {}

  /// Returns the output columns of 'node' proven never NULL.
  const PlanObjectSet& get(NodeCP node);

 private:
  // Adds to 'result' the columns of 'outputColumns' that 'source' holds.
  static void propagate(
      const PlanObjectSet& source,
      const ColumnVector& outputColumns,
      PlanObjectSet& result);

  // Caches 'result' as the answer for 'node' and returns it.
  const PlanObjectSet& save(NodeCP node, PlanObjectSet result);

  const ExprSimplifier& simplifier_;
  folly::F14NodeMap<NodeCP, PlanObjectSet> cache_;
};

} // namespace facebook::axiom::optimizer
