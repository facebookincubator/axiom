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

#include "axiom/optimizer/v2/RedundantKeys.h"

#include <folly/container/F14Set.h>

namespace facebook::axiom::optimizer::v2 {

// static
void RedundantKeys::dropFromPartitionKeys(ExprVector& partitionKeys) {
  folly::F14FastSet<ExprCP> seen;
  std::erase_if(partitionKeys, [&](ExprCP key) {
    return key->is(PlanType::kLiteralExpr) || !seen.insert(key).second;
  });
}

// static
void RedundantKeys::dropFromSortKeys(
    const ExprVector& partitionKeys,
    ExprVector& sortKeys,
    OrderTypeVector& sortTypes) {
  VELOX_CHECK_EQ(sortKeys.size(), sortTypes.size());
  folly::F14FastSet<ExprCP> seen(partitionKeys.begin(), partitionKeys.end());
  size_t numKept = 0;
  for (size_t i = 0; i < sortKeys.size(); ++i) {
    if (sortKeys[i]->is(PlanType::kLiteralExpr) ||
        !seen.insert(sortKeys[i]).second) {
      continue;
    }
    sortKeys[numKept] = sortKeys[i];
    sortTypes[numKept] = sortTypes[i];
    ++numKept;
  }
  sortKeys.resize(numKept);
  sortTypes.resize(numKept);
}

} // namespace facebook::axiom::optimizer::v2
