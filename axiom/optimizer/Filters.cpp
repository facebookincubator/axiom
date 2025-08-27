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

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer {

Cost filterCost(CPSpan<Expr> conjuncts) {
  return Cost();
}

ExprCP Optimization::combineLeftDeep(
    Name func,
    const ExprVector& set1,
    const ExprVector& set2) {
  ExprVector all = set1;
  all.insert(all.end(), set2.begin(), set2.end());
  std::sort(all.begin(), all.end(), [&](ExprCP left, ExprCP right) {
    return left->id() < right->id();
  });
  ExprCP result = all[0];
  for (auto i = 1; i < all.size(); ++i) {
    result = toGraph_.deduppedCall(
        func,
        result->value(),
        ExprVector{result, all[i]},
        result->functions() | all[i]->functions());
  }
  return result;
}

} // namespace facebook::velox::optimizer
