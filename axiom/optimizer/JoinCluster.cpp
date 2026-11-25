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

#include "axiom/optimizer/JoinCluster.h"

namespace facebook::axiom::optimizer {

JoinCluster::FreshOutputs::FreshOutputs(const JoinCluster& cluster) {
  for (JoinCP join : cluster.joins) {
    for (size_t i = 0; i < join->outputColumns().size(); ++i) {
      if (join->outputColumns()[i] != join->sourceColumns()[i]) {
        copies_.emplace(
            join->outputColumns()[i],
            std::pair{join, join->sourceColumns()[i]});
      }
    }
  }
}

ColumnCP JoinCluster::FreshOutputs::source(
    ColumnCP column,
    folly::FunctionRef<void(JoinCP)> visit) const {
  for (auto it = copies_.find(column); it != copies_.end();
       it = copies_.find(column)) {
    visit(it->second.first);
    column = it->second.second;
  }
  return column;
}

} // namespace facebook::axiom::optimizer
