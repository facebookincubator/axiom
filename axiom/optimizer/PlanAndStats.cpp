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
#include "axiom/optimizer/PlanAndStats.h"

namespace facebook::axiom::optimizer {

std::string PlanAndStats::toString() const {
  if (plan->fragments().empty()) {
    // The connector carries out the write itself, so the handle's description
    // is all there is to show.
    return finishWrite.toString();
  }

  return plan->toString(
      true,
      [&](const velox::core::PlanNodeId& planNodeId,
          std::string_view indentation,
          std::ostream& out) {
        auto it = prediction.find(planNodeId);
        if (it != prediction.end()) {
          out << indentation << "Estimate: " << it->second.toString()
              << std::endl;
        }
      });
}

} // namespace facebook::axiom::optimizer
