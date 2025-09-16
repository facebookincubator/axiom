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

#include "axiom/runner/MultiFragmentPlan.h"

namespace facebook::axiom::runner {

std::string MultiFragmentPlan::toString(
    bool detailed,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        const std::string& indentation,
        std::ostream& out)>& addContext) const {
  // Map task prefix to fragment index.
  folly::F14FastMap<std::string, int32_t> taskPrefixToIndex;
  for (auto i = 0; i < fragments_.size(); ++i) {
    taskPrefixToIndex[fragments_[i].taskPrefix] = i;
  }

  // Map plan node to the index of the input fragment.
  folly::F14FastMap<velox::core::PlanNodeId, int32_t> planNodeToIndex;
  for (const auto& fragment : fragments_) {
    for (const auto& input : fragment.inputStages) {
      planNodeToIndex[input.consumerNodeId] =
          taskPrefixToIndex[input.producerTaskPrefix];
    }
  }

  std::stringstream out;
  for (auto i = 0; i < fragments_.size(); ++i) {
    const auto& fragment = fragments_[i];
    out << fmt::format(
               "Fragment {}: {} numWorkers={}:",
               i,
               fragment.taskPrefix,
               fragment.width)
        << std::endl;

    out << fragment.fragment.planNode->toString(
               detailed,
               true,
               [&](const velox::core::PlanNodeId& planNodeId,
                   const std::string& indentation,
                   std::ostream& stream) {
                 if (addContext != nullptr) {
                   addContext(planNodeId, indentation, stream);
                 }
                 auto it = planNodeToIndex.find(planNodeId);
                 if (it != planNodeToIndex.end()) {
                   stream << indentation << "Input Fragment " << it->second
                          << std::endl;
                 }
               })
        << std::endl;
  }
  return out.str();
}

std::string MultiFragmentPlan::toSummaryString(
    velox::core::PlanSummaryOptions options) const {
  std::stringstream out;
  for (auto i = 0; i < fragments_.size(); ++i) {
    const auto& fragment = fragments_[i];
    out << fmt::format(
               "Fragment {}: {} numWorkers={}:",
               i,
               fragment.taskPrefix,
               fragment.width)
        << std::endl;
    out << fragment.fragment.planNode->toSummaryString(options) << std::endl;
    if (!fragment.inputStages.empty()) {
      out << "Inputs: ";
      for (const auto& input : fragment.inputStages) {
        out << fmt::format(
            " {} <- {} ", input.consumerNodeId, input.producerTaskPrefix);
      }
      out << std::endl;
    }
  }
  return out.str();
}

} // namespace facebook::axiom::runner
