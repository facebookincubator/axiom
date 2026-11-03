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

#include <string>

#include <folly/container/F14Map.h>
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/NodePrediction.h"

namespace facebook::axiom::optimizer {

/// A map from PlanNodeId of an executable plan to a key for
/// recording the execution for use in cost model. The key is a
/// canonical summary of the node and its inputs.
using NodeHistoryMap = folly::F14FastMap<velox::core::PlanNodeId, std::string>;

/// Plan and specification for recording execution history amd planning ttime
/// predictions.
struct PlanAndStats {
  MultiFragmentPlanPtr plan;
  NodeHistoryMap history;
  NodePredictionMap prediction;
  FinishWrite finishWrite;

  /// Returns a string representation of the plan annotated with estimates from
  /// 'prediction'.
  std::string toString() const;
};

} // namespace facebook::axiom::optimizer
