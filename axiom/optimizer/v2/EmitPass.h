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

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/ScanHandle.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer::v2 {

/// Lowers a tree-IR into the fragments of a `MultiFragmentPlan`.
class EmitPass {
 public:
  /// Output of `run`.
  struct Result {
    /// The plan's fragments, root fragment last.
    std::vector<ExecutableFragment> fragments;
    /// Set when the plan writes a table; empty otherwise.
    FinishWrite finishWrite;
  };

  /// Lowers the tree-IR rooted at 'root' into fragments, projecting to the
  /// user-visible layout described by 'outputColumns' and 'outputNames'
  /// (aligned 1:1). Each `ir.Exchange` becomes a fragment boundary (a producer
  /// fragment ending in `PartitionedOutput`, a consumer `Exchange`). For
  /// `options.numWorkers > 1` a final gather collects the distributed output
  /// into a single root fragment; for `numWorkers == 1` the result is one
  /// fragment. 'session' supplies the connector session for Scan handles;
  /// 'evaluator' folds filter constants; 'scanHandles' carries handles built by
  /// `EstimateLeafStatsPass`. Throws VELOX_NYI for unsupported node or
  /// expression types.
  static Result run(
      NodeCP root,
      const ColumnVector& outputColumns,
      const std::vector<std::string>& outputNames,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ScanHandleCache& scanHandles,
      const MultiFragmentPlan::Options& options);
};

} // namespace facebook::axiom::optimizer::v2
