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

#include "axiom/optimizer/v2/PhysicalPlanAndEmit.h"

#include "axiom/optimizer/v2/PlanPhysicalPass.h"
#include "axiom/optimizer/v2/PrecomputeProjectionsPass.h"

namespace facebook::axiom::optimizer::v2 {

EmitPass::Result physicalPlanAndEmit(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames,
    Builder& builder,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options) {
  NodeCP physicalPlanned = PlanPhysicalPass::run(
      root, builder, session.options(), options.numWorkers, options.numDrivers);
  NodeCP precomputed = PrecomputeProjectionsPass::run(physicalPlanned, builder);
  return EmitPass::run(
      precomputed, outputColumns, outputNames, session, evaluator, options);
}

} // namespace facebook::axiom::optimizer::v2
