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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/EmitPass.h"

namespace facebook::axiom::optimizer::v2 {

/// Runs the physical-planning and emit passes over 'root' and returns the emit
/// result: PlanPhysical -> PrecomputeProjections -> Emit. PlanPhysical lowers
/// DISTINCT aggregates as part of selecting their physical execution.
/// Shared by full optimization and the translate-time constant fold so the two
/// cannot drift. 'outputColumns' / 'outputNames' pin the emitted output layout.
EmitPass::Result physicalPlanAndEmit(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames,
    Builder& builder,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options);

} // namespace facebook::axiom::optimizer::v2
