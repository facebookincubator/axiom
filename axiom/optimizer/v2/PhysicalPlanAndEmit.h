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
#include <vector>

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/EmitPass.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/core/ExpressionEvaluator.h"

namespace facebook::axiom::optimizer::v2 {

/// Runs physical planning over 'root' and lowers the result to Velox nodes.
/// 'outputColumns' / 'outputNames' pin the emitted output layout.
EmitPass::Result physicalPlanAndEmit(
    NodeCP root,
    const ColumnVector& outputColumns,
    const std::vector<std::string>& outputNames,
    Builder& builder,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    const MultiFragmentPlan::Options& options);

} // namespace facebook::axiom::optimizer::v2
