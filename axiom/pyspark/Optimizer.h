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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/ToVelox.h"
#include "velox/common/memory/Memory.h"

namespace axiom::collagen {

/// Takes an Axiom logical plan as input and returns optimized Velox physical
/// plan fragments using Axiom (axiom/optimizer).
///
/// @param logicalPlan The input logical plan.
/// @param connectorId Id of the connector to be used to resolve metadata.
/// @param pool Memory pool to be used for allocating structures required by the
/// query optimizer.
::facebook::axiom::optimizer::PlanAndStats optimize(
    const ::facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
    const std::string& connectorId,
    ::facebook::velox::memory::MemoryPool* pool);
} // namespace axiom::collagen
