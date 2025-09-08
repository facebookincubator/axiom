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

#include "axiom/logical_plan/Expr.h"

namespace facebook::velox::optimizer::test {

/// Add map_row_from_map, padded_make_row_from_map, and make_named_row
/// functions to the Optimizer's registry.
void registerDfFunctions();

/// Pass to logical_plan::PlanBuilder to allow it to resolve types for
/// make-row functions.
logical_plan::ExprPtr resolveDfFunction(
    const std::string& name,
    const std::vector<logical_plan::ExprPtr>& args);

} // namespace facebook::velox::optimizer::test
