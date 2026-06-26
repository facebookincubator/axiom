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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "folly/coro/Task.h"

namespace facebook::axiom::optimizer {

/// Calls `co_pushdownPlan` on each connector that implements
/// `isPushdownSupported()` and returns the list of roots. `VELOX_CHECK`s
/// pairwise disjointness. The plan is not rewritten.
folly::coro::Task<std::vector<connector::PushdownRoot>>
collectConnectorPushdownRoots(const logical_plan::LogicalPlanNode& plan);

} // namespace facebook::axiom::optimizer
