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

#include <optional>
#include <string_view>

#include "axiom/logical_plan/Expr.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Maps a Presto function name (case-insensitive) to the metadata aggregate
/// kind it exposes, or std::nullopt if the name is not a metadata aggregate.
/// This is the Presto dialect's surface for the optimizer's
/// SpecialAggregateKind contract: the dialect chooses which names expose which
/// kinds.
std::optional<lp::SpecialAggregateKind> specialAggregateKind(
    std::string_view name);

} // namespace axiom::sql::presto
