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

#include <string_view>

namespace axiom::sql::presto {

/// QueryRuntimeStats component id for parser metrics.
inline constexpr std::string_view kStatsComponent{"parser"};

/// Runtime-metric names recorded around parsing. Producers and consumers share
/// these constants so a rename cannot silently de-correlate a metric.
inline constexpr std::string_view kParseWallNanos{"axiom-parseWallNanos"};
inline constexpr std::string_view kParseCpuNanos{"axiom-parseCpuNanos"};

} // namespace axiom::sql::presto
