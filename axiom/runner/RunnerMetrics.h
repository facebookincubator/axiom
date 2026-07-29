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

namespace facebook::axiom::runner {

/// QueryRuntimeStats component id for runner metrics.
inline constexpr std::string_view kStatsComponent{"runner"};

/// Runtime-metric names recorded by the runner. Producers and consumers share
/// these constants so a rename cannot silently de-correlate a metric.
inline constexpr std::string_view kPermissionCheckWallNanos{
    "axiom-permissionCheckWallNanos"};
inline constexpr std::string_view kPermissionCheckCpuNanos{
    "axiom-permissionCheckCpuNanos"};

inline constexpr std::string_view kExecuteWallNanos{"axiom-executeWallNanos"};
inline constexpr std::string_view kExecuteCpuNanos{"axiom-executeCpuNanos"};

inline constexpr std::string_view kListPartitionsWallNanos{
    "axiom-listPartitionsWallNanos"};
inline constexpr std::string_view kListPartitionsCpuNanos{
    "axiom-listPartitionsCpuNanos"};
inline constexpr std::string_view kListPartitionsCount{
    "axiom-listPartitionsCount"};

inline constexpr std::string_view kGetSplitsWallNanos{
    "axiom-getSplitsWallNanos"};
inline constexpr std::string_view kGetSplitsCpuNanos{"axiom-getSplitsCpuNanos"};
inline constexpr std::string_view kGetSplitsCount{"axiom-getSplitsCount"};

} // namespace facebook::axiom::runner
