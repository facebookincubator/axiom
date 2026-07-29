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

namespace facebook::axiom::optimizer {

/// QueryRuntimeStats component id for optimizer metrics.
inline constexpr std::string_view kStatsComponent{"optimizer"};

/// Runtime-metric names recorded by the optimizer. Producers and consumers
/// share these constants so a rename cannot silently de-correlate a metric.
inline constexpr std::string_view kOptimizeWallNanos{"axiom-optimizeWallNanos"};
inline constexpr std::string_view kOptimizeCpuNanos{"axiom-optimizeCpuNanos"};
inline constexpr std::string_view kOptimizeToGraphWallNanos{
    "axiom-optimizeToGraphWallNanos"};
inline constexpr std::string_view kOptimizeToGraphCpuNanos{
    "axiom-optimizeToGraphCpuNanos"};
inline constexpr std::string_view kOptimizeBestPlanWallNanos{
    "axiom-optimizeBestPlanWallNanos"};
inline constexpr std::string_view kOptimizeBestPlanCpuNanos{
    "axiom-optimizeBestPlanCpuNanos"};
inline constexpr std::string_view kOptimizeToVeloxWallNanos{
    "axiom-optimizeToVeloxWallNanos"};
inline constexpr std::string_view kOptimizeToVeloxCpuNanos{
    "axiom-optimizeToVeloxCpuNanos"};

inline constexpr std::string_view kFindTableWallNanos{
    "axiom-findTableWallNanos"};
inline constexpr std::string_view kFindTableCpuNanos{"axiom-findTableCpuNanos"};

inline constexpr std::string_view kEstimateStatsWallNanos{
    "axiom-estimateStatsWallNanos"};
inline constexpr std::string_view kEstimateStatsCpuNanos{
    "axiom-estimateStatsCpuNanos"};

} // namespace facebook::axiom::optimizer
