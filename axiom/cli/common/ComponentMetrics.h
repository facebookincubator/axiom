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

namespace facebook::axiom {

/// The components an application provisions writers for, and the names of the
/// timings it records around calls into them.
struct ComponentMetrics {
  /// Identifies a component.
  static constexpr std::string_view kOptimizerComponent{"optimizer"};
  static constexpr std::string_view kRunnerComponent{"runner"};

  /// Identifies the CLI itself, for the phases it times
  /// around calls it makes into the components above.
  static constexpr std::string_view kCliComponent{"axiomCli"};

  /// One sample per parsed statement.
  static constexpr std::string_view kParseWallNanos{"parseWallNanos"};

  /// One sample per query, covering the authorization check between parsing and
  /// optimization.
  static constexpr std::string_view kPermissionCheckWallNanos{
      "permissionCheckWallNanos"};

  /// One sample per optimizer invocation, covering it end to end. Nested
  /// optimizer timings do not add up to it.
  static constexpr std::string_view kOptimizeWallNanos{"optimizeWallNanos"};
  /// Local planning work only. The invocation also waits on connector calls for
  /// statistics, so wall exceeds CPU by that wait rather than by scheduling.
  static constexpr std::string_view kOptimizeCpuNanos{"optimizeCpuNanos"};

  /// One sample per query, covering plan execution. Split enumeration runs
  /// concurrently with it, so RunnerMetrics::kGetSplitsWallNanos overlaps this
  /// rather than adding to it.
  static constexpr std::string_view kExecuteWallNanos{"executeWallNanos"};
};

} // namespace facebook::axiom
