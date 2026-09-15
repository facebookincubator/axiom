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

/// The ids an application files a component's metrics under, and the names of
/// the phases it times around its own calls. A component records unqualified
/// names into the writer it is handed and never names itself, so this
/// vocabulary belongs to axel and the CLI rather than to the library.
struct ComponentMetrics {
  /// Identifies what the optimizer records, as against the phases an
  /// application times around calls into it.
  static constexpr std::string_view kOptimizer{"optimizer"};

  /// Identifies what the runner records, on the same split as kOptimizer.
  static constexpr std::string_view kRunner{"runner"};

  /// Identifies connector metrics, keyed connector/<connectorId>/<name> so a
  /// catalog cannot take a component's name.
  static constexpr std::string_view kConnector{"connector"};

  /// Identifies the CLI itself, for the phases it times
  /// around calls it makes into the parser, optimizer and runner.
  static constexpr std::string_view kCli{"axiomCli"};

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
  /// concurrently with it, so the runner's getSplits timing overlaps this
  /// rather than adding to it.
  static constexpr std::string_view kExecuteWallNanos{"executeWallNanos"};

  /// The sum of Velox driver CPU across the query's tasks, read locally at the
  /// end of execution. Recorded only where the drivers run in process: an
  /// application that dispatches them to workers measures something else and
  /// names it for itself.
  static constexpr std::string_view kExecuteCpuNanos{"executeCpuNanos"};
};

} // namespace facebook::axiom
