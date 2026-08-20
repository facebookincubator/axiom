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

/// The names of the split-enumeration timings, recorded by whichever component
/// drives the enumeration.
struct RunnerMetrics {
  /// One sample per scan node, covering the connector call that enumerates the
  /// partitions to read. Precedes execution rather than nesting in it.
  static constexpr std::string_view kListPartitionsWallNanos{
      "listPartitionsWallNanos"};
  /// Sum is the total number of partitions listed; count is the number of scan
  /// nodes that listed any.
  static constexpr std::string_view kListPartitionsCount{"listPartitionsCount"};

  /// Covers pulling splits out of a split source. Overlaps the execute phase
  /// timing, since enumeration runs alongside the tasks it feeds. Sampled once
  /// per enumeration loop by a component that drains the source in one go, and
  /// once per batch by one that pulls incrementally, so compare sum across
  /// recorders, not count.
  static constexpr std::string_view kGetSplitsWallNanos{"getSplitsWallNanos"};
  /// Sum is the total number of splits produced. Count follows
  /// kGetSplitsWallNanos: per enumeration loop or per batch, depending on the
  /// recorder.
  static constexpr std::string_view kGetSplitsCount{"getSplitsCount"};
};

} // namespace facebook::axiom
