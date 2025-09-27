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

#include <velox/connectors/Connector.h>

namespace facebook::axiom::connector {

/// Enumerates splits. The table and partitions to cover are given to
/// ConnectorSplitManager.
class SplitSource {
 public:
  static constexpr uint32_t kUngroupedGroupId =
      std::numeric_limits<uint32_t>::max();

  /// Result of getSplits. Each split belongs to a group. A nullptr split for
  /// group means that there are no more splits for the group. In ungrouped
  /// execution, the group is always kUngroupedGroupId.
  struct SplitAndGroup {
    std::shared_ptr<velox::connector::ConnectorSplit> split;
    uint32_t group{kUngroupedGroupId};
  };

  virtual ~SplitSource() = default;

  /// Returns a set of splits that cover up to 'targetBytes' of data.
  virtual std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) = 0;
};

/// Options for split generation.
struct SplitOptions {
  /// Make no more than one split per file.
  bool wholeFile{false};

  /// If non-0, gives a minimum number of splits to generate, e.g. at least one
  /// for each driver of each worker.
  int32_t targetSplitCount{0};

  /// Target size of split.
  uint64_t fileBytesPerSplit{128ULL << 20U};
};

/// Describes a single partition of a TableLayout. A TableLayout has at least
/// one partition, even if it has no partitioning columns.
class PartitionHandle {
 public:
  virtual ~PartitionHandle() = default;
};

using PartitionHandlePtr = std::shared_ptr<const PartitionHandle>;

class ConnectorSplitManager {
 public:
  virtual ~ConnectorSplitManager() = default;

  /// Returns a list of all partitions that match the filters in
  /// 'tableHandle'. A non-partitioned table returns one partition.
  virtual std::vector<PartitionHandlePtr> listPartitions(
      const velox::connector::ConnectorTableHandlePtr& tableHandle) = 0;

  /// Returns a SplitSource that covers the contents of 'partitions'. The set of
  /// partitions is exposed separately so that the caller may process the
  /// partitions in a specific order or distribute them to specific nodes in a
  /// cluster.
  virtual std::shared_ptr<SplitSource> getSplitSource(
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions = {}) = 0;
};

} // namespace facebook::axiom::connector
