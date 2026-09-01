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

#include <folly/container/F14Set.h>
#include <folly/coro/Task.h>
#include <velox/connectors/Connector.h>
#include <optional>
#include <utility>
#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

class PartitionType;

/// Wraps a Velox ConnectorSplit with optional placement metadata. For grouped
/// execution, groupId identifies the split's bucket (split group) and
/// partitionId the task it must run on; the connector derives partitionId from
/// groupId via the partitioning function. affinityId is a soft cross-query
/// affinity hint: schedulers prefer to route splits with the same affinityId
/// to the same task, but may choose another task for load balancing.
struct Split {
  /// The underlying Velox connector split.
  std::shared_ptr<velox::connector::ConnectorSplit> connectorSplit;

  /// Split group (bucket) this split belongs to, in [0, numGroups). Splits
  /// sharing a groupId form one group processed together on the worker. Absent
  /// for non-grouped scans.
  std::optional<int32_t> groupId{std::nullopt};

  /// Task (partition) this split must be routed to, in [0, width). Present for
  /// grouped execution. Absent means the runtime chooses the task.
  std::optional<int32_t> partitionId{std::nullopt};

  /// Stable connector-generated affinity ID for split affinity. Connectors
  /// that support split affinity must generate the same ID for repeated reads
  /// of the same physical split input. Different physical splits may share an
  /// affinityId; such collisions only cause them to share a preferred task. If
  /// unset, no affinityId-based placement hint is available for this split.
  std::optional<int64_t> affinityId{std::nullopt};
};

/// A batch of splits returned by SplitSource::co_getSplits.
struct SplitBatch {
  std::vector<Split> splits;

  /// True when there are no more splits to return.
  bool noMoreSplits{false};

  /// Groups (buckets) for which no further splits will be returned. Lets the
  /// runtime finalize a group as soon as its splits are exhausted rather than
  /// waiting for the whole source to drain.
  folly::F14FastSet<int32_t> noMoreSplitsForGroupId;
};

/// Enumerates splits. The table and partitions to cover are given to
/// ConnectorSplitManager. Callers must invoke co_close() when done consuming
/// splits, before the SplitSource is destroyed. co_close() releases background
/// resources and waits for any in-flight work to complete.
class SplitSource {
 public:
  SplitSource() = default;
  SplitSource(const SplitSource&) = delete;
  SplitSource& operator=(const SplitSource&) = delete;
  SplitSource(SplitSource&&) = delete;
  SplitSource& operator=(SplitSource&&) = delete;

  virtual ~SplitSource() {
    VELOX_CHECK(
        closed_, "co_close() must be called before destroying SplitSource");
  }

  /// Returns up to 'maxSplitCount' splits, or fewer if the source is
  /// exhausted. Sets SplitBatch::noMoreSplits when no further splits remain.
  virtual folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount) = 0;

  /// Close the split source and interrupt all background activity.
  /// Must be called exactly once before the SplitSource is destroyed.
  folly::coro::Task<void> co_close() noexcept {
    if (closed_) {
      co_return;
    }
    co_await co_closeImpl();
    closed_ = true;
  }

 protected:
  virtual folly::coro::Task<void> co_closeImpl() noexcept {
    co_return;
  }

 private:
  bool closed_{false};
};

/// Describes a single partition of a TableLayout. A TableLayout has at least
/// one partition, even if it has no partitioning columns.
class PartitionHandle {
 public:
  virtual ~PartitionHandle() = default;
};

using PartitionHandlePtr = std::shared_ptr<const PartitionHandle>;

/// The exact partitions selected for one scan and the storage partitioning the
/// connector guarantees across those partitions. A null storagePartitionType
/// means the scan must be planned as unbucketed.
struct PartitionSelection {
  std::vector<PartitionHandlePtr> partitions;
  std::shared_ptr<const PartitionType> storagePartitionType;
};

using PartitionSelectionPtr = std::shared_ptr<const PartitionSelection>;

class ConnectorSplitManager {
 public:
  virtual ~ConnectorSplitManager() = default;

  /// Selects the partitions read by 'tableHandle' and validates the declared
  /// storage partitioning for exactly that set. The default preserves the
  /// connector's declared partitioning.
  virtual folly::coro::Task<PartitionSelection> co_selectPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      std::shared_ptr<const PartitionType> declaredStoragePartitionType) {
    auto partitions = co_await co_listPartitions(session, tableHandle);
    co_return PartitionSelection{
        .partitions = std::move(partitions),
        .storagePartitionType = std::move(declaredStoragePartitionType)};
  }

  /// Returns a list of all partitions that match the filters in
  /// 'tableHandle'. A non-partitioned table returns one partition.
  virtual folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) = 0;

  /// Returns a SplitSource that covers the contents of 'partitions'. The set
  /// of partitions is exposed separately so that the caller may process them
  /// in a specific order or distribute them to specific nodes in a cluster.
  /// Connector implementations may use 'runtimeStats' to record split
  /// enumeration metrics (e.g., file listing, Metastore RPCs).
  ///
  /// When 'partitionType' is non-null, the connector tags each emitted Split
  /// with a groupId (bucket) in [0, partitionType->numGroups()) and a
  /// partitionId (target task) in [0, partitionType->numPartitions()). Pass
  /// 'nullptr' for the non-bucketed case.
  ///
  /// When 'samplePercentage' is set (TABLESAMPLE SYSTEM), the source emits each
  /// split with that probability, in the open interval (0, 100); the caller
  /// handles the 0 and 100 endpoints. The sampling happens during split
  /// enumeration so unselected splits are never produced. A connector that does
  /// not support sampling must fail rather than ignore it.
  virtual std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      const std::shared_ptr<PartitionType>& partitionType,
      std::optional<double> samplePercentage,
      QueryRuntimeStats& runtimeStats) = 0;
};

} // namespace facebook::axiom::connector
