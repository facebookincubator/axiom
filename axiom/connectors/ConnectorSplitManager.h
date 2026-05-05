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

#include <folly/coro/Task.h>
#include <velox/connectors/Connector.h>
#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

/// Controls split granularity for the split source factory.
struct SplitOptions {
  // Target number of splits to generate across all partitions.
  int32_t targetSplitCount{0};

  // Target number of file bytes per split.
  uint64_t fileBytesPerSplit{0};
};

/// A batch of splits returned by SplitSource::co_getSplits.
struct SplitBatch {
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;

  /// True when there are no more splits to return.
  bool noMoreSplits{false};
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

class ConnectorSplitManager {
 public:
  virtual ~ConnectorSplitManager() = default;

  /// Returns a list of all partitions that match the filters in
  /// 'tableHandle'. A non-partitioned table returns one partition.
  virtual folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) = 0;

  /// Returns a SplitSource that covers the contents of 'partitions'. The set of
  /// partitions is exposed separately so that the caller may process the
  /// partitions in a specific order or distribute them to specific nodes in a
  /// cluster.
  virtual std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions) = 0;
};

} // namespace facebook::axiom::connector
