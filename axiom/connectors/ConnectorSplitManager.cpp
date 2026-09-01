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

#include "axiom/connectors/ConnectorSplitManager.h"

namespace facebook::axiom::connector {

folly::coro::Task<PartitionSelection>
ConnectorSplitManager::co_selectPartitions(
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    std::shared_ptr<const PartitionType> declaredStoragePartitionType) {
  co_return PartitionSelection{
      .partitions = co_await co_listPartitions(session, tableHandle),
      .storagePartitionType = std::move(declaredStoragePartitionType)};
}

} // namespace facebook::axiom::connector
