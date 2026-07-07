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

#include "axiom/connectors/synthetic/SyntheticTable.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::synthetic {

velox::connector::ColumnHandlePtr SyntheticTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<velox::TypePtr> /*castToType*/,
    SubfieldMapping /*subfieldMapping*/) const {
  VELOX_CHECK_NOT_NULL(
      findColumn(columnName), "Column not found: {}", columnName);
  return std::make_shared<SyntheticColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr
SyntheticTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& /*evaluator*/,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  auto acceptedFilters =
      provider_->acceptFilters(std::move(filters), rejectedFilters);
  return std::make_shared<SyntheticTableHandle>(
      connectorId(),
      table().name(),
      std::move(columnHandles),
      std::move(acceptedFilters),
      sourceCatalog_);
}

SyntheticTable::SyntheticTable(
    const SchemaTableName& tableName,
    std::shared_ptr<const SyntheticRowProvider> provider,
    velox::connector::Connector* connector,
    std::string sourceCatalog)
    : Table(tableName, makeColumns(provider->rowType())) {
  layout_ = std::make_unique<SyntheticTableLayout>(
      this,
      connector,
      allColumns(),
      std::move(provider),
      std::move(sourceCatalog));
  layouts_.push_back(layout_.get());
}

folly::coro::Task<SplitBatch> SyntheticSplitSource::co_getSplits(
    uint32_t /*maxSplitCount*/) {
  SplitBatch batch;
  if (!done_) {
    batch.splits.push_back(
        Split{
            .connectorSplit = std::make_shared<SyntheticSplit>(connectorId_)});
    done_ = true;
  }
  batch.noMoreSplits = true;
  co_return batch;
}

folly::coro::Task<std::vector<PartitionHandlePtr>>
SyntheticSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> SyntheticSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    const std::shared_ptr<PartitionType>& /*partitionType*/,
    QueryRuntimeStats& /*runtimeStats*/) {
  return std::make_shared<SyntheticSplitSource>(tableHandle->connectorId());
}

} // namespace facebook::axiom::connector::synthetic
