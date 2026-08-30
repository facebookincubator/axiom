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

#include "axiom/connectors/synthetic/SyntheticConnector.h"

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::connector::synthetic {

std::vector<velox::core::TypedExprPtr> SyntheticRowProvider::acceptFilters(
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejected) const {
  rejected.insert(
      rejected.end(),
      std::make_move_iterator(filters.begin()),
      std::make_move_iterator(filters.end()));
  return {};
}

folly::dynamic SyntheticColumnHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = SyntheticColumnHandle::getClassName();
  obj["columnName"] = name_;
  return obj;
}

std::shared_ptr<SyntheticColumnHandle> SyntheticColumnHandle::create(
    const folly::dynamic& obj) {
  return std::make_shared<SyntheticColumnHandle>(obj["columnName"].asString());
}

SyntheticTableHandle::SyntheticTableHandle(
    std::string_view connectorId,
    SchemaTableName schemaTableName,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    std::vector<velox::core::TypedExprPtr> acceptedFilters,
    std::string sourceCatalog)
    : ConnectorTableHandle(std::string(connectorId)),
      schemaTableName_(std::move(schemaTableName)),
      qualifiedName_(
          fmt::format(
              "{}.{}",
              schemaTableName_.schema,
              schemaTableName_.table)),
      columnHandles_(std::move(columnHandles)),
      acceptedFilters_(std::move(acceptedFilters)),
      sourceCatalog_(std::move(sourceCatalog)) {
  VELOX_CHECK(
      !sourceCatalog_.empty(),
      "Synthetic table handle is missing its source catalog: {}",
      qualifiedName_);
}

folly::dynamic SyntheticTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = SyntheticTableHandle::getClassName();
  obj["connectorId"] = connectorId();
  obj["schemaName"] = schemaTableName_.schema;
  obj["tableName"] = schemaTableName_.table;
  obj["sourceCatalog"] = sourceCatalog_;
  folly::dynamic handles = folly::dynamic::array;
  for (const auto& handle : columnHandles_) {
    handles.push_back(handle->serialize());
  }
  obj["columnHandles"] = std::move(handles);
  folly::dynamic filters = folly::dynamic::array;
  for (const auto& filter : acceptedFilters_) {
    filters.push_back(filter->serialize());
  }
  obj["acceptedFilters"] = std::move(filters);
  return obj;
}

velox::connector::ConnectorTableHandlePtr SyntheticTableHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  auto connectorId = obj["connectorId"].asString();
  SchemaTableName schemaTableName{
      obj["schemaName"].asString(), obj["tableName"].asString()};
  auto sourceCatalog = obj["sourceCatalog"].asString();
  // False positive: deserialize() move-constructs its returned shared_ptr from
  // a local unique_ptr, handing off the heap object; no stack reference
  // escapes.
  // @lint-ignore INFERCPP USE_AFTER_LIFETIME
  auto columnHandles = velox::ISerializable::deserialize<
      std::vector<velox::connector::ColumnHandle>>(obj["columnHandles"]);
  // @lint-ignore INFERCPP USE_AFTER_LIFETIME
  auto acceptedFilters =
      velox::ISerializable::deserialize<std::vector<velox::core::ITypedExpr>>(
          obj["acceptedFilters"]);
  return std::make_shared<SyntheticTableHandle>(
      connectorId,
      std::move(schemaTableName),
      std::move(columnHandles),
      std::move(acceptedFilters),
      std::move(sourceCatalog));
}

folly::dynamic SyntheticSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = SyntheticSplit::getClassName();
  obj["connectorId"] = connectorId;
  return obj;
}

std::shared_ptr<SyntheticSplit> SyntheticSplit::create(
    const folly::dynamic& obj) {
  return std::make_shared<SyntheticSplit>(obj["connectorId"].asString());
}

SyntheticDataSource::SyntheticDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::shared_ptr<const SyntheticRowProvider> provider,
    std::vector<velox::core::TypedExprPtr> acceptedFilters,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType),
      pool_(pool),
      provider_(std::move(provider)),
      acceptedFilters_(std::move(acceptedFilters)) {
  const auto& fullSchema = provider_->rowType();
  outputColumnMappings_.reserve(outputType_->size());
  for (const auto& name : outputType_->names()) {
    auto it = columnHandles.find(name);
    VELOX_CHECK(
        it != columnHandles.end(), "No handle for output column: {}", name);
    auto idx = fullSchema->getChildIdxIfExists(it->second->name());
    VELOX_CHECK(idx.has_value(), "Column not found: {}", it->second->name());
    outputColumnMappings_.push_back(idx.value());
  }
}

void SyntheticDataSource::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) {
  split_ = std::dynamic_pointer_cast<SyntheticSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Expected SyntheticSplit");
  // completedRows_ is a running total across the source's lifetime, so it is
  // intentionally not reset here.
  results_ = nullptr;
  offset_ = 0;
}

std::optional<velox::RowVectorPtr> SyntheticDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(split_, "No split added");
  if (results_ == nullptr) {
    results_ = buildResults();
  }
  if (offset_ >= results_->size()) {
    return nullptr;
  }
  // 'size' is uint64_t; clamp before narrowing so a value above the
  // vector_size_t range can't wrap to a negative length.
  const auto remaining = results_->size() - offset_;
  const auto length = static_cast<velox::vector_size_t>(
      std::min<uint64_t>(size, static_cast<uint64_t>(remaining)));
  auto batch = std::static_pointer_cast<velox::RowVector>(
      results_->slice(offset_, length));
  offset_ += length;
  completedRows_ += length;
  return batch;
}

velox::RowVectorPtr SyntheticDataSource::buildResults() {
  auto full = provider_->buildRows(acceptedFilters_, pool_);
  VELOX_CHECK_NOT_NULL(full, "Provider returned a null vector");
  VELOX_CHECK(
      *full->type() == *provider_->rowType(),
      "Provider rows do not match its declared schema");

  std::vector<velox::VectorPtr> children;
  children.reserve(outputColumnMappings_.size());
  for (auto idx : outputColumnMappings_) {
    children.push_back(full->childAt(idx));
  }
  return std::make_shared<velox::RowVector>(
      pool_, outputType_, /*nulls=*/nullptr, full->size(), std::move(children));
}

void registerSyntheticSerDe() {
  SyntheticTableHandle::registerSerDe();
  SyntheticColumnHandle::registerSerDe();
  SyntheticSplit::registerSerDe();
}

} // namespace facebook::axiom::connector::synthetic
