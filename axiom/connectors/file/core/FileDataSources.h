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

#include "axiom/connectors/file/core/FileTable.h"

namespace facebook::axiom::connector::file {

/// Common base for all file connector data sources. Handles split
/// management and DataSource boilerplate methods.
class FileDataSource : public velox::connector::DataSource {
 public:
  FileDataSource(
      const velox::RowTypePtr& outputType,
      velox::memory::MemoryPool* pool)
      : outputType_(outputType), pool_(pool) {}

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override {
    split_ = std::dynamic_pointer_cast<FileSplit>(split);
    VELOX_CHECK(split_, "Expected FileSplit");
  }

  void addDynamicFilter(
      velox::column_index_t,
      const std::shared_ptr<velox::common::Filter>&) override {}

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return {};
  }

 protected:
  // Verifies that every output column has a matching entry in 'columnHandles'.
  // Shared by the row-data and metadata data sources so the check lives in one
  // place.
  static void validateOutputColumns(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles) {
    for (const auto& name : outputType->names()) {
      VELOX_CHECK(
          columnHandles.contains(name),
          "No handle for output column: {}",
          name);
    }
  }

  const velox::RowTypePtr outputType_;
  velox::memory::MemoryPool* pool_;
  std::shared_ptr<FileSplit> split_;
  uint64_t completedRows_{0};
};

/// Base for streaming data sources that read row data one batch at a
/// time. Subclasses implement onSplit() to initialize the reader and
/// readBatch() to return the next batch.
class StreamingDataSource : public FileDataSource {
 public:
  using FileDataSource::FileDataSource;

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override {
    FileDataSource::addSplit(std::move(split));
    onSplit(split_->filePath);
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& /*future*/) override {
    auto result = readBatch(size);
    if (result.has_value() && *result) {
      completedRows_ += (*result)->size();
    }
    return result;
  }

 protected:
  virtual void onSplit(const std::string& filePath) = 0;
  virtual std::optional<velox::RowVectorPtr> readBatch(uint64_t size) = 0;
};

/// Base for metadata data sources that produce a single batch. Handles
/// column projection from the full metadata schema to the requested
/// output columns. Subclasses implement build() to produce a full
/// RowVector with all columns.
class MetadataDataSource : public FileDataSource {
 public:
  MetadataDataSource(
      const velox::RowTypePtr& outputType,
      const velox::RowTypePtr& fullSchema,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : FileDataSource(outputType, pool), fullSchema_(fullSchema) {
    validateOutputColumns(outputType_, columnHandles);
    for (const auto& name : outputType_->names()) {
      auto handle = columnHandles.find(name)->second;
      auto index = fullSchema->getChildIdxIfExists(handle->name());
      VELOX_CHECK(index.has_value(), "Column not found: {}", handle->name());
    }
  }

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override {
    FileDataSource::addSplit(std::move(split));
    // Each split is a separate file whose metadata must be emitted, so the
    // single-batch build() runs once per split.
    needData_ = true;
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t /*size*/,
      velox::ContinueFuture& /*future*/) override {
    VELOX_CHECK(split_, "No split added");
    if (!needData_) {
      return nullptr;
    }
    needData_ = false;
    auto full = build();
    if (!full || full->size() == 0) {
      return velox::RowVector::createEmpty(outputType_, pool_);
    }
    auto result = project(full);
    completedRows_ += result->size();
    return result;
  }

 protected:
  virtual velox::RowVectorPtr build() = 0;

 private:
  velox::RowVectorPtr project(const velox::RowVectorPtr& full) {
    if (outputType_->equivalent(*fullSchema_)) {
      return full;
    }
    std::vector<velox::VectorPtr> children;
    children.reserve(outputType_->size());
    for (const auto& name : outputType_->names()) {
      auto index = fullSchema_->getChildIdx(name);
      children.push_back(full->childAt(index));
    }
    return std::make_shared<velox::RowVector>(
        pool_, outputType_, nullptr, full->size(), std::move(children));
  }

  const velox::RowTypePtr fullSchema_;
  bool needData_{true};
};

} // namespace facebook::axiom::connector::file
