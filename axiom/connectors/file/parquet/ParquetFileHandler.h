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

#include "axiom/connectors/file/FileHandler.h"

namespace facebook::axiom::connector::file {

/// FileHandler implementation for Parquet files. Reads schemas via
/// ParquetReader and exposes $row_groups and $column_chunks metadata tables.
class ParquetFileHandler : public FileHandler {
 public:
  ParquetFileHandler();

  velox::RowTypePtr resolve(
      const std::string& filePath,
      velox::memory::MemoryPool* pool) const override;

 protected:
  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::RowTypePtr& fullSchema,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool) const override;

  // Treats only files beginning with the Parquet "PAR1" magic as data files, so
  // directory listing skips subdirectories and non-Parquet sidecar files.
  bool isDataFile(
      velox::filesystems::FileSystem& fileSystem,
      const std::string& filePath) const override;
};

/// Registers the Parquet handler under the "parquet" schema name.
/// Safe to call multiple times; only the first call has an effect.
void registerParquetHandler();

} // namespace facebook::axiom::connector::file
