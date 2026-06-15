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

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::file {

/// Factory function that creates a metadata DataSource.
using MetadataSourceFactory =
    std::function<std::unique_ptr<velox::connector::DataSource>(
        const velox::RowTypePtr& outputType,
        const velox::connector::ColumnHandleMap& columnHandles,
        velox::memory::MemoryPool* pool)>;

/// Abstracts format-specific file operations. Each file type provides
/// a concrete implementation and registers itself under a schema name.
///
/// Subclasses implement resolve() and createDataSource() for
/// format-specific logic, and register metadata tables with factory
/// lambdas via addMetadataTable() in their constructor.
class FileHandler {
 public:
  virtual ~FileHandler() = default;

  /// Reads the data schema from the file header.
  virtual velox::RowTypePtr resolve(
      const std::string& filePath,
      velox::memory::MemoryPool* pool) const = 0;

  /// Returns the fixed schema for a metadata table by $-suffix.
  const velox::RowTypePtr& metadataSchema(const std::string& suffix) const;

  /// Creates a DataSource for reading from a file. Dispatches to
  /// createDataSource() for row data or the registered factory for
  /// metadata tables.
  std::unique_ptr<velox::connector::DataSource> create(
      const std::string& filePath,
      std::optional<std::string> suffix,
      const velox::RowTypePtr& outputType,
      const velox::RowTypePtr& fullSchema,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool) const;

 protected:
  /// Registers a metadata table with its schema and factory.
  void addMetadataTable(
      std::string suffix,
      velox::RowTypePtr schema,
      MetadataSourceFactory factory);

  /// Creates a streaming DataSource for reading row data.
  virtual std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::RowTypePtr& fullSchema,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool) const = 0;

 private:
  struct MetadataTable {
    velox::RowTypePtr schema;
    MetadataSourceFactory factory;
  };
  std::map<std::string, MetadataTable> metadataTables_;
};

/// Registers a handler under a schema name. Fails if already registered.
/// All registration must happen during single-threaded startup (before
/// query execution begins). Use std::call_once in the registration
/// function to guarantee idempotency.
void registerHandler(const std::string& schemaName, FileHandler& handler);

/// Returns the handler for the given schema name. Fails if not found.
FileHandler& handler(const std::string& schemaName);

/// Returns the schema names of all registered handlers.
std::vector<std::string> schemas();

/// Returns true if a handler is registered for the given schema name.
bool hasHandler(const std::string& schemaName);

} // namespace facebook::axiom::connector::file
