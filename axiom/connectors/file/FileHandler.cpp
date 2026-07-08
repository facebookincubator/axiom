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

#include "axiom/connectors/file/FileHandler.h"

#include <algorithm>
#include <map>

#include "axiom/connectors/file/core/FileTable.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::axiom::connector::file {

namespace {

std::map<std::string, FileHandler*>& handlerRegistry() {
  static std::map<std::string, FileHandler*> registry;
  return registry;
}

} // namespace

void registerHandler(const std::string& schemaName, FileHandler& handler) {
  auto [it, inserted] = handlerRegistry().emplace(schemaName, &handler);
  VELOX_CHECK(
      inserted, "FileHandler already registered for schema: {}", schemaName);
}

FileHandler& handler(const std::string& schemaName) {
  auto& registry = handlerRegistry();
  auto it = registry.find(schemaName);
  VELOX_USER_CHECK(
      it != registry.end(), "Unsupported file format: {}", schemaName);
  return *it->second;
}

std::vector<std::string> schemas() {
  auto& registry = handlerRegistry();
  std::vector<std::string> names;
  names.reserve(registry.size());
  for (const auto& [name, handler] : registry) {
    names.push_back(name);
  }
  return names;
}

bool hasHandler(const std::string& schemaName) {
  return handlerRegistry().contains(schemaName);
}

void FileHandler::addMetadataTable(
    std::string suffix,
    velox::RowTypePtr schema,
    MetadataSourceFactory factory) {
  VELOX_USER_CHECK(
      FileTable::isValidName(suffix),
      "Invalid metadata table name: {}",
      suffix);
  metadataTables_.emplace(
      std::move(suffix), MetadataTable{std::move(schema), std::move(factory)});
}

const velox::RowTypePtr& FileHandler::metadataSchema(
    const std::string& suffix) const {
  auto it = metadataTables_.find(suffix);
  VELOX_USER_CHECK(
      it != metadataTables_.end(), "Unsupported metadata table: {}", suffix);
  return it->second.schema;
}

std::vector<std::string> FileHandler::listFiles(const std::string& path) const {
  if (path.empty() || path.back() != '/') {
    return {path};
  }

  auto fileSystem = velox::filesystems::getFileSystem(path, /*config=*/nullptr);
  auto entries = fileSystem->list(path);
  std::vector<std::string> files;
  files.reserve(entries.size());
  for (auto& entry : entries) {
    const auto slash = entry.find_last_of('/');
    const std::string_view baseName = std::string_view(entry).substr(
        slash == std::string::npos ? 0 : slash + 1);
    if (baseName.empty() || baseName.front() == '_' ||
        baseName.front() == '.') {
      continue;
    }
    // Skip nested directories, detected from filesystem metadata rather than by
    // trying to read them, so genuine read errors on real files are not
    // swallowed as "not a data file".
    if (fileSystem->isDirectory(entry)) {
      continue;
    }
    // Let the handler's isDataFile() reject entries that are not data files of
    // its format (e.g. foreign formats). The base accepts every entry.
    if (!isDataFile(*fileSystem, entry)) {
      continue;
    }
    files.push_back(std::move(entry));
  }
  std::sort(files.begin(), files.end());
  VELOX_USER_CHECK(
      !files.empty(), "No data files found in directory: {}", path);
  return files;
}

bool FileHandler::isDataFile(
    velox::filesystems::FileSystem& /*fileSystem*/,
    const std::string& /*filePath*/) const {
  return true;
}

std::unique_ptr<velox::connector::DataSource> FileHandler::create(
    const std::string& /*filePath*/,
    std::optional<std::string> suffix,
    const velox::RowTypePtr& outputType,
    const velox::RowTypePtr& fullSchema,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool) const {
  if (!suffix.has_value()) {
    return createDataSource(outputType, fullSchema, columnHandles, pool);
  }
  auto it = metadataTables_.find(*suffix);
  VELOX_USER_CHECK(
      it != metadataTables_.end(), "Unsupported metadata table: {}", *suffix);
  return it->second.factory(outputType, columnHandles, pool);
}

} // namespace facebook::axiom::connector::file
