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

#include <map>

#include "velox/common/base/Exceptions.h"

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
