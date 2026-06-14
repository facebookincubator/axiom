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

#include "axiom/connectors/file/FileConnector.h"

#include "axiom/connectors/file/FileHandler.h"
#include "axiom/connectors/file/core/FileTable.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::file {

std::unique_ptr<velox::connector::DataSource> FileConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  auto* fileHandle = dynamic_cast<const FileTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(fileHandle, "Expected FileTableHandle");

  auto& fileHandler = handler(fileHandle->schemaTableName().schema);
  return fileHandler.create(
      fileHandle->filePath(),
      fileHandle->suffix().empty()
          ? std::nullopt
          : std::optional<std::string>(fileHandle->suffix()),
      outputType,
      fileHandle->fullSchema(),
      columnHandles,
      connectorQueryCtx->memoryPool());
}

} // namespace facebook::axiom::connector::file
