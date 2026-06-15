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

#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::file {

/// Read-only Velox connector for querying raw files.
/// See README.md for architecture and usage details.
class FileConnector : public velox::connector::Connector {
 public:
  explicit FileConnector(const std::string& id) : Connector(id) {}

  /// Creates a DataSource by dispatching to the file-type-specific
  /// handler identified by the table handle's schema name.
  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr,
      velox::connector::ConnectorInsertTableHandlePtr,
      velox::connector::ConnectorQueryCtx*,
      velox::connector::CommitStrategy) override {
    VELOX_UNSUPPORTED("FileConnector does not support writes");
  }
};

} // namespace facebook::axiom::connector::file
