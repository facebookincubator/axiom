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

#include "velox/frontend/optimizer/connectors/ConnectorMetadata.h"

namespace facebook::velox::connector {

class TestTable : public Table {
 public:
  TestTable(const std::string& name, const RowTypePtr& schema) : Table(name) {
    type_ = schema;
  }

  const std::unordered_map<std::string, const Column*>& columnMap()
      const override {
    VELOX_NYI();
  }

  const std::vector<const TableLayout*>& layouts() const override {
    VELOX_NYI();
  }

  uint64_t numRows() const override {
    VELOX_NYI();
  }
};

class TestConnectorMetadata : public ConnectorMetadata {
 public:
  void initialize() override {}

  const Table* findTable(const std::string& name) override {
    auto it = tables_.find(name);
    VELOX_USER_CHECK(it != tables_.end(), "Test table not found: {}", name);
    return it->second.get();
  }

  ConnectorSplitManager* splitManager() override {
    VELOX_NYI();
  }

  void addTable(const std::string& name, const RowTypePtr& schema) {
    tables_.emplace(name, std::make_unique<TestTable>(name, schema));
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<TestTable>> tables_;
};

class TestConnector : public Connector {
 public:
  explicit TestConnector(const std::string& id)
      : Connector(id), metadata_{std::make_unique<TestConnectorMetadata>()} {}

  ConnectorMetadata* metadata() const override {
    return metadata_.get();
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const ConnectorTableHandlePtr& /* tableHandle */,
      const ColumnHandleMap& /* columnHandles */,
      ConnectorQueryCtx* /* connectorQueryCtx */) override {
    VELOX_NYI();
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr /* inputType */,
      ConnectorInsertTableHandlePtr /* connectorInsertTableHandle */,
      ConnectorQueryCtx* /* connectorQueryCtx */,
      CommitStrategy /* commitStrategy */) override {
    VELOX_NYI();
  }

  void addTable(const std::string& name, const RowTypePtr& schema) {
    metadata_->addTable(name, schema);
  }

  void addTable(const std::string& name) {
    metadata_->addTable(name, ROW({}, {}));
  }

 private:
  const std::unique_ptr<TestConnectorMetadata> metadata_;
};

} // namespace facebook::velox::connector
