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

#include <gtest/gtest.h>
#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/synthetic/SyntheticExecutionConnector.h"

namespace facebook::axiom::connector::synthetic {
namespace {

class SyntheticConnectorSerDeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  SyntheticConnectorSerDeTest() {
    registerSyntheticSerDe();
  }

  static void testSerde(const SyntheticColumnHandle& handle) {
    auto obj = handle.serialize();
    auto clone = velox::ISerializable::deserialize<SyntheticColumnHandle>(obj);
    ASSERT_EQ(handle.name(), clone->name());
  }

  static void testSerde(const SyntheticTableHandle& handle) {
    auto obj = handle.serialize();
    auto clone = velox::ISerializable::deserialize<SyntheticTableHandle>(
        obj, /*context=*/nullptr);
    auto* cloned = dynamic_cast<const SyntheticTableHandle*>(clone.get());
    ASSERT_NE(cloned, nullptr);
    ASSERT_EQ(handle.connectorId(), cloned->connectorId());
    ASSERT_EQ(handle.schemaTableName(), cloned->schemaTableName());
    ASSERT_EQ(handle.name(), cloned->name());
    ASSERT_EQ(handle.sourceCatalog(), cloned->sourceCatalog());
    ASSERT_EQ(handle.columnHandles().size(), cloned->columnHandles().size());
    for (size_t i = 0; i < handle.columnHandles().size(); ++i) {
      auto* original = dynamic_cast<const SyntheticColumnHandle*>(
          handle.columnHandles()[i].get());
      auto* clonedColumn = dynamic_cast<const SyntheticColumnHandle*>(
          cloned->columnHandles()[i].get());
      ASSERT_NE(original, nullptr);
      ASSERT_NE(clonedColumn, nullptr);
      ASSERT_EQ(original->name(), clonedColumn->name());
    }
    ASSERT_EQ(
        handle.acceptedFilters().size(), cloned->acceptedFilters().size());
  }

  static void testSerde(const SyntheticSplit& split) {
    auto obj = split.serialize();
    auto clone = velox::ISerializable::deserialize<SyntheticSplit>(obj);
    ASSERT_EQ(split.connectorId, clone->connectorId);
  }
};

TEST_F(SyntheticConnectorSerDeTest, columnHandle) {
  testSerde(SyntheticColumnHandle("a"));
  testSerde(SyntheticColumnHandle("table_schema"));
  testSerde(SyntheticColumnHandle(""));
}

TEST_F(SyntheticConnectorSerDeTest, tableHandle) {
  const std::string connectorId{kSyntheticConnectorId};

  // No column handles.
  testSerde(SyntheticTableHandle(
      connectorId,
      SchemaTableName{"s", "t"},
      /*columnHandles=*/{},
      /*acceptedFilters=*/{},
      /*sourceCatalog=*/"origin"));

  // With column handles.
  std::vector<velox::connector::ColumnHandlePtr> columns = {
      std::make_shared<SyntheticColumnHandle>("a"),
      std::make_shared<SyntheticColumnHandle>("b"),
  };
  testSerde(SyntheticTableHandle(
      connectorId,
      SchemaTableName{"information_schema", "columns"},
      std::move(columns),
      /*acceptedFilters=*/{},
      /*sourceCatalog=*/"origin"));
}

TEST_F(SyntheticConnectorSerDeTest, split) {
  testSerde(SyntheticSplit(std::string(kSyntheticConnectorId)));
  testSerde(SyntheticSplit("other"));
}

} // namespace
} // namespace facebook::axiom::connector::synthetic
