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
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector {
namespace {

using namespace facebook::velox;

class TestConnectorSerDeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  TestConnectorSerDeTest() {
    Type::registerSerDe();
    core::ITypedExpr::registerSerDe();
    TestConnector::registerSerDe();
  }

  static void testSerde(const TestColumnHandle& handle) {
    auto obj = handle.serialize();
    auto clone = ISerializable::deserialize<TestColumnHandle>(obj);
    ASSERT_EQ(handle.name(), clone->name());
    ASSERT_EQ(*handle.type(), *clone->type());
  }

  static void testSerde(const TestTableHandle& handle) {
    auto obj = handle.serialize();
    auto pool = memory::memoryManager()->addLeafPool();
    auto clone = ISerializable::deserialize<TestTableHandle>(obj, pool.get());
    ASSERT_EQ(handle.toString(), clone->toString());
    ASSERT_EQ(handle.connectorId(), clone->connectorId());
    ASSERT_EQ(handle.schemaTableName(), clone->schemaTableName());
    ASSERT_EQ(handle.name(), clone->name());
    ASSERT_EQ(handle.size(), clone->size());
    ASSERT_EQ(handle.columnHandles().size(), clone->columnHandles().size());
    for (size_t i = 0; i < handle.columnHandles().size(); ++i) {
      auto* original = dynamic_cast<const TestColumnHandle*>(
          handle.columnHandles()[i].get());
      auto* cloned = dynamic_cast<const TestColumnHandle*>(
          clone->columnHandles()[i].get());
      ASSERT_NE(original, nullptr);
      ASSERT_NE(cloned, nullptr);
      ASSERT_EQ(original->name(), cloned->name());
      ASSERT_EQ(*original->type(), *cloned->type());
    }
    ASSERT_EQ(handle.filters().size(), clone->filters().size());
    for (size_t i = 0; i < handle.filters().size(); ++i) {
      ASSERT_EQ(
          handle.filters()[i]->toString(), clone->filters()[i]->toString());
    }
  }

  static void testSerde(const TestConnectorSplit& split) {
    auto obj = split.serialize();
    auto clone = ISerializable::deserialize<TestConnectorSplit>(obj);
    ASSERT_EQ(split.connectorId, clone->connectorId);
    ASSERT_EQ(split.index(), clone->index());
  }
};

TEST_F(TestConnectorSerDeTest, columnHandle) {
  testSerde(TestColumnHandle("col1", BIGINT()));
  testSerde(TestColumnHandle("col2", VARCHAR()));
  testSerde(TestColumnHandle("col3", DOUBLE()));
  testSerde(
      TestColumnHandle("col4", ROW({{"a", INTEGER()}, {"b", VARCHAR()}})));
}

TEST_F(TestConnectorSerDeTest, tableHandle) {
  const std::string connectorId = "test-connector";

  // No column handles, no filters.
  auto handle1 = TestTableHandle(
      connectorId, SchemaTableName{"schema1", "table1"}, 10, {});
  testSerde(handle1);

  // With column handles.
  std::vector<velox::connector::ColumnHandlePtr> columns = {
      std::make_shared<TestColumnHandle>("c1", BIGINT()),
      std::make_shared<TestColumnHandle>("c2", VARCHAR()),
  };
  auto handle2 = TestTableHandle(
      connectorId, SchemaTableName{"schema2", "table2"}, 5, std::move(columns));
  testSerde(handle2);

  // With filters.
  auto filterExpr =
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c1");
  std::vector<velox::connector::ColumnHandlePtr> columns2 = {
      std::make_shared<TestColumnHandle>("c1", BIGINT()),
  };
  auto handle3 = TestTableHandle(
      connectorId,
      SchemaTableName{"schema3", "table3"},
      3,
      std::move(columns2),
      {filterExpr});
  testSerde(handle3);

  // Empty schema and name.
  auto handle4 = TestTableHandle(connectorId, SchemaTableName{"", ""}, 0, {});
  testSerde(handle4);
}

TEST_F(TestConnectorSerDeTest, connectorSplit) {
  const std::string connectorId = "test-connector";

  testSerde(TestConnectorSplit(connectorId, 0));
  testSerde(TestConnectorSplit(connectorId, 5));
  testSerde(TestConnectorSplit(connectorId, 100));
}

} // namespace
} // namespace facebook::axiom::connector
