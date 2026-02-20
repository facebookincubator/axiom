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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::test {
namespace {

class TestConnectorSerdeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  TestConnectorSerdeTest() {
    velox::Type::registerSerDe();
    velox::core::ITypedExpr::registerSerDe();
    TestConnector::registerSerDe();
  }

  static void testSerde(const TestColumnHandle& handle) {
    auto obj = handle.serialize();
    auto clone =
        velox::ISerializable::deserialize<TestColumnHandle>(obj, nullptr);
    ASSERT_EQ(handle.name(), clone->name());
    VELOX_EXPECT_EQ_TYPES(handle.type(), clone->type());
  }

  static void testSerde(const TestConnectorSplit& split) {
    auto obj = split.serialize();
    auto clone = velox::ISerializable::deserialize<TestConnectorSplit>(obj);
    ASSERT_EQ(split.connectorId, clone->connectorId);
    ASSERT_EQ(split.splitWeight(), clone->splitWeight());
    ASSERT_EQ(split.isCacheable(), clone->isCacheable());
    ASSERT_EQ(split.index(), clone->index());
  }

  static void testSerde(const TestTableHandle& handle) {
    auto obj = handle.serialize();
    auto pool = velox::memory::memoryManager()->addLeafPool();
    auto clone =
        velox::ISerializable::deserialize<TestTableHandle>(obj, pool.get());
    ASSERT_EQ(handle.connectorId(), clone->connectorId());
    ASSERT_EQ(handle.name(), clone->name());
    ASSERT_EQ(handle.columnHandles().size(), clone->columnHandles().size());
    for (size_t i = 0; i < handle.columnHandles().size(); ++i) {
      ASSERT_EQ(
          handle.columnHandles()[i]->name(), clone->columnHandles()[i]->name());
    }
    ASSERT_EQ(handle.filters().size(), clone->filters().size());
    for (size_t i = 0; i < handle.filters().size(); ++i) {
      ASSERT_EQ(
          handle.filters()[i]->toString(), clone->filters()[i]->toString());
    }
  }
};

TEST_F(TestConnectorSerdeTest, columnHandle) {
  testSerde(TestColumnHandle("col_a", velox::BIGINT()));
  testSerde(TestColumnHandle("col_b", velox::VARCHAR()));
  testSerde(TestColumnHandle("col_c", velox::BOOLEAN()));
  testSerde(TestColumnHandle("col_d", velox::DOUBLE()));
  testSerde(TestColumnHandle(
      "col_e", velox::ROW({"x", "y"}, {velox::INTEGER(), velox::REAL()})));
}

TEST_F(TestConnectorSerdeTest, connectorSplit) {
  testSerde(TestConnectorSplit("test-connector", 0));
  testSerde(TestConnectorSplit("test-connector", 5));
  testSerde(TestConnectorSplit("other-connector", 42));
}

TEST_F(TestConnectorSerdeTest, tableHandle) {
  testSerde(TestTableHandle(
      "test-connector",
      "test-table",
      {std::make_shared<TestColumnHandle>("a", velox::BIGINT()),
       std::make_shared<TestColumnHandle>("b", velox::VARCHAR())}));

  testSerde(TestTableHandle(
      "test-connector",
      "empty-table",
      std::vector<velox::connector::ColumnHandlePtr>{}));

  auto filter =
      std::make_shared<velox::core::FieldAccessTypedExpr>(velox::BIGINT(), "a");
  testSerde(TestTableHandle(
      "test-connector",
      "filter-table",
      {std::make_shared<TestColumnHandle>("a", velox::BIGINT())},
      {filter}));
}

} // namespace
} // namespace facebook::axiom::connector::test
