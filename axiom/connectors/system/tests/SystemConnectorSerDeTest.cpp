// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "axiom/connectors/system/SystemConnector.h"

namespace facebook::axiom::connector::system {
namespace {

class SystemConnectorSerDeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  SystemConnectorSerDeTest() {
    SystemConnector::registerSerDe();
  }

  static void testSerde(const SystemColumnHandle& handle) {
    auto obj = handle.serialize();
    auto clone = velox::ISerializable::deserialize<SystemColumnHandle>(obj);
    ASSERT_EQ(handle.name(), clone->name());
  }

  static void testSerde(const SystemTableHandle& handle) {
    auto obj = handle.serialize();
    auto clone =
        velox::ISerializable::deserialize<SystemTableHandle>(obj, nullptr);
    ASSERT_EQ(handle.connectorId(), clone->connectorId());
    ASSERT_EQ(handle.name(), clone->name());
    ASSERT_EQ(handle.columnHandles().size(), clone->columnHandles().size());
    for (size_t i = 0; i < handle.columnHandles().size(); ++i) {
      auto* original = dynamic_cast<const SystemColumnHandle*>(
          handle.columnHandles()[i].get());
      auto* cloned = dynamic_cast<const SystemColumnHandle*>(
          clone->columnHandles()[i].get());
      ASSERT_NE(original, nullptr);
      ASSERT_NE(cloned, nullptr);
      ASSERT_EQ(original->name(), cloned->name());
    }
    // Deserialized handle should have null layout.
    ASSERT_EQ(clone->layout(), nullptr);
  }

  static void testSerde(const SystemSplit& split) {
    auto obj = split.serialize();
    auto clone = velox::ISerializable::deserialize<SystemSplit>(obj);
    ASSERT_EQ(split.connectorId, clone->connectorId);
  }
};

TEST_F(SystemConnectorSerDeTest, columnHandle) {
  testSerde(SystemColumnHandle("query_id"));
  testSerde(SystemColumnHandle("state"));
  testSerde(SystemColumnHandle(""));
}

TEST_F(SystemConnectorSerDeTest, tableHandle) {
  const std::string connectorId = "system";

  // No column handles.
  auto handle1 = SystemTableHandle(connectorId, "queries", {});
  testSerde(handle1);

  // With column handles.
  std::vector<velox::connector::ColumnHandlePtr> columns = {
      std::make_shared<SystemColumnHandle>("query_id"),
      std::make_shared<SystemColumnHandle>("state"),
      std::make_shared<SystemColumnHandle>("user"),
  };
  auto handle2 = SystemTableHandle(connectorId, "queries", std::move(columns));
  testSerde(handle2);
}

TEST_F(SystemConnectorSerDeTest, connectorSplit) {
  testSerde(SystemSplit("system"));
  testSerde(SystemSplit("other"));
}

} // namespace
} // namespace facebook::axiom::connector::system
