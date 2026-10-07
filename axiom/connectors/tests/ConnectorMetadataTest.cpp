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

#include "axiom/connectors/ConnectorMetadata.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
namespace facebook::axiom::connector {
namespace {

class TestPartitionType final : public PartitionType {
 public:
  std::shared_ptr<PartitionType> copartition(
      const PartitionType& /*other*/) const override {
    return nullptr;
  }

  std::shared_ptr<PartitionType> scaleDown(
      int32_t /*maxPartitions*/) const override {
    return nullptr;
  }

  velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<velox::column_index_t>& /*channels*/,
      const std::vector<velox::VectorPtr>& /*constants*/,
      bool /*isLocal*/) const override {
    return nullptr;
  }

  int32_t numPartitions() const override {
    return 1;
  }

  int32_t numGroups() const override {
    return 1;
  }

  std::string toString() const override {
    return "test";
  }
};

class TestInsertHandle final
    : public velox::connector::ConnectorInsertTableHandle {
 public:
  std::string toString() const override {
    return "test";
  }
};

TEST(DeleteLayoutTest, metadataOnlyHasNoWriterRequirements) {
  DeleteLayout layout{
      .hasWriter = false,
      .shuffleKeys = {"route"},
      .partitionType = std::make_shared<TestPartitionType>(),
  };
  VELOX_ASSERT_THROW(
      layout.checkConsistency(),
      "Metadata-only DELETE cannot carry writer requirements");
}

TEST(DeleteLayoutTest, shuffleKeysRequirePartitionType) {
  DeleteLayout layout{.hasWriter = true, .shuffleKeys = {"route"}};
  VELOX_ASSERT_THROW(
      layout.checkConsistency(),
      "Delete shuffle keys and partition type must be specified together");
}

TEST(DeleteLayoutTest, partitionTypeRequiresShuffleKeys) {
  DeleteLayout layout{
      .hasWriter = true,
      .partitionType = std::make_shared<TestPartitionType>(),
  };
  VELOX_ASSERT_THROW(
      layout.checkConsistency(),
      "Delete shuffle keys and partition type must be specified together");
}

TEST(DeleteLayoutTest, shuffleKeyRequiresName) {
  DeleteLayout layout{
      .hasWriter = true,
      .shuffleKeys = {""},
      .partitionType = std::make_shared<TestPartitionType>(),
  };
  VELOX_ASSERT_THROW(
      layout.checkConsistency(), "Delete shuffle key requires a name");
}

TEST(DeleteLayoutTest, sortKeyRequiresName) {
  DeleteLayout layout{
      .hasWriter = true,
      .sortKeys = {""},
      .sortOrders = {{/*isAscending=*/true, /*isNullsFirst=*/false}},
  };
  VELOX_ASSERT_THROW(
      layout.checkConsistency(), "Delete sort key requires a name");
}

TEST(DeleteLayoutTest, sortKeysAndOrdersAreOneToOne) {
  DeleteLayout layout{.hasWriter = true, .sortKeys = {"row_number"}};
  VELOX_ASSERT_THROW(
      layout.checkConsistency(),
      "Delete sort keys and sort orders must be one-to-one");
}

TEST(ConnectorDeleteHandleTest, writerFieldsArePaired) {
  VELOX_ASSERT_THROW(
      ConnectorDeleteHandle(nullptr, velox::ROW({})),
      "Delete writer handle and result type must be specified together");
  VELOX_ASSERT_THROW(
      ConnectorDeleteHandle(std::make_shared<TestInsertHandle>(), nullptr),
      "Delete writer handle and result type must be specified together");
}

} // namespace
} // namespace facebook::axiom::connector
