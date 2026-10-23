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

#include <functional>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"

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

  const std::vector<velox::TypePtr>& keyTypes() const override {
    static const std::vector<velox::TypePtr> kKeyTypes{velox::BIGINT()};
    return kKeyTypes;
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

TEST(DeleteLayoutTest, invalidRowLevel) {
  const auto one = std::make_shared<velox::core::ConstantTypedExpr>(
      velox::BIGINT(), velox::Variant(int64_t{1}));
  const auto route = std::make_shared<velox::core::FieldAccessTypedExpr>(
      velox::BIGINT(), "row_id");
  const auto partitionType = std::make_shared<TestPartitionType>();
  const SortOrder ascending{/*isAscending=*/true, /*isNullsFirst=*/false};

  const std::vector<std::pair<std::function<DeleteLayout()>, std::string>>
      cases = {
          {[] {
             return DeleteLayout::rowLevel({}, {"route"}, nullptr, {}, {});
           },
           "Delete shuffle keys and partition type must be specified together"},
          {[&] {
             return DeleteLayout::rowLevel({}, {}, partitionType, {}, {});
           },
           "Delete shuffle keys and partition type must be specified together"},
          {[&] {
             return DeleteLayout::rowLevel(
                 {}, {"route", "group"}, partitionType, {}, {});
           },
           "Delete shuffle keys must match the partition type's keys"},
          {[&] {
             return DeleteLayout::rowLevel({}, {""}, partitionType, {}, {});
           },
           "Delete shuffle key requires a name"},
          {[&] {
             return DeleteLayout::rowLevel({}, {}, nullptr, {""}, {ascending});
           },
           "Delete sort key requires a name"},
          {[] {
             return DeleteLayout::rowLevel({}, {}, nullptr, {"row_number"}, {});
           },
           "Delete sort keys and sort orders must be one-to-one"},
          {[&] {
             return DeleteLayout::rowLevel({{"", one}}, {}, nullptr, {}, {});
           },
           "Computed delete column requires a name"},
          {[] {
             return DeleteLayout::rowLevel(
                 {{"route", nullptr}}, {}, nullptr, {}, {});
           },
           "Computed delete column requires an expression: route"},
          {[&] {
             return DeleteLayout::rowLevel(
                 {{"route", route}, {"route", route}}, {}, nullptr, {}, {});
           },
           "Computed delete column name is duplicated: route"},
      };
  for (const auto& [makeLayout, error] : cases) {
    SCOPED_TRACE(error);
    VELOX_ASSERT_THROW(makeLayout(), error);
  }
}

TEST(ConnectorDeleteHandleTest, requiredFields) {
  VELOX_ASSERT_THROW(
      ConnectorDeleteHandle(nullptr, velox::ROW({})),
      "Delete writer handle must not be null");
  VELOX_ASSERT_THROW(
      ConnectorDeleteHandle(std::make_shared<TestInsertHandle>(), nullptr),
      "Delete result type must not be null");
}

} // namespace
} // namespace facebook::axiom::connector
