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

#include "axiom/connectors/ConnectorEnvironment.h"

#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"

namespace facebook::axiom::connector {
namespace {

class ConnectorEnvironmentTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(ConnectorEnvironmentTest, catalogIsolation) {
  auto firstBuilder = ConnectorEnvironment::Builder::create();
  auto secondBuilder = ConnectorEnvironment::Builder::create();
  auto firstConnector = std::make_shared<TestConnector>("catalog");
  auto secondConnector = std::make_shared<TestConnector>("catalog");

  firstBuilder->registerConnector(firstConnector, firstConnector->metadata());
  secondBuilder->registerConnector(
      secondConnector, secondConnector->metadata());
  auto firstEnvironment = firstBuilder->build();
  auto secondEnvironment = secondBuilder->build();

  EXPECT_EQ(firstEnvironment->connector("catalog"), firstConnector);
  EXPECT_EQ(secondEnvironment->connector("catalog"), secondConnector);
  EXPECT_EQ(firstEnvironment->metadata("catalog"), firstConnector->metadata());
  EXPECT_EQ(
      secondEnvironment->metadata("catalog"), secondConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, parentInheritance) {
  auto parentBuilder = ConnectorEnvironment::Builder::create();
  auto parentConnector = std::make_shared<TestConnector>("parent");
  parentBuilder->registerConnector(
      parentConnector, parentConnector->metadata());
  auto parent = parentBuilder->build();

  auto childBuilder = ConnectorEnvironment::Builder::createChild(parent);
  auto childConnector = std::make_shared<TestConnector>("child");
  childBuilder->registerConnector(childConnector, childConnector->metadata());
  auto child = childBuilder->build();

  EXPECT_EQ(child->connector("parent"), parentConnector);
  EXPECT_EQ(child->metadata("parent"), parentConnector->metadata());
  EXPECT_EQ(child->connector("child"), childConnector);
  EXPECT_EQ(child->metadata("child"), childConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, globalParent) {
  VELOX_ASSERT_THROW(
      ConnectorEnvironment::Builder::createChild(
          ConnectorEnvironment::global()),
      "Process-wide global connector environment cannot be used as a parent");
}

TEST_F(ConnectorEnvironmentTest, completedBuilder) {
  auto builder = ConnectorEnvironment::Builder::create();
  builder->build();
  auto connector = std::make_shared<TestConnector>("catalog");

  VELOX_ASSERT_THROW(
      builder->registerConnector(connector, connector->metadata()),
      "Connector environment builder has already completed");
}

TEST_F(ConnectorEnvironmentTest, registrationRollback) {
  auto builder = ConnectorEnvironment::Builder::create();
  auto existingConnector = std::make_shared<TestConnector>("existing");
  builder->registerMetadata("catalog", existingConnector->metadata());
  auto conflictingConnector = std::make_shared<TestConnector>("catalog");

  VELOX_ASSERT_THROW(
      builder->registerConnector(
          conflictingConnector, conflictingConnector->metadata()),
      "Key already registered: catalog");
  auto environment = builder->build();
  VELOX_ASSERT_THROW(
      environment->connector("catalog"),
      "Connector is not registered: catalog");
  EXPECT_EQ(environment->metadata("catalog"), existingConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, queryContext) {
  auto builder = ConnectorEnvironment::Builder::create();
  auto connector = std::make_shared<TestConnector>("catalog");
  builder->registerConnector(connector, connector->metadata());
  auto environment = builder->build();
  auto queryCtx = velox::core::QueryCtx::create();

  environment->attachTo(*queryCtx);

  EXPECT_EQ(
      velox::connector::ConnectorRegistry::tryGet(*queryCtx, "catalog"),
      connector);
  EXPECT_EQ(
      ConnectorMetadataRegistry::tryGet(*queryCtx, "catalog"),
      connector->metadata());
  EXPECT_EQ(
      queryCtx->registry<ConnectorEnvironment>(
          ConnectorEnvironment::kRegistryKey),
      environment);
}

} // namespace
} // namespace facebook::axiom::connector
