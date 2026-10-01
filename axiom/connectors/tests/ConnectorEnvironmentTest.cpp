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

TEST_F(ConnectorEnvironmentTest, isolatesCatalogsWithSameId) {
  auto firstEnvironment = ConnectorEnvironment::create();
  auto secondEnvironment = ConnectorEnvironment::create();
  auto firstConnector = std::make_shared<TestConnector>("catalog");
  auto secondConnector = std::make_shared<TestConnector>("catalog");

  firstEnvironment->registerConnector(
      firstConnector, firstConnector->metadata());
  secondEnvironment->registerConnector(
      secondConnector, secondConnector->metadata());
  firstEnvironment->seal();
  secondEnvironment->seal();

  EXPECT_EQ(firstEnvironment->connector("catalog"), firstConnector);
  EXPECT_EQ(secondEnvironment->connector("catalog"), secondConnector);
  EXPECT_EQ(firstEnvironment->metadata("catalog"), firstConnector->metadata());
  EXPECT_EQ(
      secondEnvironment->metadata("catalog"), secondConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, childInheritsSealedParent) {
  auto parent = ConnectorEnvironment::create();
  auto parentConnector = std::make_shared<TestConnector>("parent");
  parent->registerConnector(parentConnector, parentConnector->metadata());
  parent->seal();

  auto child = ConnectorEnvironment::createChild(parent);
  auto childConnector = std::make_shared<TestConnector>("child");
  child->registerConnector(childConnector, childConnector->metadata());
  child->seal();

  EXPECT_EQ(child->connector("parent"), parentConnector);
  EXPECT_EQ(child->metadata("parent"), parentConnector->metadata());
  EXPECT_EQ(child->connector("child"), childConnector);
  EXPECT_EQ(child->metadata("child"), childConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, childRejectsLegacyGlobalParent) {
  EXPECT_FALSE(ConnectorEnvironment::global()->sealed());
  VELOX_ASSERT_THROW(
      ConnectorEnvironment::global()->seal(),
      "Legacy global connector environment is mutable");
  VELOX_ASSERT_THROW(
      ConnectorEnvironment::createChild(ConnectorEnvironment::global()),
      "Legacy global connector environment cannot be used as a parent");
}

TEST_F(ConnectorEnvironmentTest, childRejectsUnsealedParent) {
  auto parent = ConnectorEnvironment::create();

  VELOX_ASSERT_THROW(
      ConnectorEnvironment::createChild(std::move(parent)),
      "Connector environment parent must be sealed");
}

TEST_F(ConnectorEnvironmentTest, sealingRejectsRegistration) {
  auto environment = ConnectorEnvironment::create();
  environment->seal();
  auto connector = std::make_shared<TestConnector>("catalog");

  VELOX_ASSERT_THROW(
      environment->registerConnector(connector, connector->metadata()),
      "Connector environment is sealed");
}

TEST_F(ConnectorEnvironmentTest, metadataFailureRollsBackConnector) {
  auto environment = ConnectorEnvironment::create();
  auto existingConnector = std::make_shared<TestConnector>("existing");
  environment->registerMetadata("catalog", existingConnector->metadata());
  auto conflictingConnector = std::make_shared<TestConnector>("catalog");

  VELOX_ASSERT_THROW(
      environment->registerConnector(
          conflictingConnector, conflictingConnector->metadata()),
      "Key already registered: catalog");
  VELOX_ASSERT_THROW(
      environment->connector("catalog"),
      "Connector is not registered: catalog");
  EXPECT_EQ(environment->metadata("catalog"), existingConnector->metadata());
}

TEST_F(ConnectorEnvironmentTest, attachesBothRegistriesToQueryContext) {
  auto environment = ConnectorEnvironment::create();
  auto connector = std::make_shared<TestConnector>("catalog");
  environment->registerConnector(connector, connector->metadata());
  environment->seal();
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
