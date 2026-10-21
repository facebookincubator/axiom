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

#include "axiom/cli/Connectors.h"

namespace facebook::axiom {
namespace {

// Connectors registers and removes a catalog only in the application-provided
// registries, leaving the process-wide registries unchanged.
TEST(ConnectorsTest, applicationRegistries) {
  constexpr auto kCatalog = "application_test";
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = connector::ConnectorMetadataRegistry::create();

  ASSERT_EQ(
      velox::connector::ConnectorRegistry::global().find(kCatalog), nullptr);
  ASSERT_EQ(
      connector::ConnectorMetadataRegistry::global().find(kCatalog), nullptr);
  {
    Connectors connectors{*connectorRegistry, *metadataRegistry};
    auto connector = connectors.registerTestConnector(kCatalog);

    EXPECT_EQ(connectorRegistry->find(kCatalog), connector);
    EXPECT_NE(metadataRegistry->find(kCatalog), nullptr);
    EXPECT_EQ(
        velox::connector::ConnectorRegistry::global().find(kCatalog), nullptr);
    EXPECT_EQ(
        connector::ConnectorMetadataRegistry::global().find(kCatalog), nullptr);
  }

  EXPECT_EQ(connectorRegistry->find(kCatalog), nullptr);
  EXPECT_EQ(metadataRegistry->find(kCatalog), nullptr);
}

} // namespace
} // namespace facebook::axiom
