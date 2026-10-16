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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "axiom/connectors/ConnectorContext.h"
#include "velox/connectors/ConnectorRegistry.h"

namespace facebook::axiom::connector {

/// Pairs an execution connector with the metadata it owns in an isolated test
/// catalog.
using TestCatalogRegistration = std::pair<
    std::shared_ptr<velox::connector::Connector>,
    std::shared_ptr<ConnectorMetadata>>;

struct IsolatedTestContext {
  std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
      connectorRegistry;
  std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry;
  ConnectorContextPtr connectorContext;
};

/// Returns a context for a test that records no metrics: every connector gets
/// a no-op writer.
inline ConnectorContextPtr makeTestContext(
    std::string_view queryId,
    std::string_view user = "test",
    ConnectorProperties properties = {}) {
  return ConnectorContext::testingCreate(
      std::string{queryId},
      std::string{user},
      std::move(properties),
      ConnectorContext::noopStatWriterProvider());
}

/// Returns a connector context and its isolated catalog registries.
inline IsolatedTestContext makeTestContext(
    std::string_view queryId,
    std::vector<TestCatalogRegistration> catalogs,
    std::string_view user = "test",
    ConnectorProperties properties = {},
    StatWriterProvider statWriterProvider =
        ConnectorContext::noopStatWriterProvider()) {
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = ConnectorMetadataRegistry::create();
  for (auto& [connector, metadata] : catalogs) {
    ConnectorMetadataRegistry::registerCatalog(
        *connectorRegistry,
        *metadataRegistry,
        std::move(connector),
        std::move(metadata));
  }
  auto context = std::make_shared<ConnectorContext>(
      std::string{queryId},
      std::string{user},
      std::move(properties),
      std::move(statWriterProvider),
      metadataRegistry);
  return {
      .connectorRegistry = std::move(connectorRegistry),
      .metadataRegistry = std::move(metadataRegistry),
      .connectorContext = std::move(context),
  };
}

/// Returns a writer for a test component or connector that records no metrics.
inline std::shared_ptr<velox::BaseRuntimeStatWriter> makeTestStatWriter() {
  return std::make_shared<velox::NoopRuntimeStatWriter>();
}

} // namespace facebook::axiom::connector
