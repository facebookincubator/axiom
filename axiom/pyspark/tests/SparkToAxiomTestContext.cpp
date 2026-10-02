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

#include "axiom/pyspark/tests/SparkToAxiomTestContext.h"

namespace axiom::collagen::test {

facebook::axiom::connector::ConnectorContextPtr
SparkToAxiomTestContext::createConnectorContext(
    const std::shared_ptr<facebook::axiom::connector::TestConnector>&
        connector) {
  auto connectorRegistry =
      facebook::velox::connector::ConnectorRegistry::create();
  auto metadataRegistry =
      facebook::axiom::connector::ConnectorMetadataRegistry::create();
  connectorRegistry->insert(connector->connectorId(), connector);
  metadataRegistry->insert(connector->connectorId(), connector->metadata());
  return std::make_shared<facebook::axiom::connector::ConnectorContext>(
      "pyspark-test",
      "test",
      facebook::axiom::connector::ConnectorProperties{},
      facebook::axiom::connector::ConnectorContext::noopStatWriterProvider(),
      std::move(connectorRegistry),
      std::move(metadataRegistry));
}

} // namespace axiom::collagen::test
