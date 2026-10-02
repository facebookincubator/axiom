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

#include "axiom/connectors/ConnectorContext.h"
#include "axiom/connectors/tests/TestConnector.h"

namespace axiom::collagen::test {

/// Builds isolated connector contexts for Spark-to-Axiom converter tests.
class SparkToAxiomTestContext {
 public:
  /// Returns an isolated context containing the fixture's connector and
  /// matching metadata.
  static facebook::axiom::connector::ConnectorContextPtr createConnectorContext(
      const std::shared_ptr<facebook::axiom::connector::TestConnector>&
          connector);
};

} // namespace axiom::collagen::test
