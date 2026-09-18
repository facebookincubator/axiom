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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::velox::core {
class ExpressionEvaluator;
} // namespace facebook::velox::core

namespace facebook::axiom::connector {
class SchemaResolver;
} // namespace facebook::axiom::connector

namespace facebook::axiom::optimizer {
class OptimizerSession;
class Schema;
} // namespace facebook::axiom::optimizer

namespace facebook::axiom::optimizer::v2 {

/// Negotiates maximal connector-local subtrees and replaces accepted roots
/// with scans over connector-provided virtual tables.
///
/// For example, a connector can replace `Filter(x > 0) -> Scan(t)` with
/// `Scan(virt_t)`. See `PushdownRoot` for the virtual-table contract.
class ConnectorPushdownPass {
 public:
  /// Replaces connector-accepted roots while preserving external column
  /// identities and producing executable scans.
  static NodeCP run(
      NodeCP root,
      Builder& builder,
      const Schema& schema,
      const connector::SchemaResolver& schemaResolver,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator);
};

} // namespace facebook::axiom::optimizer::v2
