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

#include "axiom/connectors/ConnectorContext.h"

namespace facebook::axiom::connector {

/// Returns a context for a test that records no metrics: every connector gets
/// a discarding writer.
inline ConnectorContextPtr makeTestContext(
    std::string_view queryId,
    std::string_view user = "test",
    ConnectorProperties properties = {}) {
  return std::make_shared<ConnectorContext>(
      std::string{queryId},
      std::string{user},
      std::move(properties),
      discardingStatWriters());
}

/// Returns a writer for a test component or connector that records no metrics.
inline std::shared_ptr<velox::BaseRuntimeStatWriter> makeTestStatWriter() {
  return std::make_shared<velox::NoopRuntimeStatWriter>();
}

} // namespace facebook::axiom::connector
