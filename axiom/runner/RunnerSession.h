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
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "axiom/session/BaseSession.h"

namespace facebook::axiom::runner {

/// Runner-scoped property bag.
using Properties = connector::Properties;

/// What the runner is given for one query: the query's context and its writer,
/// plus the runner's slice of the query's session properties.
class RunnerSession final : public BaseSession {
 public:
  RunnerSession(
      connector::ConnectorContextPtr context,
      std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter,
      Properties properties)
      : BaseSession(
            std::move(context),
            std::move(statsWriter),
            std::move(properties)) {}
};

using RunnerSessionPtr = std::shared_ptr<RunnerSession>;

} // namespace facebook::axiom::runner
