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
#include <utility>

#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/session/BaseSession.h"

namespace facebook::axiom::optimizer {

/// What the optimizer is given for one query: the query's context and its
/// writer, plus the typed OptimizerOptions parsed from the optimizer's slice
/// of the query's session properties.
class OptimizerSession final : public BaseSession {
 public:
  OptimizerSession(
      connector::ConnectorContextPtr context,
      std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter,
      connector::Properties properties,
      OptimizerOptions options)
      : BaseSession(
            std::move(context),
            std::move(statsWriter),
            std::move(properties)),
        options_{std::move(options)} {}

  const OptimizerOptions& options() const {
    return options_;
  }

 private:
  const OptimizerOptions options_;
};

using OptimizerSessionPtr = std::shared_ptr<OptimizerSession>;

} // namespace facebook::axiom::optimizer
