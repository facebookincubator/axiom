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

#include "axiom/session/BaseSession.h"
#include "axiom/sql/presto/ParserOptions.h"

namespace axiom::sql::presto {

/// What the parser is given for one query: the query's context and its writer,
/// plus the typed ParserOptions parsed from the parser's slice of the query's
/// session properties.
class ParserSession final : public facebook::axiom::BaseSession {
 public:
  ParserSession(
      facebook::axiom::connector::ConnectorContextPtr context,
      std::shared_ptr<facebook::velox::BaseRuntimeStatWriter> statsWriter,
      facebook::axiom::connector::Properties properties,
      ParserOptions options)
      : BaseSession(
            std::move(context),
            std::move(statsWriter),
            std::move(properties)),
        options_{std::move(options)} {}

  const ParserOptions& options() const {
    return options_;
  }

 private:
  const ParserOptions options_;
};

using ParserSessionPtr = std::shared_ptr<ParserSession>;

} // namespace axiom::sql::presto
