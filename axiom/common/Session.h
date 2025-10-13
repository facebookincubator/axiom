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
#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom {

/// Read-only query-specific information.
class Session final {
 public:
  Session(std::string queryId) : queryId_{queryId} {}

  const std::string& queryId() const {
    return queryId_;
  }

  connector::ConnectorSessionPtr toConnectorSession(
      std::string_view connectorId) const;

 private:
  const std::string queryId_;
};

using SessionPtr = std::shared_ptr<Session>;

} // namespace facebook::axiom
