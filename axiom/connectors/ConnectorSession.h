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

namespace facebook::axiom::connector {

/// Read-only query-specific information.
class ConnectorSession final {
 public:
  explicit ConnectorSession(std::string queryId)
      : queryId_{std::move(queryId)} {}

  const std::string& queryId() const {
    return queryId_;
  }

 private:
  const std::string queryId_;
};

using ConnectorSessionPtr = std::shared_ptr<ConnectorSession>;

} // namespace facebook::axiom::connector
