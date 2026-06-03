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

#include "axiom/connectors/ConnectorSession.h"

namespace facebook::axiom::connector {

std::optional<std::string> ConnectorSession::property(
    std::string_view name) const {
  auto found = properties_.find(name);
  if (found == properties_.end()) {
    return std::nullopt;
  }
  return found->second;
}

} // namespace facebook::axiom::connector
