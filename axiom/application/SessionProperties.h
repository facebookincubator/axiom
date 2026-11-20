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
#include <unordered_map>
#include <vector>

#include "axiom/common/SessionConfig.h"
#include "axiom/connectors/ConnectorSession.h"
#include "velox/common/config/Config.h"

namespace facebook::axiom::application {

/// Reads an application's session properties back as the configuration each
/// component takes. An application registers the providers it uses in a
/// ConfigRegistry, under prefixes of its choosing, and passes those prefixes
/// and connector ids here.
///
///   SessionConfig session{registry};
///   session.set("execution.spill_enabled", "true");
///   velox::core::QueryConfig config{
///       SessionProperties::queryConfig(session, "execution")};
class SessionProperties {
 public:
  /// Returns the Velox QueryConfig values: the properties under
  /// 'executionPrefix', and the Presto function properties under the keys
  /// PrestoQueryConfig reads.
  static std::unordered_map<std::string, std::string> queryConfig(
      const SessionConfig& session,
      std::string_view executionPrefix);

  /// Returns the properties of each of 'connectorIds' that has any, for a
  /// ConnectorContext.
  static connector::ConnectorProperties connectorProperties(
      const SessionConfig& session,
      const std::vector<std::string>& connectorIds);

  /// Returns the properties of each of 'connectorIds' that has any, as the
  /// per-connector configs of a Velox QueryCtx.
  static std::
      unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs(
          const SessionConfig& session,
          const std::vector<std::string>& connectorIds);
};

} // namespace facebook::axiom::application
