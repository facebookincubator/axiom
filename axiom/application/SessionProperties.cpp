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
#include "axiom/application/SessionProperties.h"

#include "velox/functions/prestosql/PrestoQueryConfig.h"

namespace facebook::axiom::application {

std::unordered_map<std::string, std::string> SessionProperties::queryConfig(
    const SessionConfig& session,
    std::string_view executionPrefix) {
  const auto executionValues = session.effectiveValues(executionPrefix);
  std::unordered_map<std::string, std::string> config(
      executionValues.begin(), executionValues.end());
  using velox::functions::prestosql::PrestoQueryConfig;
  for (const auto& [name, value] :
       session.effectiveValues(PrestoQueryConfig::kPrefix)) {
    config[PrestoQueryConfig::qualify(name)] = value;
  }
  return config;
}

connector::ConnectorProperties SessionProperties::connectorProperties(
    const SessionConfig& session,
    const std::vector<std::string>& connectorIds) {
  connector::ConnectorProperties result;
  for (const auto& id : connectorIds) {
    connector::Properties properties = session.effectiveValues(id);
    if (!properties.empty()) {
      result.emplace(id, std::move(properties));
    }
  }
  return result;
}

std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
SessionProperties::connectorConfigs(
    const SessionConfig& session,
    const std::vector<std::string>& connectorIds) {
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      result;
  for (auto& [id, properties] : connectorProperties(session, connectorIds)) {
    result.emplace(
        id,
        std::make_shared<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>(
                properties.begin(), properties.end())));
  }
  return result;
}

} // namespace facebook::axiom::application
