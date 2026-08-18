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

#include "axiom/connectors/hive/HiveMetadataConfig.h"

#include <unordered_map>

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"

namespace facebook::axiom::connector::hive {

std::string HiveMetadataConfig::localDataPath() const {
  return config_->get<std::string>(kLocalDataPath, "");
}

std::string HiveMetadataConfig::localFileFormat() const {
  return config_->get<std::string>(kLocalFileFormat, "");
}

bool HiveMetadataConfig::readTimestampPartitionValueAsLocalTime(
    const ConnectorSessionPtr& session) const {
  std::unordered_map<std::string, std::string> sessionProperties;
  if (session != nullptr) {
    const auto addProperty = [&](const std::string& key) {
      if (const auto value = session->property(key)) {
        sessionProperties.emplace(key, std::string(*value));
      }
    };
    const std::string sessionKey = velox::connector::hive::FileConfig::
        kReadTimestampPartitionValueAsLocalTimeSession;
    addProperty(sessionKey);
    addProperty(connectorId_ + "." + sessionKey);
  }

  const velox::config::ConfigBase sessionConfig(std::move(sessionProperties));
  return readTimestampPartitionValueAsLocalTime(&sessionConfig);
}

bool HiveMetadataConfig::readTimestampPartitionValueAsLocalTime(
    const velox::config::ConfigBase* sessionProperties) const {
  const std::string sessionKey = velox::connector::hive::FileConfig::
      kReadTimestampPartitionValueAsLocalTimeSession;

  // HiveConfig reads the unprefixed spelling only. The prefixed spelling is
  // more specific, so it is applied last and wins.
  std::unordered_map<std::string, std::string> normalized;
  if (sessionProperties != nullptr) {
    for (const auto& key : {sessionKey, connectorId_ + "." + sessionKey}) {
      if (const auto value = sessionProperties->get<std::string>(key)) {
        normalized[sessionKey] = *value;
      }
    }
  }

  velox::config::ConfigBase sessionConfig(std::move(normalized));
  return velox::connector::hive::HiveConfig(config_)
      .readTimestampPartitionValueAsLocalTime(&sessionConfig);
}

} // namespace facebook::axiom::connector::hive
