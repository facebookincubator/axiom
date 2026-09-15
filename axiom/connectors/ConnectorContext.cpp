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

#include "axiom/connectors/ConnectorContext.h"

namespace facebook::axiom::connector {

ConnectorContext::ConnectorContext(
    std::string queryId,
    std::string user,
    ConnectorProperties properties,
    StatWriterProvider statWriterProvider)
    : queryId_{std::move(queryId)},
      user_{std::move(user)},
      properties_{std::move(properties)},
      statWriterProvider_{std::move(statWriterProvider)} {
  VELOX_CHECK(
      statWriterProvider_, "ConnectorContext requires a stat writer provider");
}

namespace {
Properties propertiesFor(
    const ConnectorProperties& properties,
    std::string_view connectorId) {
  const auto it = properties.find(connectorId);
  if (it == properties.end()) {
    return {};
  }
  return it->second;
}
} // namespace

StatWriterProvider ConnectorContext::noopStatWriterProvider() {
  return [](std::string_view) {
    return std::make_shared<velox::NoopRuntimeStatWriter>();
  };
}

ConnectorSessionPtr ConnectorContext::sessionFor(std::string_view connectorId) {
  auto entry = sessions_.withRLock([&](const auto& sessions) {
    const auto it = sessions.find(connectorId);
    return it == sessions.end() ? nullptr : it->second;
  });
  if (!entry) {
    auto fresh = std::make_shared<Entry>();
    entry = sessions_.withWLock([&](auto& sessions) {
      return sessions.try_emplace(std::string{connectorId}, std::move(fresh))
          .first->second;
    });
  }

  folly::call_once(entry->once, [&] {
    // call_once does not latch on an exception, so a throw here leaves the
    // entry unbuilt and the next caller retries it.
    auto statsWriter = statWriterProvider_(connectorId);
    VELOX_CHECK_NOT_NULL(
        statsWriter, "Stat writer provider returned null for {}", connectorId);

    entry->session = std::make_shared<ConnectorSession>(
        queryId_,
        user_,
        propertiesFor(properties_, connectorId),
        std::move(statsWriter));
  });

  return entry->session;
}

} // namespace facebook::axiom::connector
