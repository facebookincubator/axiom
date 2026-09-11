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

#include <folly/ScopeGuard.h>

#include "axiom/connectors/ConnectorMetadata.h"

namespace facebook::axiom::connector {

ConnectorContext::ConnectorContext(
    std::string queryId,
    std::string user,
    ConnectorProperties properties,
    ConnectorStatWriterProvider statWriterProvider)
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

ConnectorSessionPtr ConnectorContext::sessionFor(
    const ConnectorMetadata& connector) {
  const auto& connectorId = connector.connectorId();

  // Read-mostly after the first call per connector, so the hit path takes a
  // shared lock and allocates nothing.
  auto entry = sessions_.withRLock([&](const auto& sessions) {
    const auto it = sessions.find(connectorId);
    return it == sessions.end() ? nullptr : it->second;
  });
  if (!entry) {
    auto fresh = std::make_shared<Entry>();
    entry = sessions_.withWLock([&](auto& sessions) {
      return sessions.try_emplace(connectorId, std::move(fresh)).first->second;
    });
  }

  VELOX_CHECK(
      entry->builder.load(std::memory_order_relaxed) !=
          std::this_thread::get_id(),
      "Connector requested a session while building its query state: {}",
      connectorId);

  // Concurrent callers wait here rather than on the map, so a connector doing
  // remote work in makeQueryState holds up only its own session.
  folly::call_once(entry->once, [&] {
    // Armed for the whole build, not just makeQueryState: the writer provider
    // is supplied by the application and is under the same prohibition, and a
    // re-entrant call that got past the check above would deadlock here.
    entry->builder.store(std::this_thread::get_id(), std::memory_order_relaxed);
    SCOPE_EXIT {
      entry->builder.store(std::thread::id{}, std::memory_order_relaxed);
    };

    // makeQueryState runs last. call_once does not latch on an exception, so a
    // throw re-runs the whole builder; ordering it last keeps a failure the
    // context itself caused from re-entering the connector's state-building.
    auto statsWriter = statWriterProvider_(connectorId);
    VELOX_CHECK_NOT_NULL(
        statsWriter, "Stat writer provider returned null for {}", connectorId);
    auto session = std::make_shared<ConnectorSession>(
        queryId_,
        user_,
        propertiesFor(properties_, connectorId),
        std::move(statsWriter));

    if (auto state = connector.makeQueryState(*session)) {
      session->initializeQueryState(std::move(state));
    }

    entry->metadata = &connector;
    entry->session = std::move(session);
  });

  VELOX_CHECK(
      entry->metadata == &connector,
      "Two ConnectorMetadata instances report the same connector id: {}",
      connectorId);

  return entry->session;
}

} // namespace facebook::axiom::connector
