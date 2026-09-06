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

#include "axiom/cli/common/QueryRuntimeStats.h"

#include <fmt/format.h>

#include <algorithm>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom {

namespace {

void checkId(std::string_view id) {
  VELOX_CHECK(!id.empty(), "Stats id must not be empty");
  VELOX_CHECK_EQ(
      id.find('/'),
      std::string_view::npos,
      "Stats id must not contain '/': {}",
      id);
}

} // namespace

std::string QueryRuntimeStats::qualifiedKey(
    std::string_view id,
    std::string_view name,
    KeySeparator separator) {
  if (separator == KeySeparator::kSlash) {
    return fmt::format("{}/{}", id, name);
  }
  std::string path(id);
  std::replace(path.begin(), path.end(), '/', '-');
  return fmt::format("{}-{}", path, name);
}

velox::BaseRuntimeStatWriter& QueryRuntimeStats::writerFor(
    std::string_view componentId) {
  checkId(componentId);
  VELOX_CHECK_NE(
      componentId,
      kConnectorNamespace,
      "Component id must not be '{}'",
      kConnectorNamespace);
  return writerForId(componentId);
}

velox::BaseRuntimeStatWriter& QueryRuntimeStats::writerForConnector(
    std::string_view connectorId) {
  checkId(connectorId);
  return writerForId(fmt::format("{}/{}", kConnectorNamespace, connectorId));
}

velox::BaseRuntimeStatWriter& QueryRuntimeStats::writerForId(
    std::string_view id) {
  const std::string key(id);
  // Every call after the first finds the bucket, so that path takes the read
  // lock and concurrent recorders do not serialize on a lookup.
  if (auto* found = buckets_.withRLock(
          [&](const auto& map) -> velox::ConcurrentRuntimeStatWriter* {
            auto it = map.find(key);
            return it == map.end() ? nullptr : it->second.get();
          })) {
    return *found;
  }
  // The writer is built before the bucket goes in, so a throw leaves no entry
  // behind for a later read to dereference. Re-check under the write lock: a
  // racing caller may have inserted 'key' since the read lock was released.
  return *buckets_.withWLock([&](auto& map) {
    if (auto it = map.find(key); it != map.end()) {
      return it->second.get();
    }
    auto writer = std::make_unique<velox::ConcurrentRuntimeStatWriter>();
    auto* inserted = writer.get();
    map.emplace(key, std::move(writer));
    return inserted;
  });
}

std::optional<velox::RuntimeMetric> QueryRuntimeStats::find(
    std::string_view id,
    std::string_view name) const {
  const std::string key(id);
  auto* writer = buckets_.withRLock(
      [&](const auto& map) -> const velox::ConcurrentRuntimeStatWriter* {
        auto it = map.find(key);
        return it == map.end() ? nullptr : it->second.get();
      });
  if (writer == nullptr) {
    return std::nullopt;
  }
  const auto metrics = writer->runtimeStats();
  auto it = metrics.find(std::string(name));
  if (it == metrics.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::unordered_map<std::string, velox::RuntimeMetric> QueryRuntimeStats::toMap(
    KeySeparator separator) const {
  std::unordered_map<std::string, velox::RuntimeMetric> result;
  buckets_.withRLock([&](const auto& map) {
    for (const auto& [id, writer] : map) {
      for (const auto& [name, metric] : writer->runtimeStats()) {
        result.emplace(qualifiedKey(id, name, separator), metric);
      }
    }
  });
  return result;
}

} // namespace facebook::axiom
