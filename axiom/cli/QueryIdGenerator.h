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

#include <mutex>
#include <string>

namespace axiom::cli {

/// Generates unique query IDs in the format
/// `YYYYMMdd_HHmmss_NNNNN_suffix`.
///
/// The suffix is a 5-character base-32 string randomly generated at
/// construction time. The 5-digit counter increments monotonically across
/// calls and resets to 0 on UTC day rollover. If the counter exceeds 99,999,
/// `createNextQueryId()` blocks until the next second to avoid overflow. The
/// timestamp portion reflects the wall-clock time of each call.
class QueryIdGenerator {
 public:
  QueryIdGenerator();

  /// Returns the 5-character base-32 query ID suffix.
  const std::string& suffix() const {
    return suffix_;
  }

  /// Generates the next query ID. Thread-safe.
  std::string createNextQueryId();

 private:
  // 5-character base-32 suffix appended to every generated ID.
  std::string suffix_;
  // Day of the last generated ID, used to detect day rollover.
  int64_t lastTimeInDays_{0};
  // Second of the last generated ID, used to cache the timestamp string.
  int64_t lastTimeInSeconds_{0};
  // Cached formatted timestamp for the current second.
  std::string lastTimestamp_;
  // Monotonically incrementing counter within the current day.
  uint32_t counter_{0};
  // Protects all mutable state for thread safety.
  std::mutex mutex_;
};

} // namespace axiom::cli
