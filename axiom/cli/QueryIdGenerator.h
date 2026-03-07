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
/// The suffix is a randomly generated 5-character base-32 string created at
/// construction time using `std::random_device` and `std::mt19937`. The
/// counter resets at day boundaries or after 99,999 IDs within a single
/// second.
class QueryIdGenerator {
 public:
  QueryIdGenerator();

  /// Returns the randomly generated 5-character base-32 query ID suffix.
  std::string suffix() const {
    return suffix_;
  }

  /// Generates the next query ID. Thread-safe.
  std::string createNextQueryId();

 private:
  std::string suffix_;
  int64_t lastTimeInDays_;
  int64_t lastTimeInSeconds_;
  std::string lastTimestamp_;
  uint32_t counter_;
  std::mutex mutex_;
};

} // namespace axiom::cli
