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

#include "axiom/cli/QueryIdGenerator.h"
#include <fmt/format.h>
#include <chrono>
#include <random>
#include <thread>

namespace axiom::cli {

// Base-32 alphabet: a-z, 2-9, excluding l, o, 0, 1.
constexpr char kBase32[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
                            'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
                            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                            '2', '3', '4', '5', '6', '7', '8', '9'};

namespace {

std::string getTimestamp(
    const std::chrono::system_clock::time_point& timePoint) {
  time_t rawTime = std::chrono::system_clock::to_time_t(timePoint);
  char buffer[20];
  struct tm gmTimeResult{};
  gmtime_r(&rawTime, &gmTimeResult);
  strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &gmTimeResult);
  return buffer;
}

} // namespace

QueryIdGenerator::QueryIdGenerator()
    : lastTimeInDays_{0}, lastTimeInSeconds_{0}, counter_{0} {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 31);
  for (int i = 0; i < 5; ++i) {
    suffix_ += kBase32[dis(gen)];
  }
}

std::string QueryIdGenerator::createNextQueryId() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Only generate 100,000 IDs per day.
  if (counter_ > 99'999) {
    // Wait for the second to roll over.
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() == lastTimeInSeconds_) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    counter_ = 0;
  }

  // Refresh timestamp when the second changes.
  auto now = std::chrono::system_clock::now();
  auto nowInSeconds =
      std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch())
          .count();
  if (nowInSeconds != lastTimeInSeconds_) {
    lastTimeInSeconds_ = nowInSeconds;
    lastTimestamp_ = getTimestamp(now);

    // Reset counter on day rollover.
    auto nowInDays =
        std::chrono::duration_cast<std::chrono::days>(now.time_since_epoch())
            .count();
    if (nowInDays != lastTimeInDays_) {
      lastTimeInDays_ = nowInDays;
      counter_ = 0;
    }
  }

  return fmt::format("{}_{:05d}_{}", lastTimestamp_, counter_++, suffix_);
}

} // namespace axiom::cli
