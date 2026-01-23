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

#include <sys/resource.h>
#include <functional>
#include <string>
#include "folly/ScopeGuard.h"
#include "velox/common/time/Timer.h"

namespace axiom::cli {

/// Timing information for a statement execution.
struct Timing {
  uint64_t micros{0};
  uint64_t userNanos{0};
  uint64_t systemNanos{0};

  std::string toString() const;
};

/// Measures wall-clock and CPU time for a function execution.
/// @param func The function to execute and time.
/// @param timing Output parameter that will be filled with timing information.
/// @return The result of calling func().
template <typename T>
T time(const std::function<T()>& func, Timing& timing) {
  struct rusage start{};
  getrusage(RUSAGE_SELF, &start);
  SCOPE_EXIT {
    struct rusage end{};
    getrusage(RUSAGE_SELF, &end);
    auto tvNanos = [](struct timeval tv) {
      return tv.tv_sec * 1'000'000'000 + tv.tv_usec * 1'000;
    };
    timing.userNanos = tvNanos(end.ru_utime) - tvNanos(start.ru_utime);
    timing.systemNanos = tvNanos(end.ru_stime) - tvNanos(start.ru_stime);
  };

  facebook::velox::MicrosecondTimer timer(&timing.micros);
  return func();
}

} // namespace axiom::cli
