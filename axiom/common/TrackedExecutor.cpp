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

#include "axiom/common/TrackedExecutor.h"

#include "axiom/common/QueryRuntimeStats.h"
#include "velox/common/time/CpuWallTimer.h"

namespace facebook::axiom {

TrackedExecutor::Func TrackedExecutor::wrapFunc(Func func) {
  auto enqueueTime = std::chrono::steady_clock::now();
  return [func = std::move(func), enqueueTime, metrics = metrics_]() mutable {
    metrics->waitTime_.addValue(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - enqueueTime)
            .count());
    velox::CpuWallTiming timing;
    {
      velox::DeltaCpuWallTimer timer(
          [&timing](const velox::CpuWallTiming& delta) { timing = delta; });
      func();
    }
    metrics->executionWallTime_.addValue(timing.wallNanos);
    metrics->executionCpuTime_.addValue(timing.cpuNanos);
  };
}

void TrackedExecutor::reportTo(
    QueryRuntimeStats& stats,
    std::string_view prefix) const {
  stats.merge(
      fmt::format("{}-{}", prefix, kExecutorWaitNanos), metrics_->waitTime_);
  stats.merge(
      fmt::format("{}-{}", prefix, kExecutorExecutionWallNanos),
      metrics_->executionWallTime_);
  stats.merge(
      fmt::format("{}-{}", prefix, kExecutorExecutionCpuNanos),
      metrics_->executionCpuTime_);
}

} // namespace facebook::axiom
