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

#include "axiom/runner/ProgressReporter.h"

#include <fmt/core.h>
#include <folly/executors/FunctionScheduler.h>
#include <glog/logging.h>
#include <algorithm>
#include <atomic>

#include "axiom/runner/QueryProgressBuilder.h"
#include "axiom/runner/Runner.h"

namespace facebook::axiom::runner {
namespace {

// Process-wide source of unique scheduler function names. Shared schedulers run
// one function per registered query, so the names must not collide.
std::string nextName() {
  static std::atomic<uint64_t> counter{0};
  return fmt::format("axiom.progress.{}", counter.fetch_add(1));
}

} // namespace

ProgressReporter::ProgressReporter(
    Runner& runner,
    folly::FunctionScheduler& scheduler,
    std::string queryId,
    QueryProgressCallback onProgress,
    int32_t intervalMs)
    : runner_(runner),
      scheduler_(scheduler),
      queryId_(std::move(queryId)),
      onProgress_(std::move(onProgress)),
      startTime_(std::chrono::steady_clock::now()) {
  if (!onProgress_) {
    return;
  }
  stages_ = QueryProgressBuilder::toStageTopology(runner_.fragments());
  const auto interval = std::chrono::milliseconds(
      std::max(intervalMs, kMinProgressReportIntervalMs));
  auto name = nextName();
  scheduler_.addFunction([this]() { poll(); }, interval, name);
  // name_ records that a poll is registered, so the destructor knows whether to
  // cancel one; set it only after addFunction registers the poll.
  name_ = std::move(name);
}

ProgressReporter::~ProgressReporter() {
  // name_ is set only after the poll is registered, so an empty name means
  // there is nothing to tear down (no callback, or registration never
  // happened).
  if (name_.empty()) {
    return;
  }
  // Blocks until the poller is no longer touching this query, so the final
  // report below is single-threaded and safe even past kRunning.
  scheduler_.cancelFunctionAndWait(name_);
  // This destructor may run while the stack unwinds from a query failure, so it
  // must not throw: swallow any error from building or delivering the final
  // report (including a builder VELOX_CHECK on a teardown-time stats snapshot).
  try {
    const auto wallMicros =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - startTime_)
            .count();
    const bool finished = runner_.state() == Runner::State::kFinished;
    onProgress_(
        QueryProgressBuilder::computeProgress(
            stages_, runner_.stats(), queryId_, wallMicros, finished));
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Final progress report failed: " << ex.what();
  }
}

void ProgressReporter::poll() {
  if (runner_.state() != Runner::State::kRunning) {
    return;
  }
  const auto wallMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - startTime_)
                              .count();
  reportProgress(
      QueryProgressBuilder::computeProgress(
          stages_, runner_.stats(), queryId_, wallMicros, /*finished=*/false));
}

void ProgressReporter::reportProgress(const QueryProgress& progress) {
  try {
    onProgress_(progress);
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Progress report failed: " << ex.what();
  }
}

} // namespace facebook::axiom::runner
