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

#include <folly/Executor.h>
#include <folly/Function.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::axiom {
class QueryRuntimeStats;
} // namespace facebook::axiom

namespace facebook::axiom {

/// Wraps an executor to measure callback wait time and execution time. All
/// tasks submitted via add() are delegated to the underlying executor after
/// being instrumented.
class TrackedExecutor final : public folly::Executor {
  using Func = folly::Function<void()>;

 public:
  static constexpr std::string_view kExecutorWaitNanos{"executorWaitNanos"};
  static constexpr std::string_view kExecutorExecutionWallNanos{
      "executorExecutionWallNanos"};
  static constexpr std::string_view kExecutorExecutionCpuNanos{
      "executorExecutionCpuNanos"};

  explicit TrackedExecutor(folly::Executor::KeepAlive<> executor)
      : executor_{std::move(executor)} {
    VELOX_CHECK(executor_);
  }

  TrackedExecutor(const TrackedExecutor&) = delete;
  TrackedExecutor& operator=(const TrackedExecutor&) = delete;
  TrackedExecutor(TrackedExecutor&&) = delete;
  TrackedExecutor& operator=(TrackedExecutor&&) = delete;

  void add(Func func) override {
    executor_->add(wrapFunc(std::move(func)));
  }

  void addWithPriority(Func func, int8_t priority) override {
    executor_->addWithPriority(wrapFunc(std::move(func)), priority);
  }

  uint8_t getNumPriorities() const override {
    return executor_->getNumPriorities();
  }

  /// Reports accumulated metrics to the given stats, prefixing each metric
  /// name with 'prefix'.
  void reportTo(QueryRuntimeStats& stats, std::string_view prefix) const;

 protected:
  bool keepAliveAcquire() noexcept override {
    return folly::Executor::keepAliveAcquire(executor_.get());
  }
  void keepAliveRelease() noexcept override {
    return folly::Executor::keepAliveRelease(executor_.get());
  }

 private:
  Func wrapFunc(Func func);

  folly::Executor::KeepAlive<> executor_;

  struct Metrics {
    // Not thread safe. Prefer potential inaccuracy over synchronization
    // overhead.
    velox::RuntimeMetric waitTime_{velox::RuntimeCounter::Unit::kNanos};
    velox::RuntimeMetric executionWallTime_{
        velox::RuntimeCounter::Unit::kNanos};
    velox::RuntimeMetric executionCpuTime_{velox::RuntimeCounter::Unit::kNanos};
  };

  const std::shared_ptr<Metrics> metrics_{std::make_shared<Metrics>()};
};

} // namespace facebook::axiom
