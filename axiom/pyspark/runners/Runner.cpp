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

#include "axiom/pyspark/runners/Runner.h"
#include "axiom/pyspark/Exception.h"

namespace axiom::collagen::runner {
namespace {

std::unordered_map<std::string, TRunnerFactory>& runnerFactories() {
  static std::unordered_map<std::string, TRunnerFactory> factories;
  return factories;
}

} // namespace

Runner::Runner(
    const std::string& runId,
    ::facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
    ::facebook::axiom::optimizer::FinishWrite finishWrite,
    std::shared_ptr<::facebook::velox::memory::MemoryPool> pool)
    : runId_(runId),
      plan_(std::move(plan)),
      finishWrite_(std::move(finishWrite)),
      rootPool_(std::move(pool)) {
  COLLAGEN_CHECK_NOT_NULL(rootPool_, "Memory pool cannot be null");
  COLLAGEN_CHECK_NOT_NULL(plan_, "Plan cannot be null");
}

void registerRunnerFactory(const std::string runnerId, TRunnerFactory factory) {
  bool status = runnerFactories().insert({runnerId, factory}).second;
  COLLAGEN_CHECK(
      status, "RunnerFactory with name '{}' is already registered.", runnerId);
}

std::unique_ptr<Runner> buildRunner(
    const std::string runnerId,
    const std::string& runId,
    ::facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
    ::facebook::axiom::optimizer::FinishWrite finishWrite,
    std::shared_ptr<::facebook::velox::memory::MemoryPool> pool) {
  auto it = runnerFactories().find(runnerId);
  VELOX_CHECK(
      it != runnerFactories().end(),
      "RunnerFactory with name '{}' not registered.",
      runnerId);
  return it->second(
      runId, std::move(plan), std::move(finishWrite), std::move(pool));
}

} // namespace axiom::collagen::runner
