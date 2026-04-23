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

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/vector/ComplexVector.h"

namespace axiom::collagen::runner {

/// API for executing physical plans.
class Runner {
 public:
  Runner(
      const std::string& runId,
      facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
      facebook::axiom::optimizer::FinishWrite finishWrite,
      std::shared_ptr<::facebook::velox::memory::MemoryPool> pool);

  virtual ~Runner() = default;

  virtual std::vector<::facebook::velox::RowVectorPtr> execute(
      const std::unordered_map<std::string, std::string>& queryConfigs =
          {}) = 0;

 protected:
  const std::string runId_;

  facebook::axiom::optimizer::MultiFragmentPlanPtr plan_;

  facebook::axiom::optimizer::FinishWrite finishWrite_;

  std::shared_ptr<::facebook::velox::memory::MemoryPool> rootPool_;
};

using TRunnerFactory = std::function<std::unique_ptr<Runner>(
    const std::string&,
    facebook::axiom::optimizer::MultiFragmentPlanPtr,
    facebook::axiom::optimizer::FinishWrite,
    std::shared_ptr<::facebook::velox::memory::MemoryPool>)>;

/// Registers a factory for a given runnerId.
void registerRunnerFactory(const std::string runnerId, TRunnerFactory factory);

/// Builds a runner using the respective factory method registered using the
/// function above.
std::unique_ptr<Runner> buildRunner(
    const std::string runnerId,
    const std::string& runId,
    facebook::axiom::optimizer::MultiFragmentPlanPtr plan,
    facebook::axiom::optimizer::FinishWrite finishWrite,
    std::shared_ptr<::facebook::velox::memory::MemoryPool> pool);

} // namespace axiom::collagen::runner
