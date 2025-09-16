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

#include "axiom/runner/Runner.h"

namespace facebook::axiom::runner {

namespace {
const auto& stateNames() {
  static const folly::F14FastMap<Runner::State, std::string_view> kNames = {
      {Runner::State::kInitialized, "initialized"},
      {Runner::State::kRunning, "running"},
      {Runner::State::kCancelled, "cancelled"},
      {Runner::State::kError, "error"},
      {Runner::State::kFinished, "finished"},
  };

  return kNames;
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Runner, State, stateNames);

} // namespace facebook::axiom::runner
