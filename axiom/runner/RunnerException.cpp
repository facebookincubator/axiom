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

#include "axiom/runner/RunnerException.h"

namespace facebook::axiom::runner {

namespace {
const auto& runnerErrorKindNames() {
  static const folly::F14FastMap<RunnerErrorKind, std::string_view> kNames = {
      {RunnerErrorKind::kQueryTimedOut, "QUERY_TIMED_OUT"},
      {RunnerErrorKind::kCancelled, "CANCELLED"},
      {RunnerErrorKind::kExecutionError, "EXECUTION_ERROR"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(RunnerErrorKind, runnerErrorKindNames);

RunnerException::RunnerException(
    RunnerErrorKind kind,
    std::string message,
    std::string messageTemplate)
    : kind_{kind},
      message_{std::move(message)},
      messageTemplate_{std::move(messageTemplate)},
      formatted_{formatWhat(kind_, message_)} {}

} // namespace facebook::axiom::runner
