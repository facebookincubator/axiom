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

#include "axiom/cli/Timing.h"
#include <sstream>
#include "velox/common/base/SuccinctPrinter.h"

using namespace facebook::velox;

namespace axiom::cli {

std::string Timing::toString() const {
  double pct = 0;
  if (micros > 0) {
    pct = 100 * (userNanos + systemNanos) / (micros * 1000);
  }

  std::stringstream out;
  out << succinctNanos(micros * 1000) << " / " << succinctNanos(userNanos)
      << " user / " << succinctNanos(systemNanos) << " system (" << pct << "%)";
  return out.str();
}

} // namespace axiom::cli
