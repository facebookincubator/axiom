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
#include "axiom/optimizer/ExprsWrapper.h"

#include <algorithm>
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

ExprsWrapper& ExprsWrapper::operator=(ExprVector exprs) {
  exprs_ = std::move(exprs);
  hasWindows_.reset();
  return *this;
}

void ExprsWrapper::push_back(ExprCP expr) {
  exprs_.push_back(expr);
  if (expr->containsWindow()) {
    hasWindows_ = true;
  }
}

void ExprsWrapper::clear() {
  exprs_.clear();
  hasWindows_.reset();
}

bool ExprsWrapper::hasWindows() const {
  if (hasWindows_) {
    return *hasWindows_;
  }

  hasWindows_ = std::ranges::any_of(
      exprs_, [](ExprCP expr) { return expr->containsWindow(); });
  return *hasWindows_;
}

} // namespace facebook::axiom::optimizer
