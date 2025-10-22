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

#include <optional>
#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::axiom::optimizer {

// Wrapper around a vector of expressions which
// evaluates hasWindows effectively.
// to support inner invariants, this class returns const references
// to the expression vector.
class ExprsWrapper {
 public:
  ExprsWrapper& operator=(ExprVector exprs);

  void push_back(ExprCP expr);

  void clear();

  bool hasWindows() const;

  // Implicit conversion to const ExprVector&
  operator const ExprVector&() const {
    return exprs_;
  }

  ExprCP operator[](size_t index) const {
    VELOX_DCHECK_LT(index, exprs_.size());
    return exprs_[index];
  }

  ExprCP at(size_t index) const {
    return exprs_.at(index);
  }

  size_t size() const {
    return exprs_.size();
  }

  auto begin() const {
    return exprs_.begin();
  }

  auto end() const {
    return exprs_.end();
  }

  bool empty() const {
    return exprs_.empty();
  }

 private:
  ExprVector exprs_;
  mutable std::optional<bool> hasWindows_;
};

} // namespace facebook::axiom::optimizer
