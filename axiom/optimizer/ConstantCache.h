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

#include <folly/container/F14Map.h>
#include <memory>
#include "axiom/logical_plan/Expr.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::optimizer {

/// Interns Literal nodes by (Type, Variant stored bytes), bypassing any
/// custom comparator the type provides.
class ConstantCache {
 public:
  /// Returns the interned Literal for `constant`, creating one on first
  /// sight. Two ConstantExprs whose stored bytes match (after recursing
  /// into ARRAY / ROW / MAP) collapse to the same Literal; otherwise the
  /// returned pointers are distinct.
  ExprCP get(const logical_plan::ConstantExpr& constant);

 private:
  struct Key {
    const velox::Type* type;
    std::shared_ptr<const velox::Variant> value;
  };

  struct KeyHash {
    size_t operator()(const Key& key) const;
  };

  struct KeyEqual {
    bool operator()(const Key& lhs, const Key& rhs) const;
  };

  folly::F14FastMap<Key, ExprCP, KeyHash, KeyEqual> entries_;
};

} // namespace facebook::axiom::optimizer
