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

#include "axiom/logical_plan/Expr.h"

namespace facebook::axiom::optimizer {

class ConstantExprEvaluator {
 public:
  /// Evaluates a constant expression and returns the result as a Variant.
  /// Throws if expression is not constant or non-deterministic or if an error
  /// occurred during evaluation (e.g. evaluating "5 / 0" expression fails with
  /// division by zero error).
  static velox::Variant evaluateConstantExpr(const logical_plan::Expr& expr);
};
} // namespace facebook::axiom::optimizer
