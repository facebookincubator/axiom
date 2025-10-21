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

#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {

class RelationOpPrinter {
 public:
  /// Returns a multi-line text representatino of the plan tree.
  static std::string toText(const RelationOp& root);

  /// Returns a one-line summary of the plan. Includes tables and joins. Useful
  /// for debugging join order.
  static std::string toOneline(const RelationOp& root);
};

} // namespace facebook::axiom::optimizer
