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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Eliminates `Apply` nodes from the tree IR.
class DecorrelatePass {
 public:
  /// Returns `root` with every `Apply` eliminated. Runs after `TranslatePass`
  /// and before any phase that doesn't understand `Apply`. Per-Apply iterative
  /// driver: peels body operators one at a time until terminus (correlation
  /// columns empty), then converts Apply → Join (with optional ESR wrap). Built
  /// up incrementally; out-of-scope sub-cases throw `VELOX_NYI` with a clear
  /// message rather than silently producing wrong results.
  static NodeCP run(NodeCP root, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
