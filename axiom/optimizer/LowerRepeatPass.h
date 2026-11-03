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

#include "axiom/optimizer/Builder.h"

namespace facebook::axiom::optimizer {

/// Replaces each `Repeat` with a `FixedPoint` that walks from the side with
/// fewer estimated rows: the start, or the end when it has fewer and is
/// present. Walking from the start:
///
/// - The anchor is the start, with the walk's current vertex set to the start
///   key and a hop count of 0.
/// - The step joins the latest walks that have fewer than `maxHops` hops with
///   the hop, on the current vertex and the hop's source. The current vertex
///   becomes the hop's destination, and the hop count grows by one.
/// - The result keeps the walks with at least `minHops` hops and joins them
///   with the end on the current vertex and the end key. Without an end, it
///   keeps the walks whose current vertex equals the end key. With `minHops`
///   of 0, the anchor's walks of zero hops are part of the result.
///
/// Walks of at most one hop need no loop: the anchor for zero hops, the side
/// joined with the hop for one, or the UNION ALL of the two.
///
/// Walking from the end is the mirror image, with each hop read from its
/// destination to its source.
///
/// Runs after leaf statistics are read, so the choice of side uses them.
class LowerRepeatPass {
 public:
  static NodeCP run(NodeCP root, Builder& builder);
};

} // namespace facebook::axiom::optimizer
