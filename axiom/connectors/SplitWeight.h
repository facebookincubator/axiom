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

#include <cstdint>

namespace facebook::axiom::connector {

/// Defines the canonical scale for ConnectorSplit::splitWeight values used
/// by Axiom-based engines. One full-sized "standard" split has weight
/// kStandard. Connectors are expected to assign each split a weight in
/// [kMin, kStandard], roughly proportional to how much work the split
/// represents (typically size in bytes). The scheduler treats the sum of
/// weights across pending splits as its work-load proxy, so a connector
/// that emits weight 0 for every split makes the sum useless and the
/// scheduler falls back to a count-based cap.
class SplitWeight {
 public:
  /// Represents the weight of a single full-sized standard split. Mirrors
  /// the UNIT_VALUE constant in com.facebook.presto.spi.SplitWeight so
  /// that values flowing from Presto-compatible producers retain their
  /// meaning.
  static constexpr int64_t kStandard = 100;

  /// Specifies the smallest legal weight. Tiny splits clamp up to this so
  /// they remain visible to the scheduler instead of collapsing to zero.
  static constexpr int64_t kMin = 1;

  /// Maps a fractional weight in (0, 1] onto the canonical integer scale
  /// [kMin, kStandard]. Rounds up so very small fractions don't degenerate
  /// to 0. Throws if 'fraction' is not finite, not positive, or exceeds 1.
  static int64_t normalize(double fraction);

  /// Weights a split by its size relative to a standard split size,
  /// clamped to a configured minimum fraction. Returns kStandard for
  /// splits at or above standardSize. 'standardSize' must be positive,
  /// 'minFraction' must be in (0, 1].
  static int64_t
  forSize(uint64_t splitSize, uint64_t standardSize, double minFraction);
};

} // namespace facebook::axiom::connector
