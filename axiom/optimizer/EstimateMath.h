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

#include <algorithm>
#include <limits>
#include <optional>

/// Arithmetic over optional statistical estimates (cardinality, fanout, ...).
/// Unknown (nullopt) propagates: if any operand is unknown, the result is
/// unknown. This keeps callers from silently treating a missing estimate as a
/// concrete value. Use these instead of unwrapping at estimate-combining sites.
///
/// Results saturate to the finite float range: a chain of estimate operations
/// (e.g. the cardinality of many cross joins) can exceed what a float holds,
/// and a non-finite estimate breaks downstream consumers. Saturating keeps a
/// pathological-but-valid query plannable instead of overflowing to infinity.
namespace facebook::axiom::optimizer {

/// Clamps a value that overflowed to +/- infinity back to the finite range.
/// NaN is intentionally not handled: it cannot arise from the finite-operand
/// arithmetic here, so a NaN estimate signals a bug upstream and must reach the
/// downstream isfinite() check rather than being masked by a fabricated value.
inline float saturate(float value) {
  if (value > std::numeric_limits<float>::max()) {
    return std::numeric_limits<float>::max();
  }
  if (value < std::numeric_limits<float>::lowest()) {
    return std::numeric_limits<float>::lowest();
  }
  return value;
}

/// Multiplication.
inline std::optional<float> mul(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value()) {
    return saturate(*a * *b);
  }
  return std::nullopt;
}

/// Addition.
inline std::optional<float> add(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value()) {
    return saturate(*a + *b);
  }
  return std::nullopt;
}

/// Subtraction.
inline std::optional<float> subtract(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value()) {
    return saturate(*a - *b);
  }
  return std::nullopt;
}

/// Division. nullopt if either operand is unknown or the divisor is 0.
inline std::optional<float> divide(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value() && *b != 0) {
    return saturate(*a / *b);
  }
  return std::nullopt;
}

/// Named maxOf/minOf rather than max/min to avoid ambiguity with std::max/min
/// under ADL (the operands are std::optional).
inline std::optional<float> maxOf(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value()) {
    return std::max(*a, *b);
  }
  return std::nullopt;
}

inline std::optional<float> minOf(
    std::optional<float> a,
    std::optional<float> b) {
  if (a.has_value() && b.has_value()) {
    return std::min(*a, *b);
  }
  return std::nullopt;
}

/// True only when both operands are known and a < b. Unknown operands are not
/// comparable, so the result is false (an unknown estimate is never the
/// smaller). Use instead of std::optional's operator<, which treats nullopt as
/// the smallest value.
inline bool lessThan(std::optional<float> a, std::optional<float> b) {
  return a.has_value() && b.has_value() && *a < *b;
}

} // namespace facebook::axiom::optimizer
