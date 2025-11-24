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

namespace facebook::axiom::optimizer {

/// Computes a saturating product using rational saturation function.
/// The result behaves like multiplication when far from max, but
/// asymptotically approaches max as the product increases.
///
/// Formula: max * P / (max + P), where P is the product of all numbers
///
/// @param max The maximum value to asymptotically approach
/// @param numbers Pointer to array of numbers to multiply
/// @param numNumbers Number of elements in the array
/// @return The saturating product value
double saturatingProduct(double max, const double* numbers, int32_t numNumbers);

} // namespace facebook::axiom::optimizer
