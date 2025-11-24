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

#include "axiom/optimizer/StatsUtils.h"

namespace facebook::axiom::optimizer {

double
saturatingProduct(double max, const double* numbers, int32_t numNumbers) {
  // Compute the product of all numbers
  double product = 1.0;
  for (int32_t i = 0; i < numNumbers; ++i) {
    product *= numbers[i];
  }

  // Apply rational saturation function: max * P / (max + P)
  return max * product / (max + product);
}

} // namespace facebook::axiom::optimizer
