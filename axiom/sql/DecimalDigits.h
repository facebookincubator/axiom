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
#include <string>
#include <string_view>

#include "axiom/logical_plan/ExprApi.h"

namespace axiom::sql {

/// The digits of a decimal literal, which give its type: the scale is the
/// number of digits after the dot, and the precision the number of
/// significant digits, at least the scale and at least 1.
///
///   auto digits = DecimalDigits::parse("-12.50");
///   digits->precision(); // 4
///   digits->toExpr();    // DECIMAL(4, 2) literal -12.50
class DecimalDigits {
 public:
  /// Parses digits with an optional sign and at most one dot. Returns nullopt
  /// for any other text. The precision may exceed the maximum of DECIMAL; the
  /// caller checks it.
  static std::optional<DecimalDigits> parse(std::string_view text);

  int32_t precision() const {
    return precision_;
  }

  int32_t scale() const {
    return scale_;
  }

  /// Returns the literal, of type DECIMAL(precision, scale). Requires a
  /// precision of at most LongDecimalType::kMaxPrecision.
  facebook::axiom::logical_plan::ExprApi toExpr() const;

 private:
  DecimalDigits(int32_t precision, int32_t scale, std::string unscaledValue)
      : precision_{precision},
        scale_{scale},
        unscaledValue_{std::move(unscaledValue)} {}

  int32_t precision_;
  int32_t scale_;
  // The digits without the dot, with the sign if negative.
  std::string unscaledValue_;
};

} // namespace axiom::sql
