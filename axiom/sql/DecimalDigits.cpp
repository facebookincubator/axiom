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

#include "axiom/sql/DecimalDigits.h"

#include <algorithm>
#include <cctype>

#include <folly/Conv.h>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace axiom::sql {

namespace lp = facebook::axiom::logical_plan;
using namespace facebook::velox;

std::optional<DecimalDigits> DecimalDigits::parse(std::string_view text) {
  std::string unscaledValue;
  if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
    if (text.front() == '-') {
      unscaledValue += '-';
    }
    text.remove_prefix(1);
  }

  const auto dot = text.find('.');
  if (text.empty() || text == "." ||
      (dot != std::string_view::npos &&
       text.find('.', dot + 1) != std::string_view::npos)) {
    return std::nullopt;
  }

  int32_t numSignificant{0};
  for (const char digit : text) {
    if (digit == '.') {
      continue;
    }
    if (!std::isdigit(static_cast<unsigned char>(digit))) {
      return std::nullopt;
    }
    if (numSignificant > 0 || digit != '0') {
      ++numSignificant;
    }
    unscaledValue += digit;
  }

  const int32_t scale =
      dot == std::string_view::npos ? 0 : text.size() - dot - 1;
  const int32_t precision = std::max({numSignificant, scale, 1});
  return DecimalDigits(precision, scale, std::move(unscaledValue));
}

lp::ExprApi DecimalDigits::toExpr() const {
  VELOX_CHECK_LE(precision_, LongDecimalType::kMaxPrecision);
  const auto type = DECIMAL(precision_, scale_);
  const auto value = folly::to<int128_t>(unscaledValue_);
  if (type->isShortDecimal()) {
    return lp::Lit(static_cast<int64_t>(value), type);
  }
  return lp::Lit(value, type);
}

} // namespace axiom::sql
