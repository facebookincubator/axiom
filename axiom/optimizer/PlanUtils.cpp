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

#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::velox::optimizer {
namespace {

/// Match the input 'value' to the most appropriate unit and return
/// a string value. The units are specified in the 'units' array.
/// unitOffset is used to skip the starting units.
/// unitScale is used to determine the unit.
/// precision is used to set the decimal digits in the final output.
std::string succinctPrint(
    double decimalValue,
    const std::string_view* units,
    int unitsSize,
    int unitOffset,
    double unitScale,
    int precision) {
  std::stringstream out;
  int offset = unitOffset;
  while ((decimalValue / unitScale) >= 1 && offset < (unitsSize - 1)) {
    decimalValue = decimalValue / unitScale;
    offset++;
  }
  if (offset == unitOffset) {
    // Print the default value.
    precision = 0;
  }
  out << std::fixed << std::setprecision(precision) << decimalValue
      << units[offset];
  return out.str();
}

} // namespace

std::string succinctNumber(double value, int32_t precision) {
  static constexpr std::string_view kUnit[] = {
      "n", "u", "m", "", "k", "M", "G", "T", "P"};

  return succinctPrint(
      value * 1e9,
      kUnit,
      sizeof(kUnit) / sizeof(std::string_view),
      0,
      1000,
      precision);
}

namespace {
template <typename T>
int64_t integerValueInner(const variant* variant) {
  return variant->value<T>();
}
} // namespace

int64_t integerValue(const Variant* variant) {
  switch (variant->kind()) {
    case TypeKind::TINYINT:
      return integerValueInner<int8_t>(variant);
    case TypeKind::SMALLINT:
      return integerValueInner<int16_t>(variant);
    case TypeKind::INTEGER:
      return integerValueInner<int32_t>(variant);
    case TypeKind::BIGINT:
      return integerValueInner<int64_t>(variant);
    default:
      VELOX_FAIL();
  }
}

std::optional<int64_t> maybeIntegerLiteral(
    const logical_plan::ConstantExpr* expr) {
  switch (expr->typeKind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return integerValue(expr->value().get());
    default:
      return std::nullopt;
  }
}

std::string conjunctsToString(const ExprVector& conjuncts) {
  std::stringstream out;
  for (auto i = 0; i < conjuncts.size(); ++i) {
    out << conjuncts[i]->toString()
        << (i == conjuncts.size() - 1 ? "" : " and ");
  }
  return out.str();
}

} // namespace facebook::velox::optimizer
