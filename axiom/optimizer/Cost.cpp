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

#include "axiom/optimizer/Cost.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/JsonUtil.h"
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::axiom::optimizer {

void History::saveToFile(const std::string& path) {
  auto json = serialize();
  std::ofstream file(path);
  file << folly::toPrettyJson(json);
  file.close();
}

void History::updateFromFile(const std::string& path) {
  auto json = readConcatenatedDynamicsFromFile(path);
  for (auto& elt : json) {
    update(elt);
  }
}

float shuffleCost(const ColumnVector& columns) {
  return byteSize(columns);
}

float shuffleCost(const ExprVector& exprs) {
  return byteSize(exprs);
}

float selfCost(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kColumnExpr: {
      auto kind = expr->value().type->kind();
      if (kind == velox::TypeKind::ARRAY || kind == velox::TypeKind::MAP) {
        return 200;
      }
      return 10;
    }
    case PlanType::kCallExpr: {
      auto metadata = expr->as<Call>()->metadata();
      if (metadata) {
        if (metadata->costFunc) {
          return metadata->costFunc(expr->as<Call>());
        }
        return metadata->cost;
      }
      return 5;
    }
    default:
      return 5;
  }
}

float costWithChildren(ExprCP expr, const PlanObjectSet& notCounting) {
  if (notCounting.contains(expr)) {
    return 0;
  }
  switch (expr->type()) {
    case PlanType::kColumnExpr:
      return selfCost(expr);
    case PlanType::kCallExpr: {
      float cost = selfCost(expr);
      for (auto arg : expr->as<Call>()->args()) {
        cost += costWithChildren(arg, notCounting);
      }
      return cost;
    }
    default:
      return 0;
  }
}

} // namespace facebook::axiom::optimizer
