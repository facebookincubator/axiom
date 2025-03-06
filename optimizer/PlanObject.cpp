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

#include "optimizer/QueryGraph.h" //@manual
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::optimizer {

size_t PlanObjectPHasher::operator()(const PlanObjectCP& object) const {
  return object->hash();
}

bool PlanObjectPComparer::operator()(
    const PlanObjectCP& lhs,
    const PlanObjectCP& rhs) const {
  if (rhs == lhs) {
    return true;
  }
  return rhs && lhs && lhs->isExpr() && rhs->isExpr() &&
      reinterpret_cast<const Expr*>(lhs)->sameOrEqual(
          *reinterpret_cast<const Expr*>(rhs));
}

size_t PlanObject::hash() const {
  size_t h = static_cast<size_t>(id_);
  for (auto& child : children()) {
    h = velox::bits::hashMix(h, child->hash());
  }
  return h;
}

void PlanObjectSet::unionColumns(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kLiteral:
      return;
    case PlanType::kColumn:
      add(expr);
      return;
    case PlanType::kField:
      unionColumns(expr->as<Field>()->base());
      return;
    case PlanType::kAggregate: {
      auto condition = expr->as<Aggregate>()->condition();
      if (condition) {
        unionColumns(condition);
      }
    }
      // Fall through.
      FMT_FALLTHROUGH;
    case PlanType::kCall: {
      auto call = reinterpret_cast<const Call*>(expr);
      unionSet(call->columns());
      return;
    }
    default:
      VELOX_UNREACHABLE();
  }
}

void PlanObjectSet::unionColumns(const ExprVector& exprs) {
  for (auto& expr : exprs) {
    unionColumns(expr);
  }
}

void PlanObjectSet::unionColumns(const ColumnVector& exprs) {
  for (auto& expr : exprs) {
    unionColumns(expr);
  }
}

std::string PlanObjectSet::toString(bool names) const {
  std::stringstream out;
  forEach([&](auto object) {
    out << object->id();
    if (names) {
      out << ": " << object->toString() << std::endl;
    } else {
      out << " ";
    }
  });
  return out.str();
}

} // namespace facebook::velox::optimizer
