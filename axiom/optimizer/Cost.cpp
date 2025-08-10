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
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::velox::optimizer {

// Collection of per operation costs for a target system.  The base
// unit is the time to memcpy a cache line in a large memcpy on one
// core. This is ~6GB/s, so ~10ns. Other times are expressed as
// multiples of that.
struct Costs {
  static float byteShuffleCost() {
    return 12; // ~500MB/s
  }

  static float hashProbeCost(float cardinality) {
    return cardinality < 10000 ? kArrayProbeCost
        : cardinality < 500000 ? kSmallHashCost
                               : kLargeHashCost;
  }

  static constexpr float kKeyCompareCost =
      6; // ~30 instructions to find, decode and an compare
  static constexpr float kArrayProbeCost = 2; // ~10 instructions.
  static constexpr float kSmallHashCost = 10; // 50 instructions
  static constexpr float kLargeHashCost = 40; // 2 LLC misses
  static constexpr float kColumnRowCost = 5;
  static constexpr float kColumnByteCost = 0.1;

  // Cost of hash function on one column.
  static constexpr float kHashColumnCost = 0.5;

  // Cost of getting a column from a hash table
  static constexpr float kHashExtractColumnCost = 0.5;

  // Minimal cost of calling a filter function, e.g. comparing two numeric
  // exprss.
  static constexpr float kMinimumFilterCost = 2;
};

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

void RelationOp::setCost(const PlanState& state) {
  cost_.inputCardinality = state.cost.fanout;
}

float ColumnGroup::lookupCost(float range) const {
  // Add 2 because it takes a compare and access also if hitting the
  // same row. log(1) == 0, so this would other wise be zero cost.
  return Costs::kKeyCompareCost * log(range + 2) / log(2);
}

float orderPrefixDistance(
    RelationOpPtr input,
    ColumnGroupP index,
    const ExprVector& keys) {
  int32_t i = 0;
  float selection = 1;
  for (; i < input->distribution().order.size() &&
       i < index->distribution().order.size() && i < keys.size();
       ++i) {
    if (input->distribution().order[i]->sameOrEqual(*keys[i])) {
      selection *= index->distribution().order[i]->value().cardinality;
    }
  }
  return selection;
}

namespace {

// For leaf nodes, the fanout represents the cardinality, and the unitCost is
// the total cost.
// For non-leaf nodes, the fanout represents the change in cardinality (output
// cardinality / input cardinality), and the unitCost is the per-row cost.
void updateLeafCost(
    float cardinality,
    const ColumnVector& columns,
    Cost& cost) {
  cost.fanout = cardinality;
  const auto size = byteSize(columns);
  const auto numColumns = columns.size();
  const auto rowCost = numColumns * Costs::kColumnRowCost +
      std::max<float>(0, size - 8 * numColumns) * Costs::kColumnByteCost;
  cost.unitCost += cost.fanout * rowCost;
}

} // namespace

void TableScan::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  if (!keys.empty()) {
    float lookupRange(index->distribution().cardinality);
    float orderSelectivity = orderPrefixDistance(this->input(), index, keys);
    auto distance = lookupRange / std::max<float>(1, orderSelectivity);
    float batchSize = std::min<float>(cost_.inputCardinality, 10000);
    if (orderSelectivity == 1) {
      // The data does not come in key order.
      float batchCost = index->lookupCost(lookupRange) +
          index->lookupCost(lookupRange / batchSize) *
              std::max<float>(1, batchSize);
      cost_.unitCost = batchCost / batchSize;
    } else {
      float batchCost = index->lookupCost(lookupRange) +
          index->lookupCost(distance) * std::max<float>(1, batchSize);
      cost_.unitCost = batchCost / batchSize;
    }
    return;
  }
  const auto cardinality =
      index->distribution().cardinality * baseTable->filterSelectivity;
  updateLeafCost(cardinality, columns_, cost_);
}

void Values::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  const auto cardinality = valuesTable.cardinality();
  updateLeafCost(cardinality, columns_, cost_);
}

void Aggregation::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  float cardinality = 1;
  for (auto key : grouping) {
    cardinality *= key->value().cardinality;
  }
  // The estimated output is input minus the times an input is a
  // duplicate of a key already in the input. The cardinality of the
  // result is (d - d * 1 - (1 / d))^n. where d is the number of
  // potentially distinct keys and n is the number of elements in the
  // input. This approaches d as n goes to infinity. The chance of one in d
  // being unique after n values is 1 - (1/d)^n.
  auto nOut = cardinality -
      cardinality * pow(1.0 - (1.0 / cardinality), cost_.inputCardinality);
  cost_.fanout = nOut / cost_.inputCardinality;
  cost_.unitCost = grouping.size() * Costs::hashProbeCost(nOut);
  float rowBytes = byteSize(grouping) + byteSize(aggregates);
  cost_.totalBytes = nOut * rowBytes;
}

template <typename V>
std::pair<float, float> shuffleCostV(const V& columns) {
  float size = byteSize(columns);
  return {size * Costs::byteShuffleCost(), size};
}

float shuffleCost(const ColumnVector& columns) {
  return shuffleCostV(columns).second;
}

float shuffleCost(const ExprVector& columns) {
  return shuffleCostV(columns).second;
}

void Repartition::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  auto pair = shuffleCostV(columns_);
  cost_.unitCost = pair.second;
  cost_.transferBytes = cost_.inputCardinality * pair.first;
}

void HashBuild::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  cost_.unitCost = keys.size() * Costs::kHashColumnCost +
      Costs::hashProbeCost(cost_.inputCardinality) +
      this->input()->columns().size() * Costs::kHashExtractColumnCost * 2;
  cost_.totalBytes =
      cost_.inputCardinality * byteSize(this->input()->columns());
}

void Join::setCost(const PlanState& input) {
  RelationOp::setCost(input);
  float buildSize = right->cost().inputCardinality;
  auto rowCost =
      right->input()->columns().size() * Costs::kHashExtractColumnCost;
  cost_.unitCost = Costs::hashProbeCost(buildSize) + cost_.fanout * rowCost +
      leftKeys.size() * Costs::kHashColumnCost;
}

void Filter::setCost(const PlanState& /*input*/) {
  cost_.unitCost = Costs::kMinimumFilterCost * exprs_.size();
  // We assume each filter selects 4/5. Small effect makes it so
  // join and scan selectivities that are better known have more
  // influence on plan cardinality. To be filled in from history.
  cost_.fanout = pow(0.8, exprs_.size());
}

void UnionAll::setCost(const PlanState& input) {
  for (auto& in : inputs) {
    cost_.inputCardinality += in->cost().inputCardinality * in->cost().fanout;
  }
}

void Limit::setCost(const PlanState& input) {
  cost_.unitCost = 0.01;
  if (input.cost.inputCardinality <= limit) {
    // Input cardinality does not exceed the limit. The limit is no-op. Doesn't
    // change cardinality.
    cost_.fanout = 1;
  } else {
    // Input cardinality exceeds the limit. Calculate fanout to ensure that
    // fanout * limit = input-cardinality.
    cost_.fanout = limit / input.cost.inputCardinality;
  }
}

float selfCost(ExprCP expr) {
  switch (expr->type()) {
    case PlanType::kColumn: {
      auto kind = expr->value().type->kind();
      if (kind == TypeKind::ARRAY || kind == TypeKind::MAP) {
        return 200;
      }
      return 10;
    }
    case PlanType::kCall: {
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
    case PlanType::kColumn:
      return selfCost(expr);
    case PlanType::kCall: {
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

} // namespace facebook::velox::optimizer
