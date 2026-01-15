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
  return byteSize(columns) * Costs::kByteShuffleCost;
}

float shuffleCost(const ExprVector& exprs) {
  return byteSize(exprs) * Costs::kByteShuffleCost;
}

namespace {

// Computes the overhead cost for accessing repeated nested data in complex
// types. For arrays and maps, the overhead is proportional to the number of
// repetitions (product of sizes in parent values) times the cost of accessing
// each length. For structs, the overhead is the sum of overhead for all
// children.
float columnRepeatOverhead(const Value& value, float parentRepeat = 1.0f) {
  if (!value.children) {
    return 0;
  }

  auto typeKind = value.type->kind();

  if (typeKind == velox::TypeKind::ROW) {
    // For structs, sum the overhead of all children
    float total = 0;
    for (const auto& childValue : value.children->values) {
      total += columnRepeatOverhead(childValue, parentRepeat);
    }
    return total;
  } else if (typeKind == velox::TypeKind::MAP) {
    // For maps with named children, use same logic as ROW (sum of children)
    if (!value.children->names.empty()) {
      float total = 0;
      for (const auto& childValue : value.children->values) {
        total += columnRepeatOverhead(childValue, parentRepeat);
      }
      return total;
    }
    // For maps with unnamed children, compute repeat overhead
    if (value.children->names.empty()) {
      // Calculate the repeat factor for this level
      float currentSize = value.size != Value::kUnknown ? value.size : 3.0f;
      float repeat = parentRepeat * currentSize;

      // Overhead is 4 * kColumnByteCost per length access.
      float overhead = 4.0f * Costs::kColumnByteCost * parentRepeat;

      // Recursively add overhead for children
      for (const auto& childValue : value.children->values) {
        overhead += columnRepeatOverhead(childValue, repeat);
      }
      return overhead;
    }
  } else if (typeKind == velox::TypeKind::ARRAY) {
    // For arrays with unnamed children, compute repeat overhead
    if (value.children->names.empty()) {
      // Calculate the repeat factor for this level
      float currentSize = value.size != Value::kUnknown ? value.size : 3.0f;
      float repeat = parentRepeat * currentSize;

      // Overhead is 4 * kColumnByteCost per length access.
      float overhead = 4.0f * Costs::kColumnByteCost * parentRepeat;

      // Recursively add overhead for children
      for (const auto& childValue : value.children->values) {
        overhead += columnRepeatOverhead(childValue, repeat);
      }
      return overhead;
    }
  }

  return 0;
}

/// Helper function to look up value for an expression.
/// If constraints is non-null and expr has an entry in the map, returns that
/// value. Otherwise returns expr->value().
const Value& value(ExprCP expr, const ConstraintMap* constraints) {
  if (constraints != nullptr) {
    auto it = constraints->find(expr->id());
    if (it != constraints->end()) {
      return it->second;
    }
  }
  return expr->value();
}

} // namespace

float selfCost(ExprCP expr, const ConstraintMap* constraints) {
  switch (expr->type()) {
    case PlanType::kColumnExpr: {
      const auto& val = value(expr, constraints);
      // Cost is byteSize * kColumnByteCost + repeat overhead for complex types
      return val.byteSize() * Costs::kColumnByteCost +
          columnRepeatOverhead(val);
    }
    case PlanType::kCallExpr: {
      auto metadata = expr->as<Call>()->metadata();
      if (metadata) {
        if (metadata->costFunc) {
          return metadata->costFunc(expr->as<Call>(), constraints);
        }
        if (metadata->cost.has_value()) {
          return metadata->cost.value();
        }
      }
      // Default cost is the cost of memcpy for the args * 4.
      // Cost of memcpy would be byteSize() / cache line size.
      // Cache line size is 16 for this purpose (64 bytes / 4).
      auto call = expr->as<Call>();
      float argsByteSize = 0;
      for (auto arg : call->args()) {
        argsByteSize += value(arg, constraints).byteSize();
      }
      return argsByteSize / 16.0f;
    }
    default:
      return 5;
  }
}

float costWithChildren(
    ExprCP expr,
    const PlanObjectSet& notCounting,
    const ConstraintMap* constraints) {
  if (notCounting.contains(expr)) {
    return 0;
  }
  switch (expr->type()) {
    case PlanType::kColumnExpr:
      return selfCost(expr, constraints);
    case PlanType::kCallExpr: {
      float cost = selfCost(expr, constraints);
      for (auto arg : expr->as<Call>()->args()) {
        cost += costWithChildren(arg, notCounting, constraints);
      }
      return cost;
    }
    default:
      return 0;
  }
}

float Costs::cacheMissClocks(float workingSet, float accessBytes) {
  // x86 cache architecture constants.

  // Cache line size in bytes.
  const float kCacheLineSize = 64.0f;
  // L1 cache: 32 KB.
  const float kL1Size = 32.0f * 1024.0f;
  // L2 cache: 256 KB.
  const float kL2Size = 256.0f * 1024.0f;
  // L3 cache: 8 MB per query, total cache is larger.
  const float kL3Size = 8.0f * 1024.0f * 1024.0f;

  // Cache latencies in CPU cycles.
  // L1 hit: 2 cycles.
  const float kL1Latency = 2.0f;
  // L2 hit: 6 cycles.
  const float kL2Latency = 6.0f;
  // L3 hit: 22 cycles
  // Memory miss: 60 cycles. The real latency is higher but for hash
  // tables where many concurrent misses pending at the same time, 65
  // agrees somewhat with observations.
  const float kL3Latency = 22.0f;

  const float kMemoryLatency = 60.0f;

  // Compute number of cache lines accessed.
  // For each byte beyond the first cache line, we count 1/64 of the miss cost,
  // which effectively means accessBytes / kCacheLineSize cache lines.
  float numCacheLines =
      1 + (accessBytes > 1 ? accessBytes / kCacheLineSize : 0);

  // Compute expected latency per cache line access based on working set size.
  // For random access patterns, the probability of finding data in a cache
  // level is proportional to the ratio of cache size to working set size.
  // When the working set exceeds a cache level, we blend between that level
  // and the next level based on the cache occupancy fraction.
  float expectedLatency;

  if (workingSet <= kL1Size) {
    // Working set fits entirely in L1 cache.
    expectedLatency = kL1Latency;
  } else if (workingSet <= kL2Size) {
    // Working set exceeds L1 but fits in L2.
    // Blend between L1 and L2 latencies proportional to L1 occupancy.
    float l1Fraction = kL1Size / workingSet;
    expectedLatency =
        l1Fraction * kL1Latency + (1.0f - l1Fraction) * kL2Latency;
  } else if (workingSet <= kL3Size) {
    // Working set exceeds L2 but fits in L3.
    // Blend between L2 and L3 latencies proportional to L2 occupancy.
    float l2Fraction = kL2Size / workingSet;
    expectedLatency =
        l2Fraction * kL2Latency + (1.0f - l2Fraction) * kL3Latency;
  } else {
    // Working set exceeds L3, spills to main memory.
    // Blend between L3 and memory latencies proportional to L3 occupancy.
    float l3Fraction = kL3Size / workingSet;
    expectedLatency =
        l3Fraction * kL3Latency + (1.0f - l3Fraction) * kMemoryLatency;
  }

  // Total cost is the number of cache lines accessed times the expected
  // latency.
  return numCacheLines * expectedLatency;
}

} // namespace facebook::axiom::optimizer
