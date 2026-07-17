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

#include "axiom/optimizer/v2/EstimateProvider.h"

#include <algorithm>
#include <cmath>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/Cost.h"
#include "axiom/optimizer/v2/JoinFanout.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

constexpr float kDefaultLeafCardinality = 1;

std::span<const ExprCP> toSpan(const ExprVector& exprs) {
  return {exprs.data(), exprs.size()};
}

// Merges 'from' into 'into', keeping existing entries (the two sides of a
// join have disjoint column-id spaces, so there is no real conflict).
void mergeConstraints(const ConstraintMap& from, ConstraintMap& into) {
  for (const auto& [id, value] : from) {
    into.emplace(id, value);
  }
}

// Scales a scan's estimated row count and per-column NDVs by a TABLESAMPLE
// SYSTEM rate, if one is set. Modeled as a scan-level selectivity, so it
// applies whether the row count came from connector stats or the base-table
// fallback.
void applySampleRate(
    const BaseTable& baseTable,
    const Scan* scan,
    Estimate& estimate) {
  if (!baseTable.sampledPercentage.has_value() ||
      !estimate.cardinality.has_value()) {
    return;
  }

  const float fraction = *baseTable.sampledPercentage / 100.0f;
  estimate.cardinality = std::max(1.0f, *estimate.cardinality * fraction);

  // A column cannot have more distinct values than the sampled row count.
  for (ColumnCP column : scan->outputColumns()) {
    Value capped = value(estimate.constraints, column);
    if (!capped.cardinality.has_value() ||
        *capped.cardinality > *estimate.cardinality) {
      capped.cardinality = *estimate.cardinality;
      estimate.constraints.insert_or_assign(column->id(), capped);
    }
  }
}

} // namespace

const Estimate& EstimateProvider::estimate(NodeCP node) {
  auto it = byNode_.find(node);
  if (it != byNode_.end()) {
    return it->second;
  }
  Estimate result = compute(node);
  // A node's cardinality estimate should never be non-finite; estimate
  // arithmetic saturates (see EstimateMath.h). Surface a violation here instead
  // of letting infinity skew downstream cost comparisons.
  VELOX_DCHECK(
      !result.cardinality.has_value() || std::isfinite(*result.cardinality),
      "Cardinality estimate is not finite. Node: {}",
      node->nodeType());
  return byNode_.emplace(node, std::move(result)).first->second;
}

Estimate EstimateProvider::compute(NodeCP node) {
  switch (node->nodeType()) {
    case NodeType::kScan: {
      const auto* scan = node->as<Scan>();
      const auto* baseTable = scan->baseTable();
      Estimate result;
      // Narrow per-column constraints from the scan filters; join propagation
      // reads these regardless of the cardinality source. The filter
      // selectivity is unknown when `conjunctsSelectivity` cannot estimate it;
      // an unknown selectivity makes the constraint-derived cardinality
      // unknown (no fabricated 1.0 fallback).
      std::optional<float> selectivity{1.0f};
      if (!scan->filters().empty()) {
        const auto sel = conjunctsSelectivity(
            result.constraints,
            toSpan(scan->filters()),
            /*updateConstraints=*/true);
        selectivity = sel.has_value() ? std::optional<float>{sel->trueFraction}
                                      : std::nullopt;
      }
      if (!baseTable->filteredCardinality.has_value()) {
        // Filtered-stats pass ran but could not estimate the post-filter row
        // count (a rejected filter's selectivity was unknown); propagate the
        // unknown rather than falling back to a fabricated estimate.
        result.cardinality = std::nullopt;
      } else if (*baseTable->filteredCardinality > 0) {
        // Connector filtered-stats pass populated the post-filter row count.
        result.cardinality = std::max(1.0f, *baseTable->filteredCardinality);
      } else if (baseTable->schemaTable != nullptr) {
        // filteredCardinality == 0: no connector stats; fall back to base-table
        // cardinality scaled by the optimizer's own selectivity.
        result.cardinality =
            maxOf(1.0f, mul(baseTable->schemaTable->cardinality, selectivity));
      } else {
        // No base-table statistics: cardinality is unknown.
        result.cardinality = std::nullopt;
      }

      applySampleRate(*baseTable, scan, result);
      return result;
    }

    case NodeType::kFilter: {
      const auto* filter = node->as<Filter>();
      const auto& input = estimate(filter->input());
      Estimate result;
      result.constraints = input.constraints;
      const auto sel = conjunctsSelectivity(
          result.constraints,
          toSpan(filter->predicates()),
          /*updateConstraints=*/true);
      // Unknown filter selectivity propagates: the output cardinality is
      // unknown rather than the unfiltered input cardinality.
      const std::optional<float> selectivity = sel.has_value()
          ? std::optional<float>{sel->trueFraction}
          : std::nullopt;
      result.cardinality = maxOf(1.0f, mul(input.cardinality, selectivity));
      return result;
    }

    case NodeType::kAggregate: {
      const auto* aggregate = node->as<Aggregate>();
      const auto& input = estimate(aggregate->input());
      const auto& groupingKeys = aggregate->groupingKeys();
      Estimate result;
      if (groupingKeys.empty()) {
        result.cardinality = 1;
      } else {
        // Product of per-key NDVs, capped at the input cardinality. Unknown
        // propagates: if any grouping key's NDV (or the input cardinality) is
        // unknown, the group count is unknown.
        std::optional<float> product{1.0f};
        for (ExprCP key : groupingKeys) {
          product = mul(product, value(input.constraints, key).cardinality);
        }
        result.cardinality = maxOf(1.0f, minOf(input.cardinality, product));
      }
      // An aggregate emits one row per group, so each output column has at most
      // `cardinality` distinct values. Derive the output-column NDVs so a
      // consumer over the aggregate reads a real value rather than the unknown
      // base NDV: grouping keys keep their input NDV clamped to the group
      // count; aggregate results (which have no meaningful base NDV) take the
      // group count. Output columns are the grouping keys, then the aggregates,
      // positionally.
      const std::optional<float> groupCount = result.cardinality;
      const auto& outputColumns = aggregate->outputColumns();
      for (size_t i = 0; i < outputColumns.size(); ++i) {
        const std::optional<float> ndv = i < groupingKeys.size()
            ? minOf(
                  value(input.constraints, groupingKeys[i]).cardinality,
                  groupCount)
            : groupCount;
        result.constraints.insert_or_assign(
            outputColumns[i]->id(), Value(outputColumns[i]->value().type, ndv));
      }
      return result;
    }

    case NodeType::kLimit: {
      const auto* limit = node->as<Limit>();
      const auto& input = estimate(limit->input());
      Estimate result;
      result.constraints = input.constraints;
      result.cardinality = minOf(
          input.cardinality,
          std::max(1.0f, static_cast<float>(limit->count())));
      return result;
    }

    case NodeType::kTopN: {
      const auto* topN = node->as<TopN>();
      const auto& input = estimate(topN->input());
      Estimate result;
      result.constraints = input.constraints;
      result.cardinality = minOf(
          input.cardinality, std::max(1.0f, static_cast<float>(topN->count())));
      return result;
    }

    case NodeType::kUnnest: {
      const auto* unnest = node->as<Unnest>();
      const auto& input = estimate(unnest->input());
      Estimate result;
      result.constraints = input.constraints;
      result.cardinality =
          maxOf(1.0f, mul(input.cardinality, kDefaultUnnestFanout));
      return result;
    }

    case NodeType::kUnionAll: {
      Estimate result;
      // Sum of children. Unknown propagates: an unknown child cardinality
      // makes the union cardinality unknown.
      std::optional<float> total{0.0f};
      for (NodeCP input : node->inputs()) {
        total = add(total, estimate(input).cardinality);
      }
      result.cardinality = maxOf(1.0f, total);
      return result;
    }

    case NodeType::kValues: {
      const auto* values = node->as<Values>();
      Estimate result;
      if (values->rows() != nullptr) {
        result.cardinality = std::max<float>(1, values->rows()->array().size());
      } else {
        result.cardinality = kDefaultLeafCardinality;
      }
      return result;
    }

    case NodeType::kJoin: {
      const auto* join = node->as<Join>();
      const auto& left = estimate(join->left());
      const auto& right = estimate(join->right());
      Estimate result;
      mergeConstraints(left.constraints, result.constraints);
      mergeConstraints(right.constraints, result.constraints);

      // Matched-row count is unknown when any join-key NDV is unknown; that
      // propagates through to the output cardinality.
      const std::optional<float> matched = JoinFanout::estimateJoinCardinality(
          left.cardinality,
          right.cardinality,
          join->leftKeys(),
          join->rightKeys(),
          result.constraints);
      const std::optional<float> cardinality = JoinFanout::outputCardinality(
          join->joinType(), left.cardinality, right.cardinality, matched);
      result.cardinality = maxOf(1.0f, cardinality);
      return result;
    }

    case NodeType::kTableWrite: {
      // A table write emits a single row-count row.
      Estimate result;
      result.cardinality = 1;
      return result;
    }

    // Cardinality-neutral operators: pass the input's estimate through.
    case NodeType::kProject:
    case NodeType::kSort:
    case NodeType::kWindow:
    case NodeType::kTopNRowNumber:
    case NodeType::kGroupId:
    case NodeType::kMarkDistinct:
    case NodeType::kEnforceSingleRow:
    case NodeType::kAssignUniqueId:
    case NodeType::kEnforceDistinct:
    case NodeType::kExchange:
    case NodeType::kApply: {
      const auto inputs = node->inputs();
      if (inputs.empty()) {
        return Estimate{};
      }
      const auto& input = estimate(inputs[0]);
      Estimate result;
      result.cardinality = input.cardinality;
      result.constraints = input.constraints;
      return result;
    }
  }
  VELOX_UNREACHABLE("EstimateProvider::compute: unhandled NodeType");
}

} // namespace facebook::axiom::optimizer::v2
