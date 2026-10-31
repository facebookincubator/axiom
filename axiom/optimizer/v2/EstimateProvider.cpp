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
#include <vector>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/ExprEstimator.h"
#include "axiom/optimizer/JoinConstraints.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/UnionConstraints.h"
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

// Scales a scan's estimated row count by a TABLESAMPLE SYSTEM rate, if one is
// set. Modeled as a scan-level selectivity, so it applies whether the row count
// came from connector stats or the base-table fallback.
void applySampleRate(const BaseTable& baseTable, Estimate& estimate) {
  if (!baseTable.sampledPercentage.has_value() ||
      !estimate.cardinality.has_value()) {
    return;
  }

  const float fraction = *baseTable.sampledPercentage / 100.0f;
  estimate.cardinality = std::max(1.0f, *estimate.cardinality * fraction);
}

// Returns 'estimate' over 'to', where 'to[i]' takes the statistics of
// 'from[i]'.
Estimate remap(
    const Estimate& estimate,
    const ColumnVector& from,
    const ColumnVector& to) {
  VELOX_CHECK_EQ(from.size(), to.size());
  Estimate result;
  result.cardinality = estimate.cardinality;
  for (size_t i = 0; i < from.size(); ++i) {
    result.constraints.insert_or_assign(
        to[i]->id(), value(estimate.constraints, from[i]));
  }
  return result;
}

// Combines the estimates of the loop's state after each round, the anchor's
// first, into one that holds for every iteration: the anchor's row count, and
// each column combined as a union's column is, except its NDV, which is the
// largest round's since the state holds one round's values. Every round's row
// count is known and at most the anchor's.
Estimate combineRounds(
    const std::vector<Estimate>& rounds,
    const ColumnVector& columns) {
  std::vector<std::optional<float>> cardinalities;
  cardinalities.reserve(rounds.size());
  for (const Estimate& round : rounds) {
    cardinalities.push_back(round.cardinality);
  }

  Estimate result;
  result.cardinality = rounds.front().cardinality;
  for (ColumnCP column : columns) {
    std::vector<const Value*> values;
    std::optional<float> largestNdv{0.0f};
    for (const Estimate& round : rounds) {
      values.push_back(&value(round.constraints, column));
      largestNdv = maxOf(largestNdv, values.back()->cardinality);
    }
    Value combined = UnionConstraints::combine(values, cardinalities);
    combined.cardinality = largestNdv;
    result.constraints.insert_or_assign(column->id(), std::move(combined));
  }
  return result;
}

// Enforces the invariant that a column's NDV cannot exceed the row count. Caps
// each output column's known NDV at 'estimate.cardinality', materializing a
// constraint for a column that would otherwise fall back to an uncapped
// base-table NDV (e.g. a column a pushed-down filter did not refine). An
// unknown NDV stays unknown; a no-op when the cardinality is unknown.
void capNdvAtCardinality(const ColumnVector& columns, Estimate& estimate) {
  if (!estimate.cardinality.has_value()) {
    return;
  }
  for (ColumnCP column : columns) {
    const Value& current = value(estimate.constraints, column);
    if (current.cardinality.has_value() &&
        *current.cardinality > *estimate.cardinality) {
      Value capped = current;
      capped.cardinality = *estimate.cardinality;
      estimate.constraints.insert_or_assign(column->id(), std::move(capped));
    }
  }
}

// The number of groups 'key' forms: its distinct values, plus one for its
// nulls when it has any. An all-null key forms the one null group.
std::optional<float> groupsOf(const Value& key) {
  if (key.nullFraction == 1.0f) {
    return 1.0f;
  }
  if (key.nullFraction.has_value() && *key.nullFraction > 0) {
    return add(key.cardinality, 1.0f);
  }
  return key.cardinality;
}

// The number of groups 'keys' form: the product of each key's groups, capped
// at 'inputCardinality'. Unknown when any key's NDV or 'inputCardinality' is.
std::optional<float> estimateNumGroups(
    const ExprVector& keys,
    const ConstraintMap& constraints,
    std::optional<float> inputCardinality) {
  std::optional<float> product{1.0f};
  for (ExprCP key : keys) {
    product = mul(product, groupsOf(value(constraints, key)));
  }
  return minOf(inputCardinality, product);
}

// The statistics of a grouping key's output column, one row per group out of
// 'numGroups'. The key keeps its values, so its bounds, size and nullability,
// and its NDV is at most 'numGroups'. Its nulls form one of its own groups.
// Assumes each of the key's groups pairs with equally many groups of the other
// keys, so the null fraction is one over the key's groups. The true fraction
// of a boolean key no longer describes the rows and is unknown.
Value groupingKeyValue(const Value& key, std::optional<float> numGroups) {
  // An unknown 'numGroups' bounds nothing.
  const auto atMostNumGroups = [&](std::optional<float> count) {
    return numGroups.has_value() ? minOf(count, numGroups) : count;
  };
  Value result = key;
  result.cardinality = atMostNumGroups(key.cardinality);
  result.trueFraction = std::nullopt;
  if (key.nullFraction.has_value() && *key.nullFraction > 0) {
    const std::optional<float> groups = atMostNumGroups(groupsOf(key));
    result.nullFraction =
        groups.has_value() ? std::optional<float>{1 / *groups} : std::nullopt;
  }
  return result;
}

// The row count of an outer join that emits each row of its kept side once:
// the other side is unique on its join keys, so the count holds whatever the
// key NDVs. Nullopt for any other join.
std::optional<float> keptRowsOfOuterJoin(
    const Join& join,
    std::optional<float> leftCardinality,
    std::optional<float> rightCardinality) {
  const Join::Sides emittedOnce = Join::emittedOnceSides(
      join.joinType(),
      join.left(),
      join.right(),
      join.leftKeys(),
      join.rightKeys());
  switch (join.joinType()) {
    case velox::core::JoinType::kLeft:
      return emittedOnce.left ? leftCardinality : std::nullopt;
    case velox::core::JoinType::kRight:
      return emittedOnce.right ? rightCardinality : std::nullopt;
    default:
      return std::nullopt;
  }
}

} // namespace

const Estimate& EstimateProvider::estimate(NodeCP node) {
  if (parent_ != nullptr && node->requiredStates().empty()) {
    return parent_->estimate(node);
  }
  auto it = byNode_.find(node);
  if (it != byNode_.end()) {
    return it->second;
  }
  Estimate result = compute(node);
  capNdvAtCardinality(node->outputColumns(), result);

  // A known cardinality estimate is finite, since estimate arithmetic
  // saturates (see EstimateMath.h), and at least 1.
  if (result.cardinality.has_value()) {
    VELOX_CHECK(
        std::isfinite(*result.cardinality),
        "Cardinality estimate is not finite. Node: {}",
        node->nodeTypeName());
    VELOX_CHECK_GE(
        *result.cardinality,
        1.0f,
        "Cardinality estimate must be at least 1. Node: {}",
        node->nodeTypeName());
  }
  return byNode_.emplace(node, std::move(result)).first->second;
}

const Estimate& EstimateProvider::estimateLoopState(
    const FixedPoint& loop,
    NodeCP anchor) {
  constexpr int32_t kNumRounds = 3;

  const ColumnVector& columns = loop.outputColumns();
  ColumnCP key = columns.front();

  // A parent's copy would describe the loop under another state of an
  // enclosing loop, so only this provider's counts.
  if (const auto it = byLoop_.find(key); it != byLoop_.end()) {
    return it->second;
  }

  // A state that is unknown, or that has more rows than the anchor after some
  // round, is unknown, and the remaining rounds are skipped.
  const auto estimateState = [&]() -> Estimate {
    std::vector<Estimate> rounds;
    rounds.push_back(remap(estimate(anchor), anchor->outputColumns(), columns));
    const std::optional<float> anchorRows = rounds.front().cardinality;
    if (!anchorRows.has_value()) {
      return {};
    }
    for (int32_t i = 0; i < kNumRounds; ++i) {
      // The scratch provider holds this round's state and asks this provider
      // for the rest: a node that reads no state, and the state of an
      // enclosing loop.
      EstimateProvider scratch;
      scratch.parent_ = this;
      scratch.byLoop_.emplace(key, rounds.back());
      rounds.push_back(remap(
          scratch.estimate(loop.step()),
          loop.step()->outputColumns(),
          columns));
      const std::optional<float> rows = rounds.back().cardinality;
      if (!rows.has_value() || *rows > *anchorRows) {
        return {};
      }
    }
    return combineRounds(rounds, columns);
  };
  return byLoop_.emplace(key, estimateState()).first->second;
}

const Estimate* EstimateProvider::findLoopState(ColumnCP key) const {
  if (const auto it = byLoop_.find(key); it != byLoop_.end()) {
    return &it->second;
  }
  return parent_ != nullptr ? parent_->findLoopState(key) : nullptr;
}

Estimate EstimateProvider::compute(NodeCP node) {
  switch (node->nodeType()) {
    case NodeType::kScan: {
      const auto* scan = node->as<Scan>();
      const auto* baseTable = scan->baseTable();
      Estimate result;
      // A connector that takes a filter reports the statistics of the rows it
      // will return, so the scan's own estimate needs no selectivity of its
      // own.
      if (!baseTable->filteredCardinality.has_value()) {
        // The post-filter row count is not known; propagate the unknown rather
        // than falling back to a fabricated estimate.
        result.cardinality = std::nullopt;
      } else if (*baseTable->filteredCardinality > 0) {
        // Connector filtered-stats pass populated the post-filter row count.
        result.cardinality = std::max(1.0f, *baseTable->filteredCardinality);
      } else if (baseTable->schemaTable != nullptr) {
        // filteredCardinality == 0: no connector stats.
        result.cardinality = maxOf(1.0f, baseTable->schemaTable->cardinality);
      } else {
        // No base-table statistics: cardinality is unknown.
        result.cardinality = std::nullopt;
      }

      applySampleRate(*baseTable, result);
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
      // Each grouping key takes the estimate of its expression over the
      // input's.
      ConstraintMap expressions = input.constraints;
      ExprEstimator estimator{expressions};
      for (ExprCP key : groupingKeys) {
        estimator.estimate(key);
      }
      Estimate result;
      // The partial passes its input's statistics on, so the final step
      // estimates its results from its arguments (see `AggregateStep`), as the
      // aggregate before the split does.
      if (aggregate->step() == AggregateStep::kPartial) {
        result.constraints = input.constraints;
      }
      if (groupingKeys.empty()) {
        result.cardinality = 1;
      } else {
        result.cardinality = maxOf(
            1.0f,
            estimateNumGroups(groupingKeys, expressions, input.cardinality));
      }
      // An aggregate emits one row per group, so each output column has at most
      // `cardinality` distinct values. Output columns are the grouping keys,
      // then the aggregates, positionally.
      const std::optional<float> numGroups = result.cardinality;
      const auto& outputColumns = aggregate->outputColumns();
      for (size_t i = 0; i < outputColumns.size(); ++i) {
        if (i < groupingKeys.size()) {
          result.constraints.insert_or_assign(
              outputColumns[i]->id(),
              groupingKeyValue(value(expressions, groupingKeys[i]), numGroups));
        } else {
          result.constraints.insert_or_assign(
              outputColumns[i]->id(),
              estimator.estimateAggregate(
                  *aggregate->aggregates()[i - groupingKeys.size()],
                  numGroups));
        }
      }
      return result;
    }

    case NodeType::kLimit: {
      const auto* limit = node->as<Limit>();
      const auto& input = estimate(limit->input());
      Estimate result;
      result.constraints = input.constraints;
      const float limitCount =
          std::max(1.0f, static_cast<float>(limit->count()));
      result.cardinality = minOf(input.cardinality, limitCount);
      return result;
    }

    case NodeType::kTopN: {
      const auto* topN = node->as<TopN>();
      const auto& input = estimate(topN->input());
      Estimate result;
      result.constraints = input.constraints;
      const float limitCount =
          std::max(1.0f, static_cast<float>(topN->count()));
      result.cardinality = minOf(input.cardinality, limitCount);
      return result;
    }

    case NodeType::kUnnest: {
      const auto* unnest = node->as<Unnest>();
      const auto& input = estimate(unnest->input());
      Estimate result;
      result.constraints = input.constraints;
      result.cardinality =
          maxOf(1.0f, mul(input.cardinality, kDefaultUnnestFanout));
      // Each unnested column takes the statistics of the array elements, or
      // of the map keys or values, it comes from. An Unnest also emits NULL
      // for each empty value it keeps with a marker, and past the end of a
      // shorter value when it unnests several: in a share of the rows the
      // estimate does not know.
      const bool padsWithNull = unnest->markerColumn() != nullptr ||
          unnest->unnestExpressions().size() > 1;
      ConstraintMap expressions = input.constraints;
      ExprEstimator estimator{expressions};
      for (size_t i = 0; i < unnest->unnestExpressions().size(); ++i) {
        const Value expanded =
            estimator.estimate(unnest->unnestExpressions()[i]);
        const auto& columns = unnest->unnestColumns()[i];
        for (size_t j = 0; j < columns.size(); ++j) {
          if (const Value* child = expanded.child(static_cast<uint32_t>(j))) {
            Value unnested = *child;
            if (padsWithNull) {
              unnested.nullable = true;
              unnested.nullFraction = std::nullopt;
            }
            result.constraints.insert_or_assign(
                columns[j]->id(), std::move(unnested));
          }
        }
      }
      return result;
    }

    case NodeType::kIndexLookupJoin: {
      const auto* join = node->as<IndexLookupJoin>();
      Estimate result;
      if (join->joinType() == velox::core::JoinType::kLeft &&
          join->index()->distribution.numKeysUnique() >=
              join->lookupKeys().size()) {
        result.cardinality = estimate(join->probe()).cardinality;
      }
      return result;
    }

    case NodeType::kUnionAll: {
      const auto* unionAll = node->as<UnionAll>();
      const auto inputs = unionAll->inputs();
      Estimate result;
      // Sum of children. Unknown propagates: an unknown child cardinality
      // makes the union cardinality unknown.
      std::optional<float> total{0.0f};
      std::vector<std::optional<float>> cardinalities;
      for (NodeCP input : inputs) {
        cardinalities.push_back(estimate(input).cardinality);
        total = add(total, cardinalities.back());
      }
      result.cardinality = maxOf(1.0f, total);
      const auto& outputColumns = unionAll->outputColumns();
      for (size_t i = 0; i < outputColumns.size(); ++i) {
        std::vector<const Value*> inputValues;
        for (size_t j = 0; j < inputs.size(); ++j) {
          inputValues.push_back(&value(
              estimate(inputs[j]).constraints, unionAll->legColumns()[j][i]));
        }
        result.constraints.insert_or_assign(
            outputColumns[i]->id(),
            UnionConstraints::combine(inputValues, cardinalities));
      }
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

      result.cardinality =
          keptRowsOfOuterJoin(*join, left.cardinality, right.cardinality);
      if (!result.cardinality.has_value()) {
        // Matched-row count is unknown when any join-key NDV is unknown; that
        // propagates through to the output cardinality.
        const std::optional<float> matched =
            JoinFanout::estimateJoinCardinality(
                left.cardinality,
                right.cardinality,
                join->leftKeys(),
                join->rightKeys(),
                result.constraints);
        result.cardinality = maxOf(
            1.0f,
            JoinFanout::outputCardinality(
                join->joinType(),
                left.cardinality,
                right.cardinality,
                matched,
                join->filter(),
                result.constraints));
      }
      JoinConstraints::refineInnerKeys(
          join->joinType(),
          join->leftKeys(),
          join->rightKeys(),
          result.constraints);
      // Consumers read the join's output columns, which may differ from its
      // source columns. An outer join's null-extended outputs do not share
      // their sources' constraints.
      for (size_t i = 0; join->isInner() && i < join->outputColumns().size();
           ++i) {
        ColumnCP source = join->sourceColumns()[i];
        ColumnCP output = join->outputColumns()[i];
        if (output == source) {
          continue;
        }
        if (const auto it = result.constraints.find(source->id());
            it != result.constraints.end()) {
          result.constraints.insert_or_assign(output->id(), it->second);
        }
      }
      return result;
    }

    case NodeType::kVectorSearch: {
      // Each matching pair of a probe row and a build row gives up to 'k'
      // rows, and a left search keeps every probe row.
      const auto* search = node->as<VectorSearch>();
      const auto& probe = estimate(search->probe());
      const auto& build = estimate(search->build());
      Estimate result;
      mergeConstraints(probe.constraints, result.constraints);
      mergeConstraints(build.constraints, result.constraints);
      const std::optional<float> matched = JoinFanout::estimateJoinCardinality(
          probe.cardinality,
          build.cardinality,
          search->probeKeys(),
          search->buildKeys(),
          result.constraints);
      result.cardinality = mul(matched, static_cast<float>(search->k()));
      if (search->joinType() == velox::core::JoinType::kLeft) {
        result.cardinality = maxOf(probe.cardinality, result.cardinality);
      }
      JoinConstraints::refineInnerKeys(
          search->joinType(),
          search->probeKeys(),
          search->buildKeys(),
          result.constraints);
      return result;
    }

    case NodeType::kTableWrite: {
      // A table write emits a single row-count row.
      Estimate result;
      result.cardinality = 1;
      return result;
    }

    // A `row_number` ranking keeps at most `limit` rows per partition, so its
    // output is capped at limit * the number of partitions. `rank` and
    // `dense_rank` keep every row tied at a rank within the limit, which that
    // product does not bound.
    case NodeType::kRowNumber:
    case NodeType::kTopNRowNumber: {
      ExprVector partitionKeys;
      std::optional<int32_t> limit;
      bool capsPartition = true;
      if (node->is(NodeType::kRowNumber)) {
        const auto* rowNumber = node->as<RowNumber>();
        partitionKeys = rowNumber->partitionKeys();
        limit = rowNumber->limit();
      } else {
        const auto* topN = node->as<TopNRowNumber>();
        partitionKeys = topN->partitionKeys();
        limit = topN->limit();
        capsPartition = topN->rankFunction() ==
            velox::core::TopNRowNumberNode::RankFunction::kRowNumber;
      }

      const auto& input = estimate(node->inputs()[0]);
      Estimate result;
      result.constraints = input.constraints;
      result.cardinality = input.cardinality;
      if (!limit.has_value() || !capsPartition) {
        return result;
      }

      const std::optional<float> numPartitions = estimateNumGroups(
          partitionKeys, input.constraints, input.cardinality);
      // An unknown partition count leaves the input cardinality unchanged.
      if (numPartitions.has_value()) {
        result.cardinality = maxOf(
            1.0f,
            minOf(
                input.cardinality,
                mul(numPartitions, static_cast<float>(limit.value()))));
      }
      return result;
    }

    case NodeType::kFixedPoint: {
      const auto* loop = node->as<FixedPoint>();
      const Estimate& state = estimateLoopState(*loop, loop->anchor());
      // An appending loop returns every iteration's rows, and the number of
      // iterations is unknown.
      if (loop->mode() == FixedPointMode::kAppend) {
        return {};
      }
      return state;
    }

    case NodeType::kRepeat: {
      // The number of walks depends on the graph's degrees and the hop bound,
      // which leaf statistics do not describe.
      Estimate result;
      result.cardinality = std::nullopt;
      return result;
    }

    case NodeType::kWorkingTable: {
      // An accumulated read is unknown: in an appending loop it returns every
      // iteration's rows.
      const auto* read = node->as<WorkingTable>();
      if (read->readMode() == WorkingTableReadMode::kAccumulated) {
        return {};
      }
      const Estimate* state = findLoopState(read->stateColumns().front());
      if (state == nullptr) {
        return {};
      }
      return remap(*state, read->stateColumns(), read->outputColumns());
    }

    case NodeType::kEnforceSingleRow: {
      const auto& input = estimate(node->inputs()[0]);
      Estimate result;
      result.cardinality = input.cardinality;
      result.constraints = input.constraints;
      return result;
    }

    case NodeType::kProject: {
      // Each output takes the estimate of its expression over the input's.
      const auto* project = node->as<Project>();
      const auto& input = estimate(node->inputs()[0]);
      Estimate result;
      result.cardinality = input.cardinality;
      result.constraints = input.constraints;
      ConstraintMap expressions = input.constraints;
      ExprEstimator estimator{expressions};
      for (size_t i = 0; i < project->exprs().size(); ++i) {
        result.constraints.insert_or_assign(
            project->outputColumns()[i]->id(),
            estimator.estimate(project->exprs()[i]));
      }
      return result;
    }

    // Cardinality-neutral operators: pass the input's estimate through.
    case NodeType::kSort:
    case NodeType::kWindow:
    case NodeType::kInference:
    case NodeType::kMarkDistinct:
    case NodeType::kAssignUniqueId:
    case NodeType::kEnforceDistinct:
    case NodeType::kGroupId:
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
