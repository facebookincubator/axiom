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
#include "axiom/optimizer/v2/ExpandAggregatePass.h"

#include <span>

#include <fmt/format.h>

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Appends each non-literal column reference in 'args' to 'keys' if not
// already present. Used to build a MarkDistinct key set as
// `groupingKeys ∪ aggregate.args()`. Literals contribute nothing.
ExprVector unionColumnArgs(const ExprVector& keys, const ExprVector& args) {
  ExprVector merged = keys;
  PlanObjectSet seen = PlanObjectSet::fromObjects(keys);
  for (ExprCP arg : args) {
    if (arg->is(PlanType::kLiteralExpr)) {
      continue;
    }
    VELOX_CHECK(
        arg->is(PlanType::kColumnExpr),
        "Expected column or literal aggregate arg: {}",
        arg->toString());
    if (seen.contains(arg)) {
      continue;
    }
    seen.add(arg);
    merged.push_back(arg);
  }
  return merged;
}

// True when every aggregate shares a single distinct signature: all DISTINCT,
// no FILTER, no ORDER BY, and the same column arguments. Velox then dedups them
// in one native distinct aggregation pass (aggregates keep `distinct=true`).
// Any other mix needs MarkDistinct.
bool canUseNativeDistinct(const AggregateCallVector& aggregates) {
  if (aggregates.empty()) {
    return false;
  }
  std::optional<PlanObjectSet> commonArgs;
  for (const auto* aggregate : aggregates) {
    if (!aggregate->isDistinct() || aggregate->condition() != nullptr ||
        !aggregate->orderKeys().empty()) {
      return false;
    }
    PlanObjectSet columnArgs =
        PlanObjectSet::fromObjects(unionColumnArgs({}, aggregate->args()));
    if (!commonArgs.has_value()) {
      commonArgs = std::move(columnArgs);
    } else if (columnArgs != *commonArgs) {
      return false;
    }
  }
  return true;
}

// One MarkDistinct group: all distinct aggregates whose key set
// `(groupingKeys ∪ args)` equals 'keys'. Within a group, each unique FILTER
// condition gets its own per-mask marker; aggregates with no FILTER share
// `markers[0]` (the no-mask marker).
struct MarkDistinctGroup {
  ExprVector keys;
  ColumnVector markers;
  ColumnVector masks;
  // Maps each FILTER condition to the marker that records first occurrence
  // among rows where that condition is true. `nullptr` keys the no-mask
  // marker (`markers[0]`).
  folly::F14FastMap<ExprCP, ColumnCP> filterToMarker;
};

struct DistinctExpansion {
  NodeCP input;
  AggregateCallVector aggregates;
};

class AggregateExpander : public NodeRewriter<> {
 public:
  explicit AggregateExpander(Builder& builder) : NodeRewriter(builder) {}

 protected:
  NodeCP rewriteAggregate(const Aggregate* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);

    // Grouping-set lowering already ran in translate, so the group-id column
    // (if any) is one of the grouping keys and MarkDistinct dedup is per
    // grouping set.
    AggregateCallVector aggregates = node->aggregates();
    DistinctExpansion distinct =
        expandDistinct(newInput, node->groupingKeys(), aggregates);
    newInput = distinct.input;
    aggregates = std::move(distinct.aggregates);

    if (newInput == node->input() && aggregates == node->aggregates()) {
      return node;
    }
    return builder().make<Aggregate>(Aggregate::Key{
        .input = newInput,
        .groupingKeys = node->groupingKeys(),
        .aggregates = std::move(aggregates),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
  }

 private:
  // Lowers DISTINCT aggregates. When they share a single distinct signature
  // (see `canUseNativeDistinct`) the aggregates are left as-is for Velox's
  // native distinct aggregation. Otherwise each unique `(groupingKeys ∪ args)`
  // set gets a `MarkDistinct` and its aggregates are rewritten as non-distinct
  // with the marker as their FILTER. Distinct aggregates whose args ⊆
  // `groupingKeys` are redundant (GROUP BY already dedups) and keep a native
  // distinct flag without a marker.
  DistinctExpansion expandDistinct(
      NodeCP input,
      const ExprVector& groupingKeys,
      const AggregateCallVector& aggregates) {
    if (canUseNativeDistinct(aggregates)) {
      return {input, aggregates};
    }

    PlanObjectSet groupingKeySet = PlanObjectSet::fromObjects(groupingKeys);
    folly::F14VectorMap<PlanObjectSet, MarkDistinctGroup> groups;
    folly::F14FastMap<const optimizer::Aggregate*, ColumnCP> aggregateToMarker;

    bool anyDistinct = false;
    for (const auto* aggregate : aggregates) {
      if (!aggregate->isDistinct()) {
        continue;
      }
      anyDistinct = true;

      ExprVector keys = unionColumnArgs(groupingKeys, aggregate->args());
      PlanObjectSet keySet = PlanObjectSet::fromObjects(keys);
      if (keySet == groupingKeySet) {
        continue;
      }

      auto [groupIt, isNewGroup] = groups.try_emplace(keySet);
      auto& group = groupIt->second;
      if (isNewGroup) {
        group.keys = std::move(keys);
        group.markers.push_back(Column::createBoolean("mark"));
        group.filterToMarker[nullptr] = group.markers.back();
      }

      ExprCP filter = aggregate->condition();
      auto [filterIt, isNewFilter] =
          group.filterToMarker.try_emplace(filter, nullptr);
      if (isNewFilter) {
        group.markers.push_back(Column::createBoolean("mark"));
        filterIt->second = group.markers.back();

        ColumnCP maskColumn = filter->as<Column>();
        VELOX_CHECK_NOT_NULL(
            maskColumn,
            "MarkDistinct mask must be a Column reference; got: {}",
            filter->toString());
        group.masks.push_back(maskColumn);
      }
      aggregateToMarker[aggregate] = filterIt->second;
    }

    if (!anyDistinct) {
      return {input, aggregates};
    }

    NodeCP currentInput = input;
    // F14VectorMap iterates in LIFO; reverse to keep insertion order so the
    // first encountered key set sits closest to the original input.
    for (auto it = groups.rbegin(); it != groups.rend(); ++it) {
      auto& group = it->second;
      ColumnVector outputColumns;
      outputColumns.reserve(
          currentInput->outputColumns().size() + group.markers.size());
      appendAll(outputColumns, currentInput->outputColumns());
      appendAll(outputColumns, group.markers);

      currentInput = builder().make<MarkDistinct>(MarkDistinct::Key{
          currentInput,
          group.markers,
          group.keys,
          group.masks,
          std::move(outputColumns),
      });
    }

    AggregateCallVector newAggregates;
    newAggregates.reserve(aggregates.size());
    for (const auto* aggregate : aggregates) {
      if (auto it = aggregateToMarker.find(aggregate);
          it != aggregateToMarker.end()) {
        newAggregates.push_back(
            aggregate->replaceDistinctAndFilterByMarker(it->second));
      } else {
        // Either non-distinct, or distinct whose args are all in
        // `groupingKeys` (per-group dedup is implicit; Velox handles
        // `distinct=true` natively for the trivial case).
        newAggregates.push_back(aggregate);
      }
    }
    return {currentInput, std::move(newAggregates)};
  }
};

} // namespace

NodeCP ExpandAggregatePass::run(NodeCP root, Builder& builder) {
  AggregateExpander expander{builder};
  return expander.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
