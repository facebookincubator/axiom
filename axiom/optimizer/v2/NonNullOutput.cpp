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

#include "axiom/optimizer/v2/NonNullOutput.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Adds the columns of each of 'keys' that a NULL makes NULL: an equi-key of
// default null behavior, which no NULL matches.
void addMatchedKeyColumns(const ExprVector& keys, PlanObjectSet& columns) {
  for (ExprCP key : keys) {
    if (!key->containsNonDefaultNullBehavior()) {
      columns.unionColumns(key);
    }
  }
}

} // namespace

const PlanObjectSet& NonNullOutput::get(NodeCP node) {
  if (const auto it = cache_.find(node); it != cache_.end()) {
    return it->second;
  }

  PlanObjectSet result;
  if (node->is(NodeType::kProject)) {
    const auto* project = node->as<Project>();
    const auto& inputNonNull = get(project->input());
    for (size_t i = 0; i < project->exprs().size(); ++i) {
      if (simplifier_.isKnownNonNull(project->exprs()[i], inputNonNull)) {
        result.add(project->outputColumns()[i]);
      }
    }
    return save(node, std::move(result));
  }

  if (node->is(NodeType::kAggregate)) {
    const auto* aggregate = node->as<Aggregate>();
    const auto& inputNonNull = get(aggregate->input());
    for (size_t i = 0; i < aggregate->groupingKeys().size(); ++i) {
      if (simplifier_.isKnownNonNull(
              aggregate->groupingKeys()[i], inputNonNull)) {
        result.add(aggregate->outputColumns()[i]);
      }
    }
    return save(node, std::move(result));
  }

  if (node->is(NodeType::kUnionAll)) {
    const auto* unionAll = node->as<UnionAll>();
    for (size_t output = 0; output < unionAll->outputColumns().size();
         ++output) {
      bool nonNull{true};
      for (size_t leg = 0; leg < unionAll->inputs().size(); ++leg) {
        if (!get(unionAll->inputs()[leg])
                 .contains(unionAll->legColumns()[leg][output])) {
          nonNull = false;
          break;
        }
      }
      if (nonNull) {
        result.add(unionAll->outputColumns()[output]);
      }
    }
    return save(node, std::move(result));
  }

  if (node->is(NodeType::kVectorSearch)) {
    const auto* search = node->as<VectorSearch>();
    const auto preserved = Join::preservedSides(search->joinType());
    if (preserved.left) {
      propagate(get(search->probe()), search->outputColumns(), result);
    }
    if (preserved.right) {
      propagate(get(search->build()), search->outputColumns(), result);
    }
    // An inner search outputs only matched rows: their keys, embedding and
    // index are non-null, and each has an entry with an id and a score.
    if (search->joinType() == velox::core::JoinType::kInner) {
      PlanObjectSet matched;
      addMatchedKeyColumns(search->probeKeys(), matched);
      addMatchedKeyColumns(search->buildKeys(), matched);
      matched.add(search->probeEmbedding());
      matched.add(search->id());
      matched.add(search->score());
      propagate(matched, search->outputColumns(), result);
    }
    return save(node, std::move(result));
  }

  if (node->is(NodeType::kJoin)) {
    const auto* join = node->as<Join>();
    const auto preserved = Join::preservedSides(join->joinType());
    if (preserved.left) {
      propagate(get(join->left()), join->outputColumns(), result);
    }
    if (preserved.right) {
      propagate(get(join->right()), join->outputColumns(), result);
    }
    if (join->isInner() && !join->nullAsValue()) {
      PlanObjectSet equiColumns;
      addMatchedKeyColumns(join->leftKeys(), equiColumns);
      addMatchedKeyColumns(join->rightKeys(), equiColumns);
      propagate(equiColumns, join->outputColumns(), result);
    }
    return save(node, std::move(result));
  }

  switch (node->nodeType()) {
    case NodeType::kFilter:
    case NodeType::kLimit:
    case NodeType::kSort:
    case NodeType::kTopN:
    case NodeType::kGroupId:
    case NodeType::kMarkDistinct:
    case NodeType::kUnnest:
    case NodeType::kWindow:
    case NodeType::kInference:
    case NodeType::kRowNumber:
    case NodeType::kTopNRowNumber:
    case NodeType::kAssignUniqueId:
    case NodeType::kEnforceDistinct:
    case NodeType::kExchange:
      VELOX_DCHECK_EQ(node->inputs().size(), 1);
      propagate(get(node->inputs()[0]), node->outputColumns(), result);
      break;
    default:
      break;
  }
  return save(node, std::move(result));
}

// static
void NonNullOutput::propagate(
    const PlanObjectSet& source,
    const ColumnVector& outputColumns,
    PlanObjectSet& result) {
  for (ColumnCP column : outputColumns) {
    if (source.contains(column)) {
      result.add(column);
    }
  }
}

const PlanObjectSet& NonNullOutput::save(NodeCP node, PlanObjectSet result) {
  return cache_.emplace(node, std::move(result)).first->second;
}

} // namespace facebook::axiom::optimizer::v2
