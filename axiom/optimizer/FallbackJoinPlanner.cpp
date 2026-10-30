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

#include "axiom/optimizer/FallbackJoinPlanner.h"

#include <algorithm>
#include <utility>

namespace facebook::axiom::optimizer {

std::vector<MemoOpCP> FallbackJoinPlanner::build(
    const std::vector<RelationSet>& components) {
  if (!graph_.expandedRelationIds().empty() ||
      !std::ranges::all_of(graph_.edges(), [](const auto& edge) {
        return !edge.isUnnest() &&
            edge.joinType() == velox::core::JoinType::kInner;
      })) {
    return {};
  }

  std::vector<MemoOpCP> roots;
  roots.reserve(components.size());
  for (const auto& component : components) {
    MemoOpCP root = buildComponent(component);
    if (root == nullptr) {
      return {};
    }
    roots.push_back(root);
  }
  if (!reordered_) {
    return {};
  }
  graph_.checkEdgesEnforced(appliedEdges_);
  return roots;
}

MemoOpCP FallbackJoinPlanner::makeLeaf(int32_t relationId) {
  auto leaf = std::make_unique<LeafOp>(Cost{}, relationId);
  MemoOpCP result = leaf.get();
  memoOps_.push_back(std::move(leaf));
  return result;
}

MemoOpCP FallbackJoinPlanner::buildComponent(const RelationSet& component) {
  RelationSet placed = RelationSet::singleton(component.min());
  MemoOpCP root = makeLeaf(component.min());
  while (placed != component) {
    int32_t firstUnplaced{-1};
    int32_t nextId{-1};
    std::optional<JoinHypergraph::CrossingEdges> ready;
    component.forEach([&](int32_t relationId) {
      if (placed.contains(relationId)) {
        return;
      }
      if (firstUnplaced < 0) {
        firstUnplaced = relationId;
      }
      if (nextId >= 0) {
        return;
      }
      auto candidateEdges =
          graph_.crossingEdges(placed, RelationSet::singleton(relationId));
      if (candidateEdges.has_value() && !candidateEdges->joinEdges.empty()) {
        nextId = relationId;
        ready = std::move(candidateEdges);
      }
    });
    if (nextId < 0) {
      return nullptr;
    }
    reordered_ |= nextId != firstUnplaced;

    std::vector<size_t> keyEdges;
    keyEdges.reserve(ready->joinEdges.size());
    for (const auto& edge : ready->joinEdges) {
      keyEdges.push_back(edge.index);
    }
    keyEdges = graph_.canonicalKeyEdges(std::move(keyEdges));

    MemoOpCP right = makeLeaf(nextId);
    auto join = std::make_unique<JoinOp>(
        Cost{},
        root,
        right,
        keyEdges.front(),
        velox::core::JoinType::kInner,
        /*reversedAnti=*/false,
        std::vector<size_t>{keyEdges.begin() + 1, keyEdges.end()},
        ready->filterEdges);
    root = join.get();
    memoOps_.push_back(std::move(join));
    appliedEdges_.insert(keyEdges.begin(), keyEdges.end());
    appliedEdges_.insert(ready->filterEdges.begin(), ready->filterEdges.end());
    placed.add(nextId);
  }
  return root;
}

} // namespace facebook::axiom::optimizer
