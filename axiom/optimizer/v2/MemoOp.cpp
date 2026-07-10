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

#include "axiom/optimizer/v2/MemoOp.h"

#include <folly/container/F14Map.h>

namespace facebook::axiom::optimizer::v2 {

namespace {

const folly::F14FastMap<MemoOpKind, std::string_view>& memoOpKindNames() {
  static const folly::F14FastMap<MemoOpKind, std::string_view> kNames = {
      {MemoOpKind::kLeaf, "Leaf"},
      {MemoOpKind::kJoin, "Join"},
      {MemoOpKind::kExchange, "Exchange"},
  };
  return kNames;
}

RelationSet combinedCover(MemoOpCP left, MemoOpCP right) {
  RelationSet result{left->cover()};
  result.unionSet(right->cover());
  return result;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(MemoOpKind, memoOpKindNames);

JoinOp::JoinOp(
    Cost cost,
    MemoOpCP leftChild,
    MemoOpCP rightChild,
    size_t edge,
    velox::core::JoinType type,
    bool reversedAnti,
    std::vector<size_t> extraEdges,
    Partitioning outputPartitioning)
    : MemoOp{cost, MemoOpKind::kJoin, std::move(outputPartitioning)},
      left{leftChild},
      right{rightChild},
      edgeIndex{edge},
      joinType{type},
      reversedAnti{reversedAnti},
      extraEdges{std::move(extraEdges)},
      cover_{combinedCover(leftChild, rightChild)} {}

} // namespace facebook::axiom::optimizer::v2
