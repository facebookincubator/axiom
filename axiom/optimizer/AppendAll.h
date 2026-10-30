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
#pragma once

#include "axiom/optimizer/PlanObject.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::optimizer {

/// Appends every element of `src` to `dst`. Used to concatenate
/// `ColumnVector` / `ExprVector` when building output column lists or
/// expression lists for new nodes.
template <typename Dst, typename Src>
void appendAll(Dst& dst, const Src& src) {
  dst.insert(dst.end(), src.begin(), src.end());
}

/// Returns `columns` as expressions without changing their identities.
inline ExprVector toExprs(const ColumnVector& columns) {
  return ExprVector(columns.begin(), columns.end());
}

/// Returns `expressions` as columns. Fails if any expression is not a column.
inline ColumnVector toColumns(const ExprVector& expressions) {
  ColumnVector columns;
  columns.reserve(expressions.size());
  for (ExprCP expression : expressions) {
    VELOX_CHECK(expression->isColumn());
    columns.push_back(expression->as<Column>());
  }
  return columns;
}

/// Returns a new vector containing the first `count` elements of `src`.
/// Caller must ensure `count <= src.size()`.
template <typename V>
V prefix(const V& src, size_t count) {
  return V(src.begin(), src.begin() + count);
}

} // namespace facebook::axiom::optimizer
