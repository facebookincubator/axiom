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

namespace facebook::axiom::optimizer::v2 {

/// Facts about the relation a node computes, independent of where its rows
/// are placed. Nodes before PlanPhysical carry these; nodes PlanPhysical
/// builds carry `PhysicalProperties` instead (see `Node`).
struct RelationalProperties {
  /// Unique keys of the relation. A unique key is a set of columns that
  /// determines the row: no two rows agree on all of them, with NULL equal to
  /// NULL. An aggregation by `a, b` is unique on `{a, b}`; the empty set means
  /// at most one row.
  QGVector<PlanObjectSet> unique;

  /// True when the relation has exactly one row, as an aggregation without
  /// grouping keys does.
  bool singleRow{false};

  /// Checks that every unique key consists of the node's own 'outputColumns'.
  void checkReferencesColumns(const PlanObjectSet& outputColumns) const;
};

} // namespace facebook::axiom::optimizer::v2
