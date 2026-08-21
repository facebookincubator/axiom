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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/ReferencedTables.h"

namespace facebook::axiom::logical_plan {

/// Derives the tables a query references from its plan, for queries that never
/// pass a SQL parser.
class ReferencedTableCollector {
 public:
  /// Returns the tables 'plan' reads and writes. Every TableScan is an input
  /// table, including scans reached through a subquery expression. A TableWrite
  /// is the output table.
  ///
  /// Fails if the plan writes more than one table, rather than reporting one of
  /// them: a caller checking permissions or recording lineage must see every
  /// table written.
  static ReferencedTables collect(const LogicalPlanNode& plan);
};

} // namespace facebook::axiom::logical_plan
