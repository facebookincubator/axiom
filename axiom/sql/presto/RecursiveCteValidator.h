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

#include <string>

#include "axiom/sql/presto/ast/AstNodesAll.h"

namespace axiom::sql::presto {

/// Detection and validation helpers for `WITH RECURSIVE` CTE bodies. The
/// translation methods (anchor / step / FixedPointNode assembly) live in
/// `RelationPlanner` -- they touch parser-private state directly.
class RecursiveCteValidator {
 public:
  /// Returns true if `body` references a table named `cteName`
  /// (canonicalized upstream). Descent stops at a nested query that
  /// redefines the same name.
  static bool referencesCte(const Node& body, const std::string& cteName);

  /// Checks that the CTE body is `UNION ALL`, the self-reference is in
  /// the recursive (right) term only, and the enclosing Query has no
  /// ORDER BY / LIMIT / OFFSET. Also runs the recursive-term validator on
  /// the right term (rejects DISTINCT, GROUP BY, HAVING, ORDER BY, LIMIT,
  /// OFFSET, window functions, aggregate functions, recursive refs in
  /// scalar / IN / EXISTS subqueries, and recursive refs on the nullable
  /// side of an outer join). Returns the validated `Union` (always
  /// non-null on success).
  static const Union* extractAndValidateRecursiveUnion(
      const WithQuery& withEntry,
      const std::string& cteName);
};

} // namespace axiom::sql::presto
