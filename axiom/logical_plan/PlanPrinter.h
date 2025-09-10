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

namespace facebook::axiom::logical_plan {

struct PlanSummaryOptions {
  /// Options that apply specifically to PROJECT nodes.
  struct ProjectOptions {
    /// For a given PROJECT node, maximum number of non-identity projection
    /// expressions to include in the summary. By default, no expression is
    /// included.
    size_t maxProjections = 0;

    /// For a given PROJECT node, maximum number of dereference (access of a
    /// struct field) expressions to include in the summary. By default, no
    /// expression is included.
    size_t maxDereferences = 0;
  };

  ProjectOptions project = {};

  /// For a given node, maximum number of output fields to include in the
  /// summary. Each field has a name and a type. The amount of type information
  /// is controlled by 'maxChildTypes' option. Use 0 to include only the number
  /// of output fields.
  size_t maxOutputFields = 5;

  /// For a given output type, maximum number of child types to include in the
  /// summary. By default, only top-level type is included: BIGINT, ARRAY, MAP,
  /// ROW. Set to 2 to include types of array elements, map keys and values as
  /// well as up to 2 fields of a struct: ARRAY(REAL), MAP(INTEGER, ARRAY),
  /// ROW(VARCHAR, ARRAY,...).
  size_t maxChildTypes = 0;

  /// Controls the maximum length of a string that is included in the plan
  /// summary.
  size_t maxLength = 50;

  /// Options that apply specifically to AGGREGATION nodes.
  struct AggregateOptions {
    /// For a given AGGREGATION node, maximum number of aggregate expressions
    /// to include in the summary. By default, no aggregate expression is
    /// included.
    size_t maxAggregations = 0;
  };

  AggregateOptions aggregate = {};
};

class PlanPrinter {
 public:
  static std::string toText(const LogicalPlanNode& root);

  static std::string summarizeToText(
      const LogicalPlanNode& root,
      const PlanSummaryOptions& options = {});

  // Returns even shorter summary of the plan. Drops all Project nodes. Drops
  // output column names and types. Drops all details except table name for
  // TableScan node.
  static std::string toSkeletonText(const LogicalPlanNode& root);
};

} // namespace facebook::axiom::logical_plan
