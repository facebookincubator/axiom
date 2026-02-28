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

namespace facebook::axiom::logical_plan::test {

/// Verifies the structure of a logical plan tree. Each matcher matches a
/// specific node type and recursively verifies its inputs.
class LogicalPlanMatcher {
 public:
  virtual ~LogicalPlanMatcher() = default;

  /// Matches the plan against this matcher. Sets gtest non-fatal failures on
  /// mismatch.
  virtual bool match(const LogicalPlanNodePtr& plan) const = 0;
};

/// Builds a LogicalPlanMatcher using a fluent API. Leaf nodes (tableScan,
/// values) must be added first, then intermediate nodes are chained on top.
///
/// For each plan node type, there is a method that matches the node by type
/// only. An optional callback can be used to capture or inspect the matched
/// node. Some node types also have overloads that verify specific properties
/// directly, e.g. limit(offset, count) and values(outputType).
///
/// Usage:
///   // Match: TableScan -> Filter -> Project
///   auto matcher = LogicalPlanMatcherBuilder()
///       .tableScan()
///       .filter()
///       .project();
///   testSelect("SELECT a + 1 FROM t WHERE a > 0", matcher);
///
///   // Match with property verification:
///   auto matcher = LogicalPlanMatcherBuilder()
///       .tableScan()
///       .limit(0, 10);
///
///   // Match with callback for custom inspection:
///   auto matcher = LogicalPlanMatcherBuilder()
///       .tableScan()
///       .project([&](const auto& node) {
///         auto project = std::dynamic_pointer_cast<const ProjectNode>(node);
///         ASSERT_EQ(1, project->expressions().size());
///       });
///
///   // Join with separately built right side:
///   auto right = LogicalPlanMatcherBuilder().tableScan().build();
///   auto matcher = LogicalPlanMatcherBuilder()
///       .tableScan()
///       .join(right)
///       .project();
class LogicalPlanMatcherBuilder {
 public:
  /// Callback invoked when a node matches. Use to capture or inspect the
  /// matched node.
  using OnMatchCallback = std::function<void(const LogicalPlanNodePtr&)>;

  /// Matches a TableWriteNode.
  LogicalPlanMatcherBuilder& tableWrite(OnMatchCallback onMatch = nullptr);

  /// Matches a TableScanNode. Must be the first node in the chain (leaf).
  LogicalPlanMatcherBuilder& tableScan(OnMatchCallback onMatch = nullptr);

  /// Matches a TableScanNode with the specified table name.
  LogicalPlanMatcherBuilder& tableScan(const std::string& tableName);

  /// Matches a ValuesNode. Must be the first node in the chain (leaf).
  LogicalPlanMatcherBuilder& values(OnMatchCallback onMatch = nullptr);

  /// Matches a ValuesNode with the specified output type.
  LogicalPlanMatcherBuilder& values(
      velox::RowTypePtr outputType,
      OnMatchCallback onMatch = nullptr);

  /// Matches a FilterNode.
  LogicalPlanMatcherBuilder& filter(OnMatchCallback onMatch = nullptr);

  /// Matches a ProjectNode.
  LogicalPlanMatcherBuilder& project(OnMatchCallback onMatch = nullptr);

  /// Matches a ProjectNode with the specified expressions. Each expected
  /// expression is parsed with DuckDB and printed in a format compatible with
  /// lp::ExprPrinter, then compared against expressionAt(i)->toString().
  LogicalPlanMatcherBuilder& project(
      const std::vector<std::string>& expressions);

  /// Matches an AggregateNode.
  LogicalPlanMatcherBuilder& aggregate(OnMatchCallback onMatch = nullptr);

  /// Matches an AggregateNode used for deduplication (no aggregate functions,
  /// no grouping sets, all output columns are grouping keys).
  LogicalPlanMatcherBuilder& distinct();

  /// Matches an UnnestNode. Can be a leaf or intermediate node.
  LogicalPlanMatcherBuilder& unnest(OnMatchCallback onMatch = nullptr);

  /// Matches a JoinNode with the specified right-side matcher.
  LogicalPlanMatcherBuilder& join(
      const std::shared_ptr<LogicalPlanMatcher>& rightMatcher,
      OnMatchCallback onMatch = nullptr);

  /// Matches a SetNode with the specified operation and right-side matcher.
  LogicalPlanMatcherBuilder& setOperation(
      SetOperation op,
      const std::shared_ptr<LogicalPlanMatcher>& matcher,
      OnMatchCallback onMatch = nullptr);

  /// Matches a SetNode with kUnionAll operation.
  LogicalPlanMatcherBuilder& unionAll(
      const std::shared_ptr<LogicalPlanMatcher>& matcher,
      OnMatchCallback onMatch = nullptr);

  /// Matches a SetNode with kExcept operation.
  LogicalPlanMatcherBuilder& except(
      const std::shared_ptr<LogicalPlanMatcher>& matcher,
      OnMatchCallback onMatch = nullptr);

  /// Matches a SetNode with kIntersect operation.
  LogicalPlanMatcherBuilder& intersect(
      const std::shared_ptr<LogicalPlanMatcher>& matcher,
      OnMatchCallback onMatch = nullptr);

  /// Matches a SortNode.
  LogicalPlanMatcherBuilder& sort(OnMatchCallback onMatch = nullptr);

  /// Matches any LimitNode.
  LogicalPlanMatcherBuilder& limit(OnMatchCallback onMatch = nullptr);

  /// Matches a LimitNode with the specified offset and count.
  LogicalPlanMatcherBuilder& limit(int64_t offset, int64_t count);

  /// Matches a SampleNode.
  LogicalPlanMatcherBuilder& sample(OnMatchCallback onMatch = nullptr);

  /// Matches an OutputNode.
  LogicalPlanMatcherBuilder& output(OnMatchCallback onMatch = nullptr);

  /// Matches an OutputNode with the specified output column names.
  LogicalPlanMatcherBuilder& output(
      const std::vector<std::string>& expectedNames);

  /// Builds and returns the constructed LogicalPlanMatcher.
  std::shared_ptr<LogicalPlanMatcher> build();

 private:
  std::shared_ptr<LogicalPlanMatcher> matcher_;
};

} // namespace facebook::axiom::logical_plan::test
