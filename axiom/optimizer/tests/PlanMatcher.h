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

#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"

namespace facebook::velox::core {

/// PlanMatcher is used to verify the structure and content of a Velox plan.
///
/// Expression Syntax:
/// ------------------
/// String expressions (e.g., in filter(), project(), singleAggregation()) are
/// parsed as DuckDB SQL. Use DuckDB-compatible syntax for expressions.
/// See https://duckdb.org/docs/stable/sql/functions/overview for reference.
///
/// Symbol Rewriting:
/// -----------------
/// PlanMatcher supports symbol (alias) capture and rewriting to allow
/// verification of expressions that reference columns from child nodes.
///
/// When a matcher specifies an expression with an alias (e.g., "count(*) as
/// c"), the alias is captured and mapped to the actual column name in the plan.
/// Subsequent matchers can then use the alias in their expressions, and it will
/// be rewritten to the actual column name before comparison.
///
/// Example:
///   .singleAggregation({}, {"count(*) as c"})  // Captures alias 'c'
///   .filter("not(eq(c, 0))")                   // 'c' is rewritten to actual
///                                              // column name (e.g.,
///                                              // "__count6")
///
/// Symbol rewriting is supported by: ProjectMatcher, AggregationMatcher,
/// FilterMatcher, JoinMatcher. The symbols are propagated from child matchers
/// to parent matchers during plan matching.
class PlanMatcher {
 public:
  virtual ~PlanMatcher() = default;

  struct MatchResult {
    const bool match;

    /// Mapping from an alias specified in the PlanMatcher to the actual symbol
    /// found in the plan.
    const std::unordered_map<std::string, std::string> symbols;

    static MatchResult success(
        std::unordered_map<std::string, std::string> symbols = {}) {
      return MatchResult{true, std::move(symbols)};
    }

    static MatchResult failure() {
      return MatchResult{false, {}};
    }
  };

  bool match(const PlanNodePtr& plan) const {
    return match(plan, {}).match;
  }

  virtual MatchResult match(
      const PlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols) const = 0;
};

class PlanMatcherBuilder {
 public:
  /// Matches any TableScan node regardless of table name or output type.
  PlanMatcherBuilder& tableScan();

  /// Matches a TableScan node with the specified table name.
  /// @param tableName The expected table name.
  PlanMatcherBuilder& tableScan(const std::string& tableName);

  /// Matches a TableScan node with the specified table name and output type.
  /// @param tableName The name of the table.
  /// @param outputType The list of schema names and types of columns in the
  /// output of the scan node.
  PlanMatcherBuilder& tableScan(
      const std::string& tableName,
      const RowTypePtr& outputType);

  /// Matches a Hive TableScan node with the specified table name, subfield
  /// filters, and optional remaining filter.
  /// @param tableName The name of the table.
  /// @param subfieldFilters Filters pushed down into the scan as subfield
  /// filters.
  /// @param remainingFilter Optional filter expression that couldn't be pushed
  /// down into the scan.
  PlanMatcherBuilder& hiveScan(
      const std::string& tableName,
      common::SubfieldFilters subfieldFilters,
      const std::string& remainingFilter = "");

  /// Matches any Values node regardless of type.
  PlanMatcherBuilder& values();

  /// Matches a Values node with the specified output type.
  /// @param outputType The expected output type of the Values node.
  PlanMatcherBuilder& values(const RowTypePtr& outputType);

  /// Matches any Filter node regardless of predicate.
  PlanMatcherBuilder& filter();

  /// Matches a Filter node with the specified predicate expression.
  /// Supports symbol rewriting from child matchers.
  /// @param predicate The expected filter predicate (DuckDB SQL syntax).
  PlanMatcherBuilder& filter(const std::string& predicate);

  /// Matches any Project node regardless of expressions.
  PlanMatcherBuilder& project();

  /// Matches a Project node with the specified projection expressions.
  /// Expressions with aliases (e.g., "a + b as c") capture the alias for use
  /// in parent matchers via symbol rewriting.
  /// @param expressions The expected projection expressions (DuckDB SQL
  /// syntax).
  PlanMatcherBuilder& project(const std::vector<std::string>& expressions);

  /// Matches any ParallelProject node regardless of expressions.
  PlanMatcherBuilder& parallelProject();

  /// Matches a ParallelProject node with the specified projection expressions.
  /// @param expressions The expected projection expressions (DuckDB SQL
  /// syntax).
  PlanMatcherBuilder& parallelProject(
      const std::vector<std::string>& expressions);

  /// Matches any Unnest node regardless of expressions.
  PlanMatcherBuilder& unnest();

  /// Matches an Unnest node with the specified replicate and unnest
  /// expressions.
  /// @param replicateExprs Expressions that are replicated for each unnested
  /// row. Supports symbol rewriting from child matchers (see class
  /// documentation).
  /// @param unnestExprs Array/map expressions to unnest. Supports symbol
  /// rewriting from child matchers (see class documentation).
  /// @param ordinalityName Optional name for the ordinality column.
  PlanMatcherBuilder& unnest(
      const std::vector<std::string>& replicateExprs,
      const std::vector<std::string>& unnestExprs,
      const std::optional<std::string>& ordinalityName = std::nullopt);

  /// Matches any Aggregation node regardless of step or expressions.
  PlanMatcherBuilder& aggregation();

  /// Matches any single (non-distributed) Aggregation node.
  PlanMatcherBuilder& singleAggregation();

  /// Matches a single (non-distributed) Aggregation node with the specified
  /// grouping keys and aggregate expressions.
  /// @param groupingKeys Columns to group by.
  /// @param aggregates Aggregate expressions (e.g., "sum(x)", "count(*) as c").
  /// Supports alias capture for symbol rewriting in parent matchers.
  PlanMatcherBuilder& singleAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  /// Matches any partial Aggregation node.
  PlanMatcherBuilder& partialAggregation();

  /// Matches a partial Aggregation node with the specified grouping keys and
  /// aggregate expressions.
  /// @param groupingKeys Columns to group by.
  /// @param aggregates Aggregate expressions.
  PlanMatcherBuilder& partialAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  /// Matches any final Aggregation node.
  PlanMatcherBuilder& finalAggregation();

  /// Matches a final Aggregation node with the specified grouping keys and
  /// aggregate expressions.
  /// @param groupingKeys Columns to group by.
  /// @param aggregates Aggregate expressions.
  PlanMatcherBuilder& finalAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  /// Matches a HashJoin node with the specified right side matcher.
  /// @param rightMatcher Matcher for the right (build) side of the join.
  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher);

  /// Matches a HashJoin node with the specified right side matcher, join type,
  /// and nullAware flag.
  /// @param rightMatcher Matcher for the right (build) side of the join.
  /// @param joinType Type of join (e.g., kInner, kLeft, kRight).
  /// @param nullAware When true, the join semantic is IN / NOT IN. When false,
  /// the join semantic is EXISTS / NOT EXISTS. Applies only to semi project
  /// and anti joins.
  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType,
      bool nullAware = false);

  /// Matches a NestedLoopJoin node with the specified right side matcher and
  /// join type.
  /// @param rightMatcher Matcher for the right side of the join.
  /// @param joinType Type of join (defaults to kInner).
  PlanMatcherBuilder& nestedLoopJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType = JoinType::kInner);

  /// Matches any LocalPartition node.
  PlanMatcherBuilder& localPartition();

  /// Matches a LocalPartition node with the specified source matchers.
  /// @param matcher Matchers for the partition sources.
  PlanMatcherBuilder& localPartition(
      std::initializer_list<std::shared_ptr<PlanMatcher>> matcher);

  /// Matches a LocalPartition node with a single source matcher.
  /// @param matcher Matcher for the partition source.
  PlanMatcherBuilder& localPartition(
      const std::shared_ptr<PlanMatcher>& matcher) {
    return localPartition({matcher});
  }

  /// Matches any LocalMerge node.
  PlanMatcherBuilder& localMerge();

  /// Matches any PartitionedOutput node.
  PlanMatcherBuilder& partitionedOutput();

  /// Matches any Exchange node.
  PlanMatcherBuilder& exchange();

  /// Matches any MergeExchange node.
  PlanMatcherBuilder& mergeExchange();

  /// Matches any Limit node regardless of offset, count, or partial/final step.
  PlanMatcherBuilder& limit();

  /// Matches a partial Limit node with the specified offset and count.
  /// @param offset Number of rows to skip.
  /// @param count Maximum number of rows to return.
  PlanMatcherBuilder& partialLimit(int64_t offset, int64_t count);

  /// Matches a final Limit node with the specified offset and count.
  /// @param offset Number of rows to skip.
  /// @param count Maximum number of rows to return.
  PlanMatcherBuilder& finalLimit(int64_t offset, int64_t count);

  /// Matches any TopN node.
  PlanMatcherBuilder& topN();

  /// Matches a TopN node with the specified count.
  /// @param count Maximum number of rows to return.
  PlanMatcherBuilder& topN(int64_t count);

  /// Matches any OrderBy node.
  PlanMatcherBuilder& orderBy();

  /// Matches an OrderBy node with the specified ordering.
  /// @param ordering List of sort keys (e.g., {"a ASC", "b DESC"}).
  PlanMatcherBuilder& orderBy(const std::vector<std::string>& ordering);

  /// Matches any TableWrite node.
  PlanMatcherBuilder& tableWrite();

  /// Builds and returns the constructed PlanMatcher.
  /// @throws VeloxUserError if matcher is empty.
  std::shared_ptr<PlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(matcher_, "Cannot build an empty PlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<PlanMatcher> matcher_;
};

} // namespace facebook::velox::core
