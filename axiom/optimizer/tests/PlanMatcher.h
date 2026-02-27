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

#include "axiom/runner/MultiFragmentPlan.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Filter.h"

namespace facebook::velox::core {

/// PlanMatcher is used to verify the structure and content of a Velox plan.
/// It supports both single-node plans (via match(PlanNodePtr)) and distributed
/// multi-fragment plans (via match(MultiFragmentPlan)).
///
/// For distributed plans, use shuffle() or shuffleMerge() to mark fragment
/// boundaries. These boundaries are verified against the actual plan structure
/// to ensure Exchange/MergeExchange nodes align with PartitionedOutput nodes.
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

  // Context for distributed plan matching. Passed through the match() method
  // to provide fragment information needed by ShuffleBoundaryMatcher.
  struct DistributedMatchContext {
    const std::vector<axiom::runner::ExecutableFragment>* fragments;
    const axiom::runner::ExecutableFragment* currentFragment;
    const std::unordered_map<std::string, int32_t>* taskPrefixToFragmentIndex;
  };

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

  /// Matches the plan against this matcher.
  /// On mismatch, sets gtest failures with detailed diagnostics. The caller
  /// should not handle 'false' result in any way other than failing the test.
  bool match(const PlanNodePtr& plan) const {
    return match(plan, {}, nullptr).match;
  }

  /// Matches a distributed multi-fragment plan.
  /// On mismatch, sets gtest failures with detailed diagnostics. The caller
  /// should not handle 'false' result in any way other than failing the test.
  /// The matcher must contain shuffle() boundaries that correspond to
  /// fragment boundaries in the plan. Verifies:
  ///   - Plan structure matches within each fragment
  ///   - PartitionedOutput terminates producer fragments
  ///   - Exchange consumes from correct producer fragments
  ///   - Fragment topology matches shuffle boundary structure
  bool match(const axiom::runner::MultiFragmentPlan& plan) const;

  /// Matches the plan against this matcher with symbol rewriting support.
  /// On mismatch, sets gtest failures with detailed diagnostics.
  /// @param plan The plan node to match.
  /// @param symbols Mapping from aliases to actual column names for expression
  /// rewriting.
  /// @param context Optional context for distributed plan matching. When
  /// non-null, enables ShuffleBoundaryMatcher to verify Exchange nodes and
  /// match producer fragments.
  /// @return MatchResult with match status and updated symbol mappings.
  virtual MatchResult match(
      const PlanNodePtr& plan,
      const std::unordered_map<std::string, std::string>& symbols,
      const DistributedMatchContext* context) const = 0;

  /// Returns the number of shuffle boundaries in this matcher and all nested
  /// matchers. Override in subclasses that contain nested matchers.
  virtual int32_t shuffleBoundaryCount() const {
    return 0;
  }
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

  /// Matches any streaming Aggregation node (input is pre-grouped on all
  /// grouping keys).
  PlanMatcherBuilder& streamingAggregation();

  /// Matches a streaming Aggregation node with the specified grouping keys and
  /// aggregate expressions.
  /// @param groupingKeys Columns to group by.
  /// @param aggregates Aggregate expressions.
  PlanMatcherBuilder& streamingAggregation(
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

  /// Matches a HashJoin node with the specified right side matcher, join type,
  /// and expected output column names. Column names are verified as a set —
  /// order does not matter. Supports symbol rewriting from child matchers.
  /// @param rightMatcher Matcher for the right (build) side of the join.
  /// @param joinType Type of join (e.g., kInner, kLeft, kRight).
  /// @param outputColumnNames Expected column names in the join output.
  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType,
      const std::vector<std::string>& outputColumnNames);

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

  /// Matches any Exchange node.
  [[deprecated("Use shuffle() with AXIOM_ASSERT_DISTRIBUTED_PLAN instead")]]
  PlanMatcherBuilder& exchange();

  /// Marks a shuffle boundary (data exchange between fragments).
  /// In a distributed plan:
  ///   - Producer side expects PartitionedOutput node
  ///   - Consumer side expects Exchange node
  /// Cannot be used with match(PlanNodePtr) - use match(MultiFragmentPlan).
  PlanMatcherBuilder& shuffle();

  /// Marks a shuffle boundary with verification of partition keys.
  /// @param keys Expected partition key column names. Supports symbol rewriting
  /// from child matchers.
  PlanMatcherBuilder& shuffle(const std::vector<std::string>& keys);

  /// Marks an ordered shuffle boundary (uses MergeExchange instead of
  /// Exchange). In a distributed plan:
  ///   - Producer side expects PartitionedOutput node
  ///   - Consumer side expects MergeExchange node
  /// Cannot be used with match(PlanNodePtr) - use match(MultiFragmentPlan).
  PlanMatcherBuilder& shuffleMerge();

  /// Matches a broadcast shuffle boundary in a distributed plan.
  /// Verifies that PartitionedOutputNode::isBroadcast() is true.
  PlanMatcherBuilder& broadcast();

  /// Matches a gather shuffle boundary in a distributed plan.
  /// Verifies that PartitionedOutputNode::numPartitions() == 1.
  PlanMatcherBuilder& gather();

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

  /// Matches the distributed limit pattern: partialLimit(0, offset + count) →
  /// localPartition → finalLimit(0, offset + count) → gather →
  /// finalLimit(offset, count).
  /// @param offset Number of rows to skip.
  /// @param count Maximum number of rows to return.
  PlanMatcherBuilder& distributedLimit(int64_t offset, int64_t count);

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

  /// Matches an EnforceSingleRow node, which validates that its input
  /// produces exactly one row. Used for scalar subqueries.
  PlanMatcherBuilder& enforceSingleRow();

  /// Matches an AssignUniqueId node, which assigns unique identifiers to
  /// each input row. Used for decorrelating subqueries with non-equi
  /// correlation conditions.
  PlanMatcherBuilder& assignUniqueId();

  /// Matches an AssignUniqueId node and captures the unique ID column name
  /// as a symbol alias for use in parent matchers via symbol rewriting.
  /// @param alias The alias to use for the unique ID column.
  PlanMatcherBuilder& assignUniqueId(const std::string& alias);

  /// Matches an EnforceDistinct node, which validates that input has unique
  /// values for the specified key columns. Throws if duplicates are found.
  PlanMatcherBuilder& enforceDistinct();

  /// Matches an EnforceDistinct node and verifies the distinct key expressions.
  /// @param distinctKeys List of expected distinct key expressions (DuckDB SQL
  /// syntax). Supports symbol rewriting from child matchers.
  PlanMatcherBuilder& enforceDistinct(
      const std::vector<std::string>& distinctKeys);

  /// Matches any Window node regardless of functions or partitioning.
  PlanMatcherBuilder& window();

  /// Matches a Window node with the specified SQL window expressions.
  /// Each expression should be a complete window clause, e.g.
  /// "row_number() OVER (PARTITION BY n_regionkey ORDER BY n_name) as rn".
  /// Verifies function names, partition keys, and order by keys.
  /// @param windowExprs SQL window expressions to match.
  PlanMatcherBuilder& window(const std::vector<std::string>& windowExprs);

  /// Matches any RowNumber node.
  PlanMatcherBuilder& rowNumber();

  /// Matches a RowNumber node with the specified partition keys and limit.
  /// @param partitionKeys Expected partition key column names.
  /// @param limit Expected per-partition limit.
  PlanMatcherBuilder& rowNumber(
      const std::vector<std::string>& partitionKeys,
      std::optional<int32_t> limit = std::nullopt);

  /// Matches any TopNRowNumber node.
  PlanMatcherBuilder& topNRowNumber();

  /// Matches a TopNRowNumber node with the specified partition keys, sorting
  /// keys, and limit.
  /// @param partitionKeys Expected partition key column names.
  /// @param sortingKeys Expected sorting key column names.
  /// @param limit Expected per-partition row limit.
  PlanMatcherBuilder& topNRowNumber(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys,
      int32_t limit);

  /// Matches any GroupId node regardless of grouping sets or inputs.
  PlanMatcherBuilder& groupId();

  /// Matches a GroupId node with the specified grouping sets, aggregation
  /// inputs, and group ID column name. Each grouping set is a list of output
  /// column names for the active keys in that set.
  /// @param groupingSets The expected grouping sets (each set is a list of
  /// output column names).
  /// @param aggregationInputs The expected aggregation input expressions.
  /// @param groupIdColumn The expected name of the group ID output column.
  PlanMatcherBuilder& groupId(
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<std::string>& aggregationInputs,
      const std::string& groupIdColumn);
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
