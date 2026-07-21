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

#include <functional>
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/core/PlanNode.h"
#include "velox/parse/ExpressionsParser.h"
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
/// To match any subexpression, use the ExprMatcher wildcard written as
/// `"any"()` — double-quoted, since DuckDB rejects the bare identifier `any`.
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
    const std::vector<axiom::optimizer::ExecutableFragment>* fragments;
    const axiom::optimizer::ExecutableFragment* currentFragment;
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
  bool match(const axiom::optimizer::MultiFragmentPlan& plan) const;

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

  /// Binds aliases to the matched node's output columns by position. See
  /// PlanMatcherBuilder::aliases() for usage. nullopt entries are skipped.
  void setAliases(std::vector<std::optional<std::string>> aliases) {
    aliases_ = std::move(aliases);
  }

  /// Registers a callback invoked with the matched node on a successful match.
  void setOnMatch(std::function<void(const PlanNodePtr&)> onMatch) {
    onMatch_ = std::move(onMatch);
  }

 protected:
  explicit PlanMatcher(parse::ParseOptions options = {})
      : options_{std::move(options)} {}

  /// Parses `expr` as a DuckDB SQL expression using this matcher's parse
  /// options. Use instead of constructing a DuckSqlExpressionsParser directly
  /// so the matcher's options (e.g. integer-literal kind) are honored.
  ExprPtr parseExpr(const std::string& expr) const {
    return parse::DuckSqlExpressionsParser(options_).parseExpr(expr);
  }

  // Aliases bound to the matched node's output columns by position.
  // Applied on successful match by PlanMatcherImpl::match().
  std::vector<std::optional<std::string>> aliases_;

  // Invoked with the matched node on a successful match, if set.
  std::function<void(const PlanNodePtr&)> onMatch_;

  // Parse options for SQL expressions this matcher parses.
  const parse::ParseOptions options_;
};

/// Match details for a HashJoin node beyond join type. Each field is
/// optional: leave as nullopt to skip the check, set a value to assert it.
struct HashJoinDetails {
  /// When set, asserts plan.isNullAware() matches.
  std::optional<bool> nullAware;

  /// When set, asserts the join's equi-keys. Each entry is a single
  /// equality expression written as "<left> = <right>" (DuckDB SQL
  /// syntax). The LHS of each `=` matches `plan.leftKeys()[i]`; the
  /// RHS matches `plan.rightKeys()[i]`. Supports alias rewriting from
  /// child matchers.
  std::optional<std::vector<std::string>> keys;

  /// When set, asserts the join filter expression (e.g., "a = b AND c > 0").
  /// Empty string asserts no filter.
  std::optional<std::string> filter;

  /// When set, asserts output column names (order-insensitive, duplicates
  /// rejected).
  std::optional<std::vector<std::string>> outputColumnNames;
};

class PlanMatcherBuilder {
 public:
  /// Callback invoked with the matched node on a successful match.
  using OnMatchCallback = std::function<void(const PlanNodePtr&)>;

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
  /// @param sampleRate When set, asserts the scan's sample rate equals
  /// this value (Hive's `rand() < k` encoding).
  PlanMatcherBuilder& hiveScan(
      const std::string& tableName,
      common::SubfieldFilters subfieldFilters,
      const std::string& remainingFilter = "",
      std::optional<double> sampleRate = std::nullopt);

  /// Matches any Values node regardless of type. 'onMatch', if set, is invoked
  /// with the matched Values node.
  PlanMatcherBuilder& values(OnMatchCallback onMatch = nullptr);

  /// Matches a Values node with the specified output type. Only the number of
  /// columns and their types are asserted, not the column names. The names in
  /// 'outputType' are registered as aliases (by position) so downstream
  /// matchers can reference the columns by those names.
  /// @param outputType The expected output type of the Values node.
  PlanMatcherBuilder& values(const RowTypePtr& outputType);

  /// Matches a Values node whose rows equal 'expected' (order-insensitive).
  /// Only the number of columns and their types are asserted, not the column
  /// names. The names in 'expected's row type are registered as aliases (by
  /// position) so downstream matchers can reference the columns by those names.
  PlanMatcherBuilder& values(const std::vector<RowVectorPtr>& expected);

  /// Matches any Filter node regardless of predicate.
  PlanMatcherBuilder& filter();

  /// Adds a Filter matcher (see filter()) only when 'condition' is true;
  /// otherwise a no-op.
  PlanMatcherBuilder& filterIf(bool condition) {
    return condition ? filter() : *this;
  }

  /// Matches a Filter node with the specified predicate expression.
  /// Supports symbol rewriting from child matchers.
  /// @param predicate The expected filter predicate (DuckDB SQL syntax).
  ///
  /// The predicate is parsed and compared to the plan's filter expression by
  /// structure, so it must match the optimizer's form, not just be logically
  /// equivalent. Gotchas:
  ///   - Conjunctions/disjunctions of three or more terms: the optimizer emits
  ///     a flat n-ary call, but 'a and b and c' parses as nested binary
  ///     'and(and(a, b), c)' and will not match. Use the function-call form
  ///     '"and"(a, b, c)' for a flat call.
  ///   - Numeric literal type: a literal is compared by both value and type
  ///     kind. A bare integer such as '24' is INTEGER and will not match a
  ///     constant the optimizer coerced to the column's type. Write '24.0' to
  ///     compare against a DOUBLE column.
  /// @param options Parse options for 'predicate'. Defaults parse integer
  /// literals as BIGINT; pass '{.parseIntegerAsBigint = false}' when the plan
  /// uses INTEGER literals.
  PlanMatcherBuilder& filter(
      const std::string& predicate,
      const parse::ParseOptions& options = {});

  PlanMatcherBuilder& filterIf(
      bool condition,
      const std::string& predicate,
      const parse::ParseOptions& options = {}) {
    return condition ? filter(predicate, options) : *this;
  }

  /// Matches any Project node regardless of expressions.
  PlanMatcherBuilder& project();

  /// Adds a Project matcher (see project()) only when 'condition' is true;
  /// otherwise a no-op.
  PlanMatcherBuilder& projectIf(bool condition) {
    return condition ? project() : *this;
  }

  /// Matches a Project node with the specified projection expressions.
  /// Expressions with aliases (e.g., "a + b as c") capture the alias for use
  /// in parent matchers via symbol rewriting.
  /// @param expressions The expected projection expressions (DuckDB SQL
  /// syntax).
  /// @param options Parse options for 'expressions'. Defaults parse integer
  /// literals as BIGINT; pass '{.parseIntegerAsBigint = false}' when the plan
  /// uses INTEGER literals (e.g. Presto array constructors) and the expression
  /// contains a complex constant where the scalar INTEGER/BIGINT tolerance does
  /// not apply.
  PlanMatcherBuilder& project(
      const std::vector<std::string>& expressions,
      const parse::ParseOptions& options = {});

  PlanMatcherBuilder& projectIf(
      bool condition,
      const std::vector<std::string>& expressions,
      const parse::ParseOptions& options = {}) {
    return condition ? project(expressions, options) : *this;
  }

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

  /// Matches an Aggregation node that performs DISTINCT (all input columns as
  /// grouping keys, no aggregate functions).
  PlanMatcherBuilder& distinct();

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
  /// and optional details. Supports symbol rewriting from child matchers.
  /// @param rightMatcher Matcher for the right (build) side of the join.
  /// @param joinType Type of join.
  /// @param details Join details. See HashJoinDetails.
  PlanMatcherBuilder& hashJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType,
      const HashJoinDetails& details = {});

  /// Typed shortcuts for `hashJoin` with a fixed `JoinType`.
  PlanMatcherBuilder& hashJoinInner(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kInner, details);
  }

  PlanMatcherBuilder& hashJoinLeft(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kLeft, details);
  }

  PlanMatcherBuilder& hashJoinRight(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kRight, details);
  }

  PlanMatcherBuilder& hashJoinFull(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kFull, details);
  }

  PlanMatcherBuilder& hashJoinLeftSemiFilter(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kLeftSemiFilter, details);
  }

  PlanMatcherBuilder& hashJoinLeftSemiProject(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kLeftSemiProject, details);
  }

  PlanMatcherBuilder& hashJoinRightSemiFilter(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kRightSemiFilter, details);
  }

  PlanMatcherBuilder& hashJoinRightSemiProject(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kRightSemiProject, details);
  }

  PlanMatcherBuilder& hashJoinAnti(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      const HashJoinDetails& details = {}) {
    return hashJoin(rightMatcher, JoinType::kAnti, details);
  }

  /// Matches a NestedLoopJoin node with the specified right side matcher and
  /// join type.
  /// @param rightMatcher Matcher for the right side of the join.
  /// @param joinType Type of join (defaults to kInner).
  PlanMatcherBuilder& nestedLoopJoin(
      const std::shared_ptr<PlanMatcher>& rightMatcher,
      JoinType joinType = JoinType::kInner);

  /// Matches any LocalPartition node.
  PlanMatcherBuilder& localPartition();

  /// Matches a LocalPartition node with repartition type and the specified
  /// partition keys.
  PlanMatcherBuilder& localPartition(
      const std::vector<std::string>& partitionKeys);

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

  /// Matches a LocalPartition node with gather type (N-to-1, empty partition
  /// keys).
  PlanMatcherBuilder& localGather();

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

  /// Marks a shuffle boundary (see shuffle()) only when 'condition' is true;
  /// otherwise a no-op.
  PlanMatcherBuilder& shuffleIf(bool condition) {
    return condition ? shuffle() : *this;
  }

  /// Matches a PartitionedOutput node with a single partition.
  PlanMatcherBuilder& partitionedOutputSingle();

  /// Adds a single-partition PartitionedOutput matcher (see
  /// partitionedOutputSingle()) only when 'condition' is true; otherwise a
  /// no-op.
  PlanMatcherBuilder& partitionedOutputSingleIf(bool condition) {
    return condition ? partitionedOutputSingle() : *this;
  }

  /// Marks a shuffle boundary with verification of partition keys.
  /// @param keys Expected partition key column names. Supports symbol rewriting
  /// from child matchers.
  /// @param replicateNullsAndAny Expected value of the replicateNullsAndAny
  /// flag on the PartitionedOutput node.
  PlanMatcherBuilder& shuffle(
      const std::vector<std::string>& keys,
      bool replicateNullsAndAny = false);

  /// Marks a shuffle boundary with partition keys (see shuffle(keys,
  /// replicateNullsAndAny)) only when 'condition' is true; otherwise a no-op.
  PlanMatcherBuilder& shuffleIf(
      bool condition,
      const std::vector<std::string>& keys,
      bool replicateNullsAndAny = false) {
    return condition ? shuffle(keys, replicateNullsAndAny) : *this;
  }

  /// Marks an ordered shuffle boundary (uses MergeExchange instead of
  /// Exchange). In a distributed plan:
  ///   - Producer side expects PartitionedOutput node
  ///   - Consumer side expects MergeExchange node
  /// Cannot be used with match(PlanNodePtr) - use match(MultiFragmentPlan).
  PlanMatcherBuilder& shuffleMerge();

  /// Like `shuffleMerge()`, additionally verifying the MergeExchange's sort
  /// ordering. Each entry is an ORDER BY expression with optional direction,
  /// e.g. "c" or "c DESC NULLS FIRST". Supports symbol rewriting from the
  /// producer matcher.
  PlanMatcherBuilder& shuffleMerge(const std::vector<std::string>& ordering);

  /// Matches a broadcast shuffle boundary in a distributed plan.
  /// Verifies that PartitionedOutputNode::isBroadcast() is true.
  PlanMatcherBuilder& broadcast();

  /// Matches an arbitrary (round-robin) shuffle boundary in a distributed plan.
  /// Verifies that PartitionedOutputNode::isArbitrary() is true.
  PlanMatcherBuilder& arbitrary();

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

  /// Matches the local limit pattern: partialLimit(0, offset + count) →
  /// localPartition → finalLimit(offset, count). Used when data is already on a
  /// single node and no gather is needed.
  /// @param offset Number of rows to skip.
  /// @param count Maximum number of rows to return.
  PlanMatcherBuilder& localLimit(int64_t offset, int64_t count);

  /// Matches the distributed limit pattern: localLimit(0, offset + count) →
  /// gather → finalLimit(offset, count).
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
  /// @param ordering List of sort keys (e.g., {"a ASC", "b DESC"}). Parsed by
  /// the DuckDB SQL parser: an omitted direction defaults to `ASC` and an
  /// omitted null ordering to `NULLS LAST`. So "c" means "c ASC NULLS LAST" and
  /// "c DESC" means "c DESC NULLS LAST".
  PlanMatcherBuilder& orderBy(const std::vector<std::string>& ordering);

  /// Matches any TableWrite node.
  PlanMatcherBuilder& tableWrite();

  /// Matches any TableWriteMerge node.
  PlanMatcherBuilder& tableWriteMerge();

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

  /// Binds aliases to the previous matcher's output columns by position so
  /// downstream matchers can reference them by stable names regardless of
  /// optimizer-generated internal names. For each i where aliases[i] is set,
  /// maps the alias to the i-th output column name. nullopt entries are
  /// skipped. Fails at match time if aliases.size() exceeds the matched
  /// node's output column count.
  ///
  /// Use lowercase aliases. Downstream expressions are parsed as DuckDB SQL,
  /// which lowercases unquoted identifiers, so an alias like "myCol" registered
  /// here would be looked up as "mycol".
  /// @param aliases The aliases to register, indexed by output column
  /// position. Use std::nullopt to skip a column.
  PlanMatcherBuilder& aliases(
      const std::vector<std::optional<std::string>>& aliases);

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

  /// Matches the distributed mark-distinct pattern:
  /// shuffle → localPartition(keys) → markDistinct(keys, markerAliases). The
  /// localPartition is expected only in multiThreaded() mode.
  PlanMatcherBuilder& distributedMarkDistinct(
      const std::vector<std::string>& keys,
      const std::vector<std::string>& markerAliases);

  /// Sets whether this matcher describes a multi-threaded (numDrivers > 1)
  /// plan. When enabled (the default for a new builder), the distributed*
  /// helpers (distributedAggregation, distributedSingleAggregation,
  /// distributedOrderBy, distributedMarkDistinct) expect the additional
  /// local-exchange nodes that intra-node parallelism inserts, e.g.
  /// localPartition/localGather before a final aggregation and a partial sort +
  /// LocalMerge before a merge boundary. Pass false for a single-driver plan.
  PlanMatcherBuilder& multiThreaded(bool enabled);

  /// Matches the distributed (split) aggregation pattern:
  /// partialAggregation(groupingKeys, aggregates) → shuffle →
  /// localPartition(groupingKeys) → finalAggregation.
  /// For empty groupingKeys, uses gather instead of shuffle. The local exchange
  /// (localPartition / localGather) between the shuffle and the final
  /// aggregation is expected only in multiThreaded() mode.
  PlanMatcherBuilder& distributedAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  /// Matches the distributed sort feeding a MergeExchange: an OrderBy on
  /// 'ordering' followed by the merge boundary (see shuffleMerge). In
  /// multiThreaded() mode the OrderBy is partial and a LocalMerge precedes the
  /// boundary; otherwise the OrderBy is final and there is no LocalMerge.
  PlanMatcherBuilder& distributedOrderBy(
      const std::vector<std::string>& ordering);

  /// Matches the distributed single-step aggregation pattern:
  /// shuffle → localPartition(groupingKeys) → singleAggregation(groupingKeys,
  /// aggregates). For empty groupingKeys, uses gather instead of shuffle. The
  /// local exchange (localPartition / localGather) between the shuffle and the
  /// aggregation is expected only in multiThreaded() mode.
  PlanMatcherBuilder& distributedSingleAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  /// Matches any MarkDistinct node regardless of distinct keys.
  PlanMatcherBuilder& markDistinct();

  /// Matches a MarkDistinct node with the specified distinct keys and
  /// registers symbol aliases for every marker column it produces. A
  /// MarkDistinct node emits 1 + masks().size() marker columns: a no-mask
  /// marker at index 0 and one per-mask marker at index i+1 for masks()[i].
  /// @param distinctKeys List of expected distinct keys. Supports symbol
  ///   rewriting from child matchers.
  /// @param markerAliases Aliases for every marker column, in position order.
  ///   Must have exactly one entry per marker the node produces. Pass the
  ///   no-argument `markDistinct()` overload to match without verifying
  ///   marker count.
  PlanMatcherBuilder& markDistinct(
      const std::vector<std::string>& distinctKeys,
      const std::vector<std::string>& markerAliases);

  /// Matches a GroupId node with the specified grouping sets, aggregation
  /// inputs, and group ID column alias. Each grouping set is a list of output
  /// column names for the active keys in that set.
  /// @param groupingSets The expected grouping sets (each set is a list of
  /// output column names).
  /// @param aggregationInputs The expected aggregation input expressions.
  /// @param groupIdAlias Alias for the group ID output column. The actual
  ///   column name is captured as a symbol so downstream matchers can reference
  ///   it by this alias.
  /// @param keyAliases Maps input column names to test aliases for keys that
  ///   also appear as aggregate inputs. Downstream matchers can use the alias
  ///   to reference the auto-generated output key name.
  PlanMatcherBuilder& groupId(
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<std::string>& aggregationInputs,
      const std::string& groupIdAlias,
      const std::vector<std::pair<std::string, std::string>>& keyAliases = {});

  /// Asserts the current fragment has a non-empty groupedNodes map (bucketed
  /// scheduling is active).
  PlanMatcherBuilder& bucketed();

  /// Asserts the current fragment has an empty groupedNodes map (no bucketed
  /// scheduling).
  PlanMatcherBuilder& notBucketed();

  /// Asserts the current fragment has exactly 'count' entries in groupedNodes
  /// with a non-null PartitionType (bucketed scans).
  PlanMatcherBuilder& bucketedScans(int32_t count);

  /// Asserts the current fragment has exactly 'count' entries in groupedNodes
  /// with a null PartitionType (consumer-side hash exchanges).
  PlanMatcherBuilder& hashExchanges(int32_t count);

  /// Asserts the current fragment has fragment.width == 'width'.
  PlanMatcherBuilder& fragmentWidth(int32_t width);

  /// Asserts the current fragment has fragment.type == 'type'.
  PlanMatcherBuilder& fragmentType(axiom::optimizer::FragmentType type);

  /// Builds and returns the constructed PlanMatcher.
  /// @throws VeloxUserError if matcher is empty.
  std::shared_ptr<PlanMatcher> build() {
    VELOX_USER_CHECK_NOT_NULL(matcher_, "Cannot build an empty PlanMatcher.");
    return matcher_;
  }

 private:
  std::shared_ptr<PlanMatcher> matcher_;

  // When true, distributed helpers expect the local exchanges that
  // numDrivers > 1 inserts. Defaults to true to match the default multi-driver
  // plan config; set via multiThreaded(false) for single-driver plans.
  bool localExchanges_{true};
};

} // namespace facebook::velox::core
