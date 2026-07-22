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

#include <array>
#include <span>

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/v2/PhysicalProperties.h"
#include "velox/core/PlanNode.h"

/// Tree-IR for the optimizer.
///
/// Nodes are immutable and, like all `PlanObject`s, arena-allocated from the
/// shared `QueryGraphContext`.
namespace facebook::axiom::optimizer::v2 {

/// Discriminator for Node subtypes.
enum class NodeType : uint8_t {
  kScan,
  kFilter,
  kProject,
  kLimit,
  kSort,
  kTopN,
  kAggregate,
  kGroupId,
  kMarkDistinct,
  kValues,
  kUnnest,
  kUnionAll,
  kJoin,
  kWindow,
  kTopNRowNumber,
  kApply,
  kEnforceSingleRow,
  kAssignUniqueId,
  kEnforceDistinct,
  kExchange,
  kTableWrite,
};

AXIOM_DECLARE_ENUM_NAME(NodeType);

class Node;
class NodeVisitor;
class NodeVisitorContext;
using NodeCP = const Node*;
using NodeVector = QGVector<NodeCP>;

/// Base class for tree-IR operator nodes. Immutable. All members are const
/// and initialized via the constructor. Abstract — instantiate via a subclass.
///
/// Cross-node references — predicates, join/grouping/sort keys, expression
/// operands, per-leg layouts — bind by pointer identity, and keys may be
/// arbitrary expressions, not just columns: `GROUP BY mod(a, 2)` carries a
/// `Call` grouping key, and sort and partition keys are the same. Positional
/// indices into a producer's `outputColumns()` are never used: pruning,
/// duplicate-column collapse, and any other rewrite that reshapes a producer's
/// `outputColumns` would then force rewiring at every consumer. Same-node
/// sub-vector indices (e.g., `GroupId::groupingSets` into `groupingKeyColumns`)
/// are positional and OK — they reference structure internal to the node, not a
/// producer. The IR never binds by column name; names on `Column::name_` /
/// `alias_` exist only as display labels for EXPLAIN.
///
/// A node's `PhysicalProperties`, by contrast, always reference this node's own
/// output columns — an invariant each derivation upholds and the base
/// constructor verifies (`checkReferencesColumns`). Velox's column-key
/// requirement for aggregation / ordering / windowing / exchange is a separate,
/// emit-time concern that `PrecomputeProjections` materializes.
///
/// Each node kind enforces its own column-identity contract: outputs contain no
/// duplicate columns, and each kind extends or transforms its input schema in a
/// specific way. See each subclass's docblock for the exact rules.
///
/// Instantiate nodes only through a `Builder` (`builder.make<T>(key)`), never
/// by constructing a subclass directly: `make` hash-conses on the subclass's
/// nested `Key` struct (its identity — two nodes of a kind intern to the same
/// instance iff their `Key`s compare equal: input pointer(s) plus the node's
/// parameter expression / column pointers). Direct construction bypasses
/// interning. See `Builder.h`.
class Node : public PlanObject {
 public:
  NodeType nodeType() const {
    return nodeType_;
  }

  bool is(NodeType nodeType) const {
    return nodeType_ == nodeType;
  }

  std::string_view nodeTypeName() const {
    return NodeTypeName::toName(nodeType_);
  }

  /// Columns produced by this node, in output order.
  const ColumnVector& outputColumns() const {
    return outputColumns_;
  }

  /// Physical properties of this node's output: per-driver local ordering /
  /// grouping, distribution, and relation-level facts. Derived from the inputs
  /// at construction and immutable. Fields stay unset until decided — e.g.
  /// distribution is unspecified until an exchange is placed, which mints a new
  /// node rather than mutating this one.
  const PhysicalProperties& physicalProperties() const {
    return physicalProperties_;
  }

  /// Inputs in dependency order; empty for leaves. Returns a view into
  /// node-owned storage; callers must not retain it past the node's
  /// lifetime.
  virtual std::span<const NodeCP> inputs() const = 0;

  /// Double-dispatch hook for `NodeVisitor`.
  virtual void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const = 0;

  std::string toString() const override;

 protected:
  Node(
      NodeType nodeType,
      ColumnVector outputColumns,
      PhysicalProperties properties)
      : PlanObject(PlanType::kV2Node),
        nodeType_(nodeType),
        outputColumns_(std::move(outputColumns)),
        physicalProperties_(std::move(properties)) {
    physicalProperties_.checkReferencesColumns(
        PlanObjectSet::fromObjects(outputColumns_));
  }

 private:
  const NodeType nodeType_;
  const ColumnVector outputColumns_;
  const PhysicalProperties physicalProperties_;
};

/// Reads rows from a connector-backed table (`BaseTable`).
class Scan : public Node {
 public:
  struct Key {
    BaseTableCP baseTable;
    /// Strict consumer-required output columns; may be empty (e.g.
    /// `SELECT count(1) FROM t` needs no column values from the table).
    /// Distinct by pointer; both `outputName()` and the underlying `name()`
    /// are unique within the vector. `filters` may reference columns that are
    /// NOT in this set; the connector-side read schema is computed as
    /// `outputColumns ∪ refs(filters)` when building the final Velox plan.
    ColumnVector outputColumns;
    /// Filters on the scanned rows, as conjuncts joined with AND; may be empty.
    /// At Emit the connector evaluates those it can; the rest are re-applied as
    /// a `Filter` node above the `TableScan` in the final Velox plan.
    ExprVector filters;
  };

  /// Transparent hasher for interning `Scan`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Scan* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Scan`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Scan* left, const Scan* right) const;
    bool operator()(const Key& key, const Scan* node) const;
    bool operator()(const Scan* node, const Key& key) const;
  };

  explicit Scan(Key key);

  BaseTableCP baseTable() const {
    return baseTable_;
  }

  const ExprVector& filters() const {
    return filters_;
  }

  std::span<const NodeCP> inputs() const override {
    return {};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const BaseTableCP baseTable_;
  const ExprVector filters_;
};

using ScanCP = const Scan*;

/// Selects rows from its input that match all of its predicates (joined with
/// AND). Does not change the schema: `outputColumns()` are the input's columns
/// unchanged (the same `Column` pointers).
class Filter : public Node {
 public:
  struct Key {
    /// Input node whose rows are filtered.
    NodeCP input;
    /// Predicates joined with AND; non-empty.
    ExprVector predicates;
  };

  /// Transparent hasher for interning `Filter`s by identity (input +
  /// predicates), read from either a stored `Filter*` or a lookup `Key`.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Filter* filter) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Filter`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Filter* left, const Filter* right) const;
    bool operator()(const Key& key, const Filter* filter) const;
    bool operator()(const Filter* filter, const Key& key) const;
  };

  explicit Filter(Key key);

  NodeCP input() const {
    return input_;
  }

  /// Predicates joined with AND. Non-empty.
  const ExprVector& predicates() const {
    return predicates_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector predicates_;
};

using FilterCP = const Filter*;

/// Computes output columns from input expressions. Output schema is
/// `outputColumns`; the i-th output column is produced by `exprs[i]`.
///
/// Example: `SELECT a, b + 1 AS c FROM t` translates to:
///
///     Project(input=t, exprs=[t.a, t.b + 1], outputColumns=[t.a, c])
///
/// where `t.a` is a pass-through (same `Column*` as the input), and
/// `c` is a fresh `Column*` for the computed expression.
class Project : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Output expressions, positional with `outputColumns`.
    ExprVector exprs;
    /// Output columns, positional with `exprs` (see the class docblock for the
    /// pass-through vs fresh-column rule).
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `Project`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Project* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Project`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Project* left, const Project* right) const;
    bool operator()(const Key& key, const Project* node) const;
    bool operator()(const Project* node, const Key& key) const;
  };

  explicit Project(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& exprs() const {
    return exprs_;
  }

  /// True if no output expression contains a non-deterministic function, so an
  /// output can be substituted (inlined) into a consumer without changing
  /// semantics.
  bool isDeterministic() const;

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector exprs_;
};

using ProjectCP = const Project*;

/// Returns up to `count` rows starting from `offset`. Does not change the
/// schema: `outputColumns()` are the input's columns unchanged (the same
/// `Column` pointers).
class Limit : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Number of leading rows to skip; >= 0.
    int64_t offset;
    /// Maximum number of rows to return; >= 0.
    /// `std::numeric_limits<int64_t>::max()` means no limit (e.g. `OFFSET n`
    /// with no `LIMIT`), 0 means empty result.
    int64_t count;
  };

  /// Transparent hasher for interning `Limit`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Limit* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Limit`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Limit* left, const Limit* right) const;
    bool operator()(const Key& key, const Limit* node) const;
    bool operator()(const Limit* node, const Key& key) const;
  };

  explicit Limit(Key key);

  NodeCP input() const {
    return input_;
  }

  int64_t offset() const {
    return offset_;
  }

  int64_t count() const {
    return count_;
  }

  /// Returns offset + count, saturated at INT64_MAX so a no-limit count
  /// (INT64_MAX) does not overflow. The '>=' matches the no-limit boundary used
  /// by RelationOp's isNoLimit.
  int64_t offsetPlusCount() const {
    return count_ >= INT64_MAX - offset_ ? INT64_MAX : offset_ + count_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const int64_t offset_;
  const int64_t count_;
};

using LimitCP = const Limit*;

/// Sorts rows by `orderKeys` with directions in `orderTypes`. Does not change
/// the schema: `outputColumns()` are the input's columns unchanged (the same
/// `Column` pointers).
class Sort : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Sort keys, outermost first; positional with `orderTypes`, non-empty.
    ExprVector orderKeys;
    /// Sort direction per key; positional with `orderKeys`.
    OrderTypeVector orderTypes;
  };

  /// Transparent hasher for interning `Sort`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Sort* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Sort`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Sort* left, const Sort* right) const;
    bool operator()(const Key& key, const Sort* node) const;
    bool operator()(const Sort* node, const Key& key) const;
  };

  explicit Sort(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector orderKeys_;
  const OrderTypeVector orderTypes_;
};

using SortCP = const Sort*;

/// Sorts rows by `orderKeys` / `orderTypes` and returns up to `count` rows
/// starting from `offset` — a bounded Sort. A Limit directly over a Sort fuses
/// into this. Does not change the schema:
/// `outputColumns()` are the input's columns unchanged (the same `Column`
/// pointers).
class TopN : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Sort keys, outermost first; positional with `orderTypes`, non-empty.
    ExprVector orderKeys;
    /// Sort direction per key; positional with `orderKeys`.
    OrderTypeVector orderTypes;
    /// Number of leading rows to skip; >= 0.
    int64_t offset;
    /// Maximum number of rows to return; >= 0.
    int64_t count;
  };

  /// Transparent hasher for interning `TopN`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const TopN* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `TopN`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const TopN* left, const TopN* right) const;
    bool operator()(const Key& key, const TopN* node) const;
    bool operator()(const TopN* node, const Key& key) const;
  };

  explicit TopN(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  int64_t offset() const {
    return offset_;
  }

  int64_t count() const {
    return count_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector orderKeys_;
  const OrderTypeVector orderTypes_;
  const int64_t offset_;
  const int64_t count_;
};

using TopNCP = const TopN*;

/// Vector of `optimizer::Aggregate*` (an aggregate-function Call). The
/// type is distinct from `ExprVector` so consumers don't need to
/// downcast each element to access aggregate-specific accessors
/// (`condition`, `orderKeys`, etc.); the `Aggregate` node carries
/// this type directly.
using AggregateCallVector = QGVector<const optimizer::Aggregate*>;

/// Stage of a (possibly distributed) aggregation, reusing Velox's enum.
/// `kSingle` computes the complete result in one pass. `kPartial`
/// pre-aggregates its input and emits intermediate accumulators (one per
/// aggregate, typed by `optimizer::Aggregate::intermediateType()`); `kFinal`
/// consumes those accumulators and emits the final result. A two-stage
/// distributed aggregate is `kFinal` over a remote `Exchange` over `kPartial`.
using AggregateStep = velox::core::AggregationNode::Step;

/// Groups input rows by `groupingKeys` and computes `aggregates` per
/// group. Output schema is `groupingKeys` followed by `aggregates`
/// (one output column each).
///
/// Example: `SELECT a, sum(b) FROM t GROUP BY a` translates to:
///
///     Aggregate(input=t, groupingKeys=[t.a], aggregates=[sum(t.b)],
///               outputColumns=[t.a, sum_b])
///
/// One output row per distinct value of `t.a`, with `sum_b` the sum
/// of `t.b` within that group.
///
/// GROUPING SETS / ROLLUP / CUBE have no dedicated representation here: they
/// are lowered to a `GroupId` input plus a plain aggregate keyed on an explicit
/// group-id column, so this node has no grouping-set concept of its own. That
/// group-id column, when present, is also recorded in `groupId` (with
/// `globalGroupingSets`).
class Aggregate : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Grouping keys; empty for a global aggregate.
    ExprVector groupingKeys;
    /// Aggregate functions, one per output aggregate column.
    AggregateCallVector aggregates;
    /// Output columns: the `groupingKeys`, then the `aggregates`, positionally.
    ColumnVector outputColumns;
    /// Aggregation stage. For a `kPartial` step, `outputColumns`' aggregate
    /// positions hold intermediate accumulators (typed by
    /// `intermediateType()`), not final results.
    AggregateStep step{AggregateStep::kSingle};

    /// Group-id column from the upstream `GroupId`, or null for a plain
    /// aggregate. Set iff `globalGroupingSets` is non-empty; when set it also
    /// appears in `groupingKeys`.
    ColumnCP groupId{nullptr};
    /// Group-id values of the global (empty) grouping sets. Non-empty only for
    /// a GROUPING SETS / ROLLUP / CUBE aggregate with a global `()` set; drives
    /// Velox's default-row-per-global-set behavior over empty input.
    QGVector<int32_t> globalGroupingSets;
  };

  /// Transparent hasher for interning `Aggregate`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Aggregate* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Aggregate`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Aggregate* left, const Aggregate* right) const;
    bool operator()(const Key& key, const Aggregate* node) const;
    bool operator()(const Aggregate* node, const Key& key) const;
  };

  explicit Aggregate(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& groupingKeys() const {
    return groupingKeys_;
  }

  const AggregateCallVector& aggregates() const {
    return aggregates_;
  }

  AggregateStep step() const {
    return step_;
  }

  ColumnCP groupId() const {
    return groupId_;
  }

  const QGVector<int32_t>& globalGroupingSets() const {
    return globalGroupingSets_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector groupingKeys_;
  const AggregateCallVector aggregates_;
  const AggregateStep step_;
  const ColumnCP groupId_;
  const QGVector<int32_t> globalGroupingSets_;
};

using AggregateCP = const Aggregate*;

/// Replicates each input row once per grouping set, emitting NULL for keys
/// inactive in that set and tagging the copy with the set's index. The
/// downstream `Aggregate` then groups by
/// `groupingKeyColumns ++ [groupId]` and runs the aggregates over the
/// replicated rows; the SQL `GROUPING SETS` / `ROLLUP` / `CUBE` semantics
/// fall out of that grouping.
///
/// `outputColumns` = `groupingKeyColumns ++ aggregationInputs' columns ++
/// [groupId]`. Velox `GroupingKeyInfo` maps each `groupingKeys` expression to
/// the corresponding `groupingKeyColumns` output name; both vectors are
/// positionally aligned.
class GroupId : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Input-side expressions whose values populate each replicated grouping
    /// key; positionally aligned with `groupingKeyColumns`.
    ExprVector groupingKeys;
    /// Input columns referenced by the downstream `Aggregate`'s aggregate
    /// functions; passed through to the output unchanged.
    ExprVector aggregationInputs;
    /// Non-empty; each entry lists the `groupingKeyColumns` indices active for
    /// that set. Inactive keys are emitted as NULL in that set's output rows.
    QGVector<QGVector<int32_t>> groupingSets;
    /// Fresh output columns for the (possibly NULL-padded) grouping keys.
    ColumnVector groupingKeyColumns;
    /// Output column holding the grouping-set index (BIGINT, zero-based).
    ColumnCP groupId;
    /// Output columns: `groupingKeyColumns`, then `aggregationInputs`' columns,
    /// then `groupId`.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `GroupId`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const GroupId* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `GroupId`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const GroupId* left, const GroupId* right) const;
    bool operator()(const Key& key, const GroupId* node) const;
    bool operator()(const GroupId* node, const Key& key) const;
  };

  explicit GroupId(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& groupingKeys() const {
    return groupingKeys_;
  }

  const ExprVector& aggregationInputs() const {
    return aggregationInputs_;
  }

  const QGVector<QGVector<int32_t>>& groupingSets() const {
    return groupingSets_;
  }

  const ColumnVector& groupingKeyColumns() const {
    return groupingKeyColumns_;
  }

  ColumnCP groupId() const {
    return groupId_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector groupingKeys_;
  const ExprVector aggregationInputs_;
  const QGVector<QGVector<int32_t>> groupingSets_;
  const ColumnVector groupingKeyColumns_;
  const ColumnCP groupId_;
};

using GroupIdCP = const GroupId*;

/// Marks the first occurrence of each `(distinctKeys)` tuple from `input`
/// with `true` and subsequent occurrences with `false`. Used to lower
/// `DISTINCT` aggregates to non-distinct ones with a FILTER mask: a
/// downstream `Aggregate` reads `agg(x) FILTER (marker)` and sees each
/// `(group, x)` tuple at most once.
///
/// One `MarkDistinct` serves all distinct aggregates over the same key set.
/// `markers[0]` is the no-mask marker: true on the first occurrence of each
/// tuple overall, read by the unfiltered distinct aggregates. Each `masks[i]`
/// is the `FILTER` condition of a filtered distinct aggregate, paired with
/// `markers[i+1]`, which is true on the first occurrence of each tuple among
/// the rows where `masks[i]` holds. `masks` is empty when no distinct aggregate
/// over this key set carries a `FILTER`.
///
/// `outputColumns` = `input.outputColumns ++ markers` (input columns pass
/// through unchanged).
class MarkDistinct : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Output marker columns (BOOLEAN); `markers[0]` is the no-mask marker.
    /// See the class doc.
    ColumnVector markers;
    /// Columns / expressions tested for distinctness, in positional order.
    ExprVector distinctKeys;
    /// FILTER condition per filtered distinct aggregate, positionally aligned
    /// with `markers[1..]`. Empty when no distinct aggregate over the key set
    /// carries a FILTER.
    ColumnVector masks;
    /// Output columns: `input`'s columns, then `markers`.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `MarkDistinct`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const MarkDistinct* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `MarkDistinct`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const MarkDistinct* left, const MarkDistinct* right) const;
    bool operator()(const Key& key, const MarkDistinct* node) const;
    bool operator()(const MarkDistinct* node, const Key& key) const;
  };

  explicit MarkDistinct(Key key);

  NodeCP input() const {
    return input_;
  }

  const ColumnVector& markers() const {
    return markers_;
  }

  const ExprVector& distinctKeys() const {
    return distinctKeys_;
  }

  const ColumnVector& masks() const {
    return masks_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ColumnVector markers_;
  const ExprVector distinctKeys_;
  const ColumnVector masks_;
};

using MarkDistinctCP = const MarkDistinct*;

/// Embeds a constant table in the plan. `outputColumns` defines the schema
/// in all three cases. At most one of `source` / `rows` is non-null:
///   - `source != nullptr, rows == nullptr`: pass through a
///     `logical_plan::ValuesNode` whose `Variants` or `Vectors` data is
///     read when building the final Velox plan.
///   - `source == nullptr, rows != nullptr`: an arena-allocated
///     `Variant::array(...)` of row `Variant`s (each element is
///     `Variant::row(...)`). Produced by translate-time folding of
///     `lp::ValuesNode::Exprs`.
///   - `source == nullptr, rows == nullptr`: empty table — zero rows with
///     schema from `outputColumns`. Used for the `LIMIT 0` rewrite.
class Values : public Node {
 public:
  struct Key {
    /// Set to pass through a logical-plan `ValuesNode` (see class doc).
    const logical_plan::ValuesNode* source;
    /// Set to embed arena-folded row `Variant`s (see class doc).
    const velox::Variant* rows;
    /// Output schema (defines the columns in all cases).
    ColumnVector outputColumns;
    /// For each output column, its index into the underlying data (`source`
    /// schema / `rows` fields). Identity `0..n-1` when the Values emits every
    /// data column; pushdown pruning removes entries, leaving holes so a
    /// dropped data column is never materialized. Must have one entry per
    /// output column — Builder::makeValues fills identity for the emit-all
    /// case.
    QGVector<velox::column_index_t> channels;
  };

  /// Transparent hasher for interning `Values`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Values* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Values`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Values* left, const Values* right) const;
    bool operator()(const Key& key, const Values* node) const;
    bool operator()(const Values* node, const Key& key) const;
  };

  explicit Values(Key key);

  const logical_plan::ValuesNode* source() const {
    return source_;
  }

  const velox::Variant* rows() const {
    return rows_;
  }

  /// For each output column, its index into the underlying data. See Key.
  const QGVector<velox::column_index_t>& channels() const {
    return channels_;
  }

  std::span<const NodeCP> inputs() const override {
    return {};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const logical_plan::ValuesNode* const source_;
  const velox::Variant* const rows_;
  const QGVector<velox::column_index_t> channels_;
};

using ValuesCP = const Values*;

/// Expands array / map expressions row-wise. The output has three disjoint
/// kinds of columns, exposed as separate accessors so consumers never need
/// positional decoding: `replicatedColumns` (pass-through input columns),
/// `unnestColumns[i]` (per-expression result columns), and the optional
/// `ordinalityColumn`. `outputColumns()` returns the concatenation in that
/// order, for downstream consumers that need a single vector view.
class Unnest : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Array / map expressions expanded row-wise, in positional order.
    ExprVector unnestExpressions;
    /// Subset of `input->outputColumns()` to pass through; may be empty.
    ColumnVector replicatedColumns;
    /// One `ColumnVector` per `unnestExpressions[i]`: the result columns
    /// produced by that expression (1 for ARRAY, 2 for MAP — key, value).
    QGVector<ColumnVector> unnestColumns;
    /// Optional ordinality column; null when this node does not produce
    /// ordinality.
    ColumnCP ordinalityColumn;
    /// Visible schema. Every `Column*` here is a `replicatedColumns` column, a
    /// column in some `unnestColumns[i]`, or `ordinalityColumn`. All
    /// `replicatedColumns` and `ordinalityColumn` must appear; only
    /// per-expression `unnestColumns` may be pruned out.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `Unnest`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Unnest* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Unnest`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Unnest* left, const Unnest* right) const;
    bool operator()(const Key& key, const Unnest* node) const;
    bool operator()(const Unnest* node, const Key& key) const;
  };

  explicit Unnest(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& unnestExpressions() const {
    return unnestExpressions_;
  }

  const ColumnVector& replicatedColumns() const {
    return replicatedColumns_;
  }

  const QGVector<ColumnVector>& unnestColumns() const {
    return unnestColumns_;
  }

  ColumnCP ordinalityColumn() const {
    return ordinalityColumn_;
  }

  bool withOrdinality() const {
    return ordinalityColumn_ != nullptr;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector unnestExpressions_;
  const ColumnVector replicatedColumns_;
  const QGVector<ColumnVector> unnestColumns_;
  const ColumnCP ordinalityColumn_;
};

using UnnestCP = const Unnest*;

/// Concatenates rows from all inputs (UNION ALL semantics).
/// `outputColumns` is a fresh duplicate-free output schema; `legColumns[i]`
/// maps each output position to a `Column*` from `inputs[i]`.
///
/// Example: `SELECT a, b FROM t UNION ALL SELECT x, y FROM u` translates to:
///
///     UnionAll(inputs=[t, u],
///              legColumns=[[t.a, t.b],  // leg 0: which t columns feed outputs
///                          [u.x, u.y]], // leg 1: which u columns feed outputs
///              outputColumns=[union_col_0, union_col_1])
///
/// `legColumns[i][j]` is the `Column*` from `inputs[i]->outputColumns()`
/// that supplies values for union output position `j`. The same
/// `Column*` may repeat within a leg to fan one input column into
/// multiple union positions (e.g., `SELECT a, a FROM t` after the
/// translate-time deduplication collapses both selected `a`s to one input
/// column).
class UnionAll : public Node {
 public:
  struct Key {
    /// Inputs to concatenate; >= 2.
    NodeVector inputs;
    /// Per input, the `Column*`s (from that input's `outputColumns()`) that
    /// feed each output position: one entry per input, each sized like
    /// `outputColumns`. See the class doc.
    QGVector<ColumnVector> legColumns;
    /// Fresh, duplicate-free output schema.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `UnionAll`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const UnionAll* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `UnionAll`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const UnionAll* left, const UnionAll* right) const;
    bool operator()(const Key& key, const UnionAll* node) const;
    bool operator()(const UnionAll* node, const Key& key) const;
  };

  explicit UnionAll(Key key);

  std::span<const NodeCP> inputs() const override {
    return inputs_;
  }

  const QGVector<ColumnVector>& legColumns() const {
    return legColumns_;
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeVector inputs_;
  const QGVector<ColumnVector> legColumns_;
};

using UnionAllCP = const UnionAll*;

/// Joins two inputs by `joinType` with paired equi-keys plus an optional
/// residual `filter`. `outputColumns` projects columns from the inputs; see the
/// field doc.
///
/// Example: `SELECT * FROM t INNER JOIN u ON t.a = u.a AND t.b > u.b`
/// translates to:
///
///     Join(left=t, right=u, joinType=kInner,
///          leftKeys=[t.a], rightKeys=[u.a],
///          filter=(t.b > u.b),
///          outputColumns=[t.a, t.b, u.a, u.b])
///
/// `leftKeys[i]` joins against `rightKeys[i]` via equality; the two
/// vectors must have the same size. The match condition is
/// `AND(leftKeys[i] = rightKeys[i] for all i) AND AND(filter)`.
/// `filter` is a flat vector of predicates joined with AND (the producer is
/// responsible for flattening and for lifting any equi-pairs into the key
/// vectors).
///
/// Lowering depends on key presence:
///   - `leftKeys` non-empty → `HashJoinNode`, with `filter` as the
///     optional residual non-equi predicate.
///   - `leftKeys` empty, `filter` empty → `NestedLoopJoinNode` cross
///     product.
///   - `leftKeys` empty, `filter` non-empty → `NestedLoopJoinNode`
///     filter-only join.
///
/// `nullAware` (semi / anti only) enables NULL-aware semantics for
/// `IN` / `NOT IN`. `nullAsValue` (set ops INTERSECT / EXCEPT) treats
/// `NULL == NULL` as true on equi-keys. Mutually exclusive.
class Join : public Node {
 public:
  struct Key {
    /// Left input.
    NodeCP left;
    /// Right input.
    NodeCP right;
    /// Join type (inner / left / full / semi / anti / ...).
    velox::core::JoinType joinType;
    /// Left equi-keys; positional with `rightKeys` and the same size.
    ExprVector leftKeys;
    /// Right equi-keys; positional with `leftKeys`.
    ExprVector rightKeys;
    /// Residual non-equi predicates joined with AND; may be empty.
    ExprVector filter;
    /// NULL-aware semantics for IN / NOT IN (semi / anti only).
    bool nullAware{false};
    /// Treats NULL == NULL as true on equi-keys (INTERSECT / EXCEPT).
    bool nullAsValue{false};
    /// Output columns, each drawn from the left or right input; semi / anti
    /// joins draw only from the preserved side. Pruned to what consumers need.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `Join`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Join* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Join`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Join* left, const Join* right) const;
    bool operator()(const Key& key, const Join* node) const;
    bool operator()(const Join* node, const Key& key) const;
  };

  explicit Join(Key key);

  NodeCP left() const {
    return inputs_[0];
  }

  NodeCP right() const {
    return inputs_[1];
  }

  velox::core::JoinType joinType() const {
    return joinType_;
  }

  std::string_view joinTypeName() const {
    return velox::core::JoinTypeName::toName(joinType_);
  }

  bool isInner() const {
    return joinType_ == velox::core::JoinType::kInner;
  }

  bool isLeft() const {
    return joinType_ == velox::core::JoinType::kLeft;
  }

  bool isFull() const {
    return joinType_ == velox::core::JoinType::kFull;
  }

  const ExprVector& leftKeys() const {
    return leftKeys_;
  }

  const ExprVector& rightKeys() const {
    return rightKeys_;
  }

  const ExprVector& filter() const {
    return filter_;
  }

  bool nullAware() const {
    return nullAware_;
  }

  bool nullAsValue() const {
    return nullAsValue_;
  }

  std::span<const NodeCP> inputs() const override {
    return inputs_;
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const std::array<NodeCP, 2> inputs_;
  const velox::core::JoinType joinType_;
  const ExprVector leftKeys_;
  const ExprVector rightKeys_;
  const ExprVector filter_;
  const bool nullAware_;
  const bool nullAsValue_;
};

using JoinCP = const Join*;

/// One window function invocation. Frame is per-function (as in Velox
/// `WindowNode::Function`), so functions sharing a partition / order but
/// differing only in frame still group under a single `Window` node.
struct WindowFunction {
  /// The window function and its arguments.
  ExprCP call;

  /// The row/range window over which this function is evaluated.
  Frame frame;

  /// When true, NULL inputs are skipped when evaluating the function
  /// (e.g. the `IGNORE NULLS` option in SQL frontends).
  bool ignoreNulls;

  bool operator==(const WindowFunction&) const = default;
};

using WindowFunctions = QGVector<WindowFunction>;

/// Evaluates one or more window function calls. Output schema is input
/// columns followed by one output column per window function.
class Window : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Window function invocations sharing this partition / order; non-empty.
    /// Each carries its own frame.
    WindowFunctions functions;
    /// Partition-by keys; empty for a single global partition.
    ExprVector partitionKeys;
    /// Order-by keys; positional with `orderTypes`.
    ExprVector orderKeys;
    /// Sort direction per order key; positional with `orderKeys`.
    OrderTypeVector orderTypes;
    /// Output columns: `input`'s columns, then one per window function.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `Window`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Window* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Window`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Window* left, const Window* right) const;
    bool operator()(const Key& key, const Window* node) const;
    bool operator()(const Window* node, const Key& key) const;
  };

  explicit Window(Key key);

  NodeCP input() const {
    return input_;
  }

  const WindowFunctions& functions() const {
    return functions_;
  }

  const ExprVector& partitionKeys() const {
    return partitionKeys_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const WindowFunctions functions_;
  const ExprVector partitionKeys_;
  const ExprVector orderKeys_;
  const OrderTypeVector orderTypes_;
};

using WindowCP = const Window*;

/// Per-partition top-`limit` rows ranked by a ranking function (row_number /
/// rank / dense_rank). Produced by fusing
/// a `Filter(rankColumn <op> n)` over a single-ranking-function `Window`. The
/// rank column is emitted only when `rankColumn` is set; otherwise the output
/// is just the input columns.
class TopNRowNumber : public Node {
 public:
  using RankFunction = velox::core::TopNRowNumberNode::RankFunction;

  struct Key {
    /// Input node.
    NodeCP input;
    /// Ranking function (row_number / rank / dense_rank).
    RankFunction rankFunction;
    /// Partition-by keys; empty for a single global partition.
    ExprVector partitionKeys;
    /// Order-by keys; positional with `orderTypes`, non-empty.
    ExprVector orderKeys;
    /// Sort direction per order key; positional with `orderKeys`.
    OrderTypeVector orderTypes;
    /// Per-partition row cap; > 0.
    int32_t limit;
    /// Column carrying the ranking-function result in the output, or null to
    /// omit it (the output is then just the input columns).
    ColumnCP rankColumn;
    /// Output columns: `input`'s columns, plus `rankColumn` when set.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `TopNRowNumber`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const TopNRowNumber* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `TopNRowNumber`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const TopNRowNumber* left, const TopNRowNumber* right)
        const;
    bool operator()(const Key& key, const TopNRowNumber* node) const;
    bool operator()(const TopNRowNumber* node, const Key& key) const;
  };

  explicit TopNRowNumber(Key key);

  NodeCP input() const {
    return input_;
  }

  RankFunction rankFunction() const {
    return rankFunction_;
  }

  const ExprVector& partitionKeys() const {
    return partitionKeys_;
  }

  const ExprVector& orderKeys() const {
    return orderKeys_;
  }

  const OrderTypeVector& orderTypes() const {
    return orderTypes_;
  }

  int32_t limit() const {
    return limit_;
  }

  ColumnCP rankColumn() const {
    return rankColumn_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const RankFunction rankFunction_;
  const ExprVector partitionKeys_;
  const ExprVector orderKeys_;
  const OrderTypeVector orderTypes_;
  const int32_t limit_;
  const ColumnCP rankColumn_;
};

using TopNRowNumberCP = const TopNRowNumber*;

/// Apply is a "true dependent join" — a relational join whose right side
/// (`body`, conceptually evaluated per `input` row) may reference `input`'s
/// columns.
///
/// "Body" rather than "subquery": at translate time Apply's right side IS
/// the translated subquery, but as decorrelate iterates it peels operators
/// off and `body` evolves. The decorrelate pass and downstream consumers
/// reason about Apply as a generic dependent-join node whose right side is
/// `body`; only Translate uses the SQL term "subquery" in its prose.
///
/// Example: `SELECT (SELECT max(u.b) FROM u WHERE u.a = t.a) FROM t`
/// translates to:
///
///     Apply(kLeft, input=t,
///           body=Aggregate(Filter(u.a=t.a, u), max(u.b)),
///           correlationColumns=[t.a],
///           filter=null,
///           enforceSingleRow=false)
///
/// Apply is transient: produced by `translate` at every `lp::SubqueryExpr`
/// site, eliminated by `pass.decorrelate` before any later pass.
///
/// Per-kind semantics:
///
///   kLeft             Correlated scalar / LEFT JOIN LATERAL. Left-outer-join
///                     semantics. Body may produce N rows per outer. Output:
///                     `input.outputColumns ++ unique(body.outputColumns)
///                     ++ [includeMarker]` — a body column already in input
///                     occupies a single output slot. (NULL-padded for
///                     outers with no match; `includeMarker` is `true`
///                     on real rows, NULL on pad rows.)
///   kInner            CROSS / INNER JOIN LATERAL. Inner-join semantics: no
///                     NULL-padded rows and hence no `includeMarker`; outers
///                     with no matching body row are dropped. Body may produce
///                     N rows per outer. Output:
///                     `input.outputColumns ++ unique(body.outputColumns)`.
///   kLeftSemiProject  EXISTS / IN. Output:
///                     `input.outputColumns ++ markColumn` — a fresh BOOLEAN
///                     true iff any body row exists for the outer
///                     matching `filter` AND (for IN) `inLhs = inBodyKey`.
///                     NULL-aware for IN.
///
/// `kLeftSemiFilter` and `kAnti` never appear at Apply — they emerge only
/// from a post-decorrelate peephole that recognizes
/// `Filter(mark, Join(kLeftSemiProject, ..., mark))`.
class Apply : public Node {
 public:
  struct Key {
    /// Left ("outer") input.
    NodeCP input;
    /// Right side, evaluated per `input` row; may reference `input`'s columns.
    NodeCP body;
    /// `input` columns the current `body` still references; recomputed by
    /// decorrelate after each peel. Empty at terminus, where Apply becomes a
    /// Join.
    ColumnVector correlationColumns;
    /// `kLeft` (correlated scalar / lateral) or `kLeftSemiProject` (EXISTS /
    /// IN).
    velox::core::JoinType kind;
    /// Residual predicates joined with AND; become `Join.filter` at terminus.
    ExprVector filter;
    /// Assert the body returns 0-or-1 row per outer (>1 rows errors);
    /// correlated scalar subqueries only.
    bool enforceSingleRow;
    /// `kLeftSemiProject` mark output (BOOLEAN); null for `kLeft`.
    ColumnCP markColumn;
    /// IN equi-pair left side (the `lhs` of `lhs IN (SELECT ...)`), kept as a
    /// pair rather than `eq(inLhs, inBodyKey)` so consumers read each side
    /// directly; drives `nullAware()`. Null unless an IN subquery.
    ExprCP inLhs;
    /// IN equi-pair body side; see `inLhs`. Null unless an IN subquery.
    ExprCP inBodyKey;
    /// `kLeft` pad-row marker (BOOLEAN); null for `kLeftSemiProject`.
    ColumnCP includeMarker;
    /// Output columns; per-kind layout in the class doc.
    ColumnVector outputColumns;
  };

  /// Transparent hasher for interning `Apply`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Apply* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Apply`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Apply* left, const Apply* right) const;
    bool operator()(const Key& key, const Apply* node) const;
    bool operator()(const Apply* node, const Key& key) const;
  };

  explicit Apply(Key key);

  NodeCP input() const {
    return inputs_[0];
  }

  NodeCP body() const {
    return inputs_[1];
  }

  const ColumnVector& correlationColumns() const {
    return correlationColumns_;
  }

  velox::core::JoinType kind() const {
    return kind_;
  }

  std::string_view kindName() const {
    return velox::core::JoinTypeName::toName(kind_);
  }

  bool isLeft() const {
    return kind_ == velox::core::JoinType::kLeft;
  }

  bool isInner() const {
    return kind_ == velox::core::JoinType::kInner;
  }

  bool isLeftSemiProject() const {
    return kind_ == velox::core::JoinType::kLeftSemiProject;
  }

  const ExprVector& filter() const {
    return filter_;
  }

  bool enforceSingleRow() const {
    return enforceSingleRow_;
  }

  ColumnCP markColumn() const {
    return markColumn_;
  }

  ExprCP inLhs() const {
    return inLhs_;
  }

  ExprCP inBodyKey() const {
    return inBodyKey_;
  }

  ColumnCP includeMarker() const {
    return includeMarker_;
  }

  bool nullAware() const {
    return inLhs_ != nullptr;
  }

  std::span<const NodeCP> inputs() const override {
    return inputs_;
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const std::array<NodeCP, 2> inputs_;
  const ColumnVector correlationColumns_;
  const velox::core::JoinType kind_;
  const ExprVector filter_;
  const bool enforceSingleRow_;
  const ColumnCP markColumn_;
  const ExprCP inLhs_;
  const ExprCP inBodyKey_;
  const ColumnCP includeMarker_;
};

using ApplyCP = const Apply*;

/// Asserts the input contains at most one row and passes it through unchanged;
/// an empty input yields a single all-NULL row, and more than one row raises an
/// error. Does not change the schema: `outputColumns()` are the input's columns
/// unchanged (the same `Column` pointers). Used to enforce the 0-or-1-row
/// cardinality of an uncorrelated scalar subquery.
class EnforceSingleRow : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
  };

  /// Transparent hasher for interning `EnforceSingleRow`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const EnforceSingleRow* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `EnforceSingleRow`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const EnforceSingleRow* left, const EnforceSingleRow* right)
        const;
    bool operator()(const Key& key, const EnforceSingleRow* node) const;
    bool operator()(const EnforceSingleRow* node, const Key& key) const;
  };

  explicit EnforceSingleRow(Key key);

  NodeCP input() const {
    return input_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
};

using EnforceSingleRowCP = const EnforceSingleRow*;

/// Emits each input row unchanged, appending `idColumn` — a value unique across
/// all output rows.
class AssignUniqueId : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Appended id column (BIGINT).
    ColumnCP idColumn;
  };

  /// Transparent hasher for interning `AssignUniqueId`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const AssignUniqueId* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `AssignUniqueId`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const AssignUniqueId* left, const AssignUniqueId* right)
        const;
    bool operator()(const Key& key, const AssignUniqueId* node) const;
    bool operator()(const AssignUniqueId* node, const Key& key) const;
  };

  explicit AssignUniqueId(Key key);

  NodeCP input() const {
    return input_;
  }

  ColumnCP idColumn() const {
    return idColumn_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ColumnCP idColumn_;
};

using AssignUniqueIdCP = const AssignUniqueId*;

/// Asserts that `distinctKeys` are unique across the input — at most one row
/// per distinct combination of their values — raising a runtime error carrying
/// `errorMessage` on the first duplicate. Does not change the schema:
/// `outputColumns()` are the input's columns unchanged (the same `Column`
/// pointers). Used to enforce the 0-or-1-row cardinality of a correlated scalar
/// subquery.
class EnforceDistinct : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Keys asserted unique across the input; non-empty.
    ExprVector distinctKeys;
    /// Error message raised on the first duplicate.
    Name errorMessage;
  };

  /// Transparent hasher for interning `EnforceDistinct`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const EnforceDistinct* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `EnforceDistinct`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const EnforceDistinct* left, const EnforceDistinct* right)
        const;
    bool operator()(const Key& key, const EnforceDistinct* node) const;
    bool operator()(const EnforceDistinct* node, const Key& key) const;
  };

  explicit EnforceDistinct(Key key);

  NodeCP input() const {
    return input_;
  }

  const ExprVector& distinctKeys() const {
    return distinctKeys_;
  }

  Name errorMessage() const {
    return errorMessage_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const ExprVector distinctKeys_;
  const Name errorMessage_;
};

using EnforceDistinctCP = const EnforceDistinct*;

/// Redistributes input rows across tasks with a remote exchange — lowered to a
/// `PartitionedOutput`→`Exchange` pair straddling a fragment boundary — setting
/// `partitioning` as the output's global partitioning. Does not change the
/// schema: `outputColumns()` are the input's columns unchanged (the same
/// `Column` pointers). The only node that repartitions: it imposes a global
/// partitioning via a shuffle, whereas other nodes derive theirs from their
/// input. The distributed memo places it when a consumer needs a partitioning
/// the input lacks.
class Exchange : public Node {
 public:
  struct Key {
    /// Input node.
    NodeCP input;
    /// Global partitioning imposed on the output via the shuffle.
    Partitioning partitioning;
  };

  /// Transparent hasher for interning `Exchange`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const Exchange* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `Exchange`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const Exchange* left, const Exchange* right) const;
    bool operator()(const Key& key, const Exchange* node) const;
    bool operator()(const Exchange* node, const Key& key) const;
  };

  explicit Exchange(Key key);

  NodeCP input() const {
    return input_;
  }

  const Partitioning& partitioning() const {
    return partitioning_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const Partitioning partitioning_;
};

using ExchangeCP = const Exchange*;

/// Writes its input rows to a table (CTAS / INSERT; DELETE / UPDATE deferred).
/// A root sink: its single BIGINT output column is the
/// written row count, consumed by no node.
class TableWrite : public Node {
 public:
  struct Key {
    /// Input node (rows to write).
    NodeCP input;
    /// Target table; emit reads its `type()` and `layouts()` for the insert
    /// handle.
    const connector::Table* table;
    /// Write form (INSERT / CTAS / ...).
    connector::WriteKind kind;
    /// Per-target-column values, non-empty and 1:1 with the table schema
    /// (`table->type()`).
    ExprVector columnExprs;
  };

  /// Transparent hasher for interning `TableWrite`s by identity.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const TableWrite* node) const;
    size_t operator()(const Key& key) const;
  };

  /// Transparent equality for interning `TableWrite`s by identity.
  struct KeyEq {
    using is_transparent = void;
    bool operator()(const TableWrite* left, const TableWrite* right) const;
    bool operator()(const Key& key, const TableWrite* node) const;
    bool operator()(const TableWrite* node, const Key& key) const;
  };

  explicit TableWrite(Key key);

  NodeCP input() const {
    return input_;
  }

  const connector::Table* table() const {
    return table_;
  }

  connector::WriteKind kind() const {
    return kind_;
  }

  const ExprVector& columnExprs() const {
    return columnExprs_;
  }

  std::span<const NodeCP> inputs() const override {
    return {&input_, 1};
  }

  void accept(const NodeVisitor& visitor, NodeVisitorContext& context)
      const override;

 private:
  const NodeCP input_;
  const connector::Table* const table_;
  const connector::WriteKind kind_;
  const ExprVector columnExprs_;
};

using TableWriteCP = const TableWrite*;

} // namespace facebook::axiom::optimizer::v2

AXIOM_ENUM_FORMATTER(facebook::axiom::optimizer::v2::NodeType);
