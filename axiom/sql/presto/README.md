# Presto SQL Parser

C++ implementation of the Presto SQL parser. Converts SQL text into an
Axiom logical plan, which is then fed into the Axiom cost-based optimizer
to produce an executable multi-fragment Velox plan.

## Architecture

The parser processes SQL in four phases.

```
SQL text
   │
   │  Phase 1: Lexing + Parsing (ANTLR4)
   ▼
Concrete Syntax Tree (CST)
   │
   │  Phase 2: AST Building (AstBuilder)
   ▼
Abstract Syntax Tree (AST)
   │
   │  Phase 3: Logical Plan Construction (PlanBuilder)
   ▼
LogicalPlanNode tree
   │
   │  Phase 4: Wrapping
   ▼
SqlStatement (SelectStatement / InsertStatement / CreateTableStatement / ...)
```

### Phase 1: ANTLR4 Lexing and Parsing

The grammar file `grammar/PrestoSql.g4` is the only hand-written artifact in
this phase; the lexer, parser and visitor classes are auto-generated from it
by ANTLR4 (see `grammar/gen.sh`). The grammar is a copy of Presto's
`SqlBase.g4` with minor additions (e.g. extra `EXPLAIN` types). `UpperCaseInputStream`
uppercases characters at the look-ahead level, making keyword matching
case-insensitive while preserving original casing in identifiers and string
literals. A two-pass parsing strategy tries [SLL] mode first (fast) and falls
back to full [LL] mode if needed.

The output is a Concrete Syntax Tree (CST) that mirrors the grammar rules
one-to-one, including punctuation, keywords and intermediate rule nodes.

### Phase 2: AST Building

`AstBuilder` implements the ANTLR4-generated `PrestoSqlVisitor` interface.
Each `visitXxx()` method converts a CST context into a typed AST node that
drops syntactic noise and provides clean, named accessors (e.g.
`JoinOn::expression()` instead of navigating raw child indices).
The AST node hierarchy lives in `ast/`:

| File | Contents |
|------|----------|
| `AstNode.h` | Base `Node` class, `NodeType` enum |
| `AstExpressions.h` | Expression nodes (arithmetic, comparison, etc.) |
| `AstFunctions.h` | Function calls, casts, window specifications |
| `AstRelations.h` | Relation nodes (Table, Join, Unnest, etc.) |
| `AstStatements.h` | Statement nodes (CreateTable, Insert, etc.) |
| `AstLiterals.h` | Literal nodes (string, number, boolean, etc.) |
| `AstSupport.h` | Supporting types (Window, Property, etc.) |

### Phase 3: Logical Plan Construction

Axiom aims to provide both SQL and fluent dataframe-like APIs (e.g. PySpark).
To ensure consistency, both interfaces are backed by a single `PlanBuilder`
that owns the core logic: column name resolution, type checking, plan node
construction, and name scoping (see [No First-Class Scope](#no-first-class-scope)
below). This works because any SQL query can be expressed as a sequence of
fluent API calls.

The SQL parser is a thin translation layer that converts SQL into a sequence
of `PlanBuilder` calls, much like a dataframe program would. For example:

```sql
SELECT n_name, count(*)
FROM nation
WHERE n_regionkey > 2
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5
```

translates roughly into:

```cpp
builder.tableScan("nation")
    .filter("n_regionkey > 2")
    .aggregate({"n_name"}, {"count(*)"})
    .orderBy({"count DESC"})
    .limit(5)
    .project({"n_name", "count"});
```

Three collaborating classes perform the translation:

- **RelationPlanner** (defined in `PrestoParser.cpp`). Handles FROM clauses,
  joins, subqueries, unnest, set operations, WITH queries, ORDER BY, LIMIT.
- **ExpressionPlanner**. Translates AST expression nodes into `ExprApi`
  objects. Does not perform name resolution; that is deferred to the
  `PlanBuilder` / `ExprResolver` layer.
- **GroupByPlanner**. Plans GROUP BY, ROLLUP, CUBE and GROUPING SETS clauses.

### Phase 4: Output Wrapping

The `LogicalPlanNode` tree is wrapped in a `SqlStatement` subclass:

| SQL | Class |
|-----|-------|
| SELECT | `SelectStatement` |
| INSERT | `InsertStatement` |
| CREATE TABLE | `CreateTableStatement` |
| CREATE TABLE AS SELECT | `CreateTableAsSelectStatement` |
| DROP TABLE | `DropTableStatement` |
| EXPLAIN | `ExplainStatement` |
| USE | `UseStatement` |

## Differences from Presto Java

### No Separate Analysis Phase

In Presto Java the parser produces an AST, a separate `Analyzer` performs
semantic analysis (type checking, scope resolution), and then `LogicalPlanner`
converts the analyzed AST into a plan. Axiom skips the analysis step:
`AstBuilder` produces the AST, then `RelationPlanner` immediately converts it
into a `LogicalPlanNode` tree. Type resolution and scope handling are
embedded in the `PlanBuilder` API.

### No First-Class Scope

This is the most significant architectural difference.

Scoping is inherently a SQL concept. In a dataframe API, there is no
ambiguity: each step in the fluent chain produces a well-defined set of
columns and the user refers to them directly. There are no nested queries
and no implicit column visibility rules. `PlanBuilder` reflects this: its
`NameMappings` simply tracks which columns are available at the current
point in the fluent chain.

SQL has one notable scoping quirk: ORDER BY can reference both output column
aliases from SELECT and input columns from the FROM clause. Axiom handles
this correctly in the GROUP BY path (via `GroupByPlanner`), but not in the
non-GROUP BY path where `addOrderBy()` runs after `addProject()` has
narrowed the scope to only the SELECT list columns. See Known Gaps.

Presto Java models scope as a first-class object with a class hierarchy and
a linked-list scope chain. Axiom avoids this by splitting the two concerns
that `Scope` owns — name resolution and parent-child relationships — into
simpler, orthogonal mechanisms described below.

#### How Presto Java Works

Presto Java has an explicit `Scope` class that models lexical scoping:

- Each `Scope` holds a `RelationType` describing visible columns.
- Each `Scope` has a `parent` pointer forming a linked list (the scope chain).
- Specialized subclasses (`JoinScope`, etc.) handle specific scoping rules.
- The `Analyzer` traverses the AST and builds a scope tree. Every AST node is
  annotated with its enclosing scope. Column references are resolved by
  walking up the scope chain.

The Scope object owns two concerns: (1) what names are visible and how they
map to columns, and (2) the parent-child relationship between nested query
levels.

#### How Axiom Works

Axiom separates these two concerns into simpler, orthogonal mechanisms.

All columns in the logical plan are identified by auto-generated internal IDs
that are guaranteed unique within a query plan. These IDs use user-provided
names as prefixes to keep them readable (e.g. a column named `revenue`
gets IDs like `revenue`, `revenue_0`, `revenue_1`). User-facing output names
are tracked by the SQL parser and recorded in a root-only `OutputNode`.
Unlike `ProjectNode`, `OutputNode` allows duplicate and empty output column
names, matching Presto behavior (e.g. `SELECT a AS x, b AS x` or
`SELECT 1 AS ""`).

**NameMappings** handles name-to-column mapping. Each `PlanBuilder` carries
its own `NameMappings` that represents which columns are available at this
point in the fluent chain. It is a flat dictionary from
`QualifiedName{alias, name}` to internal IDs.

| Operation | Description |
|-----------|-------------|
| `lookup(name)` | Resolve an unqualified column name to an internal ID. |
| `lookup(alias, name)` | Resolve a qualified column name (`alias.name`) to an internal ID. |
| `merge()` | Combine two NameMappings for JOINs. Remove ambiguous unqualified names. |
| `setAlias()` | Add qualified entries for every column under a table alias. |
| `enableUnqualifiedAccess()` | Add unqualified entries for column names that appear exactly once. |

`merge()` is called when building JOINs. If an unqualified name exists in
both sides, it is removed, forcing the user to qualify it. Qualified names
(`t.column`) are always kept.

`setAlias()` is called by `PlanBuilder::as("t")` after a table scan to
enable qualified references like `t.column`. For example, after
`builder.tableScan("nation").as("n")`, column `n_name` can be referenced as
both `n_name` and `n.n_name`.

`enableUnqualifiedAccess()` is called after operations that build new
NameMappings from scratch (e.g. `project()`, `aggregate()`, `joinUsing()`).
For example, consider `SELECT * FROM orders o JOIN customers c USING (id)`.
After the JOIN, `merge()` removes unqualified `id` because it exists in both
sides — only `o.id` and `c.id` are accessible. The `joinUsing()` projection
deduplicates `id` into a single column and calls `enableUnqualifiedAccess()`,
which makes the now-unique `id` available without qualification again.

**Scope closures** handle the parent-child relationship. A `Scope` is a plain
`std::function`:

```cpp
using Scope = std::function<ExprPtr(
    const std::optional<std::string>& alias,
    const std::string& name)>;
```

`PlanBuilder::resolveInputName()` first looks up the name in its own
`outputMapping_`. If not found, it delegates to `outerScope_` (a Scope
closure captured at construction time). This creates an implicit scope chain
of arbitrary depth via nested closures.

#### Join Scoping

When processing a JOIN ON clause, `RelationPlanner` captures the left and
right scopes as closures and combines them into a `joinScope` lambda. The
`resolveJoinColumn()` helper handles resolution:

- **Qualified names** (`t.col`): tries left, then right. Table aliases are
  unique, so no ambiguity check is needed.
- **Unqualified names**: uses `PlanBuilder::hasColumn()` to probe both sides.
  If both sides have the column, it raises an ambiguity error. If neither
  side has it, it delegates to the outer scope (for correlated subqueries).
  Otherwise, it resolves from the side that has it.

This temporary scope is used only for translating the ON expression. After
the join, `NameMappings::merge()` combines both sides' mappings for
downstream use.

#### Correlated Subqueries

When a subquery is encountered in an expression, the current builder's scope
is captured and passed as the `outerScope_` of a new builder. Any column
reference that fails to resolve locally falls through to the outer scope.
This works for arbitrary nesting depth.

#### CTEs (WITH Queries)

CTEs are stored as AST nodes in a `withQueries_` map keyed by name.
`processQuery()` saves the map on entry and restores it on exit via
`SCOPE_EXIT`, so CTEs defined in a subquery are not visible outside of it
and inner CTEs shadow outer CTEs with the same name.

When a CTE name appears in a FROM clause, `processTable()` looks it up in
`withQueries_` and re-plans the CTE body from the AST. Each reference
produces an independent plan subtree — the same behavior as Presto Java,
where CTEs are inlined during planning.

#### Qualified Name Ambiguity

`ExpressionPlanner` translates `t.column` into `Col("column", Col("t"))`,
which is ambiguous: it could be a table-qualified column or a struct field
access. `ExprResolver` disambiguates by first trying
`NameMappings::lookup("t", "column")`. If that resolves, it is a column
reference. Otherwise, it resolves the inner expression and treats it as a
struct dereference. This means table aliases take priority over struct column
names: if both a table alias `t` and a struct-typed column `t` exist in scope,
`t.x` resolves to the table-qualified column. This matches Presto Java
semantics, where the same `DereferenceExpression` AST node is used for both
cases and disambiguation happens during analysis.

#### Summary

| Concept | Presto Java | Axiom |
|---------|-------------|-------|
| Name registry | `RelationType` inside `Scope` | `NameMappings` on `PlanBuilder` |
| Scope chain | `Scope.parent` linked list | Nested closures (`outerScope_`) |
| Join scope | `JoinScope` subclass | Lambda combining left + right scopes |
| Column vs. struct | Resolved during analysis | Resolved in `ExprResolver` |
| Correlated subqueries | Scope chain traversal | `outerScope_` callback delegation |

The Axiom design is simpler because `NameMappings` is a plain dictionary and
scope relationships are just closures. This avoids the complexity of Presto's
`Scope` class hierarchy while achieving the same resolution semantics.

## Adding New SQL Features

### Step 1: Grammar

Edit `grammar/PrestoSql.g4` and regenerate ANTLR files:

```bash
./axiom/sql/presto/grammar/gen.sh
```

### Step 2: AST Nodes

1. Add a `NodeType` entry in `ast/AstNode.h`.
2. Create the node class in the appropriate `ast/Ast*.h` file.
3. Add `accept()` implementation in `ast/AstNodesAll.cpp`.

### Step 3: AST Visitor

1. Add a `visitXxx()` method to `ast/AstVisitor.h`.
2. Add traversal logic to `ast/DefaultTraversalVisitor.h`.
3. Add printing logic to `ast/AstPrinter.cpp`.

### Step 4: AstBuilder

Add the corresponding `visitXxx()` method to `ast/AstBuilder.cpp` to convert
the ANTLR parse tree context into the new AST node.

### Step 5: Logical Plan

- **New statement types**: Add handling in `PrestoParser::doPlan()`. May need
  a new `SqlStatement` subclass.
- **New expressions**: Add handling in `ExpressionPlanner::toExpr()`.
- **New relation types**: Add a `processXxx()` method to `RelationPlanner`.
- **New PlanBuilder operations**: Add methods to `PlanBuilder` (see below).

#### PlanBuilder Design

`PlanBuilder` provides a fluent API modeled after PySpark's DataFrame API.
Each method appends a plan node and returns `*this`, so calls chain naturally:

```cpp
PlanBuilder(ctx)
    .tableScan("orders")
    .filter("o_totalprice > 100")
    .aggregate({"o_custkey"}, {"sum(o_totalprice)"})
    .sort({"sum DESC"})
    .limit(10)
    .build();
```

This is the single API backing both SQL and programmatic (dataframe) query
construction. The SQL parser is a thin layer that translates SQL into a
sequence of `PlanBuilder` calls (see [Phase 3](#phase-3-logical-plan-construction)).

**Dual interface.** Most methods come in two flavors: SQL strings
(`filter("a > 0")`) and `ExprApi` objects (`filter(Col("a") > 0)`). The
SQL interface is convenient for simple cases; the `ExprApi` interface is
required for subqueries and programmatic construction. When adding a new
method, provide both overloads. The SQL overload typically delegates to the
`ExprApi` one after parsing.

**Naming conventions.** Method names should match familiar DataFrame
terminology: `filter`, `project` (aliased as `map`), `aggregate`, `join`,
`sort` (aliased as `orderBy`), `limit`, `distinct`, `unnest`, `with`
(append columns), `as` (set alias). Prefer established names over
SQL-specific ones (e.g. `sort` not `ORDER BY`).

**NameMappings.** Each method must update `outputMapping_` to reflect the
new set of available columns. This is how downstream operations resolve
column references. After operations that rebuild NameMappings from scratch
(e.g. `project`, `aggregate`), call `enableUnqualifiedAccess()` to make
unique column names available without qualification.

**Internal column IDs.** Use `newName(userVisibleName)` to allocate unique
internal IDs. User-provided names are tracked in `outputMapping_` and only
applied in the final projection produced by `build()`.

**Return type.** Always return `PlanBuilder&` to support chaining.

### Step 6: Tests

Add tests in the appropriate file under `tests/`:

| File | Scope |
|------|-------|
| `PrestoParserTest.cpp` | General queries |
| `ExpressionParserTest.cpp` | Expression-level features |
| `AggregationParserTest.cpp` | Aggregation features |
| `DdlParserTest.cpp` | DDL statements |
| `TableExtractorTest.cpp` | Table reference extraction |

All test classes extend `PrestoParserTestBase`, which registers Presto
functions and creates a `TestConnector` pre-populated with TPC-H table
schemas (`nation`, `region`, `orders`, `lineitem`, etc.). Tests that need
non-TPC-H schemas add tables via `connector_->addTable()`:

```cpp
connector_->addTable("t1", ROW({"id", "a"}, {INTEGER(), VARCHAR()}));
```

#### Plan node tests

Most tests verify the shape of the logical plan produced by a SQL query.
Build a matcher chain from leaf to root using `matchScan()` (or
`matchValues()`) and fluent methods like `.filter()`, `.project()`,
`.aggregate()`, `.join()`, `.sort()`, `.limit()`, `.distinct()`. Then pass
the SQL and matcher to `testSelect()`:

```cpp
auto matcher = matchScan().filter().project();
testSelect("SELECT n_name FROM nation WHERE n_regionkey > 2", matcher);
```

For joins, build a separate matcher for the right side:

```cpp
auto matcher = matchScan().join(matchScan()).project();
testSelect("SELECT * FROM nation JOIN region ON n_regionkey = r_regionkey", matcher);
```

Some matcher methods have overloads that verify specific properties directly,
without requiring a callback. Prefer these over generic `onMatch` callbacks:

```cpp
// Verify limit offset and count.
auto matcher = matchScan().limit(0, 10);

// Verify project expressions.
auto matcher = matchScan().project({"plus(n_regionkey, 1:INTEGER)"});

// Verify values output type.
auto matcher = matchValues(ROW({"a"}, {INTEGER()}));
```

When a common verification pattern keeps appearing in `onMatch` callbacks,
add a new typed overload to `LogicalPlanMatcherBuilder` instead. For
example, `limit(offset, count)` was added so tests don't have to
`dynamic_pointer_cast` and check fields manually.

For one-off or complex checks where no typed overload exists, use an
`onMatch` callback. The callback receives the matched `LogicalPlanNodePtr`:

```cpp
auto matcher = matchScan().project([&](const auto& node) {
  const auto& outputType = node->outputType();
  EXPECT_EQ(outputType->childAt(0)->kind(), TypeKind::INTEGER);
});
testSelect("SELECT n_regionkey FROM nation", matcher);
```

#### Expression tests

`ExpressionParserTest` focuses on individual expressions rather than full
query plans. Use `parseExpr()` to parse a standalone expression and inspect
the resulting `ExprPtr` directly:

```cpp
auto expr = parseExpr("cast(null as boolean)");
VELOX_EXPECT_EQ_TYPES(expr->type(), BOOLEAN());
ASSERT_TRUE(expr->isSpecialForm());
```

Or use `testNationExpr()` to verify an expression's string representation
within a `SELECT ... FROM nation` query:

```cpp
testNationExpr("n_regionkey + 1", "plus(n_regionkey, 1:INTEGER)");
```

#### Running tests

```bash
buck test fbcode//axiom/sql/presto/tests:
```

**Note:** The test framework is incomplete. Matchers currently verify the
shape of the plan (which node types appear in which order) but lack support
for tight verification of node properties. An `ExprMatcher` for matching
expression trees is missing, and most plan node types need typed overloads
on `LogicalPlanMatcherBuilder` (only `limit`, `project`, and `values` have
them today). Contributions to close these gaps are welcome.

## Known Gaps and Limitations

- **ORDER BY cannot reference FROM columns not in SELECT (without GROUP BY).**
  In standard SQL, `SELECT x FROM t ORDER BY z` is valid — ORDER BY can
  reference any column from the FROM clause, not just the SELECT list. Axiom
  handles this correctly in the GROUP BY path (`GroupByPlanner` appends extra
  projections for ORDER BY expressions and drops them after sorting), but in
  the non-GROUP BY path `addOrderBy()` runs after `addProject()`, which
  narrows the scope to only the SELECT list columns. Referencing a FROM
  column not in SELECT will fail with "Cannot resolve column".

[SLL]: https://www.antlr.org/api/Java/org/antlr/v4/runtime/atn/PredictionMode.html "SLL — Simple LL. A faster but less powerful prediction mode that ignores the parser call stack (full context). Falls back to LL on ambiguity."
[LL]: https://en.wikipedia.org/wiki/LL_parser "LL — a top-down parsing strategy that reads input Left-to-right and produces a Leftmost derivation"
