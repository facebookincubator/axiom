# Presto SQL Extensions

Axiom extends Presto SQL with several features not available in Presto Java.
These are Axiom-specific syntax additions that go beyond the standard Presto
SQL dialect.

## Friendly SQL

Some extensions are gated behind the `friendlySql` flag in `ParserOptions`.
These are inspired by [DuckDB's Friendly SQL](https://duckdb.org/docs/sql/dialect/friendly_sql)
and are enabled by default. To disable them, set `friendlySql = false` on
the `ParserOptions` passed to `ParserSession`, or use
`SET SESSION parser.friendly_sql = false` at the CLI.

Friendly SQL features:

| Feature | Example |
|---------|---------|
| Named ROW constructor | `ROW(1 AS id, 'Alice' AS name)` |
| Trailing commas in SELECT, WITH, VALUES | `SELECT a, b, FROM t` |
| FROM-first syntax | `FROM t WHERE x = 1` |
| Digit separators | `SELECT 1_000_000` |
| Method-call syntax | `'hello'.upper().substr(1, 3)` |
| Lateral column aliases | `SELECT i+1 AS j, j+2 AS k` |
| EXCLUDE | `SELECT * EXCLUDE (col1, col2) FROM t` |
| REPLACE | `SELECT * REPLACE (expr AS col) FROM t` |
| COLUMNS | `SELECT COLUMNS('regex') FROM t` |

## Named WINDOW Clause

Axiom supports the SQL:2003 `WINDOW` clause. It appears after `HAVING` and
before the query's `ORDER BY`, and allows multiple window functions to reuse a
window specification:

```sql
WINDOW name AS (
  [existing_window_name]
  [PARTITION BY expression, ...]
  [ORDER BY sort_item, ...]
  [frame]
) [, ...]
```

A function can use the name directly with `OVER name`, or refine it with
`OVER (name ...)`:

```sql
WITH t(a, b) AS (
  VALUES (1, 10), (1, 20), (2, 5)
)
SELECT
  a,
  b,
  sum(b) OVER by_a AS partition_sum,
  sum(b) OVER (ordered ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rolling_sum
FROM t
WINDOW by_a AS (PARTITION BY a),
       ordered AS (by_a ORDER BY b)
ORDER BY a, b
```

The result is:

| a | b  | partition_sum | rolling_sum |
|---|----|---------------|-------------|
| 1 | 10 | 30            | 10          |
| 1 | 20 | 30            | 30          |
| 2 | 5  | 5             | 5           |

Named windows follow these rules:

- Names are scoped to one query specification and compared case-insensitively.
- A definition can reference only a definition that appears earlier in the
  same `WINDOW` clause.
- A derived specification cannot add `PARTITION BY`.
- A derived specification cannot add `ORDER BY` when the referenced window
  already has one.
- A derived specification cannot reference a window that has a frame.
- A direct `OVER name` reference uses the complete named specification,
  including its frame.
- Expressions declared in the `WINDOW` clause resolve against query inputs.
  Parts added inline in the query's `ORDER BY` can resolve `SELECT` aliases.

### Unreferenced Definitions

Every definition is analyzed, including one that no `OVER` clause references.
Window functions run after grouping and aggregation, so their specifications
can use grouping keys and aggregates. An aggregate in a definition therefore
belongs to the surrounding query block.

For example, this query aggregates two input rows and then applies `rank()` to
the single result row:

```sql
SELECT 1, rank() OVER w
FROM (VALUES 10, 20) AS t(x)
WINDOW w AS (ORDER BY sum(x))
```

It returns `(1, 1)`. Removing the window function leaves the aggregate in the
query specification:

```sql
SELECT 1
FROM (VALUES 10, 20) AS t(x)
WINDOW w AS (ORDER BY sum(x))
```

This query still returns one row. Selecting `x` instead fails grouping
validation because `x` is neither grouped nor aggregated. The unreferenced
definition itself produces no window operator.

## Named ROW Constructor


See https://github.com/prestodb/presto/issues/26205.

Constructs a ROW value with named fields without requiring an explicit CAST.

**Syntax:**
```sql
ROW(expr1 AS name1, expr2 AS name2, ...)
```

**Equivalent Presto SQL:**
```sql
CAST(ROW(expr1, expr2, ...) AS ROW(name1 type1, name2 type2, ...))
```

Types are inferred from the expressions. All fields must be named — mixing
named and unnamed fields is not supported.

**Examples:**
```sql
-- Create a named ROW.
SELECT ROW(1 AS id, 'Alice' AS name)
-- Returns: {id=1, name=Alice}

-- Access fields by name.
SELECT a.id, a.name
FROM (SELECT ROW(1 AS id, 'Alice' AS name) AS a)
-- Returns: 1, Alice

-- Use with complex expressions.
SELECT ROW(count(*) AS total, max(price) AS max_price)
FROM orders
```

## Trailing Commas


Allows a trailing comma after the last item in SELECT, WITH, and VALUES lists.

```sql
SELECT
    n_nationkey,
    n_name,
    n_regionkey,
FROM nation

WITH
    a AS (SELECT 1),
    b AS (SELECT 2),
SELECT * FROM a, b

VALUES 1, 2, 3,
```

## FROM-First Syntax


Allows queries to start with `FROM` instead of `SELECT`. An implicit
`SELECT *` is added.

```sql
-- Equivalent to: SELECT * FROM nation
FROM nation

-- With WHERE clause.
FROM nation WHERE n_regionkey = 1
```

## Underscores in Numeric Literals


Allows underscores as digit separators in integer, decimal, and double
literals for readability.

```sql
SELECT 1_000_000
-- Returns: 1000000

SELECT 1_000.50
-- Returns: 1000.5

SELECT 1_000E3
-- Returns: 1000000.0
```

## Method-Call Syntax for Function Chaining


Allows calling functions using dot notation, where the base expression becomes
the first argument: `expr.func(args...)` desugars to `func(expr, args...)`.

```sql
-- Basic method call.
SELECT 'hello'.upper()
-- Equivalent to: SELECT upper('hello')

-- With arguments.
SELECT 'hello'.substr(1, 3)
-- Equivalent to: SELECT substr('hello', 1, 3)

-- Chaining multiple calls (left-to-right).
SELECT 'hello'.trim().upper().substr(1, 3)
-- Equivalent to: SELECT substr(upper(trim('hello')), 1, 3)

-- Works with any non-identifier base expression.
SELECT (price * quantity).abs()
-- Equivalent to: SELECT abs(price * quantity)
```

The method-call syntax supports simple function calls only — no `DISTINCT`,
`ORDER BY`, `FILTER`, or `OVER` clauses. For those, use standard function call
syntax.

## Lateral Column Aliases


Allows referencing aliases defined in earlier SELECT items within the same
SELECT list. The alias is expanded inline — semantically identical to writing
the expression out by hand.

```sql
-- Basic reuse.
SELECT price * quantity AS total, total * tax_rate AS tax FROM orders
-- Equivalent to: SELECT price * quantity AS total, (price * quantity) * tax_rate AS tax

-- Chaining.
SELECT a + 1 AS x, x + 2 AS y, y * 3 AS z FROM t
-- Equivalent to: SELECT a + 1 AS x, (a + 1) + 2 AS y, ((a + 1) + 2) * 3 AS z

-- With functions (combines with method-call syntax).
SELECT upper(name) AS u, u.substr(1, 3) AS abbrev FROM t
```

Rules:
- **Left-to-right only.** Forward references and self-references fail.
- **Columns take priority.** If a name is both a FROM column and an alias, the
  column wins (preserves backward compatibility).
- **SELECT list scope only.** Aliases are not expanded in GROUP BY, HAVING,
  WHERE, or ORDER BY.

## SELECT * EXCLUDE


Removes named columns from a star expansion.

**Syntax:**
```sql
SELECT * EXCLUDE (column1, column2, ...) FROM t
SELECT t.* EXCLUDE (column1) FROM t JOIN s
```

**Examples:**
```sql
-- Remove n_comment from output.
SELECT * EXCLUDE (n_comment) FROM nation
-- Returns: n_nationkey, n_name, n_regionkey

-- With qualified star in a join.
SELECT nation.* EXCLUDE (n_comment) FROM nation, region
WHERE n_regionkey = r_regionkey
```

Raises an error if an excluded column does not exist or if all columns are
excluded.

## SELECT * REPLACE


Substitutes expressions for named columns in a star expansion. The column keeps
its position and name, but uses the replacement expression.

**Syntax:**
```sql
SELECT * REPLACE (expr AS column, ...) FROM t
```

**Examples:**
```sql
-- Replace n_name with its uppercase version.
SELECT * REPLACE (upper(n_name) AS n_name) FROM nation
-- Returns: n_nationkey, upper(n_name) AS n_name, n_regionkey, n_comment

-- Replace with a constant.
SELECT * REPLACE (0 AS n_regionkey) FROM nation

-- Combine with EXCLUDE.
SELECT * EXCLUDE (n_comment) REPLACE (upper(n_name) AS n_name) FROM nation
```

Raises an error if the replaced column does not exist (including if it was
removed by EXCLUDE). Each column can appear in REPLACE at most once.

EXCLUDE and REPLACE can be chained on the same star expression. EXCLUDE is
applied first, then REPLACE.

**Limitation:** The REPLACE target is an unqualified column name. In joins with
overlapping column names, REPLACE on an ambiguous column raises an error. Use
qualified star and qualified column references in the expression:

```sql
-- Fails: n_name is ambiguous across both tables.
SELECT * REPLACE (upper(n_name) AS n_name) FROM nation a, nation b ...

-- Works: qualify both the star and the expression.
SELECT a.* REPLACE (upper(a.n_name) AS n_name) FROM nation a, nation b ...
```

## SELECT COLUMNS


Selects columns matching a regular expression pattern (RE2 syntax).

**Syntax:**
```sql
SELECT COLUMNS('regex') FROM t
SELECT t.COLUMNS('regex') FROM t JOIN s
```

**Examples:**
```sql
-- Select columns starting with 'n_n'.
SELECT COLUMNS('n_n.*') FROM nation
-- Returns: n_nationkey, n_name

-- All columns (equivalent to SELECT *).
SELECT COLUMNS('.*') FROM nation

-- Only from a specific table in a join.
SELECT nation.COLUMNS('n_n.*') FROM nation, region
WHERE n_regionkey = r_regionkey

-- Combine with EXCLUDE.
SELECT COLUMNS('n_n.*') EXCLUDE (n_name) FROM nation
-- Returns: n_nationkey

-- Mix with other select items.
SELECT * EXCLUDE (n_name, n_comment), COLUMNS('n_n.*') FROM nation
-- Returns: n_nationkey, n_regionkey, n_nationkey, n_name
```

Raises an error if the regex matches no columns or if the regex pattern is
invalid.

EXCLUDE and REPLACE modifiers can be applied to COLUMNS, same as with star
expressions.

### COLUMNS in Expressions

`COLUMNS('regex')` can appear inside an expression. The expression is expanded
into one select item per matched column — each is a copy of the enclosing
expression with `COLUMNS(...)` replaced by the column reference. This is syntax
sugar for writing the same expression for each matching column.

```sql
-- Syntax sugar for: SELECT n_nationkey + 1, n_regionkey + 1
SELECT COLUMNS('.*key') + 1 FROM nation

-- Syntax sugar for: SELECT cast(n_nationkey AS varchar), cast(n_regionkey AS varchar)
SELECT cast(COLUMNS('.*key') AS varchar) FROM nation

-- Alias applies to all expanded columns.
SELECT COLUMNS('.*key') + 1 AS x FROM nation
-- Equivalent to: SELECT n_nationkey + 1 AS x, n_regionkey + 1 AS x
```

Multiple `COLUMNS()` calls in the same expression are expanded pairwise (zip).
All calls must match the same number of columns. The i-th output expression
replaces every `COLUMNS()` call with the i-th matched column from that call's
pattern.

```sql
-- Given table t with columns: x_a, x_b, x_c, y_a, y_b, y_c
-- Equivalent to: SELECT x_a + y_a, x_b + y_b, x_c + y_c
SELECT COLUMNS('x_.*') + COLUMNS('y_.*') FROM t

-- Equivalent to: SELECT cast(x_a + y_a AS varchar), ...
SELECT cast(COLUMNS('x_.*') + COLUMNS('y_.*') AS varchar) FROM t
```

Raises an error if the patterns match different numbers of columns.

### COLUMNS in GROUP BY, ORDER BY, and WHERE

`COLUMNS('regex')` can appear in GROUP BY, ORDER BY, and WHERE clauses. The
expansion semantics match the clause:

- **GROUP BY**: each matched column becomes a separate grouping key.
- **ORDER BY**: each matched column becomes a separate sort key with the same
  ordering direction.
- **WHERE**: each matched column produces a copy of the enclosing expression,
  and the copies are combined with AND.

```sql
-- Equivalent to: GROUP BY n_nationkey, n_regionkey
SELECT n_nationkey, n_regionkey, count(*) FROM nation
GROUP BY COLUMNS('.*key')

-- Equivalent to: ORDER BY n_nationkey DESC, n_regionkey DESC
SELECT * FROM nation ORDER BY COLUMNS('.*key') DESC

-- Equivalent to: WHERE n_nationkey > 0 AND n_regionkey > 0
SELECT * FROM nation WHERE COLUMNS('.*key') > 0
```

### Column Name Matching

EXCLUDE, REPLACE, and COLUMNS operate on user-defined column names only.
Columns without explicit names (e.g., anonymous computed expressions like
`SELECT a + 1`) cannot be targeted by these modifiers.

For joins with overlapping column names (e.g., self-joins), EXCLUDE removes
all columns matching the name regardless of source table. Use qualified star
(`t.* EXCLUDE (...)`) to restrict to a specific table.

## EXCEPT ALL / INTERSECT ALL

Multiset versions of EXCEPT and INTERSECT that preserve duplicates.

**Syntax:**
```sql
query1 EXCEPT ALL query2
query1 INTERSECT ALL query2
```

Standard Presto supports only `EXCEPT` and `INTERSECT` (which are set
operations that deduplicate). `EXCEPT ALL` and `INTERSECT ALL` treat inputs as
multisets: a row that appears N times in the left and M times in the right
appears `max(N - M, 0)` times in `EXCEPT ALL` and `min(N, M)` times in
`INTERSECT ALL`.

## EXPLAIN (TYPE OPTIMIZED)

Outputs the optimizer's plan after its last pass, with estimates. `last_pass`
stops the optimizer after the named pass instead and outputs the plan that pass
produced. Pass names are matched ignoring case; an unknown name fails with a
list of the valid ones.

**Syntax:**
```sql
EXPLAIN (TYPE OPTIMIZED) query
EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'pass_name')) query
```

See `axiom/optimizer/docs/OptimizerPasses.md` for the passes.

## Graph Queries

Axiom parses SQL/PGQ graph queries: `GRAPH_TABLE` in `FROM`, which matches a
path pattern against a property graph and returns a table, and `WITH GRAPH`, an
Axiom extension that defines a property graph for one query. Presto Java
supports neither.

Graph queries are gated behind the `graphQueries` flag in `ParserOptions`, which
is off by default. To enable them, set `graphQueries = true`, or use
`SET SESSION parser.graph_queries = true` at the CLI. With the flag on, the
parser plans a `GRAPH_TABLE` as a GraphMatch logical plan node, which
`EXPLAIN (TYPE LOGICAL)` shows. The v2 optimizer runs graph patterns; the v1
optimizer does not run them. With v2, a query that uses any of these fails
with an error that names it:

- a path mode other than `WALK` on a path with repetition or on a subpath;
- a repetition with no upper bound, or a shortest-path selector on a path
  with repetition;
- a repeated or optional subpath that starts or ends with an edge pattern;
- a repetition or an optional subpath inside a repeated or optional subpath;
- `path_length` of a named optional subpath;
- an edge pattern without a vertex pattern on each side;
- an undirected edge pattern whose name another pattern reuses;
- a pattern whose tables have keys of different data types or numbers of
  columns, when two edge patterns meet at it, a selector's path starts or ends
  at it, or two paths with a selector share it;
- a pattern name in a predicate other than in a property reference, or a
  subquery in a predicate.

```sql
WITH GRAPH g AS (
  VERTEX TABLES (t KEY (a) LABEL A PROPERTIES (a, b))
  EDGE TABLES (u KEY (a, b)
    SOURCE KEY (a) REFERENCES t (a)
    DESTINATION KEY (b) REFERENCES t (a)
    LABEL B))
SELECT *
FROM GRAPH_TABLE (g
  MATCH p = ANY SHORTEST (x IS A WHERE x.a = 1) -[e IS B]->+ (y IS A)
  WHERE y.b > 0
  COLUMNS (y.a, path_length(p) AS hops))
```

### Graph definitions

`WITH GRAPH name AS (VERTEX TABLES (...) EDGE TABLES (...))` defines a graph
alongside the query's CTEs. Each element table names a table, an optional
alias, a `KEY`, and its labels:

- `LABEL name` adds a label; a table may have several. Without one, the table
  has a single label named after the table.
- `PROPERTIES (expr [AS name], ...)` lists a label's properties. Without it,
  the label exposes all columns. A property that is not a plain column
  reference needs `AS name`.
- An edge table also has `SOURCE KEY (...) REFERENCES vertex_table (...)` and
  `DESTINATION KEY (...) REFERENCES vertex_table (...)`.

Each label becomes a vertex or an edge of the graph, backed by every table that
has it. The definition must also satisfy:

- The referenced columns are the vertex table's key, in any order.
- Every table with a label exposes the same properties for it, and the labels
  of one table give a shared property name the same expression.

### Patterns

The pattern syntax follows the GQL grammar (ISO/IEC 39075), whose pattern
language SQL/PGQ shares.

| Construct | Syntax |
|-----------|--------|
| Node | `(v IS Label WHERE predicate)`; each part optional |
| Edge | `-[e IS Label WHERE predicate]->`, `<-[...]-`, `-[...]-`; abbreviated `->`, `<-`, `-` |
| Label expression | `!`, `&`, `\|`, `%` (any label), parentheses; `!` binds tightest, then `&`, then `\|` |
| Quantifier | `*`, `+`, `{n}`, `{m,n}` with either bound optional; on an edge or a parenthesized subpath |
| Optional subpath | `( ... )?` |
| Subpath | `(q = mode path WHERE predicate)` |
| Path mode | `WALK`, `TRAIL`, `ACYCLIC`, `SIMPLE`, optionally followed by `PATH` or `PATHS` |
| Selector | `ANY SHORTEST`, `ALL SHORTEST`, `ANY [k]`, `SHORTEST k`, `SHORTEST [k] GROUP`, `ALL` |
| Path | `p = selector mode pattern`, each prefix optional |

Several path patterns may be separated by commas. `MATCH ... WHERE` filters
matches after selection, and `COLUMNS (expr [AS name], ...)` defines the
output columns. Predicates and columns read a property of a matched vertex or
edge as `v.property`, and the length of a named path or subpath as
`path_length(p)`. Elements need not alternate between node and edge patterns:
adjacent node patterns bind the same node, and an edge with no node pattern
next to it has an implicit one.

Rules beyond the grammar:

- Labels are written with `IS`. Presto identifiers may contain `:`, so
  `(a:Person)` is rejected rather than read as a variable named `a:Person`.
- An unbounded quantifier (`*`, `+`, `{m,}`) must be in the scope of a selector
  other than `ALL` or a path mode other than `WALK`.
- `MATCH ... WHERE` cannot read a pattern under a quantifier, and
  `path_length` cannot read a subpath under a quantifier.
- Undirected edges (`~[...]~`) and `<-[...]->` are not supported; every edge
  table is directed.
- Match modes (`REPEATABLE ELEMENTS`, `DIFFERENT EDGES`), `KEEP`, and element
  property specifications (`{k: v}`) are not supported.
