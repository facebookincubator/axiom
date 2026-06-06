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

## EXPLAIN (TYPE GRAPH)

Outputs the query plan as a graph representation.

**Syntax:**
```sql
EXPLAIN (TYPE GRAPH) query
```

## EXPLAIN (TYPE OPTIMIZED)

Outputs the optimized physical plan.

**Syntax:**
```sql
EXPLAIN (TYPE OPTIMIZED) query
```
