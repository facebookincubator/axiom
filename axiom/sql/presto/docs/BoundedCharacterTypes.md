# Bounded Character Types — CHAR(n) and VARCHAR(n): Design Proposal

*April 1, 2026*

## Three distinct string types

PrestoSQL has three string types. They look similar but have different
semantics:

| Type | Length | On CAST | Comparison | typeof() |
|------|--------|---------|------------|----------|
| `VARCHAR` | Unbounded | No truncation, no padding | Exact (spaces significant) | `varchar` |
| `VARCHAR(n)` | Variable, ≤ n | Truncates to n | Exact (spaces significant) | `varchar(n)` |
| `CHAR(n)` | Fixed, exactly n | Truncates to n, pads with spaces | Pad-space (trailing spaces ignored) | `char(n)` |

These are three different types, not variations of the same type:

- `VARCHAR` and `VARCHAR(n)` differ in cast behavior: `CAST('abcdef' AS
  VARCHAR(3))` returns `'abc'`, but `CAST('abcdef' AS VARCHAR)` returns
  `'abcdef'`.
- `VARCHAR(n)` and `CHAR(n)` differ in padding and comparison:
  `CAST('abc' AS VARCHAR(5))` returns `'abc'` (3 chars), but
  `CAST('abc' AS CHAR(5))` returns `'abc  '` (5 chars). `CHAR` ignores
  trailing spaces in comparisons; `VARCHAR` does not.

String literals have type `VARCHAR(n)` where n is the literal's length:
`'abc'` is `VARCHAR(3)`, `''` is `VARCHAR(0)`. Unbounded `VARCHAR` (no bound)
is a separate type — only produced by an explicit `CAST(x AS VARCHAR)`.

In the Presto Java implementation, unbounded `VARCHAR` has length
`Integer.MAX_VALUE`. `CHAR` has a max length of `65536`.

All semantics verified on production Presto (see Appendix A).

Note: Hive connector do not support bounded character types. These
types appear in practice when reading from and writing to MySQL tables,
which natively support `CHAR(n)` and `VARCHAR(n)`.

## Why execution doesn't care

The execution engine (Velox) operates only on unbounded `VARCHAR`. It does not
need to know about `CHAR(n)` or `VARCHAR(n)`. All string values in memory are
variable-length byte sequences — the bound does not affect physical
representation.

The planner handles bounded-type semantics by inserting explicit operations into
the plan *before* it reaches the execution engine:

- **Truncation and padding** become explicit CAST nodes.
- **Comparison** is resolved by the planner casting both sides to a common type
  before the comparison reaches the execution engine.

The common supertype rules for comparisons (see Appendix B for production
Presto plans):

| Comparison | Common supertype | Rule |
|---|---|---|
| `CHAR(5)` = `CHAR(10)` | `CHAR(10)` | CHAR wins; `max(n, m)` |
| `CHAR(5)` = `VARCHAR(10)` | `CHAR(10)` | CHAR wins; `max(n, m)` |
| `CHAR(5)` = `VARCHAR` | `CHAR(65536)` | CHAR wins; 65536 = max CHAR length (*) |
| `VARCHAR(5)` = `VARCHAR(10)` | `VARCHAR(10)` | `max(n, m)` |
| `VARCHAR(5)` = `VARCHAR` | `VARCHAR` | Unbounded wins |

By the time the `=` reaches the execution engine, both values have the same
type — the comparison is a plain byte-for-byte `=` on VARCHAR strings. This is
how Prestissimo works today without knowing about bounded types.

(*) The `CHAR(65536)` coercion truncates VARCHAR values longer than 65536
characters before comparison, which can produce incorrect results. This is a
known Presto limitation.

## Why the parser and optimizer must care

Although the execution engine doesn't need bounded types, the parser and
optimizer must preserve them as first-class types. The bound is not a
display-only property — it drives three critical planner responsibilities:

**Schema derivation.** The output schema of a query is derived from the types
of its columns. `SELECT * FROM t` where `t.name` is `CHAR(10)` must produce an
output schema with `CHAR(10)`, not `VARCHAR`. `CREATE TABLE t2 AS SELECT ...`
uses this output schema to define the new table.

**Cast insertion.** The planner inserts truncation and padding casts based on
the bound. `INSERT INTO t VALUES ('abcdefghijklmnop')` where `t.name` is
`CHAR(10)` must insert a cast that truncates to 10 characters and pads with
spaces. Without the bound in the type, the planner cannot generate this cast.

**Type propagation.** String operations compute output bounds from input bounds.
`CHAR(5) || CHAR(5)` produces `CHAR(10)`. `substr(VARCHAR(10), 1, 3)` preserves
`VARCHAR(10)`. See "Why Velox's type system must also care" for how this is
implemented.

Consider what happens when the bound is lost:

```sql
CREATE TABLE t (first_name CHAR(10), last_name CHAR(10));

-- The planner must propagate the bound through the concat expression.
SELECT typeof(first_name || ' ' || last_name) FROM t;      -- 'char(21)', not 'varchar'

-- CTAS must preserve the bound in the new table's schema.
CREATE TABLE u AS SELECT first_name || ' ' || last_name AS full_name FROM t;
SHOW CREATE TABLE u;
-- full_name CHAR(21)    ← correct
-- full_name VARCHAR     ← wrong: bound was lost

-- Subsequent inserts must truncate and pad to 21.
INSERT INTO u VALUES ('short');
INSERT INTO u VALUES ('this value is way too long');
SELECT full_name, length(full_name) FROM u;
-- 'short                ', 21    (padded)
-- 'this value is way too', 21    (truncated)
```

If the type degrades to unbounded `VARCHAR` anywhere in the planning pipeline,
the bound is lost. `u` ends up with schema `VARCHAR` instead of `CHAR(21)`,
and subsequent inserts silently skip truncation and padding. The `typeof()`
function is the simplest way to catch this: if `typeof(first_name || ' ' ||
last_name)` returns `varchar` instead of `char(21)`, the type has been lost. It
catches the same class of bugs as a full CTAS → INSERT → SELECT pipeline, but
in a single expression.

## Why Velox's type system must also care

Expression type resolution — function signature matching, return type inference,
implicit cast insertion — happens during planning in Axiom, but uses Velox's
function registry and type system. If bounded types are not registered in
Velox, expressions still work (Prestissimo proves
this by converting `CHAR(n)` to unbounded `VARCHAR` at the type parser level),
but the bound is silently lost. Once lost, all downstream operations — schema
derivation, cast insertion, type propagation — operate on unbounded `VARCHAR`
and produce wrong results.

To preserve bounds through expressions, two things must happen in Velox:

1. **Bounded types must be registered** in Velox's type system so the type
   resolver recognizes them.
2. **Function signatures must accept and return bounded types** so the type
   resolver can compute output bounds from input bounds. Velox already has the
   machinery for this: `SignatureVariable` constraints (used by DECIMAL) allow
   signatures to compute output type parameters from input type parameters. For
   example, `concat(CHAR(n1), CHAR(n2)) → CHAR(n1+n2)` can be expressed the
   same way as `multiply(DECIMAL(p1,s1), DECIMAL(p2,s2)) →
   DECIMAL(min(38, p1+p2), s1+s2)`.

Not all string functions need bounded-type signatures. In Presto, only a subset
accepts bounded types (let `c` be `CHAR(10)` and `v` be `VARCHAR(10)`):

Functions that accept both CHAR and VARCHAR(n) and preserve the bound:

    typeof(c)                → char(10)       typeof(v)                → varchar(10)
    typeof(substr(c, 1, 3))  → char(10)       typeof(substr(v, 1, 3))  → varchar(10)
    typeof(upper(c))         → char(10)       typeof(upper(v))         → varchar(10)
    typeof(lower(c))         → char(10)       typeof(lower(v))         → varchar(10)
    typeof(trim(c))          → char(10)       typeof(trim(v))          → varchar(10)
    typeof(c || c)           → char(20)       typeof(v || v)           → varchar

Functions that accept VARCHAR(n) but reject CHAR:

    typeof(reverse(v))              → varchar(10)       reverse(c) → error
    typeof(replace(v, 'a', 'xx'))   → varchar(21)       replace(c, ...) → error
    typeof(position('b' IN v))      → bigint            position('b' IN c) → error

Functions that reject CHAR require an explicit `CAST(x AS VARCHAR)`. The
number of function signatures that need bounded-type overloads is small.

## What's broken today

Velox's type system has no representation for `CHAR(n)` or `VARCHAR(n)`. There
is only `TypeKind::VARCHAR` — an unbounded, variable-length string with no
length parameter.

1. **Parsing fails.** `VARCHAR(255)` hits
   `VELOX_USER_FAIL("Unknown parametric type: VARCHAR")`. `CHAR` is completely
   unknown. `CREATE TABLE t (name VARCHAR(255))` does not work.

2. **No truncation or padding.** `CAST('abcdef' AS VARCHAR(3))` should return
   `'abc'`. `CAST('abc' AS CHAR(5))` should return `'abc  '`. Neither works.

3. **Schema roundtrip is lossy.** `CREATE TABLE AS SELECT` loses bounded types.
   `SHOW CREATE TABLE` and `SHOW COLUMNS` cannot display the original bounds.

4. **`typeof()` is wrong.** Returns `'varchar'` instead of `'varchar(255)'` or
   `'char(10)'`. This is a symptom of the type degradation described above.

## Alternatives Considered

### Read-only support (no type propagation)

Map `CHAR(n)` and `VARCHAR(n)` to unbounded `VARCHAR` in `parseType()`, and
rewrite `CAST(x AS VARCHAR(n))` to `substr(x, 1, n)` and
`CAST(x AS CHAR(n))` to `rpad(substr(x, 1, n), n, ' ')`. This is a
parser-only change — no Velox type registration, no function signature changes.
This is similar to what Prestissimo does today.

Reading from existing XDB tables and writing to them works correctly — column
types come from the XDB schema, so the planner can insert truncation/padding
casts at the INSERT boundary. CAST expressions also work correctly via the
rewrite.

**Risk:** The bound is lost in expressions (`typeof()` returns `varchar`).
Creating new tables via CTAS produces wrong schemas:
`CREATE TABLE u AS SELECT first_name || ' ' || last_name ...` creates `u` with
`VARCHAR` instead of `CHAR(21)`, because the bound was lost. Subsequent writes
to `u` silently skip truncation and padding.

### Do nothing — refuse to support bounded types

Since Hive and Prism do not support bounded types and they appear only when
reading from and writing to MySQL tables, one option is to simply not
support them in Axiom. Queries referencing XDB tables with `CHAR(n)` or
`VARCHAR(n)` columns would fail with an unsupported type error.

This is the simplest option, but it limits Axiom's compatibility with Presto
and Spark, both of which support bounded character types. Queries that work in
Java Presto or Spark against MySQL tables would fail in Axiom.

## Proposed Solution

Full support for `CHAR(n)` and `VARCHAR(n)` as first-class types.

**Velox:** Register bounded types as parameterized custom types extending
`VarcharType`. Add cast operators for truncation/padding. Register implicit
cast rules so existing VARCHAR functions work automatically. Add parameterized
function signatures for the small set of functions that must preserve bounds.

**Axiom:** Extend the parser to handle bounded types. Everything else works
automatically once the types are registered in Velox.

**Phasing:** CHAR(n) first (no Velox name collision), then VARCHAR(n), then
type propagation through function signatures.

### Velox changes

**Type registration.** Register `CHAR(n)` and `VARCHAR(n)` as parameterized
custom types using `CustomTypeFactory`, extending `VarcharType` (same physical
representation) with a length parameter stored as `TypeParameter(kLongLiteral)`.
Each type class overrides `name()`, `toString()`, `parameters()`,
`equivalent()`, `serialize()`.

- `CHAR` is not a built-in Velox type — no name collision.
- `VARCHAR` is a built-in singleton. `getType("VARCHAR", {TypeParameter(n)})`
  currently returns unbounded `VARCHAR`, ignoring the parameter. Fix: when
  parameters are non-empty and a singleton built-in exists with the same name,
  check custom types first. (~5 lines, backwards-compatible.)

**Cast operators.** Registered via each factory's `getCastOperator()`:

- `CAST(x AS VARCHAR(n))` — truncates to n characters. No padding.
- `CAST(x AS CHAR(n))` — truncates to n, then right-pads with spaces.

**Implicit cast rules.** Register via `CastRulesRegistry`:

- `CHAR(n) → VARCHAR` (strips trailing spaces)
- `VARCHAR(n) → VARCHAR` (identity)

This allows all existing VARCHAR functions (`length`, `position`, `replace`,
etc.) to work on bounded types automatically via implicit coercion, without
adding function overloads.

**Common supertype.** Extend `TypeCoercer::leastCommonSuperType()` to handle
bounded types. This is used by function resolution when both sides of a
comparison (or any `Generic<T1>` signature) have different types. Without this,
`CHAR(5) = CHAR(10)` would coerce both sides to `VARCHAR` (via implicit cast),
losing pad-space semantics. The rules:

- `CHAR(n)` vs `CHAR(m)` → `CHAR(max(n, m))`
- `CHAR(n)` vs `VARCHAR(m)` → `CHAR(max(n, m))`
- `CHAR(n)` vs `VARCHAR` → `CHAR(65536)` (max CHAR length)
- `VARCHAR(n)` vs `VARCHAR(m)` → `VARCHAR(max(n, m))`
- `VARCHAR(n)` vs `VARCHAR` → `VARCHAR`

**typeof.** Extend `typeof()` in `TypeOf.cpp` to handle parameterized custom
types generically (append parameters like it already does for DECIMAL).
(~3 lines.)

**Function signatures for type propagation.** For the small set of functions
that should preserve bounds, add parameterized signatures using
`SignatureVariable` constraints (same mechanism as DECIMAL):

- `concat(CHAR(n1), CHAR(n2)) → CHAR(n1 + n2)`
- `substr(CHAR(n), ...) → CHAR(n)`, `substr(VARCHAR(n), ...) → VARCHAR(n)`
- `upper/lower(CHAR(n)) → CHAR(n)`, `upper/lower(VARCHAR(n)) → VARCHAR(n)`
- `trim(CHAR(n)) → CHAR(n)`, `trim(VARCHAR(n)) → VARCHAR(n)`

Functions without bounded-type signatures fall back to implicit cast to
VARCHAR.

### Axiom changes

**Parser.** Extend `parseType()` in `ExpressionPlanner.cpp` to handle `CHAR`
and `VARCHAR` with integer parameters — parse the integer and pass it to
Velox's `getType()` as a `TypeParameter(kLongLiteral)`. Similar to how
`DECIMAL(p, s)` is already handled.

Comparison coercion happens automatically through Velox's function resolution
and `leastCommonSuperType`. Connector metadata, `SHOW CREATE TABLE`, and
`SHOW COLUMNS` also work automatically once the types are registered.

### Phasing

**Phase 1: CHAR(n).** No Velox name collision. Type registration, cast
operator, parser, implicit cast to VARCHAR, typeof. Functions degrade to
VARCHAR (no type propagation). CTAS from CHAR columns preserves the type
through schema derivation.

**Phase 2: VARCHAR(n).** Requires the `getType()` change in Velox. Same
pattern as CHAR(n): type class, cast operator, implicit cast.

**Phase 3: Type propagation.** Add parameterized function signatures for the
small set of functions that should preserve bounds. Enables correct CTAS schemas
for expressions like `first_name || ' ' || last_name`.

## Testing Strategy

The bulk of changes are in Velox, so most tests belong there:

### Velox tests

- **Type registration**: type creation, `toString()`, `equivalent()`,
  serialization/deserialization, `typeof()`.
- **Cast operators**: truncation and padding for all combinations of
  `VARCHAR`/`VARCHAR(n)`/`CHAR(n)` inputs.
- **Implicit casts**: `CHAR(n) → VARCHAR`, `VARCHAR(n) → VARCHAR`.
- **Common supertype**: `leastCommonSuperType()` for all bounded type
  combinations.
- **Function signatures**: type propagation through `concat`, `substr`,
  `upper`, `lower`, `trim`.

### Axiom tests: expression-level (SqlTest `.sql` files)

End-to-end CAST, `typeof()`, comparison, and ordering tests as SELECT queries
using the SqlTest framework. DuckDB treats bounded types as unbounded VARCHAR,
so most tests need `-- duckdb:` overrides.

See `axiom/optimizer/tests/sql/bounded_types.sql`.


### Axiom tests: DDL (C++ test fixture)

CREATE TABLE, INSERT, SHOW CREATE TABLE, SHOW COLUMNS, and CTAS use the
`SqlQueryRunnerTest` pattern.


## Appendix A: Production Presto verification

All semantics described in this document were verified on production Presto
(Presto CLI).

### Cast and typeof

```sql
SELECT CAST('abcdef' AS VARCHAR);    -- 'abcdef'   (no truncation)
SELECT CAST('abcdef' AS VARCHAR(3)); -- 'abc'      (truncated)
SELECT CAST('abc' AS CHAR(5));       -- 'abc  '    (padded)
SELECT CAST('abcdef' AS CHAR(3));    -- 'abc'      (truncated)
SELECT CAST('' AS CHAR(3));          -- '   '      (padded)

SELECT typeof(CAST('x' AS VARCHAR));    -- 'varchar'
SELECT typeof(CAST('x' AS VARCHAR(5))); -- 'varchar(5)'
SELECT typeof(CAST('x' AS CHAR(5)));    -- 'char(5)'
```

### Comparison

```sql
-- VARCHAR: trailing spaces are significant.
SELECT CAST('abc' AS VARCHAR(3)) = CAST('abc  ' AS VARCHAR(5));  -- false

-- CHAR: trailing spaces are ignored.
SELECT CAST('abc' AS CHAR(3)) = CAST('abc' AS CHAR(5));          -- true
SELECT CAST('abc' AS CHAR(10)) = CAST('abc' AS CHAR(12));        -- true
SELECT CAST('abc' AS CHAR(5)) = CAST('abc' AS VARCHAR);          -- true

-- CHAR: different content.
SELECT CAST('abc' AS CHAR(10)) = CAST('abcd' AS CHAR(12));       -- false
```

### Type propagation

```sql
SELECT typeof(CAST('abc' AS VARCHAR(5)) || 'def');               -- 'varchar'
SELECT typeof(CAST('abc' AS CHAR(5)) || 'def');                  -- 'char(8)' (5 + 3)
SELECT typeof(substr(CAST('abc' AS VARCHAR(10)), 1, 3));         -- 'varchar(10)'
```

## Appendix B: Comparison coercion plans

Plans produced by production Presto (`EXPLAIN` on `tpch.tiny.nation`) showing
how the planner coerces both sides of a comparison to a common type before it
reaches the execution engine.

```
-- CHAR(5) = CHAR(10) → both to CHAR(10)
CAST(CAST(n_name AS char(5)) AS char(10)) = CAST(n_comment AS char(10))

-- CHAR(5) = VARCHAR(10) → both to CHAR(10)
CAST(CAST(n_name AS char(5)) AS char(10)) = CAST(CAST(n_comment AS varchar(10)) AS char(10))

-- CHAR(5) = VARCHAR → both to CHAR(65536)
CAST(CAST(n_name AS char(5)) AS char(65536)) = CAST(CAST(n_comment AS varchar) AS char(65536))

-- VARCHAR(5) = VARCHAR(10) → both to VARCHAR(10)
CAST(CAST(n_name AS varchar(5)) AS varchar(10)) = CAST(n_comment AS varchar(10))

-- VARCHAR(5) = VARCHAR → both to VARCHAR
CAST(CAST(n_name AS varchar(5)) AS varchar) = CAST(n_comment AS varchar)
```
