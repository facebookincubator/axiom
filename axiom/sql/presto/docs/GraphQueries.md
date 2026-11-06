# Graph Queries

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

## Graph definitions

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

## Patterns

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
