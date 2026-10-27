# Graph Queries — Overview

*How Axiom represents a SQL/PGQ graph match in the logical plan and how
optimizer v2 plans it. The lowering of patterns without repetition is in
[GraphTable-lowering.md](GraphTable-lowering.md); repetition is in
[GraphTable-repetition.md](GraphTable-repetition.md).*

## Problem

Graph questions over relational data, such as who is within three hops of a
user, are awkward in SQL. A fixed-length path is a chain of self-joins whose
shape the author writes by hand. A variable-length path needs
`WITH RECURSIVE`, which carries hop counts and termination in user code.

SQL/PGQ (ISO/IEC 9075-16:2023) adds a declarative form. A property graph maps
existing tables to vertices and edges, and `GRAPH_TABLE` matches a path
pattern against it and returns a table:

```sql
SELECT * FROM GRAPH_TABLE (social
  MATCH (a IS Person WHERE a.name = 'Alice') -[k IS Knows]-> (b IS Person)
  COLUMNS (b.name AS friend, k.since))
```

A front-end turns a `GRAPH_TABLE` into a `GraphMatchNode`, a logical plan node
that carries the graph and the whole pattern. Every front-end shares its
semantics, and the optimizer sees the pattern intact.

## Running example

```sql
person(id BIGINT, name VARCHAR)
knows(src BIGINT, dst BIGINT, since DATE)
```

`social` has one vertex table and one edge table:

- **Vertices:** `person`, key `id`, label `Person`.
- **Edges:** `knows`, key `(src, dst)`, label `Knows`. `src` references the
  source `person` and `dst` the destination `person`.

## The property graph

`PropertyGraph` has a schema and a backing. The front-end resolves both fully,
so nothing is left to defaults.

**Schema.** Patterns are written against it.

- A vertex label, such as `Person`, and an edge label, such as `Knows`, each
  have a name, a list of properties, and the IDs of the tables that back it.
- A property has a name and a data type, like a column.

**Backing.** Element tables map the schema to relational data.

- Each element table has an ID unique within the graph, a plan, and the key
  columns that identify a row.
- It defines each property of the labels it backs with an expression over its
  columns.
- An edge table also has, for each endpoint, the vertex table it references
  and the columns that hold that table's key.
- Several tables may back one label, and one table may back several labels.

An edge label connects the vertex labels that its edge tables' endpoint
tables back. `Knows` may connect `Person` to `Person` on one edge table and
`Person` to `Company` on another.

**Keys and identity.**

- A key identifies a row within its table: unique and never null, like a
  primary key. The graph's definer guarantees this; Axiom cannot check it.
- All tables that back one label have keys with the same data types. Axiom
  checks this.
- An element's identity is its table plus its key. `employee` 5 and
  `contractor` 5 are two different `Person` vertices.
- A name that appears twice in a pattern binds the same element both times,
  the same backing row rather than one with equal properties. Relationally,
  this is a join on identity.

## GraphMatchNode

One `GraphMatchNode` represents one `GRAPH_TABLE`'s whole `MATCH`. Selectors,
path lengths and the choice of where a search starts span a whole path, so
splitting the pattern is left to the optimizer. The node has no inputs; the
element tables carry their own plans. It holds:

- **Graph:** the `PropertyGraph`.
- **Paths:** each with a name, a selector, a path mode, and terms. A term is a
  vertex pattern, an edge pattern or a subpath, each with a name and a
  predicate. Vertex and edge patterns have a label expression; an edge
  pattern also has a direction and a repetition.
- **Predicate:** the `MATCH ... WHERE` condition across paths.
- **Outputs:** each reads a property of a vertex or edge pattern, or the length
  of a path. A property of a pattern under a repetition is an array with one
  entry per repetition.

A predicate reads pattern names as ROW-typed inputs, so `a.name` is a
dereference of `a`. A term's predicate reads its own name.

The node's meaning follows the standard's evaluation order (Deutsch et al.,
"Graph Pattern Matching in GQL and SQL/PGQ", SIGMOD 2022, §5.2 and §6):

1. Term predicates and path modes apply during matching.
2. Each path's selector applies to that path's matches, per pair of endpoints.
3. Paths that share a name join on element identity.
4. The node's predicate applies to the joined matches.

The constructor checks:

- Every label in a vertex pattern is a vertex label, and every label in an
  edge pattern is an edge label.
- Every property a predicate or output reads is defined by a table the pattern
  can match, and all such tables give it the same data type. A table that does
  not define it gives NULL.
- A name under a repetition appears once. A reused name matches only tables
  every pattern with that name can match.
- Path names are unique and distinct from element names.

For the opening query:

```
- GraphMatch [graph=social] -> friend, since
    (a IS Person WHERE a.name = 'Alice') -[k IS Knows]-> (b IS Person)
    friend := b.name, since := k.since
```

Front-ends that do not parse SQL build the graph with `GraphBuilder` and the
node with `PlanBuilder::startGraphMatch()`.

## Who plans what

`TranslatePass` lowers a `GraphMatchNode` into relational IR: scans, unions,
joins, filters and ranking. Each graph decision for a pattern without
repetition is then a relational decision that a pass already owns:

| Graph decision | Relational form | Owner |
| --- | --- | --- |
| Where the search starts | Join order | `PlanPhysical` (DPhyp) |
| Predicate placement | Filter pushdown | `PushdownAndPrune` |
| Columns to read | Column pruning | `PushdownAndPrune` |
| Tables to scan | Union legs | `Translate` |

A graph node for the whole match would duplicate join ordering and pushdown.
Translate decides which tables each pattern can match, since it has the
pattern intact.

A repeated pattern needs a loop, and the side the loop starts from depends on
statistics. Translate lowers it to a `Repeat` node, which the passes treat as
an operator over its inputs; `LowerRepeat` replaces it with a `FixedPoint`
once leaf statistics are known. [FixedPoint.md](FixedPoint.md#use-cases)
explains why a pattern and a loop are separate logical nodes, and maps each use
case onto them.

## Not supported yet

Each fails with an error that names it:

- Repeated or optional subpaths that start or end with an edge pattern, and
  repetition with no upper bound.
- A repetition or an optional subpath inside a repeated or optional subpath.
- Path modes other than `WALK` on a path with repetition or on a subpath, and
  shortest-path selectors on a path with repetition.
- The length of a named optional subpath.
- A vertex whose tables have keys of different types where its identity is
  compared.
- Two vertex patterns next to each other whose names already bind different
  vertices.
- References from inside `GRAPH_TABLE` to earlier `FROM` items.
