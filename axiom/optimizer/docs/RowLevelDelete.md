# Row-Level DELETE

Axiom plans a row-level `DELETE` as a query that produces the columns identifying
the rows to remove, followed by a connector writer. The connector supplies its
input, distribution, ordering, and commit contract. The engine supplies the
projection, exchange, sort, and write operators.

This document describes the engine contract for connector and optimizer
developers.

## The connector chooses the delete form

A connector can satisfy a `DELETE` as a metadata operation or with a writer.

| Form | Input to the connector | Execution |
|---|---|---|
| Metadata | The target scan's complete constraint | Calls `finishDelete` with no writer results. |
| Row-level | The rows selected by the query | Runs writers, then passes their results to `finishDelete`. |

A metadata layout is legal only when the write's input is the target scan
itself. Operators that still narrow the scan require the row-level
form, even when the scan has absorbed only partition filters. The connector
also decides whether it can express the scan's constraint as a metadata change.

The forms can have different storage outcomes and count-reporting contracts.

## Translation identifies the target and its columns

The frontend represents `DELETE FROM t WHERE p` as `TableWrite(kDelete)` over
the source, with no written-value expressions. A predicate can name a hidden
column through ordinary scan resolution.

`Table::rowIdColumns(WriteKind::kDelete)` supplies the columns a row-level delete
reads, in connector order. An empty list leaves the metadata form available.
`Translator` resolves the list against the target table while translating its
scan. Hidden columns absent from the SQL query receive fresh output symbols;
a connector name absent from the table schema fails an internal check.

The translated write retains these `Column` objects and the target's
`BaseTable`. Columns identify what the writer reads; the `BaseTable` identifies
which reference to the table the write changes. Optimizer rewrites preserve
both.

This distinction matters for a self-subquery:

```sql
DELETE FROM t
WHERE a IN (SELECT a FROM t WHERE b = 1);
```

Decorrelation can produce two scans of `t`. Each reference has its own
`BaseTable`, so the write still identifies the target after the plan becomes a
semi-join. Every candidate scan over that reference carries the same
`BaseTable`; a table name or a connector handle address is insufficient.

The logical write input must be the target scan under an optional filter. The
filter may contain subqueries. This shape selects each target row at most once.
The writer consumes that selected stream; duplicate target rows would also
duplicate its reported deletion count.

## Physical planning negotiates distribution and ordering

After filter pushdown and physical planning of the input, `PlanPhysicalPass`
finds the target scan and calls `deleteLayout(session, scanHandle, exact)`.
`exact` says whether the entire input is that scan. The layout is negotiated
while the input's physical properties are available, so compatible
distribution or ordering can satisfy a writer requirement directly.

```cpp
class DeleteLayout {
 public:
  struct ComputedColumn {
    std::string name;
    velox::core::TypedExprPtr expr;
  };

  static DeleteLayout metadata();
  static DeleteLayout rowLevel(
      std::vector<ComputedColumn> computedColumns,
      std::vector<std::string> shuffleKeys,
      std::shared_ptr<const PartitionType> partitionType,
      std::vector<std::string> sortKeys,
      std::vector<SortOrder> sortOrders);
};
```

The writer receives the table's row-ID columns, in their declared order,
followed by the layout's computed columns. Computed expressions reference the
table's row-ID column names. Shuffle and sort keys name columns in this final
writer input; keys add no further columns.

Physical planning binds connector names to the retained `Column` objects. It
adds the computed projection and applies the distribution and ordering
requirements. It checks that computed names are distinct, referenced columns
exist, and shuffle-key types agree with the connector partition type.

A metadata layout requires an exact input. A residual filter or a semi-join can
still narrow rows on the row-level path, where the stream describes the selected
rows. Those operators cannot be discarded for a metadata operation.

Row-ID columns are resolved before the delete form is selected. A metadata
delete therefore carries the row-ID columns through planning even though its
emitted plan has no scan or writer.

## Emission begins the delete before emitting its input

The emission order is:

```text
Negotiate deleteLayout during physical planning
  ↓
Find the target scan during emission
  ↓
Call beginDelete with its connector handle and negotiated layout
  ↓
Retain the session and delete handle in FinishWrite
  ↓
Emit the input scans and the writer for a row-level layout
  ↓
Execute writers and call finishDelete
```

`beginDelete` runs before `emit(input)`. A connector that needs target-query
state at this point must resolve it from the table reference ID already on
the handle. The input's Velox scan IDs become available later, while those
scans are emitted.

A metadata layout returns a handle with no Velox writer and produces no
execution fragments. A row-level layout returns a Velox insert handle used by
`TableWriteNode`. An input proven empty produces a zero count and starts no
write.

The engine distinguishes its output symbols from connector column names.
`TableWriteNode::columns` resolves children of the input; `columnNames` labels
those same children for the connector, position for position. This lets the
connector refer to its computed columns and stored schema names independently
of optimizer-generated symbols.

## Query state distinguishes queries and table references

One query's planning and execution share a `ConnectorContext`, its connector
sessions, and the Velox query context. The whole-query ID belongs to that
lifetime. A connector can keep query state in its session, or return null state
when its contract is stateless.

State indexed by table uses two identities:

| Identity | Meaning | Availability |
|---|---|---|
| Query ID | The entire query and its connector sessions | Planning through execution. |
| Table reference ID | One logical reference to a table, `BaseTable::id()` | Handle construction and everything after it. |

`TableLayout::createTableHandle` receives the table reference ID after the
connector session. A connector that needs state per reference keeps the ID in
its handle. Rebuilt and deserialized handles keep it, and so do the delete
handle, partition listing and commit that start from that handle. One reference
may emit several scans, all of which reach the same connector state. Distinct
references to the same table retain distinct states.

The ID is copied as a query-local value that outlives the optimizer arena.
Connector-owned state holds the copy, not the arena's `BaseTable` pointer.
Callers outside the optimizer pass an explicit ID. A raw
Velox handle replayed in a new query reads its ID from the handle and gets a
fresh state in that query. Stateless connectors return null state and ignore
the ID. Connector-specific read consistency belongs to the connector.

## Correctness boundaries

- The logical input selects each target row at most once.
- The target `BaseTable` survives rewrites and identifies the target scan.
- Row-ID columns survive to the writer in connector order.
- Computed, shuffle, and sort references bind to the declared writer input.
- Metadata deletion consumes an exact constraint; row-level deletion consumes
  the selected stream.
- Planning and execution retain the same query's connector session.

A missing target scan or dropped row-ID column after translation is an
optimizer invariant failure. A connector reports unsupported input and storage
conflicts through its own delete contract.

## Not supported yet

- A `DELETE` whose logical input is more than the target scan under an optional
  filter, such as a join or an aggregation over the target. Translation fails
  it with "DELETE requires a scan with an optional filter", because such an
  input can select a target row more than once.
