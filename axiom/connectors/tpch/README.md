# TPC-H Connector

## Overview

The TPC-H connector provides read-only access to standard TPC-H benchmark
tables generated in memory. No files on disk are needed — data is generated
on the fly.

## Capabilities

**Reads:**
- In-memory table scans for the 8 standard TPC-H tables.
- Scale factor controls dataset size (see [Scale Factors](#scale-factors)).

**Writes:**
- None. This is a read-only connector.

**Not supported:**
- `CREATE TABLE`, `INSERT INTO`, `DROP TABLE`.
- Filter pushdown.
- Partitioning and bucketing.
- Statistics. The connector does not provide per-column statistics to the
  optimizer.
- Persistence. All generated data is lost when the process exits.

## Usage

The TPC-H connector is always registered under catalog `tpch` and is the
default catalog when `--data_path` is not set:

```bash
# Use default scale factor (tiny).
buck run @mode/opt axiom/cli:cli -- --query "SELECT count(*) FROM orders"

# Use sf1 via --schema flag.
buck run @mode/opt axiom/cli:cli -- --query "SELECT count(*) FROM orders" --schema sf1

# Use sf1 as a schema qualifier in SQL.
buck run @mode/opt axiom/cli:cli -- --query "SELECT count(*) FROM sf1.orders"
```

All 22 standard TPC-H queries are available in
`axiom/optimizer/tests/tpch/queries/` (e.g. `q1.sql` through `q22.sql`):

```bash
cat axiom/optimizer/tests/tpch/queries/q1.sql | \
    buck run @mode/opt axiom/cli:cli -- --query ""
```

## Tables

Each table has a primary key that uniquely identifies rows. See
[schemas](../../optimizer/tests/tpch/queries/schemas) for the full
column definitions.

| Table | Primary Key | Rows (tiny=0.01) | Rows (sf1) | Rows (sf10) |
|-------|-------------|-----------------|-----------|------------|
| `nation` | `n_nationkey` | 25 | 25 | 25 |
| `region` | `r_regionkey` | 5 | 5 | 5 |
| `part` | `p_partkey` | 2,000 | 200,000 | 2,000,000 |
| `supplier` | `s_suppkey` | 100 | 10,000 | 100,000 |
| `partsupp` | `ps_partkey`, `ps_suppkey` | 8,000 | 800,000 | 8,000,000 |
| `customer` | `c_custkey` | 1,500 | 150,000 | 1,500,000 |
| `orders` | `o_orderkey` | 15,000 | 1,500,000 | 15,000,000 |
| `lineitem` | `l_orderkey`, `l_linenumber` | ~60,000 | ~6,000,000 | ~60,000,000 |

## Scale Factors

The scale factor controls dataset size. Row counts for most tables scale
linearly (e.g. `orders` has 1.5M rows at sf1, 15M at sf10). The `nation` and
`region` tables are fixed at 25 and 5 rows respectively regardless of scale
factor. The `lineitem` table scales approximately linearly but the exact count
depends on the data generation algorithm.

The scale factor is specified via the `--schema` CLI flag or as a schema
qualifier in SQL:

| Schema | Scale Factor | Approx. Total Rows |
|--------|-------------|-------------------|
| `tiny` (default) | 0.01 | ~87K |
| `sf1` | 1.0 | ~8.7M |
| `sf10` | 10.0 | ~87M |
| `sf100` | 100.0 | ~870M |
