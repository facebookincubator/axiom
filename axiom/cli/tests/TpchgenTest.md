# Smoke tests for tpchgen

## Missing --data_path shows error

```scrut
$ $TPCHGEN 2>&1
Error: --data_path must be specified.
[1]
```

## Invalid --sf shows error

```scrut
$ $TPCHGEN --data_path "$TMPDIR/tpch" --sf 0 2>&1
Error: --sf must be positive.
[1]
```

## Invalid --data_format shows error

```scrut
$ $TPCHGEN --data_path "$TMPDIR/tpch" --data_format csv 2>&1
Error: Unsupported data format: 'csv'. Use 'parquet' or 'dwrf'.
[1]
```

## Unwritable --data_path shows error

```scrut
$ $TPCHGEN --data_path /dev/null/invalid 2>&1
Error: Cannot create directory '/dev/null/invalid': * (glob)
[1]
```

## Generate parquet data at sf=0.01

```scrut
$ $TPCHGEN --data_path "$TMPDIR/tpch-parquet" --sf 0.01 2>&1
Generating TPC-H data (sf=0.01, format=parquet, compression=none) in * (glob)
Generating part * (glob)
Generated part: 2000 rows in * seconds. (glob)
Generating supplier * (glob)
Generated supplier: 100 rows in * seconds. (glob)
Generating partsupp * (glob)
Generated partsupp: 8000 rows in * seconds. (glob)
Generating customer * (glob)
Generated customer: 1500 rows in * seconds. (glob)
Generating orders * (glob)
Generated orders: 15000 rows in * seconds. (glob)
Generating lineitem * (glob)
Generated lineitem: 60175 rows in * seconds. (glob)
Generating nation * (glob)
Generated nation: 25 rows in * seconds. (glob)
Generating region * (glob)
Generated region: 5 rows in * seconds. (glob)
```

## Verify parquet files exist

```scrut
$ ls "$TMPDIR/tpch-parquet" | sort
customer
lineitem
nation
orders
part
partsupp
region
supplier
```

## Define count query

```scrut
$ QUERY="SELECT 'customer' AS tbl, count(*) AS cnt FROM customer UNION ALL SELECT 'lineitem', count(*) FROM lineitem UNION ALL SELECT 'nation', count(*) FROM nation UNION ALL SELECT 'orders', count(*) FROM orders UNION ALL SELECT 'part', count(*) FROM part UNION ALL SELECT 'partsupp', count(*) FROM partsupp UNION ALL SELECT 'region', count(*) FROM region UNION ALL SELECT 'supplier', count(*) FROM supplier ORDER BY 1"
```

## Verify parquet row counts using CLI

```scrut
$ $CLI --data_path "$TMPDIR/tpch-parquet" --query "$QUERY" 2>/dev/null
ROW<tbl:VARCHAR,cnt:BIGINT>
---------+------
tbl      |   cnt
---------+------
customer |  1500
lineitem | 60175
nation   |    25
orders   | 15000
part     |  2000
partsupp |  8000
region   |     5
supplier |   100
(8 rows in 1 batches)

```

## Generate dwrf

```scrut
$ $TPCHGEN --data_path "$TMPDIR/tpch-dwrf" --sf 0.01 --data_format dwrf --compression zstd 2>&1 | grep -v '^I[0-9]'
Generating TPC-H data (sf=0.01, format=dwrf, compression=zstd) in * (glob)
Generating part * (glob)
Generated part: 2000 rows in * seconds. (glob)
Generating supplier * (glob)
Generated supplier: 100 rows in * seconds. (glob)
Generating partsupp * (glob)
Generated partsupp: 8000 rows in * seconds. (glob)
Generating customer * (glob)
Generated customer: 1500 rows in * seconds. (glob)
Generating orders * (glob)
Generated orders: 15000 rows in * seconds. (glob)
Generating lineitem * (glob)
Generated lineitem: 60175 rows in * seconds. (glob)
Generating nation * (glob)
Generated nation: 25 rows in * seconds. (glob)
Generating region * (glob)
Generated region: 5 rows in * seconds. (glob)
```

## Verify dwrf files exist

```scrut
$ ls "$TMPDIR/tpch-dwrf" | sort
customer
lineitem
nation
orders
part
partsupp
region
supplier
```

## Verify dwrf row counts using CLI

```scrut
$ $CLI --data_path "$TMPDIR/tpch-dwrf" --data_format dwrf --query "$QUERY" 2>/dev/null
ROW<tbl:VARCHAR,cnt:BIGINT>
---------+------
tbl      |   cnt
---------+------
customer |  1500
lineitem | 60175
nation   |    25
orders   | 15000
part     |  2000
partsupp |  8000
region   |     5
supplier |   100
(8 rows in 1 batches)

```
