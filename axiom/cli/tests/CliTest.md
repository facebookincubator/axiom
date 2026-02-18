# Smoke tests for CLI

## TPC-H is the default catalog

```scrut
$ $CLI --query "SELECT count(*) as cnt FROM nation" 2>/dev/null
ROW<cnt:BIGINT>
---
cnt
---
 25
(1 rows in 1 batches)

```

## Create and query table using test connector

```scrut
$ $CLI --query "CREATE TABLE test.default.t(a int, b int, c int); SELECT * FROM test.default.t" 2>/dev/null
Created table: default.t
(0 rows in 0 batches)

```

## Insert and select using test connector

```scrut
$ $CLI --query "CREATE TABLE test.default.t(a int, b int); INSERT INTO test.default.t VALUES (1, 2); SELECT * FROM test.default.t" 2>/dev/null
Created table: default.t
ROW<rows:BIGINT>
----
rows
----
   1
(1 rows in 1 batches)

ROW<a:INTEGER,b:INTEGER>
--+--
a | b
--+--
1 | 2
(1 rows in 1 batches)

```

## Use --catalog and --schema flags

```scrut
$ $CLI --catalog test --schema default --query "CREATE TABLE t(x bigint, y bigint); INSERT INTO t VALUES (10, 20); INSERT INTO t VALUES (30, 40); SELECT * FROM t ORDER BY x" 2>/dev/null
Created table: default.t
ROW<rows:BIGINT>
----
rows
----
   1
(1 rows in 1 batches)

ROW<rows:BIGINT>
----
rows
----
   1
(1 rows in 1 batches)

ROW<x:BIGINT,y:BIGINT>
---+---
 x |  y
---+---
10 | 20
30 | 40
(2 rows in 1 batches)

```

## Drop table

```scrut
$ $CLI --catalog test --schema default --query "CREATE TABLE t(a int); DROP TABLE t; SELECT 1 as ok" 2>/dev/null
Created table: default.t
Dropped table: default.t
ROW<ok:INTEGER>
--
ok
--
 1
(1 rows in 1 batches)

```

## Drop non-existent table with IF EXISTS

```scrut
$ $CLI --catalog test --schema default --query "DROP TABLE IF EXISTS t; SELECT 1 as ok" 2>/dev/null
Table doesn't exist: default.t
ROW<ok:INTEGER>
--
ok
--
 1
(1 rows in 1 batches)

```
