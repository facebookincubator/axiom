# Smoke tests for CLI

## TPC-H is the default catalog

```scrut
$ $CLI --query "SELECT count(*) as cnt FROM nation" 2>/dev/null
---
cnt
---
 25
(1 rows in 1 batches)

```

## Create and query table using test connector

```scrut
$ $CLI --query "CREATE TABLE test.default.t(a int, b int, c int); SELECT * FROM test.default.t; SHOW STATS FOR test.default.t" 2>/dev/null
Created table: "default"."t"
(0 rows in 0 batches)

----------+-------------+----------------+-----------------------+------------+-----------+-----------
row_count | column_name | nulls_fraction | distinct_values_count | avg_length | low_value | high_value
----------+-------------+----------------+-----------------------+------------+-----------+-----------
        0 | null        |           null |                  null |       null | null      | null
     null | a           |           null |                  null |       null | null      | null
     null | b           |           null |                  null |       null | null      | null
     null | c           |           null |                  null |       null | null      | null
(4 rows in 1 batches)

```

## Insert and select using test connector

```scrut
$ $CLI --query "CREATE TABLE test.default.t(a int, b int); INSERT INTO test.default.t VALUES (1, 2); SELECT * FROM test.default.t; SHOW STATS FOR test.default.t" 2>/dev/null
Created table: "default"."t"
----
rows
----
   1
(1 rows in 1 batches)

--+--
a | b
--+--
1 | 2
(1 rows in 1 batches)

----------+-------------+----------------+-----------------------+------------+-----------+-----------
row_count | column_name | nulls_fraction | distinct_values_count | avg_length | low_value | high_value
----------+-------------+----------------+-----------------------+------------+-----------+-----------
        1 | null        |           null |                  null |       null | null      | null
     null | a           |              0 |                     1 |       null | 1         | 1
     null | b           |              0 |                     1 |       null | 2         | 2
(3 rows in 1 batches)

```

## Use --catalog and --schema flags

```scrut
$ $CLI --catalog test --schema default --query "CREATE TABLE t(x bigint, y bigint); INSERT INTO t VALUES (10, 20); INSERT INTO t VALUES (30, 40); SELECT * FROM t ORDER BY x" 2>/dev/null
Created table: "default"."t"
----
rows
----
   1
(1 rows in 1 batches)

----
rows
----
   1
(1 rows in 1 batches)

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
Created table: "default"."t"
Dropped table: "default"."t"
--
ok
--
 1
(1 rows in 1 batches)

```

## Drop non-existent table with IF EXISTS

```scrut
$ $CLI --catalog test --schema default --query "DROP TABLE IF EXISTS t; SELECT 1 as ok" 2>/dev/null
Table doesn't exist: "default"."t"
--
ok
--
 1
(1 rows in 1 batches)

```

## USE catalog.schema

```scrut
$ $CLI --query "USE test.default; CREATE TABLE t(x int, y int); INSERT INTO t VALUES (1, 2); SELECT * FROM t" 2>/dev/null
Using test.default
Created table: "default"."t"
----
rows
----
   1
(1 rows in 1 batches)

--+--
x | y
--+--
1 | 2
(1 rows in 1 batches)

```

## USE with non-existent catalog

```scrut
$ $CLI --query "USE blah.default" 2>&1 | grep Reason
Reason: Catalog does not exist: blah
```
