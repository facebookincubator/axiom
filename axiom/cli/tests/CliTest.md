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

## Time and timestamp types display formatted values

Verify that current_timestamp, localtime, and current_time display the same
as CAST to VARCHAR.

```scrut
$ for expr in current_timestamp localtime current_time; do $CLI --query "SELECT CAST($expr AS VARCHAR) AS a, $expr AS b" 2>/dev/null | grep -E '^[0-9]' | awk -F ' \\| ' -v e="$expr" '{print e ": " (($1 == $2) ? "match" : "mismatch")}'; done
current_timestamp: match
localtime: match
current_time: match
```

## Interval types display formatted values

```scrut
$ $CLI --query "SELECT INTERVAL '1' DAY AS d, INTERVAL '13' MONTH AS m" 2>/dev/null | grep '00:00'
1 00:00:00.000 | 1-1
```

## IPADDRESS displays a formatted IP address

```scrut
$ $CLI --query "SELECT CAST('192.168.255.255' AS IPADDRESS) AS ipv4, CAST('2001:db8::ff00:42:8329' AS IPADDRESS) AS ipv6, ip_prefix(CAST('192.168.255.255' AS IPADDRESS), 9) AS prefix, ip_prefix(CAST('1234:5678:90ab:cdef:1234:5678:90ab:cdef' AS IPADDRESS), 128) AS long_prefix" 2>/dev/null
----------------+------------------------+---------------+--------------------------------------------
           ipv4 |                   ipv6 |        prefix |                                 long_prefix
----------------+------------------------+---------------+--------------------------------------------
192.168.255.255 | 2001:db8::ff00:42:8329 | 192.128.0.0/9 | 1234:5678:90ab:cdef:1234:5678:90ab:cdef/128
(1 rows in 1 batches)

```

## UUID displays a formatted UUID

```scrut
$ $CLI --query "SELECT uuid()" 2>/dev/null | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1 | wc -c | awk '{print ($1 == 37) ? "valid uuid format" : "invalid"}'
valid uuid format
```

## SET SESSION and SHOW SESSION

```scrut
$ $CLI --query "SET SESSION session_timezone = 'UTC'; SHOW SESSION LIKE '%timezone%'" 2>/dev/null
Session 'session_timezone' set to 'UTC'
*-+-* (glob)
Name*| Value (glob)
*-+-* (glob)
adjust_timestamp_to_session_timezone | true
session_timezone                     | UTC
(2 rows in 1 batches)

```

## Cleanly log dictionary wrapped result vectors (window functions produce encoded vectors)

```scrut
$ $CLI --query "SELECT * FROM (SELECT x, count(*) OVER () as cnt, row_number() OVER () AS rn FROM unnest(array[1,2,3]) AS t(x)) WHERE rn <= 2" 2>/dev/null
--+-----+---
x | cnt | rn
--+-----+---
1 |   3 |  1
2 |   3 |  2
(2 rows in 1 batches)

```
