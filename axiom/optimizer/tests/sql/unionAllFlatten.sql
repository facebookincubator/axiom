-- setup_file: common_setup.sql

-- Tests for UNION ALL single-child collapse.
-- Each test uses a CTE with a specific layer, wraps it in UNION ALL with a
-- constant boolean column, and applies an outer WHERE that eliminates one leg.
-- Filter pushdown drops the constant-false leg, and the surviving leg
-- replaces the SetDt in the parent's slot with its shared columns rebound
-- to the leg. The tests verify that the collapse preserves correctness for
-- each DT layer.
--
-- See docs/DerivedTableLayers.md for the DT layer model.

-- Layer 2: Outer join — LEFT JOIN produces nullable columns.
WITH t AS (
    SELECT a.x, b.y
    FROM (VALUES (1), (2)) a (x)
    LEFT JOIN (VALUES (1, 10)) b (x, y) ON a.x = b.x
)
SELECT x, y FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 3: Filter only (no column-producing layers above base tables).
WITH t AS (
    SELECT x FROM (VALUES (1), (2), (3)) v (x) WHERE x > 1
)
SELECT x FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 4: Aggregation with simple grouping key.
WITH t AS (
    SELECT x, count(*) cnt
    FROM (VALUES (1), (1), (2)) v (x)
    GROUP BY 1
)
SELECT x, cnt FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 4: Aggregation with computed grouping key (the original bug).
WITH t AS (
    SELECT upper(x) a
    FROM (VALUES ('cat1'), ('cat2')) v (x)
    GROUP BY 1
)
SELECT a FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 4: Aggregation with multiple grouping keys and aggregates.
WITH t AS (
    SELECT x, y, sum(z) total, count(*) cnt
    FROM (VALUES (1, 'a', 10), (1, 'a', 20), (2, 'b', 30)) v (x, y, z)
    GROUP BY 1, 2
)
SELECT x, y, total, cnt FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 5: Aggregation with HAVING.
WITH t AS (
    SELECT x, count(*) cnt
    FROM (VALUES (1), (1), (2)) v (x)
    GROUP BY 1
    HAVING count(*) > 1
)
SELECT x, cnt FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 6: Window function (row_number).
WITH t AS (
    SELECT x, row_number() OVER (ORDER BY x) rn
    FROM (VALUES (1), (2), (3)) v (x)
)
SELECT x, rn FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 6: Window function (rank with partition).
WITH t AS (
    SELECT x, y, rank() OVER (PARTITION BY x ORDER BY y) rnk
    FROM (VALUES (1, 10), (1, 20), (2, 30)) v (x, y)
)
SELECT x, y, rnk FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 6: Window function with running sum.
WITH t AS (
    SELECT x, y, sum(y) OVER (PARTITION BY x ORDER BY y) running_sum
    FROM (VALUES (1, 10), (1, 20), (2, 30)) v (x, y)
)
SELECT x, y, running_sum FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layers 2+4: Outer join + aggregation.
WITH t AS (
    SELECT a.x, count(b.y) cnt
    FROM (VALUES (1), (2)) a (x)
    LEFT JOIN (VALUES (1, 10), (1, 20)) b (x, y) ON a.x = b.x
    GROUP BY 1
)
SELECT x, cnt FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layers 4+6: Aggregation + window.
WITH t AS (
    SELECT x, cnt, row_number() OVER (ORDER BY cnt DESC, x) rn
    FROM (
        SELECT x, count(*) cnt
        FROM (VALUES (1), (1), (2), (3)) v (x)
        GROUP BY 1
    )
)
SELECT x, cnt, rn FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layers 2+4+6: Outer join + aggregation + window.
WITH t AS (
    SELECT x, cnt, row_number() OVER (ORDER BY cnt DESC) rn
    FROM (
        SELECT a.x, count(b.y) cnt
        FROM (VALUES (1), (2)) a (x)
        LEFT JOIN (VALUES (1, 10), (1, 20)) b (x, y) ON a.x = b.x
        GROUP BY 1
    )
)
SELECT x, cnt, rn FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 6: Dependent window functions. rank() orders by rn, which is the
-- output of row_number(). After window merging, both functions are in the
-- same WindowPlan; the collapse must preserve it on the surviving leg.
WITH t AS (
    SELECT x, rn, rank() OVER (ORDER BY rn) rnk
    FROM (
        SELECT x, row_number() OVER (ORDER BY x) rn
        FROM (VALUES (1), (2), (3)) v (x)
    )
)
SELECT x, rn, rnk FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
----
-- Layer 7: Order by on surviving child.
WITH t AS (
    SELECT x FROM (VALUES (3), (1), (2)) v (x)
)
SELECT x FROM (
    SELECT *, FALSE f FROM t
    UNION ALL
    SELECT *, TRUE f FROM t
) WHERE f = FALSE
ORDER BY x
----
-- Multiple surviving children (no flattening). Both legs survive.
-- Verifies non-flattening path still works.
WITH t AS (
    SELECT upper(x) a
    FROM (VALUES ('cat1'), ('cat2')) v (x)
    GROUP BY 1
)
SELECT a FROM (
    SELECT *, 1 f FROM t
    UNION ALL
    SELECT *, 2 f FROM t
) WHERE f IN (1, 2)
