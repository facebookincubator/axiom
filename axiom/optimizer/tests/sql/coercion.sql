-- setup_file: common_setup.sql

SELECT CAST(c AS DECIMAL(4,1)) * CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) + CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) - CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(10,2)) * c FROM t
----
SELECT CAST(b AS DECIMAL(10,0)) * c FROM t
----
SELECT a, SUM(CAST(c AS DECIMAL(4,1)) * CAST(b AS DOUBLE)) FROM t GROUP BY a
----
SELECT CAST(c AS DECIMAL(4,1)) = CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) / CAST(b AS DOUBLE) FROM t
----
SELECT SIN(CAST(c AS DECIMAL(4,1))) FROM t
----
-- count 1
SELECT 1 WHERE date_format(
    parse_datetime('2026-05-07+10:00UTC', 'yyyy-MM-dd+HH:mmz')
        AT TIME ZONE 'America/Los_Angeles',
    '%Y-%m-%d %H:%i') = '2026-05-07 03:00'
----
-- Two AT TIME ZONE folds over the same instant must produce distinct
-- formatted strings for distinct target timezones.
-- count 1
SELECT
    date_format(
        TIMESTAMP '2026-05-07 10:00:00 UTC' AT TIME ZONE 'America/Los_Angeles',
        '%H') AS la,
    date_format(
        TIMESTAMP '2026-05-07 10:00:00 UTC' AT TIME ZONE 'America/New_York',
        '%H') AS ny
FROM (VALUES (1)) AS t(x)
WHERE date_format(
        TIMESTAMP '2026-05-07 10:00:00 UTC' AT TIME ZONE 'America/Los_Angeles',
        '%H') = '03'
  AND date_format(
        TIMESTAMP '2026-05-07 10:00:00 UTC' AT TIME ZONE 'America/New_York',
        '%H') = '06'
----
-- count 1
SELECT 1 WHERE hour(TIMESTAMP '2026-05-07 10:00:00 UTC' AT TIME ZONE 'America/Los_Angeles') = 3
