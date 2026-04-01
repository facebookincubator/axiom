-- Table t(a BIGINT, b BIGINT, c DOUBLE) with 15 rows across 3 splits.
--
-- Axiom translates VARCHAR(n) to substr(CAST(x AS VARCHAR), 1, n) and
-- CHAR(n) to rpad(CAST(x AS VARCHAR), n, ' '). DuckDB does not support
-- these parametric types, so each query uses an equivalent DuckDB expression
-- as the reference.

-- VARCHAR(n) truncates long strings.
-- duckdb: SELECT 'hello'
SELECT CAST('hello world' AS VARCHAR(5))
----
-- VARCHAR(n) returns short strings unchanged.
-- duckdb: SELECT 'hi'
SELECT CAST('hi' AS VARCHAR(5))
----
-- VARCHAR(n) with exact-length input.
-- duckdb: SELECT 'hello'
SELECT CAST('hello' AS VARCHAR(5))
----
-- CHAR(n) pads short strings with spaces.
-- duckdb: SELECT rpad('hi', 5, ' ')
SELECT CAST('hi' AS CHAR(5))
----
-- CHAR(n) truncates long strings.
-- duckdb: SELECT 'hello'
SELECT CAST('hello world' AS CHAR(5))
----
-- CHAR(n) with exact-length input.
-- duckdb: SELECT rpad('hello', 5, ' ')
SELECT CAST('hello' AS CHAR(5))
----
-- VARCHAR(n) on integer column truncates to 1 character.
-- duckdb: SELECT substr(CAST(a AS VARCHAR), 1, 1) FROM t
SELECT CAST(a AS VARCHAR(1)) FROM t
----
-- CHAR(n) on integer column pads to 3 characters.
-- duckdb: SELECT rpad(CAST(a AS VARCHAR), 3, ' ') FROM t
SELECT CAST(a AS CHAR(3)) FROM t
----
-- typeof on CHAR(n) cast.
-- duckdb: SELECT 'varchar'
SELECT typeof(CAST('foo' AS CHAR(10)))
----
-- typeof on VARCHAR(n) cast.
-- duckdb: SELECT 'varchar'
SELECT typeof(CAST('foo' AS VARCHAR(10)))
