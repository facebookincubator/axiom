-- setup
CREATE TABLE rows (value BIGINT)
----
INSERT INTO rows VALUES (10), (20)
----
INSERT INTO rows VALUES (30)
-- end_setup

SELECT * FROM rows
----
-- duckdb: VALUES (0), (1), (2)
SELECT "$row_id" FROM rows
