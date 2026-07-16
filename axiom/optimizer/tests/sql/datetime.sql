-- setup_file: common_setup.sql

SELECT day(DATE '2026-07-14' + INTERVAL '1' DAY * n) FROM (VALUES (1), (2)) _(n)
