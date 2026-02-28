-- JOIN with UNION ALL subquery.
SELECT t1.a, t1.b FROM t t1 JOIN (SELECT a FROM t WHERE a = 1 UNION ALL SELECT a FROM t WHERE a = 2) t2 ON t1.a = t2.a
