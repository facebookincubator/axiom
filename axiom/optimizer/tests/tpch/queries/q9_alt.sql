-- TPC-H Q9 variant: `p_name like '%green%'` replaced with `p_size <= 3`, an
-- estimable filter of similar selectivity (~6% vs green's ~5%). The `like`
-- predicate is not estimable from column stats, so the optimizer over-estimates
-- it and joins `supplier` before the selective `part`; with an estimable filter
-- it reduces lineitem by `part` first — the optimal order. See statistics.md.
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n.n_name as nation,
			extract(year from o.o_orderdate) as o_year,
			l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
		from
			part as p,
			supplier as s,
			lineitem as l,
			partsupp as ps,
			orders as o,
			nation as n
		where
			s.s_suppkey = l.l_suppkey
			and ps.ps_suppkey = l.l_suppkey
			and ps.ps_partkey = l.l_partkey
			and p.p_partkey = l.l_partkey
			and o.o_orderkey = l.l_orderkey
			and s.s_nationkey = n.n_nationkey
			and p.p_size <= 3
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
