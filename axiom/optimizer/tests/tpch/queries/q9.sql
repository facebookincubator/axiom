-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
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
			and p.p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
