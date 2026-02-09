-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
select
	s.s_name,
	s.s_address
from
	supplier as s,
	nation as n
where
	s.s_suppkey in (
		select
            ps.ps_suppkey
		from
			partsupp as ps
		where
			ps.ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'forest%'
			)
			and ps.ps_availqty > (
				select
					0.5 * sum(l.l_quantity)
				from
					lineitem as l
				where
					l.l_partkey = ps.ps_partkey
					and l.l_suppkey = ps.ps_suppkey
					and l.l_shipdate >= date '1994-01-01'
					and cast(l.l_shipdate as date) < date('1994-01-01') + interval '1' year
			)
	)
	and s.s_nationkey = n.n_nationkey
	and n.n_name = 'CANADA'
order by
	s.s_name;
