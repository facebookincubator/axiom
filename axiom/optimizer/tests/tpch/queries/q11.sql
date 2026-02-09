-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
select
	ps.ps_partkey,
	sum(ps.ps_supplycost * ps.ps_availqty) as value
from
	partsupp as ps,
	supplier as s,
	nation as n
where
	ps.ps_suppkey = s.s_suppkey
	and s.s_nationkey = n.n_nationkey
	and n.n_name = 'GERMANY'
group by
	ps.ps_partkey
having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
        select
            sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001
        from
            partsupp as ps,
            supplier as s,
            nation as n
        where
            ps.ps_suppkey = s.s_suppkey
            and s.s_nationkey = n.n_nationkey
            and n.n_name = 'GERMANY'
    )
order by value desc
;
