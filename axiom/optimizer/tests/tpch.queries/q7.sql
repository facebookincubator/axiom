-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- Functional Query Definition
-- Approved February 1998
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l.l_shipdate) as l_year,
			l.l_extendedprice * (1 - l.l_discount) as volume
		from
			supplier as s,
			lineitem as l,
			orders as o,
			customer as c,
			nation as n1,
			nation as n2
		where
			s.s_suppkey = l.l_suppkey
			and o.o_orderkey = l.l_orderkey
			and c.c_custkey = o.o_custkey
			and s.s_nationkey = n1.n_nationkey
			and c.c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
			)
			and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
    ;
