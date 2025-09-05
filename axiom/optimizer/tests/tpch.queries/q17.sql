-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
select
	sum(l.l_extendedprice) / 7.0 as avg_yearly
from
	lineitem as l,
	part as p
where
	p.p_partkey = l.l_partkey
	and p.p_brand = 'Brand#23'
	and p.p_container = 'MED BOX'
	and l.l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p.p_partkey
	);
