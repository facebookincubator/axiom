-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998
select
	c.c_name,
	c.c_custkey,
	o.o_orderkey,
	o.o_orderdate,
	o.o_totalprice,
	sum(l.l_quantity)
from
	customer as c,
	orders as o,
	lineitem as l
where
	o.o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey
		having
            sum(l_quantity) > 300
	)
	and c.c_custkey = o.o_custkey
	and o.o_orderkey = l.l_orderkey
group by
	c.c_name,
	c.c_custkey,
	o.o_orderkey,
	o.o_orderdate,
	o.o_totalprice
order by
	o.o_totalprice desc,
	o.o_orderdate
limit 100;
