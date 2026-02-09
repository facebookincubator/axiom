-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
select
	o.o_orderpriority,
	count(*) as order_count
from
	orders as o
where
	o.o_orderdate >= date '1993-07-01'
	and cast(o.o_orderdate as date) < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem as l
		where
			l.l_orderkey = o.o_orderkey
			and l.l_commitdate < l.l_receiptdate
	)
group by
	o.o_orderpriority
order by
	o.o_orderpriority;
