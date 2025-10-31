-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998
with revenue as (
	select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and cast(l_shipdate as date) < date '1996-01-01' + interval '3' month
	group by l_suppkey
)
select
	s.s_suppkey,
	s.s_name,
	s.s_address,
	s.s_phone,
	total_revenue
from
	supplier as s,
	revenue
where
	s.s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue
	)
order by
	s.s_suppkey;
