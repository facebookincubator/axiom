-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
select
	p.p_brand,
	p.p_type,
	p.p_size,
	count(distinct ps.ps_suppkey) as supplier_cnt
from
	partsupp as ps,
	part as p
where
	p.p_partkey = ps.ps_partkey
	and p.p_brand <> 'Brand#45'
	and p.p_type not like 'MEDIUM POLISHED%'
	and cast(p.p_size as bigint) in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps.ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p.p_brand,
	p.p_type,
	p.p_size
order by
	supplier_cnt desc,
	p.p_brand,
	p.p_type,
	p.p_size;
