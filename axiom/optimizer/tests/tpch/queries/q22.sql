-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998
select
	cntrycode,
	count(*) as numcust,
	sum(acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal as acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					orders as o
				where
					o.o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
