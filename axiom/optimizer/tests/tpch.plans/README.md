# TPC-H Plans

This document covers TPC-H with Axiom Optimizer under different
configurations of execution (single node and scale-out) and different data
layouts, as in bucketed with optional sorting on keys.

We wil extend this as support is added to the different data layouts and access
methods. We begin with analyzing multithreaded single node execution.
Subsequent sections will cover differences brought by distributed execution or
different data layouts.

## Environment

We run the single node experiments using Axiom CLI on a laptop against a 30G
dataset with Parquet and Snappy.  To repeat the below runs, use the query
formulations in tpch.queries. To see plans and stats, use

```
SQL> flag optimizer_trace = 15;
SQL> flag include_runtime_stats = true;
SQL> flag num_workers = 1;
SQL> flag num_drivers = <number of core threads>;
```

We set num_workers = 1 to force single node plans. These run faster on a
single system and are easier to read when we do not have exchanges between
stages.

## Single Node Multithreaded Execution

### Q1

The query is a simple scan of lineitem with a very low cardinality group by.
All the work is in the table scan (65%) and partial aggregation (30%).

### Q2

The query takes partsupp info for a set of parts and a set of suppliers
limited by geography. The selection on supplier is 1/5 and the selection on
part is 1/300. So we first join partsupp with part and then with supplier.
Then we get to the correlated subquery. This is flattened into a left join
against a group by on the correlation keys which are p.p_partkey and
s.s_suppkey. The aggregate is the min of cost. When building the group by, we
add a semijoin with part that comes from the probe side so that we do not
calculate the min cost for parts that will in any case not be probed.

The plan is:

```
partsupp t4*H
  (part t2
    Build
  )*H
  (supplier t3*H
    (nation t5*H
      (region t6
        Build
      )
      project 2 columns
      Build
    )
    project 7 columns
    Build
  )*H left
  (partsupp t8*H exists
    (part t2
      Build
    )*H
    (supplier t9*H
      (nation t10*H
        (region t11
          Build
        )
        project 1 columns
        Build
      )
      project 1 columns
      Build
    )
    PARTIAL agg
    FINAL agg
    project 1 columns
    Build
  )
  order by 4 columns
  project 8 columns
```

Note the exists with part in the subquery right of the left outer join. The
existence being more selective than the supplier join, we do the existence
first. This plan is likely the best or very close. The group by from the
subquery is smaller than partsupp, so doing this as a left outer join instead
of right hand is better.

### Q3

The query is straightforward to do by hash. We select 1/5 of customer and 1/2
of orders. We first join orders x customer, build on that and then probe on
lineitem. There is anti-correlation between the date filters on lineitem and
orders but that does not affect the best plan choice.

### Q4

The trick in q4 is using a right hand semijoin to do the exists. If we probed
with orders and built on lineitem, we would get a much larger build side. But
using the right hand semijoin, we get to build on the smaller side. If we had
shared key order between lineitem and orders we could look at other plans but
in the hash based plan space we have the best outcome.

### Q5

The filters are on region and order date. There is also a diamond between
supplier and customer, this being that they have the same nation.

We get the plan:

```
lineitem t4*H
  (supplier t5*H
    (nation t6*H
      (region t7
        Build
      )
      project 2 columns
      Build
    )
    project 4 columns
    Build
  )*H
  (orders t3
    Build
  )*H
  (customer t2
    Build
  )
  project 2 columns
  PARTIAL agg
  FINAL agg
  order by 1 columns
  project 2 columns
```

Lineitem is the driving table that is joined to 1/5 of supplier and then 1/7
of orders. Finally we join with customer on c_custkey and c_nationkey. The
build of customer could have been restricted on c_nationkey being in the range
of s_nationkey but we did not pick this restriction because this would have
gone through a single equality in a join edge of two equalities. The plan is
otherwise good and the extra reduction on customer ends up not being very
important. This is a possible enhancement for completeness.

### Q6

We have a single scan, so there are no query optimization choices here.

### Q7

The query has filters on lineitem ship date and on customer and supplier
nation. The trick is to understand the customer from France and supplier from
Germany or customer from Germany and supplier from France construct. Customers
not from France or Germany and suppliers not from France or Germany cannot be
part of the result. The condition is broken up and pushed down into the scans
of nation that are a reducing join against both customer and supplier. The
plan that we get:

```
lineitem t3*H
  (supplier t2*H
    (nation t6
      Build
    )
    project 2 columns
    Build
  )*H
  (orders t4*H
    (customer t5*H
      (nation t7
        Build
      )
      project 2 columns
      Build
    )
    project 2 columns
    Build
  )
  filter 1 exprs
  project 4 columns
  PARTIAL agg
  FINAL agg
  order by 3 columns
  project 4 columns
```

First joins with supplier because this is the smaller table and the reduction
is the same as the one with orders, i.e. 2/25 in both cases. The bigger table
is slower to probe so we reduce with the smaller one first.

### Q8

The query has filters on region, selecting 1/5 of supplier or customer and the
most selective filter on part, plus 2/7 selection on orders.

The join order of first joining with part and then orders joined with customer
makes sense, doing the more reducing join first. At the tail we have supplier
and the supplier's nation.

```
lineitem t4*H
  (part t2
    Build
  )*H
  (orders t5*H
    (customer t6*H
      (nation t7*H
        (region t9
          Build
        )
        project 1 columns
        Build
      )
      project 1 columns
      Build
    )
    project 2 columns
    Build
  )*H
  (supplier t3
    Build
  )*H
  (nation t8
    Build
  )
  project 3 columns
  PARTIAL agg
  FINAL agg
  order by 1 columns
  project 2 columns
```

### Q9

The plan is a natural join of lineitem, orders, part, partsupp, supplier and
nation. The only selection is 1/17 of part.

The outcome is actually quite ingenious. One would think we should begin with
lineitem x part. Instead we get:

```
orders t6*H
  (partsupp t5*H
    (lineitem t4*H
      (part t2
        Build
      )
      project 6 columns
      Build
    )
    project 7 columns
    Build
  )*H
  (supplier t3
    Build
  )*H
  (nation t7
    Build
  )
  project 3 columns
  PARTIAL agg
  FINAL agg
  order by 2 columns
  project 3 columns
```

We get a complicated build side that has 1/17 of lineitem as a build and
partsupp as a probe. Since partsupp is 1/8 of lineitem, building on the join
of lineitem and part builds on the smaller, as one should. Then we probe this
with orders, which is 1/4th of lineitem, thus the biggest table. The rest is
just 1:1 joins to supplier and nation.


### Q10

The join order is as expected, from largest to smallest. The trick question
here is that the grouping keys are functionally dependent on c_custkey, so do
not need to figure in the group by at all. This information is not known
because the schema does not have primary key information, so we cannot take
advantage of this.

```
lineitem t4*H
  (orders t3
    Build
  )*H
  (customer t2
    Build
  )*H
  (nation t5
    Build
  )
  project 8 columns
  PARTIAL agg
  FINAL agg
  order by 1 columns
  project 8 columns
```

### Q11

The join order is the usual, from large to small. The only particularity is
the non-correlated subquery that repeats the same join steps. An optimization
opportunity could be to reuse build sides but this is not something that Velox
plans support at this time. Also, practical need for this is not very high.

### Q12

In this query, we end up building on lineitem since the filters on it make it
the smaller of the two tables. Everything else is unsurprising.

### Q13

This query has only two possible plans, left and right hand outer join. We
correctly produce the right outer join, building on the left, i.e. customer,
as it is much smaller than orders.

### Q14

The only noteworthy aspect is that we build on lineitem since its filters
(1 month out of 7 years) make it smaller than part.

### Q15

The plan

```
lineitem t4
  project 2 columns
  PARTIAL agg
  FINAL agg
  project 2 columns
*H
  (lineitem t6
    project 2 columns
    PARTIAL agg
    FINAL agg
    project 1 columns
    PARTIAL agg
    FINAL agg
    project 1 columns
    Build
  )*H
  (supplier t2
    Build
  )
  order by 1 columns
  project 5 columns
```

We join with the aggregation on lineitem, which is the most selective join
available for the first lineitem, then we join with supplier.

### Q16

The join is biggest table first, with  part joined first because it is quite
selective, more so than the exists with supplier.

### Q17

The trick here is that we have a correlated subquery that flattens into a
group by that aggregates over all of lineitem. We correctly observe that only
lineitems with a very specific part will occur on the probe side, so we copy
the restriction inside the group by as a semijoin (exists).

```
lineitem t2*H
  (part t3
    Build
  )*H left
  (lineitem t5*H exists
    (part t3
      Build
    )
    PARTIAL agg
    FINAL agg
    project 2 columns
    Build
  )
  filter 1 exprs
  PARTIAL agg
  FINAL agg
  project 1 columns
```

### Q18

This query has exotic optimization possibilities that have to do with pushing
down the top k through order by. Velox does not have execution support for this
though.

The standard full scan + hash join model of executing will as usual probe with
the larger tables, producing the order lineitem, orders, customer. There is the
subquery with lineitem with an actually very selective having. The best join order
would be lineitem x subquery x orders x customer. We get

```
lineitem t4*H
  (orders t3
    Build
  )*H exists-flag
  (lineitem t6
    PARTIAL agg
    FINAL agg
    filter 1 exprs
    project 1 columns
    Build
  )
  filter 1 exprs
*H
  (customer t2
    Build
  )
  PARTIAL agg
  FINAL agg
  order by 2 columns
  project 6 columns
```

instead because the semijoin edge to the subquery is not translated via the equivalence
class of l_orderkey, o_orderkey. If this were an inner edge it would be. So, expanding
implied edges outside of just inner join edges would improve the plan by a little.

### Q19

The trick is to extract common pieces to push down into the scan of lineitem
and part from the OR of three ANDs in the single where clause. We extract the
join condition that is present in all three disjuncts of the or. Then we
extract an OR to push down into the scan of part and lineitem.  We build on
part, as it is the smaller table.

### Q20

This is one of the harder queries. The main filters are 1/25 of supplier
nations, 1/7 years of lineitem and ~1/20 of part.  The larger compute is the
subquery with lineitem that adds up the volume for part and supplier
combinations.

The subquery flattens into a left join with a group by derived table on the
right.

```
lineitem t7
  PARTIAL agg
  FINAL agg
  project 3 columns
*H right
  (partsupp t5*H exists-flag
    (part t11
      project 1 columns
      Build
    )
    filter 1 exprs
  *H exists
    (supplier t2*H
      (nation t3
        Build
      )
      project 1 columns
      Build
    )
    Build
  )
  filter 1 exprs
  project 1 columns
*H right exists-flag
  (supplier t2*H
    (nation t3
      Build
    )
    Build
  )
  filter 1 exprs
  order by 1 columns
  project 2 columns
```

We should have the exists with part inside the aggregation on lineitem. To do
this we need to consider existences as well as inner joins for when gathering
reducing joins.

### Q21

The key insight is that the exists and not exists will apply to only 1/50 or
so of lineitem. The worst plan would be to build hash tables on the contents of
the subqueries.

The plan we get builds on lineitem l1 joined to supplier joined to nation
joined to orders. This is probed in a right hand semijoin by the scan of
lineitem l2 and this is built and then probed by lineitem l3 in a right
semijoin. The right semijoin flag marks the build side rows that get probed
by the semijoined side and once the probe is complete, flags are produced for
all build side rows to indicate if they were hit.

The plan is  as expected.

```
lineitem t7
  project 2 columns
*H right exists-flag
  (lineitem t9
    project 2 columns
  *H right exists-flag
    (lineitem t3*H
      (supplier t2*H
        (nation t5
          Build
        )
        project 2 columns
        Build
      )*H
      (orders t4
        Build
      )
      Build
    )
    filter 1 exprs
    Build
  )
  filter 1 exprs
  PARTIAL agg
  FINAL agg
  order by 2 columns
  project 2 columns
```

### Q22

The query is straightforward, with the not exists resolved with a right
semijoin and the non-correlated subquery becoming a cross join to the one row
result set of the non-grouped aggregation.
