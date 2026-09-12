# Where Expressions Are Evaluated

An optimizer rarely evaluates an expression where the query wrote it. It may
compute an operator's compound arguments below that operator, push a filter
that reads one side of a join below the join, or compute the parts of a cross
join's filter that read one side in that side. This document states what such a
move preserves and what it does not. The moves named here are examples, not a
list of the ones an optimizer may make.

Two kinds of move need to be told apart, because only one of them is
constrained.

## Moving within the same rows

Computing an expression immediately below the operator that reads it does not
change the rows it is evaluated on: one value for each row the operator would
have seen anyway.

```sql
SELECT length(name), count(*) FROM t GROUP BY length(name)
```

```
Aggregate[keys: length(name)]      Aggregate[keys: p]
  Scan t                    =>       Project p := length(name)
                                       Scan t
```

Whether `length(name)` is evaluated by the aggregation or by a projection
directly beneath it, it is evaluated once per row of `t`. Nothing is at risk,
and a non-deterministic expression may move the same way: `rand()` as an
aggregate argument is evaluated once per input row either way.

## Moving across an operator

Moving an expression to the other side of an operator changes the rows it is
evaluated on, in either direction. Two examples:

- Pushing a filter that reads one side of a join below the join. Above the join
  it sees only the rows that survived; below it also sees the rows the join
  would have dropped.

  ```
  Filter[length(name) > 10]        Join[t.id = u.id]
    Join[t.id = u.id]         =>     Filter[length(name) > 10]
      Scan t                           Scan t
      Scan u                         Scan u
  ```

- Hoisting the parts of a cross join's filter that read one side into that
  side. The filter is evaluated once per pair of rows, the input once per row.

  ```
  Join[filter: threshold < length(name)]      Join[filter: threshold < p]
    Scan big                             =>     Scan big
    Scan small                                  Project p := length(name)
                                                  Scan small
  ```

Two things follow.

A non-deterministic expression cannot move, because the rows it is asked about
are not the rows it would have been asked about.

An expression that can fail may now fail on a row where it did not before.
Within a filter, `a <> 0 AND 1000 / a > x` does not fail, but does once
`1000 / a` is computed separately, since a filter discards an error from one
conjunct for a row that another rejects. Across a join:

```sql
SELECT * FROM t, u WHERE a >= b AND 1/a > 7
```

`1/a > 7` reads only `t`, so it can be evaluated there -- including on a row
with `a = 0`, which no pair satisfying `a >= b` contains. SQL does not define
an evaluation order within a filter, nor require that a failing row be kept out
of reach, so neither is a property the optimizer preserves. Both cases are
pinned in `optimizer/tests/sql/filterPushdown.sql` and
`optimizer/tests/sql/join.sql`.

## Arguments a special form may skip

`IF`, `CASE`, `COALESCE` and `TRY` decide at runtime which of their arguments to
evaluate, and only they can. Moving such an argument out makes it evaluate for
every row, which is not the same query:

```sql
IF(a = 1, true, fail('bad'))
```

Computing the third argument separately fails the query even where `a = 1`.
This holds for both kinds of move above, and unlike error masking it is not a
liberty the optimizer takes: an argument a special form may skip is never moved
out of it. Their first argument is evaluated for every row, so that one may
move, and any of these forms may move whole.
