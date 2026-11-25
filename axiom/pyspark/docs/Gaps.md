# Collagen gaps

Collagen runs PySpark DataFrame programs on Axiom. This document records where
its results differ from Spark's: a query that fails in Collagen but runs in
Spark, or returns a different answer.

When a gap is fixed, remove its entry and update any test that checks the gap
to check Spark's result instead. Such tests cite the gap's ID. IDs are not
reused.

To reproduce a gap, start the Collagen server against the test catalog and
connect a PySpark client with
`SparkSession.builder.remote("sc://localhost:<port>")`.

## Gaps

A GC gap returns a different answer than Spark. A GF gap fails where Spark
runs. A new gap takes the next number.

| ID | Gap |
| --- | --- |
| GC1 | `countDistinct` and `sumDistinct` count every row |
| GC2 | `*` is supported only in `count(*)` |
| GC3 | Implicit type coercion follows Presto's rules |
| GC4 | Aggregate and window functions are a mix of Spark's and Presto's |
| GF5 | `exists()` with a lambda fails |
| GF6 | `createDataFrame` with a DDL schema string fails |
| GF7 | Sort and filter on a column a projection dropped fail |
| GF8 | `explode_outer` and `posexplode_outer` fail |
| GF9 | A join on USING columns fails |
| GF10 | Writes support only append and error-if-exists |

## Details

### GC1: `countDistinct` and `sumDistinct` count every row

```python
df.groupBy("k").agg(F.countDistinct("v"))
```

Collagen returns `count(v)`, the number of non-NULL values with duplicates.
Spark returns the number of distinct values. Collagen drops the distinct flag of
an aggregate call.

### GC2: `*` is supported only in `count(*)`

```python
df.select("*")
```

Collagen translates `*` as a TRUE constant, so this returns one boolean column.
Spark returns all of the input's columns.

### GC3: Implicit type coercion follows Presto's rules

Collagen coerces function arguments and set operation inputs with Presto's
rules. Spark's rules differ in places, so a call Spark coerces may fail in
Collagen, or the other way around. Velox has no Spark coercer.

### GC4: Aggregate and window functions are a mix of Spark's and Presto's

Velox's Spark SQL package lacks many aggregate and window functions, such as
`count`, `bool_or`, `lead` and `lag`. Collagen registers Velox's Presto
aggregate and window functions, then the Spark ones over them, as Gluten does.

Where Spark and Presto share a name, Collagen runs Spark's function. Elsewhere
it runs Presto's, with Presto's semantics where they differ from Spark's. A
Presto-only name, such as `approx_distinct`, also resolves, and a Spark name
missing from both packages, such as `approx_count_distinct`, fails.

### GF5: `exists()` with a lambda fails

```python
df.select(F.exists(df.arr, lambda x: x > 25))
```

Collagen fails with `Function name cannot match special form name: exists`.
Spark returns whether any element satisfies the lambda.

Velox registers Spark's function under the name `exists`, and the Axiom logical
plan reserves that name for the EXISTS special form.

### GF6: `createDataFrame` with a DDL schema string fails

```python
spark.createDataFrame([(1, "x")], "a BIGINT, b STRING")
```

Collagen fails to parse the schema: it reads only the JSON form of a local
relation's schema. A `StructType` schema or a list of column names works.

### GF7: Sort and filter on a column a projection dropped fail

```python
df.select("id").orderBy(df["amount"])
```

Collagen fails with `Column is not available in the current input: amount`.
Spark carries `amount` through the projection for the sort, or a filter, and
drops it after. A select of the dropped column fails in both.

### GF8: `explode_outer` and `posexplode_outer` fail

```python
df.select(F.explode_outer("arr"))
```

Collagen fails as not implemented. Spark returns a NULL row for an empty or
NULL array.

### GF9: A join on USING columns fails

```python
df1.join(df2, "id")
```

Collagen fails as not implemented. A join condition, such as
`df1.id == df2.id`, works.

### GF10: Writes support only append and error-if-exists

```python
df.write.mode("overwrite").saveAsTable("t")
```

Collagen fails as not implemented for:

- the overwrite and ignore modes;
- writes to a path;
- schema-qualified table names.
