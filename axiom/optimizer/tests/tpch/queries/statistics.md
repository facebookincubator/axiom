# TPC-H Statistics (companion to `schemas`)

Per-column statistics (`SHOW STATS FOR <table>`) and measured cardinalities, to
ground cardinality / join-order reasoning about TPC-H plans. **TPC-H data has no
NULLs** (`nulls_fraction` = 0 for every column), so it is omitted below.

## Scale and foreign-key integrity

Row counts scale with the scale factor: region and nation are fixed; every other
table is its base size × SF (see Table sizes). In standard TPC-H, foreign keys are
valid by construction — every fact-table key references an existing dimension row,
so a fact-to-dimension join on a key is non-reducing (it only filters out rows the
other predicates already removed).

**sf0.1 violates this — but as a generator bug, not a property of TPC-H.** Velox's
TPC-H generator clamps the scale factor to 1 when computing value ranges for any
SF<1, while still emitting the reduced row counts. So at sf0.1 the fact tables
carry keys spanning the **SF=1** domain (`l_partkey` → 200,000, `l_suppkey` →
10,000) while the dimensions are sized for SF=0.1 (part 20,000, supplier 1,000).
~90% of `lineitem`/`partsupp` part/supplier keys then reference non-existent rows,
and those joins **reduce the fact table ~10×**. Only `part` and `supplier` are
affected; `orders`/`customer`/`nation` keys stay valid. See
https://github.com/facebookincubator/velox/issues/16399. Measured survival:

| join | survives | of | ≈ |
|---|---|---|---|
| lineitem-part | 59,765 | 600,572 | 10% |
| lineitem-supplier | 60,154 | 600,572 | 10% |
| partsupp-supplier | 8,000 | 80,000 | 10% |
| lineitem-orders | 600,572 | 600,572 | 100% |
| orders-customer | 14,882 | 15,000 | ~99% |
| partsupp-part, supplier-nation, customer-nation | — | — | 100% |

The bug only mis-fires for SF<1 (the clamp is a no-op at SF≥1), so at sf1 and sf10
all FKs are valid (verified: at sf1, lineitem-part and lineitem-supplier both
survive 6,001,215 / 6,001,215).

**Consequence — plans on sf0.1 data can differ from plans on correct data.** With
the bug, the part/supplier joins are ~10× reducers, so the cost-optimal join order
on sf0.1 can differ from the order on FK-valid data (SF≥1, or sf0.1 once the bug is
fixed). A plan validated against the current sf0.1 data is not necessarily the plan
for correct TPC-H, so plan-shape expectations must state which data they assume.
Any join-order reasoning that hinges on the part/supplier reduction (e.g. q9) is a
property of the bug, not of TPC-H.

## Table sizes

Standard TPC-H scaling (region/nation are scale-independent; the rest are
base × SF). Measured spot-checks match.

| table | sf0.1 | sf1 | sf10 |
|---|---|---|---|
| region | 5 | 5 | 5 |
| nation | 25 | 25 | 25 |
| supplier | 1,000 | 10,000 | 100,000 |
| customer | 15,000 | 150,000 | 1,500,000 |
| part | 20,000 | 200,000 | 2,000,000 |
| partsupp | 80,000 | 800,000 | 8,000,000 |
| orders | 150,000 | 1,500,000 | 15,000,000 |
| lineitem | 600,572 | 6,001,215 | ~60,000,000 |

## Per-column statistics (sf0.1)

String `low`/`high` are truncated. For other scales, NDV of key/dimension columns
scales with the table; `low`/`high` ranges are stable except where the sf0.1 FK
mismatch applies (e.g. `l_partkey`/`l_suppkey` high values already span the SF=1
domain at sf0.1).

### region — 5 rows
| column | ndv | low | high |
|---|---|---|---|
| r_regionkey | 5 | 0 | 4 |
| r_name | 5 | AFRICA | MIDDLE EAST |

### nation — 25 rows
| column | ndv | low | high |
|---|---|---|---|
| n_nationkey | 25 | 0 | 24 |
| n_name | 25 | ALGERIA | VIETNAM |
| n_regionkey | 5 | 0 | 4 |

### supplier — 1,000 rows
| column | ndv | low | high |
|---|---|---|---|
| s_suppkey | 1000 | 1 | 1000 |
| s_nationkey | 25 | 0 | 24 |
| s_acctbal | 999 | -966.2 | 9993.46 |

### customer — 15,000 rows
| column | ndv | low | high |
|---|---|---|---|
| c_custkey | 15000 | 1 | 15000 |
| c_nationkey | 25 | 0 | 24 |
| c_acctbal | 14985 | -999.95 | 9999.72 |
| c_mktsegment | 5 | AUTOMOBILE | MACHINERY |

### part — 20,000 rows
| column | ndv | low | high |
|---|---|---|---|
| p_partkey | 20000 | 1 | 20000 |
| p_mfgr | 5 | Manufacturer#1 | Manufacturer#5 |
| p_brand | 25 | Brand#11 | Brand#55 |
| p_type | 1000 | ECONOMY ANODIZED … | STANDARD POLISHED … |
| p_size | 50 | 1 | 50 |
| p_container | 40 | JUMBO BAG | WRAP PKG |
| p_retailprice | 7993 | 901 | 1918.99 |

### partsupp — 80,000 rows
| column | ndv | low | high |
|---|---|---|---|
| ps_partkey | 20000 | 1 | 20000 |
| ps_suppkey | 9749 | 1 | 10000 |
| ps_availqty | 9995 | 1 | 9999 |
| ps_supplycost | 76346 | 1.01 | 999.99 |

### orders — 150,000 rows
| column | ndv | low | high |
|---|---|---|---|
| o_orderkey | 150000 | 1 | 600000 |
| o_custkey | 138196 | 1 | 149999 |
| o_orderstatus | 3 | F | P |
| o_totalprice | 149943 | 917.35 | 496620.48 |
| o_orderdate | 2405 | 1992-01-01 | 1998-08-02 |
| o_orderpriority | 5 | 1-URGENT | 5-LOW |
| o_shippriority | 1 | 0 | 0 |

### lineitem — 600,572 rows
| column | ndv | low | high |
|---|---|---|---|
| l_orderkey | 149788 | 1 | 600000 |
| l_partkey | 199998 | 1 | 200000 |
| l_suppkey | 9999 | 1 | 10000 |
| l_linenumber | 7 | 1 | 7 |
| l_quantity | 50 | 1 | 50 |
| l_extendedprice | 579906 | 901 | 104899.5 |
| l_discount | 11 | 0 | 0.1 |
| l_tax | 9 | 0 | 0.08 |
| l_returnflag | 3 | A | R |
| l_linestatus | 2 | F | O |
| l_shipdate | 2521 | 1992-01-03 | 1998-12-01 |
| l_commitdate | 2460 | 1992-01-31 | 1998-10-31 |
| l_receiptdate | 2542 | 1992-01-04 | 1998-12-27 |
| l_shipinstruct | 4 | COLLECT COD | TAKE BACK RETURN |
| l_shipmode | 7 | AIR | TRUCK |

## Hard-to-estimate filters

Predicates whose selectivity is **impossible to estimate from per-column stats
alone** — it needs history or sampling — so the optimizer falls back to a default,
making the query's plan **estimation-sensitive**. This is not a bug in the
estimator: nothing in the column stats (ndv, min/max) tells you how many `p_name`
values contain `green`. In q9 the `like` default (~0.8 × table) over-estimates
`p_name like '%green%'` at ≈16,000 vs the true ~1,075, so the optimizer joins
`supplier` before the more selective `part` (a suboptimal order). See the q9
analysis in
[../../../v2/docs/TpchV1V2PlanComparison.md](../../../v2/docs/TpchV1V2PlanComparison.md).

Each row gives the filter's true selectivity and an **estimable substitute** of
similar selectivity — a range over a uniform column (or a key range) the optimizer
estimates accurately. Swapping the substitute in (as `q9_alt.sql` does) isolates a
query's plan from the estimation gap and locks the optimal join order. Scan-filter
selectivities are scale-invariant, so a substitute chosen here holds at every scale.

| query | hard predicate | true selectivity | estimable substitute (~selectivity) |
|---|---|---|---|
| q2 | `p_type like '%BRASS'` | 4,017 / 20,000 = 20% | `p_size <= 10` (20%) |
| q9 | `p_name like '%green%'` | 1,075 / 20,000 = 5.4% | `p_size <= 3` (6%) |
| q13 | `o_comment not like '%special%requests%'` | 148,318 / 150,000 = 99% | `o_orderkey < 594000` (99%) |
| q14 | `p_type like 'PROMO%'` | 3,309 / 20,000 = 17% | `p_size <= 8` (16%) |
| q16 | `p_type not like 'MEDIUM POLISHED%'` | 19,358 / 20,000 = 97% | `p_size <= 48` (96%) |
| q16 | `s_comment like '%Customer%Complaints%'` | 1 / 1,000 = 0.1% | `s_suppkey <= 1` (0.1%) |
| q20 | `p_name like 'forest%'` | 190 / 20,000 = 0.95% | `p_partkey <= 200` (1%) |
| q4 | `l_commitdate < l_receiptdate` | 379,809 / 600,572 = 63% | `l_quantity <= 32` (64%) |
| q12 | `l_commitdate < l_receiptdate AND l_shipdate < l_commitdate` | 72,009 / 600,572 = 12% | `l_quantity <= 6` (12%) |
| q21 | `l_receiptdate > l_commitdate` | 379,809 / 600,572 = 63% | `l_quantity <= 32` (64%) |
| q22 | `substring(c_phone, 1, 2) in (...)` | 4,115 / 15,000 = 27% | `c_nationkey <= 6` (28%) |

The hard predicates are substring/prefix `like` (no per-column stat applies),
column-to-column compares (need cross-column correlation), and expressions over a
column. The substitutes use `p_size`, `l_quantity`, and `c_nationkey` (uniform, so
`<= k` is exactly k/ndv) or dense key ranges (`p_partkey`, `s_suppkey`, `o_orderkey`).
