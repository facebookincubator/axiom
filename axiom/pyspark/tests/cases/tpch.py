# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Queries over the TPC-H tables at scale factor 0.01. Local Spark has no TPC-H
# data, so the cases check row counts in Collagen.

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession

CATALOG = "tpch"
SCHEMA = "tiny"
COMPARE_WITH_SPARK = False

TABLES = [
    "nation",
    "region",
    "customer",
    "orders",
    "lineitem",
    "supplier",
    "part",
    "partsupp",
]


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {name: spark.read.table(name) for name in TABLES}


def cases(
    nation: DataFrame,
    region: DataFrame,
    customer: DataFrame,
    orders: DataFrame,
    lineitem: DataFrame,
    supplier: DataFrame,
    part: DataFrame,
    partsupp: DataFrame,
) -> None:
    # count 25
    case(nation)

    # count 5
    case(region)

    # count 1500
    case(customer)

    # count 15000
    case(orders)

    # count 60175
    case(lineitem)

    # count 100
    case(supplier)

    # count 2000
    case(part)

    # count 8000
    case(partsupp)

    # count 5
    case(nation.filter(nation.n_regionkey == 1))

    # count 1
    case(nation.filter(nation.n_name == "UNITED STATES"))

    # count 7304
    case(orders.filter(orders.o_orderstatus == "F"))

    # count 18
    case(
        customer.filter(
            (customer.c_nationkey == 1) & (customer.c_mktsegment == "BUILDING")
        )
    )

    # count 14637
    case(orders.filter((orders.o_orderstatus == "F") | (orders.o_orderstatus == "O")))

    # count 5
    case(nation.groupBy("n_regionkey").count())

    # count 5
    case(customer.groupBy("c_mktsegment").count())

    # count 3
    case(orders.groupBy("o_orderstatus").agg(F.sum("o_totalprice").alias("total")))

    # count 3
    case(
        lineitem.groupBy("l_returnflag").agg(
            F.count("*").alias("cnt"),
            F.sum("l_quantity").alias("sum_qty"),
            F.avg("l_extendedprice").alias("avg_price"),
        )
    )

    # count 5
    case(nation.limit(5))

    # count 100
    case(orders.limit(100))

    # count 10
    case(lineitem.limit(10))

    # count 50
    case(orders.filter(orders.o_orderstatus == "F").limit(50))

    # count 25
    case(nation.select("n_nationkey", "n_name"))

    # count 10
    case(
        lineitem.select(
            lineitem.l_orderkey,
            lineitem.l_quantity,
            (lineitem.l_extendedprice * lineitem.l_discount).alias("discount_amount"),
        ).limit(10)
    )

    # count 25
    case(nation.join(region, nation.n_regionkey == region.r_regionkey))

    # count 100
    case(customer.join(nation, customer.c_nationkey == nation.n_nationkey).limit(100))

    # count 100
    case(orders.join(customer, orders.o_custkey == customer.c_custkey).limit(100))
