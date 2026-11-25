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

# How a column reference binds to a column: col("x") by name in the current
# input, df["x"] through the DataFrame that produced the column, and
# col("t.x") through a DataFrame alias.

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "orders": spark.createDataFrame(
            [(1, 10, 5.0), (2, 20, 7.0)], ["id", "customer_id", "amount"]
        ),
        "customers": spark.createDataFrame([(10, "a"), (20, "b")], ["id", "name"]),
    }


def cases(orders: DataFrame, customers: DataFrame) -> None:
    joined = orders.join(customers, orders.customer_id == customers.id)

    case(orders.select(F.col("amount")))

    case(orders.join(customers, orders.customer_id == customers.id, "left"))

    # error: Reference is ambiguous: id
    case(joined.select(F.col("id")))

    # error: Unable to resolve reference: total
    case(orders.select(F.col("total")))

    # df["x"] names the column 'df' output as 'x' while the column passes
    # through unchanged.
    case(joined.select(orders.id, customers.id, customers.name))

    big = orders.filter(orders.amount > 1)
    case(big.join(customers, big.customer_id == customers.id).select(orders.id))

    case(orders.select("id", "amount").filter(orders.amount > 6).select(orders.id))

    case(orders.groupBy("customer_id").count().select(orders.customer_id))

    ids = orders.select("id").union(customers.select("id"))
    case(ids.select(orders.id))

    # df["x"] fails once the column it names is no longer in the input. A
    # rename, an alias or a recomputation produces a new column.
    # error: Column is not available in the current input: amount
    case(orders.withColumnRenamed("amount", "total").select(orders.amount))

    # error: Unable to resolve reference: total
    case(orders.withColumnRenamed("amount", "total").select(orders["total"]))

    # error: Column is not available in the current input: amount
    case(orders.select(orders.amount.alias("total")).select(orders.amount))

    # error: Column is not available in the current input: amount
    case(orders.withColumn("amount", orders.amount * 2).select(orders.amount))

    # error: Column is not available in the current input: amount
    case(orders.select("id").select(orders.amount))

    # error: Column is not available in the current input: amount
    case(orders.groupBy("customer_id").count().select(orders.amount))

    # error: Column is not available in the current input: id
    case(ids.select(customers.id))

    # A DataFrame on both sides of a join has the same plan ID on both.
    # error: Reference is ambiguous: id
    case(orders.join(orders, orders.id == orders.customer_id))

    o = orders.alias("o")
    case(
        o.join(customers, F.col("o.customer_id") == customers.id).select(
            F.col("o.id"), orders.id
        )
    )

    # error: Unable to find Spark Connect reference: t
    case(orders.select(F.col("t.id")))
