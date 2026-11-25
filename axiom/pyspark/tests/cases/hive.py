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


# Tables that setup() writes to the catalog and reads back.

import uuid

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession

CATALOG = "local-hive"


def _write(spark: SparkSession, rows: list[tuple], columns: list[str]) -> str:
    name = f"t_{uuid.uuid4().hex[:8]}"
    spark.createDataFrame(rows, columns).write.mode("error").saveAsTable(name)
    return name


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    people = _write(
        spark,
        [(1, "John", "Doe", 30), (2, "Jane", "Smith", 25), (3, "Carol", "Brown", 41)],
        ["id", "first_name", "last_name", "age"],
    )
    sales = _write(
        spark,
        [("A", 10), ("A", 20), ("B", 30), ("B", 40), ("C", 50)],
        ["category", "amount"],
    )
    numbers = _write(spark, [(i, i * 10) for i in range(100)], ["id", "value"])

    appended = _write(spark, [(1, "first")], ["id", "value"])
    spark.createDataFrame([(2, "second")], ["id", "value"]).write.mode(
        "append"
    ).saveAsTable(appended)

    return {
        name: spark.read.table(table)
        for name, table in [
            ("people", people),
            ("sales", sales),
            ("numbers", numbers),
            ("appended", appended),
        ]
    }


def cases(
    people: DataFrame, sales: DataFrame, numbers: DataFrame, appended: DataFrame
) -> None:
    case(people)

    case(people.select("first_name", "age"))

    case(numbers.filter(F.col("value") > 950))

    case(sales.groupBy("category").agg(F.sum("amount").alias("total")))

    # count 10
    case(numbers.limit(10))

    case(appended)
