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

import datetime

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "employees": spark.createDataFrame(
            [
                ("Alice", 25, 50000.0, "Engineering"),
                ("Bob", 35, 60000.0, "Marketing"),
                ("Carol", 28, 55000.0, "Engineering"),
                ("David", 32, 70000.0, "Sales"),
                ("Eve", 26, 52000.0, "Engineering"),
            ],
            ["name", "age", "salary", "department"],
        ),
        "sales": spark.createDataFrame(
            [
                ("A", "A1", 10, 100.5, True, datetime.date(2024, 1, 1)),
                ("A", "A2", 20, 50.0, False, datetime.date(2024, 1, 2)),
            ],
            StructType(
                [
                    StructField("category", StringType()),
                    StructField("subcategory", StringType()),
                    StructField("value", IntegerType()),
                    StructField("amount", DoubleType()),
                    StructField("active", BooleanType()),
                    StructField("date", DateType()),
                ]
            ),
        ),
    }


def cases(employees: DataFrame, sales: DataFrame, spark: SparkSession) -> None:
    e = employees

    case(e.groupBy("department").count())

    case(
        e.groupBy("department").agg(
            F.count("name").alias("employee_count"),
            F.avg("salary").alias("avg_salary"),
            F.max("age").alias("max_age"),
            F.min("age").alias("min_age"),
            F.sum("salary").alias("total_salary"),
        )
    )

    case(sales.groupBy("category", "active").count())

    case(
        sales.groupBy("date").agg(
            F.count("*").alias("total_count"),
            F.sum("value").alias("sum_value"),
            F.sum("amount").alias("sum_amount"),
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("date").alias("min_date"),
        )
    )

    case(sales.agg(F.count("*").alias("n"), F.max("amount").alias("m")))

    scores = spark.createDataFrame(
        [("a", 1), ("a", 1), ("a", 2), ("b", 3), ("b", 3), ("b", None)], ["k", "v"]
    )

    case(
        scores.groupBy("k").agg(
            F.countDistinct("v").alias("count_distinct"),
            F.sumDistinct("v").alias("sum_distinct"),
            F.count("v").alias("count"),
        )
    )

    case(scores.agg(F.countDistinct("k").alias("n")))
