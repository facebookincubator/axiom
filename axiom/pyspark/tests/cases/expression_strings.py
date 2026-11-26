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


# SQL text in expressions: F.expr, DataFrame.selectExpr and filter with a
# string.

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "people": spark.createDataFrame(
            [("Alice", 25), ("Bob", 31), ("Anna", 40)],
            StructType(
                [
                    StructField("name", StringType()),
                    StructField("age", LongType()),
                ]
            ),
        ),
        "nested": spark.createDataFrame(
            [
                (1, [1, 2, 3], {"k": 5}, ("x", 10)),
                (2, [10], {"k": 7}, ("y", 20)),
            ],
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("a", ArrayType(IntegerType())),
                    StructField("m", MapType(StringType(), LongType())),
                    StructField(
                        "s",
                        StructType(
                            [
                                StructField("f", StringType()),
                                StructField("g", LongType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    }


def cases(people: DataFrame, nested: DataFrame) -> None:
    case(people.selectExpr("name", "age + 1 AS next_age"))

    case(people.filter("age > 26 AND name LIKE 'A%'"))

    case(people.select(F.expr("upper(name)").alias("upper")))

    case(
        people.selectExpr(
            "CASE WHEN age > 30 THEN 'old' ELSE 'young' END AS bucket",
            "age BETWEEN 30 AND 35 AS thirties",
            "age IN (25, 40) AS listed",
        )
    )

    case(people.groupBy().agg(F.expr("count(*) AS n"), F.expr("sum(age) AS total")))

    case(people.alias("t").selectExpr("t.name"))

    case(
        people.selectExpr(
            "1.5 AS d",
            "2e1 AS e",
            "10L AS l",
            "'it''s' AS s",
            "DATE '2025-01-01' AS dt",
            "cast(age AS decimal(10, 2)) AS dec",
        )
    )

    case(
        nested.selectExpr(
            "id",
            "transform(a, x -> x + 1) AS plus_one",
            "a[0] AS first",
            "m['k'] AS k",
            "s.f",
        )
    )

    case(nested.alias("t").selectExpr("t.s.g"))

    case(nested.select(nested["s.f"], F.col("s.g")))

    # A lambda parameter hides a column of the same name, and only that one.
    case(
        people.alias("t").selectExpr(
            "transform(array(1), age -> age + t.age) AS r",
            "transform(array(1), name -> name) AS n",
        )
    )

    case(nested.selectExpr("transform(array(s), x -> x.f) AS r"))

    case(people.selectExpr("TIMESTAMP '2025-01-01 16:00:00' AS ts"))

    # error: Unable to resolve reference: missing
    case(people.selectExpr("missing"))

    # gap: GF20
    case(people.selectExpr("CASE age WHEN 25 THEN 'young' ELSE 'other' END"))

    # error: Syntax error at line 1, position 6
    case(people.selectExpr("age +"))
