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


# Column names and types: every type a local relation can have, and the
# columns each operation outputs.

import datetime

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "all_types": spark.createDataFrame(
            [
                (
                    1,
                    1,
                    1,
                    1,
                    0.5,
                    0.9,
                    True,
                    "col",
                    bytearray(b"raw"),
                    datetime.date(2025, 1, 1),
                    ["my", "array"],
                    {"k1": 1, "k2": 2},
                    {"k1": [1, 2, 3]},
                    (1, 2),
                )
            ],
            StructType(
                [
                    StructField("c1", ByteType()),
                    StructField("c2", ShortType()),
                    StructField("c3", IntegerType()),
                    StructField("c4", LongType()),
                    StructField("c5", FloatType()),
                    StructField("c6", DoubleType()),
                    StructField("c7", BooleanType()),
                    StructField("c8", StringType()),
                    StructField("c9", BinaryType()),
                    StructField("c11", DateType()),
                    StructField("c12", ArrayType(StringType())),
                    StructField("c13", MapType(StringType(), IntegerType())),
                    StructField("c14", MapType(StringType(), ArrayType(IntegerType()))),
                    StructField(
                        "c15",
                        StructType(
                            [
                                StructField("child1", ByteType()),
                                StructField("child2", ShortType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        "timestamps": spark.createDataFrame(
            [(datetime.datetime(2025, 1, 1, 16, 0, 0),)],
            StructType([StructField("t", TimestampType())]),
        ),
        "inferred": spark.createDataFrame([Row(id=1, name="Alice", age=30.5)]),
        "people": spark.createDataFrame(
            [("Alice", 25, "Engineer"), ("Bob", 30, "Manager")],
            ["name", "age", "job"],
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
        "viewers": spark.createDataFrame([(1,), (2,)], ["viewer_rid"]),
        "features": spark.createDataFrame([(1,), (3,)], ["primary_rid"]),
    }


def cases(
    all_types: DataFrame,
    timestamps: DataFrame,
    inferred: DataFrame,
    people: DataFrame,
    sales: DataFrame,
    viewers: DataFrame,
    features: DataFrame,
) -> None:
    case(all_types)

    case(all_types.select(all_types.c12[0].alias("a")))

    case(all_types.select(all_types.c13["k2"].alias("b")))

    # GF14 in axiom/spark_connect/docs/Gaps.md.
    # error: Expected 1 children for imported format '+l'
    case(all_types.select(all_types.c14["k1"].alias("c")))

    case(
        all_types.select(
            all_types.c15["child1"].alias("d"), all_types.c15.child2.alias("e")
        )
    )

    # GC13 in axiom/spark_connect/docs/Gaps.md.
    # count 1
    case(timestamps)

    case(inferred)

    case(
        people.select(
            F.lit(1).alias("a"),
            F.lit(0.99).alias("b"),
            F.lit("bla").alias("c"),
            F.lit(False).alias("d"),
            F.lit(bytearray(b"raw")).alias("e"),
            F.lit(datetime.date(2025, 1, 1)).alias("f"),
        )
    )

    case(people.select(people.age, "name"))

    # Aliases name the columns, which may repeat.
    case(people.select(people.age.alias("a"), (people.age + 1).alias("a")))

    case(
        people.select(
            (people.age + people.age * 2).alias("a"), F.upper(people.name).alias("b")
        )
    )

    # Local Spark 3.4 has no offset, so the case checks the row count.
    # count 1
    # pyrefly: ignore[not-callable]
    case(people.orderBy("age").offset(1).limit(10))

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

    case(viewers.join(features, viewers.viewer_rid == features.primary_rid, "left"))

    v = viewers.alias("v")
    f = features.alias("f")
    case(v.join(f, F.col("v.viewer_rid") == F.col("f.primary_rid")))

    # A set operation takes its column names from its left input.
    case(viewers.union(features))

    case(
        people.withColumnRenamed("name", "first_name").withColumnRenamed(
            "first_name", "full_name"
        )
    )

    case(people.select("name", "age").withColumnRenamed("name", "person"))

    case(people.withColumn("active", F.lit(True)))

    # withColumn replaces a column of the same name.
    case(people.withColumn("age", F.lit("x")))

    case(people.withColumn("doubled", F.col("age") * 2).withColumn("flag", F.lit(1)))
