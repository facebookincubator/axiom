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


# The text DataFrame.show() prints: alignment, truncation, nulls, escapes,
# binary and full-width characters, the footer, and the vertical form.

import datetime

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession
from pyspark.sql.types import (
    BinaryType,
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
        "ids": spark.createDataFrame(
            [
                ("8f3a2c1e9b7d4e6a5f0c", 2417),
                ("1c9e7b3a5d2f8e4c6a0b", 983),
                ("a7d4e2c9b1f6e3a8d5c0x", 5120),
                ("short", None),
            ],
            ["crawl_id", "text_length"],
        ),
    }


def cases(ids: DataFrame, spark: SparkSession) -> None:
    # A value longer than 20 characters is cut to 17 and "...".
    # show 3
    case(ids)

    # show
    case(ids)

    # show truncate=0
    case(ids)

    # show truncate=2
    case(ids)

    # show 1
    case(ids)

    # show 2 vertical
    case(ids)

    # show 0
    case(ids)

    # The largest row count shows every row.
    # show 2147483647
    case(ids)

    types = spark.createDataFrame(
        [
            (
                1.5,
                True,
                datetime.date(2025, 1, 1),
                "a\tb\nc",
                bytearray(b"hel"),
                "日本語",
            )
        ],
        StructType(
            [
                StructField("d", DoubleType()),
                StructField("b", BooleanType()),
                StructField("dt", DateType()),
                StructField("escaped", StringType()),
                StructField("bin", BinaryType()),
                StructField("wide", StringType()),
            ]
        ),
    )

    # show
    case(types)

    # show truncate=0
    case(types)

    empty = spark.createDataFrame([], StructType([StructField("a", IntegerType())]))

    # show
    case(empty)

    # show vertical
    case(empty)

    # gap: GF19
    # show
    case(ids.select(F.array("text_length").alias("arr")))
