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


# Every type a local relation can have, and reading the fields of nested types.

import datetime

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, Row, SparkSession
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
    }


def cases(
    all_types: DataFrame,
    spark: SparkSession,
) -> None:
    case(all_types)

    case(all_types.select(all_types.c12[0].alias("a")))

    case(all_types.select(all_types.c13["k2"].alias("b")))

    # gap: GF14
    case(all_types.select(all_types.c14["k1"].alias("c")))

    case(
        all_types.select(
            all_types.c15["child1"].alias("d"), all_types.c15.child2.alias("e")
        )
    )

    # gap: GC13
    case(
        spark.createDataFrame(
            [(datetime.datetime(2025, 1, 1, 16, 0, 0),)],
            StructType([StructField("t", TimestampType())]),
        )
    )

    # Types inferred from Python values.
    case(spark.createDataFrame([Row(id=1, name="Alice", age=30.5)]))
