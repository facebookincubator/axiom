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


# Array, map and struct functions, including lambdas, and explode.

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
        "arrays": spark.createDataFrame(
            [
                (1, [1, 2, 3, 4, 5], [10, 20, 30, 40, 50]),
                (2, [10, 20, 30], [1, 2, 3]),
                (3, [100], [50]),
                (4, [], []),
            ],
            StructType(
                [
                    StructField("id", LongType(), False),
                    StructField("a", ArrayType(IntegerType())),
                    StructField("b", ArrayType(IntegerType())),
                ]
            ),
        ),
        "maps": spark.createDataFrame(
            [
                (1, {"math": 80, "science": 45}, {"math": 10, "science": 20}),
                (2, {"art": 30, "music": 95}, {"art": 100, "music": 200}),
            ],
            StructType(
                [
                    StructField("id", LongType(), False),
                    StructField("m", MapType(StringType(), LongType())),
                    StructField("n", MapType(StringType(), LongType())),
                ]
            ),
        ),
        "mixed": spark.createDataFrame([(1, [10, 20], {"k": 5})], ["id", "arr", "m"]),
    }


def cases(
    arrays: DataFrame,
    maps: DataFrame,
    mixed: DataFrame,
    spark: SparkSession,
) -> None:
    a = arrays

    case(a.select("id", F.size("a").alias("size")))

    case(a.select("id", F.slice("a", 2, 3).alias("slice")))

    case(a.select("id", F.concat("a", "b").alias("concat")))

    case(
        a.select(
            "id",
            F.element_at("a", 1).alias("first"),
            F.element_at("a", 2).alias("second"),
        )
    )

    case(a.select("id", F.transform("a", lambda x: x * 2).alias("doubled")))

    case(a.select("id", F.filter("a", lambda x: x > 10).alias("large")))

    case(a.select("id", F.zip_with("a", "b", lambda x, y: x + y).alias("sum")))

    case(a.select("id", F.forall("a", lambda x: x % 2 == 0).alias("all_even")))

    # gap: GF5
    case(a.select("id", F.exists("a", lambda x: x > 25).alias("has_large")))

    case(maps.select("id", maps.m["math"].alias("math")))

    case(maps.select("id", F.transform_values("m", lambda k, v: v * 2).alias("m")))

    case(maps.select("id", F.transform_keys("m", lambda k, v: F.upper(k)).alias("m")))

    case(maps.select("id", F.map_filter("m", lambda k, v: v >= 50).alias("m")))

    case(
        maps.select(
            "id", F.map_zip_with("m", "n", lambda k, v1, v2: v1 + v2).alias("m")
        )
    )

    traits = spark.createDataFrame(
        [(1, {7: [1, 2, 3, 4, 5, 6]}), (2, {7: [10, 20, 30]})],
        StructType(
            [
                StructField("id", LongType(), False),
                StructField("t", MapType(LongType(), ArrayType(LongType()))),
            ]
        ),
    )

    case(
        traits.select(
            "id", F.transform_values("t", lambda k, v: F.slice(v, 2, 3)).alias("t")
        )
    )

    structs = spark.createDataFrame(
        [((1, "x"),), ((2, "y"),)],
        StructType(
            [
                StructField(
                    "s",
                    StructType(
                        [
                            StructField("a", IntegerType()),
                            StructField("b", StringType()),
                        ]
                    ),
                )
            ]
        ),
    )

    case(structs.filter(structs.s["a"] > 1).select(structs.s.b.alias("b")))

    # error: Struct field not found: c
    case(structs.select(structs.s["c"]))

    # explode names its output 'col', or 'key' and 'value', unless aliased.
    # posexplode also emits the 0-based position, before the element.
    case(mixed.select("id", F.explode("arr")))

    case(mixed.select(F.explode("arr").alias("x")))

    case(mixed.select(F.explode("m")))

    case(mixed.select(F.explode("m").alias("k", "v")))

    # gap: GC11
    case(mixed.select(F.posexplode("arr")))

    # gap: GF8
    case(mixed.select(F.explode_outer("arr")))

    # error: Only one generator allowed per select clause
    case(mixed.select(F.explode("arr"), F.explode("m")))

    # gap: GC16
    case(mixed.select(F.explode("arr"), "id"))

    # gap: GF19
    case(mixed.select("id", F.col("arr").cast("string").alias("s")))

    # gap: GF19
    case(mixed.select(F.col("m").cast("string").alias("s")))
