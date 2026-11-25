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

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "left": spark.createDataFrame([("a",), ("a",), ("a",), ("b",)], ["x"]),
        "right": spark.createDataFrame([("a",), ("a",), ("c",)], ["x"]),
        "ab": spark.createDataFrame([(1, "x")], ["a", "b"]),
        "ba": spark.createDataFrame([("y", 2)], ["b", "a"]),
        "bc": spark.createDataFrame([("z", 3.0)], ["b", "c"]),
        "people": spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"]),
        "nulls": spark.createDataFrame(
            [("Alice", 25), ("Bob", None), (None, 30)],
            StructType(
                [StructField("name", StringType()), StructField("age", IntegerType())]
            ),
        ),
        "mixed": spark.createDataFrame(
            [(1, 10.5, True, "a"), (2, 20.3, False, "b"), (3, 30.7, True, "a")],
            ["id", "value", "active", "group"],
        ),
        "empty": spark.createDataFrame(
            [],
            StructType(
                [StructField("name", StringType()), StructField("age", IntegerType())]
            ),
        ),
    }


def cases(
    left: DataFrame,
    right: DataFrame,
    ab: DataFrame,
    ba: DataFrame,
    bc: DataFrame,
    people: DataFrame,
    empty: DataFrame,
    nulls: DataFrame,
    mixed: DataFrame,
) -> None:
    case(left.union(right))

    case(left.unionAll(right))

    case(nulls.union(nulls))

    case(mixed.union(mixed.filter(mixed.active)))

    case(
        mixed.filter(mixed.group == "a")
        .select("id", "value")
        .union(mixed.filter(mixed.id > 1).select("id", "value"))
    )

    counts = mixed.groupBy("group").count()
    case(counts.union(counts))

    case(left.intersect(right))

    case(left.intersectAll(right))

    case(left.subtract(right))

    case(left.exceptAll(right))

    # unionByName matches columns by name.
    case(ab.unionByName(ba))

    # With allowMissingColumns, a column one input lacks reads as NULL there.
    # pyrefly: ignore[unexpected-keyword]
    case(ab.unionByName(bc, allowMissingColumns=True))

    # error: Column of the left input is missing from the right input: a
    case(ab.unionByName(bc))

    # The union widens INTEGER to BIGINT.
    case(people.union(empty))
