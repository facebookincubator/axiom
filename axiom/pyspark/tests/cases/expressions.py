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
from pyspark.sql import DataFrame, functions as F, SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "people": spark.createDataFrame(
            [
                ("alice johnson", "alice@example.com"),
                ("bob smith", "BOB@EXAMPLE.COM"),
                ("carol brown", "carol.brown@test.org"),
            ],
            ["name", "email"],
        ),
        "numbers": spark.createDataFrame(
            [
                (1, 10, 20, 30),
                (2, None, 20, 30),
                (3, None, None, 30),
                (4, 10, None, 30),
            ],
            StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("a", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("c", IntegerType()),
                ]
            ),
        ),
        "strings": spark.createDataFrame(
            [(1, "100"), (2, "-5"), (3, "0")], ["id", "s"]
        ),
    }


def cases(people: DataFrame, numbers: DataFrame, strings: DataFrame) -> None:
    case(
        people.select(
            F.upper(people.name).alias("upper"),
            F.lower(people.email).alias("lower"),
            F.length(people.name).alias("length"),
            F.substring(people.email, 1, 5).alias("prefix"),
        )
    )

    case(people.select(F.split(people.name, " ").alias("parts")))

    case(people.select(F.split(people.name, " ", 100).alias("parts")))

    case(numbers.select("id", F.coalesce("a", "b", "c").alias("first")))

    case(numbers.filter(numbers.a.isNull()))

    case(numbers.filter(numbers.a.isNotNull()))

    case(numbers.select("id", F.isnull("b").alias("b_is_null")))

    case(strings.select("id", strings.s.cast("int").alias("i")))

    n = strings.select("id", strings.s.cast("int").alias("n"))

    case(
        n.select(
            "id", F.when(n.n > 0, "positive").otherwise("non-positive").alias("sign")
        )
    )

    case(n.select("id", F.when(n.n > 0, 1).otherwise(0).alias("is_positive")))
