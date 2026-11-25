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


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {}


def cases(spark: SparkSession) -> None:
    case(spark.range(3))

    case(spark.range(2, 5))

    case(spark.range(0, 10, 2))

    case(spark.range(10, 0, -2))

    case(spark.range(0, 100, 25))

    # GF12 in axiom/spark_connect/docs/Gaps.md.
    # error: Tried reading schema message, was null or length 0
    case(spark.range(5, 5))

    case(spark.range(1, 2))

    case(spark.range(10).filter(F.col("id") > 5))

    case(spark.range(3).select((F.col("id") * 2).alias("doubled")))
