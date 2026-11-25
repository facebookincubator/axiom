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


# withColumnsRenamed, withColumns and toDF.

import datetime

from axiom.pyspark.tests.dataframe_cases import case
from pyspark.sql import DataFrame, functions as F, SparkSession


def setup(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "employees": spark.createDataFrame(
            [
                (1, "Alice", 75000.0, 25, True, datetime.date(2023, 1, 15)),
                (2, "Bob", 85000.0, 30, False, datetime.date(2022, 6, 20)),
            ],
            ["id", "name", "salary", "age", "active", "hire_date"],
        ),
    }


def cases(employees: DataFrame) -> None:
    e = employees

    case(e.withColumnsRenamed({"name": "employee_name"}))

    case(
        e.withColumnsRenamed(
            {
                "id": "employee_id",
                "name": "employee_name",
                "salary": "annual_salary",
                "age": "employee_age",
                "active": "is_active",
                "hire_date": "start_date",
            }
        )
    )

    case(e.withColumnsRenamed({}))

    case(
        e.withColumnsRenamed({"name": "first_name"}).withColumnsRenamed(
            {"first_name": "employee_name", "age": "employee_age"}
        )
    )

    case(
        e.select("name", "salary")
        .withColumnsRenamed({"name": "employee_name", "salary": "annual_salary"})
        .select("employee_name", "annual_salary")
    )

    # withColumns replaces a column of the same name in place and appends the
    # others in order.
    case(e.withColumns({"c": F.lit(42), "age": e.age * 10}))

    # toDF renames the columns by position.
    case(e.select("id", "name").toDF("x", "y"))

    case(
        e.withColumnRenamed("name", "first_name").withColumnRenamed(
            "first_name", "full_name"
        )
    )

    case(e.select("name", "age").withColumnRenamed("name", "person"))

    case(e.withColumn("bonus", F.lit(True)))

    # withColumn replaces a column of the same name, whatever its type.
    case(e.withColumn("age", F.lit("x")))

    case(e.withColumn("doubled", F.col("age") * 2).withColumn("flag", F.lit(1)))

    case(e.select("id", "name").toDF("name", "id"))

    case(e.withColumns({"c": F.lit(1), "b": F.lit(2), "a": F.lit(3)}))
