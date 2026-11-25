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
    }


def cases(employees: DataFrame) -> None:
    e = employees

    case(e.filter(e.department == "Engineering"))

    case(e.filter(e.salary > 55000))

    case(e.filter((e.department == "Engineering") & (e.age < 30)))

    case(e.filter((e.age > 30) | (e.department == "Sales")))

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

    case(
        e.select(
            e.name,
            (e.salary * 1.1).alias("salary_with_bonus"),
            F.upper(e.department).alias("dept_upper"),
        )
    )

    # ordered
    case(e.orderBy("age"))

    # ordered
    case(e.orderBy(F.col("salary").desc()))

    # ordered
    case(e.orderBy("department", F.col("salary").desc()))
