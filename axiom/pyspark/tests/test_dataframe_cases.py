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

"""Runs the case files in cases/ against Collagen and local Spark. See
dataframe_cases.py for the file format."""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
import tempfile
import unittest
from types import ModuleType

from parameterized import parameterized
from pyspark.sql import SparkSession

from . import cases as case_files
from .collagen_test_base import CollagenTest
from .dataframe_cases import Case, catalog, compares_with_spark, run_cases


def _case_file_names() -> list[str]:
    return [info.name for info in pkgutil.iter_modules(case_files.__path__)]


def _case_file(name: str) -> ModuleType:
    return importlib.import_module(f"{case_files.__name__}.{name}")


_local_spark: SparkSession | None = None


def _get_local_spark() -> SparkSession:
    global _local_spark
    if _local_spark is None:
        # Spark's Python workers must run the same interpreter, with the same
        # modules, as this test.
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)
        _local_spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate()
        )
    return _local_spark


def _check(test: unittest.TestCase, case: Case) -> None:
    annotations = case.annotations
    collagen = case.collagen
    spark = case.spark

    if annotations.error is not None:
        assert collagen.error is not None, "Collagen did not fail"
        test.assertIn(annotations.error, collagen.error)
        return

    test.assertIsNone(collagen.error, f"Collagen failed: {collagen.error}")
    if annotations.count is not None:
        test.assertEqual(len(collagen.rows), annotations.count)
        return

    test.assertIsNotNone(spark, "A case not compared with Spark needs '# count'")
    assert spark is not None
    test.assertIsNone(spark.error, f"Spark failed: {spark.error}")
    test.assertEqual(collagen.columns, spark.columns)
    if annotations.ordered:
        test.assertEqual(collagen.rows, spark.rows)
    else:
        test.assertEqual(sorted(collagen.rows, key=repr), sorted(spark.rows, key=repr))


class DataFrameCasesTest(CollagenTest):
    """Runs each case file as its own test, with a Collagen server that reads
    from the file's catalog."""

    def options(self) -> dict[str, str | int]:
        name = self._testMethodName.removeprefix("test_")
        catalog_name, schema = catalog(_case_file(name))
        return {"catalog": catalog_name, "schema": schema, "runner_id": "local"}

    @parameterized.expand(
        _case_file_names(), name_func=lambda func, num, param: f"test_{param.args[0]}"
    )
    def test(self, name: str) -> None:
        module = _case_file(name)
        spark = _get_local_spark() if compares_with_spark(module) else None
        for case in run_cases(module, self._spark, spark):
            with self.subTest(f"{name}.py:{case.line}"):
                _check(self, case)


def tearDownModule() -> None:
    if _local_spark is not None:
        _local_spark.stop()
