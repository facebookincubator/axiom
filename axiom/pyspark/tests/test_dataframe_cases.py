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
import pkgutil
import unittest
from types import ModuleType

from parameterized import parameterized

from . import cases as case_files
from .collagen_test_base import CollagenTest
from .dataframe_cases import (
    Case,
    catalog,
    comparable_rows,
    compares_with_spark,
    get_local_spark,
    matches,
    Outcome,
    run_cases,
    stop_local_spark,
)


def _case_file_names() -> list[str]:
    return [info.name for info in pkgutil.iter_modules(case_files.__path__)]


def _case_file(name: str) -> ModuleType:
    return importlib.import_module(f"{case_files.__name__}.{name}")


def _check_gap(
    test: unittest.TestCase,
    gap: str,
    collagen: Outcome,
    spark: Outcome,
    ordered: bool,
) -> None:
    """Checks that Collagen still shows the kind of gap Gaps.md records: a GF
    gap fails where Spark runs, a GC gap returns a different answer."""
    if gap.startswith("GF"):
        test.assertIsNotNone(
            collagen.error,
            f"{gap} records a failure, but Collagen ran: if it matches Spark, "
            "remove the '# gap' annotation and the entry in Gaps.md",
        )
        return

    test.assertIsNone(
        collagen.error, f"{gap} records a different answer, but Collagen failed"
    )
    test.assertFalse(
        matches(collagen, spark, ordered),
        f"{gap} is fixed: remove the '# gap' annotation and the entry in "
        "axiom/spark_connect/docs/Gaps.md",
    )


def _check(test: unittest.TestCase, case: Case) -> None:
    annotations = case.annotations
    collagen = case.collagen
    spark = case.spark

    if annotations.error is not None:
        assert collagen.error is not None, "Collagen did not fail"
        test.assertIn(annotations.error, collagen.error)
        return

    if annotations.count is not None:
        test.assertIsNone(collagen.error, f"Collagen failed: {collagen.error}")
        test.assertEqual(len(collagen.rows), annotations.count)
        return

    assert spark is not None, "A case not compared with Spark needs '# count'"
    test.assertIsNone(spark.error, f"Spark failed: {spark.error}")
    if annotations.gap is not None:
        _check_gap(test, annotations.gap, collagen, spark, annotations.ordered)
        return

    test.assertIsNone(collagen.error, f"Collagen failed: {collagen.error}")
    test.assertEqual(collagen.columns, spark.columns)
    test.assertEqual(
        comparable_rows(collagen, annotations.ordered),
        comparable_rows(spark, annotations.ordered),
    )


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
        spark = get_local_spark() if compares_with_spark(module) else None
        for case in run_cases(module, self._spark, spark):
            with self.subTest(f"{name}.py:{case.line}"):
                _check(self, case)


def tearDownModule() -> None:
    stop_local_spark()
