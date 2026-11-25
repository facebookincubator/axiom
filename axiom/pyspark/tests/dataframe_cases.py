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

"""Runs DataFrame case files against Collagen and against local Spark.

A case file is a Python module with two functions:

    def setup(spark):
        return {"t": spark.createDataFrame([(1, "a")], ["a", "b"])}

    def cases(t):
        u = t.filter(t.a > 0)

        case(u.select("b"))

        # error: Unable to resolve reference: c
        case(t.select("c"))

setup() returns the DataFrames the cases use, by name. cases() takes them as
parameters of the same names, and the session as a parameter named 'spark'.
setup() holds the tables several cases share. A table that one case uses is
created in cases(), next to the case. Its body is a sequence of statements: each
case(...) call is a case, and other statements, such as assignments, run in
order. A case(...) call must be its own statement in the body of cases().

The runner executes the body one statement at a time, once with a Collagen
session and once with a local Spark session, and compares each case's result.
Each case runs in its own try block, so a case that fails, even while building
its DataFrame, does not stop the others.

Case files refer to functions as 'F', as in F.col("a"); the runner binds 'F'
to the functions of each session's kind.

A case file may set:
  - CATALOG and SCHEMA: where Collagen reads tables; "test" and "default" by
    default.
  - COMPARE_WITH_SPARK = False: the cases run only in Collagen, for data local
    Spark does not have, such as TPC-H. Every case then needs a '# count' or a
    '# error' annotation.

Comment lines directly above a case annotate it:
  - '# error: <message>': Collagen fails with an error containing <message>.
  - '# ordered': row order is part of the result.
  - '# count N': Collagen returns N rows; their values are not compared.
  - '# gap: <ID>': Collagen shows gap <ID> from axiom/spark_connect/docs/Gaps.md.
    For a GF gap, Collagen fails where Spark runs; for a GC gap, Collagen runs
    and returns a different answer than Spark. The case fails once Collagen
    matches Spark, so a fix also updates the case and Gaps.md.
Other comment lines are prose.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
from types import ModuleType
from typing import Any, TypeGuard

from pyspark.sql import DataFrame, functions, Row, SparkSession
from pyspark.sql.connect import functions as connect_functions
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession


def case(df: DataFrame) -> None:
    """Marks a case in cases(). The runner reads it from the source."""


@dataclasses.dataclass
class Annotations:
    error: str | None = None
    ordered: bool = False
    count: int | None = None
    gap: str | None = None


@dataclasses.dataclass
class Outcome:
    """The result of one case in one session: columns and rows, or an error."""

    columns: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    rows: list[Any] = dataclasses.field(default_factory=list)
    error: str | None = None


@dataclasses.dataclass
class Case:
    line: int
    annotations: Annotations
    collagen: Outcome
    # None if the file does not compare with Spark.
    spark: Outcome | None


def _is_case_call(node: ast.AST) -> TypeGuard[ast.Call]:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "case"
    )


def _is_case(statement: ast.stmt) -> bool:
    return isinstance(statement, ast.Expr) and _is_case_call(statement.value)


def _cases_function(tree: ast.Module, file: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "cases":
            for statement in node.body:
                if _is_case(statement):
                    continue
                for inner in ast.walk(statement):
                    if _is_case_call(inner):
                        raise ValueError(
                            f"{file}:{inner.lineno}: case() must be its own "
                            "statement in the body of cases()"
                        )
            return node
    raise ValueError(f"{file}: missing cases()")


def _annotations(lines: list[str], line: int, file: str) -> Annotations:
    annotations = Annotations()
    index = line - 2
    while index >= 0 and lines[index].strip().startswith("#"):
        text = lines[index].strip().lstrip("#").strip()
        if text.startswith("error:"):
            annotations.error = text[len("error:") :].strip()
        elif text == "ordered":
            annotations.ordered = True
        elif text.startswith("count "):
            annotations.count = int(text[len("count ") :])
        elif text.startswith("gap:"):
            annotations.gap = text[len("gap:") :].strip()
        index -= 1
    return annotations


def _to_python(value: Any) -> Any:
    if isinstance(value, Row):
        return tuple(_to_python(v) for v in value)
    if isinstance(value, list):
        return [_to_python(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_python(v) for k, v in value.items()}
    return value


def _evaluate(expression: ast.expr, namespace: dict[str, Any], file: str) -> Outcome:
    try:
        df = eval(compile(ast.Expression(expression), file, "eval"), namespace)
        columns = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        rows = [_to_python(row) for row in df.collect()]
        return Outcome(columns=columns, rows=rows)
    except Exception as e:
        return Outcome(error=str(e))


def _run(
    module: ModuleType, function: ast.FunctionDef, spark: SparkSession, file: str
) -> dict[int, Outcome]:
    tables = module.setup(spark)
    namespace = dict(vars(module))
    # A classic and a Spark Connect session each need columns of their own
    # kind, so 'F' refers to the functions of the session's kind.
    namespace["F"] = (
        connect_functions if isinstance(spark, ConnectSparkSession) else functions
    )
    for arg in function.args.args:
        namespace[arg.arg] = spark if arg.arg == "spark" else tables[arg.arg]

    outcomes = {}
    for statement in function.body:
        if _is_case(statement):
            assert isinstance(statement, ast.Expr)
            call = statement.value
            assert isinstance(call, ast.Call)
            outcomes[statement.lineno] = _evaluate(call.args[0], namespace, file)
        else:
            exec(
                compile(ast.Module(body=[statement], type_ignores=[]), file, "exec"),
                namespace,
            )
    return outcomes


def catalog(module: ModuleType) -> tuple[str, str]:
    """Returns the catalog and schema the cases in 'module' read from."""
    return (getattr(module, "CATALOG", "test"), getattr(module, "SCHEMA", "default"))


def compares_with_spark(module: ModuleType) -> bool:
    return getattr(module, "COMPARE_WITH_SPARK", True)


def run_cases(
    module: ModuleType, collagen: SparkSession, spark: SparkSession | None
) -> list[Case]:
    """Runs the cases in 'module' with Collagen, and with 'spark' unless it is
    None."""
    source = inspect.getsource(module)
    file = inspect.getsourcefile(module) or module.__name__
    lines = source.splitlines()
    function = _cases_function(ast.parse(source), file)

    collagen_outcomes = _run(module, function, collagen, file)
    spark_outcomes = _run(module, function, spark, file) if spark else None
    return [
        Case(
            line=line,
            annotations=_annotations(lines, line, file),
            collagen=collagen_outcomes[line],
            spark=spark_outcomes[line] if spark_outcomes else None,
        )
        for line in sorted(collagen_outcomes)
    ]
