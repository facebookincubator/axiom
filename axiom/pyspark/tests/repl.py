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

"""Runs DataFrame expressions in Collagen or in local Spark.

Starts a session of the engine --engine names and binds the tables of a case
file's setup() in it, with 'df' naming one of them. For Collagen, it starts a
server for the case file's catalog. Each line is a Python statement. An
expression that yields a DataFrame prints its columns and rows; a line that
starts with a DataFrame method applies it to 'df':

    > select(F.col("arr").cast("string"))
    > t = df.filter(df.id > 1)
    > t.select("m")
"""

import argparse
import ast
import importlib
import os
import re
import sys
from typing import Any, TypeGuard

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from pyspark.sql import DataFrame

from . import cases as case_files
from .collagen_test_base import CollagenServer, use_test_time_zone
from .dataframe_cases import (
    catalog,
    evaluate,
    get_local_spark,
    make_namespace,
    Outcome,
    stop_local_spark,
)

_MAX_ROWS = 20


def _is_dataframe(value: Any) -> bool:
    # Spark Connect has a DataFrame class of its own.
    return type(value).__name__ == "DataFrame"


def _with_df(line: str, namespace: dict[str, Any]) -> str:
    name = re.match(r"[A-Za-z_]\w*", line)
    if name and name.group() not in namespace and hasattr(DataFrame, name.group()):
        return "df." + line
    return line


def _yields_dataframe(statement: ast.stmt) -> TypeGuard[ast.Expr]:
    # A bare expression, except a call to an action such as show() that prints
    # and returns nothing.
    if not isinstance(statement, ast.Expr):
        return False
    call = statement.value
    return not (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr in ("show", "printSchema", "explain")
    )


def _error_message(error: str) -> str:
    # A Spark Connect error carries the server's message in its details.
    details = re.search(r'details = "(.*)"', error)
    return details.group(1) if details else error


def _print_outcome(outcome: Outcome) -> None:
    if outcome.error is not None:
        print(f"error: {_error_message(outcome.error)}")
        return
    print(", ".join(f"{name} {type}" for name, type in outcome.columns))
    for row in outcome.rows[:_MAX_ROWS]:
        print(f"  {row}")
    if len(outcome.rows) > _MAX_ROWS:
        print(f"  ... {len(outcome.rows) - _MAX_ROWS} more rows")


def _run_line(line: str, namespace: dict[str, Any]) -> None:
    line = line.strip()
    if not line:
        return
    try:
        tree = ast.parse(_with_df(line, namespace))
    except SyntaxError as e:
        print(f"error: {e}")
        return

    if len(tree.body) == 1 and _yields_dataframe(tree.body[0]):
        _print_outcome(evaluate(tree.body[0].value, namespace, "<repl>", None))
        return

    try:
        exec(compile(tree, "<repl>", "exec"), namespace)
    except Exception as e:
        print(f"error: {_error_message(str(e))}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--engine",
        choices=["collagen", "spark"],
        default="collagen",
        help="Engine to run the lines in.",
    )
    parser.add_argument(
        "--cases",
        default="nested_types",
        help="Case file whose catalog and tables to use.",
    )
    parser.add_argument(
        "--df", default="mixed", help="Table of the case file that 'df' names."
    )
    parser.add_argument(
        "-c",
        "--command",
        action="append",
        help="Line to run instead of reading lines interactively. Repeatable.",
    )
    args = parser.parse_args()
    use_test_time_zone()

    module = importlib.import_module(f"{case_files.__name__}.{args.cases}")
    server = None
    if args.engine == "collagen":
        catalog_name, schema = catalog(module)
        server = CollagenServer(
            {"catalog": catalog_name, "schema": schema, "runner_id": "local"}
        )
    try:
        spark = server.spark if server else get_local_spark()
        namespace = make_namespace(module, spark)
        namespace["df"] = namespace[args.df]

        if args.command:
            for line in args.command:
                print(f"> {line}")
                _run_line(line, namespace)
            return 0

        tables = [name for name, value in namespace.items() if _is_dataframe(value)]
        print(f"Tables: {', '.join(sorted(tables))}. 'df' is {args.df}.")
        session = PromptSession(
            history=FileHistory(os.path.expanduser("~/.axiom_spark_repl_history"))
        )
        while True:
            try:
                line = session.prompt("> ")
            except KeyboardInterrupt:
                continue
            except EOFError:
                return 0
            _run_line(line, namespace)
    finally:
        if server:
            server.stop()
        stop_local_spark()


if __name__ == "__main__":
    sys.exit(main())
