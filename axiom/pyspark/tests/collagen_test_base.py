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

from __future__ import annotations

import importlib.resources
import subprocess
import tempfile
import time
import unittest

import psutil
from pyspark.sql.connect.session import SparkSession

COLLAGEN_SERVER = "collagen_server"


def detect_server_port(pid: int, timeout: float = 60.0) -> int:
    start = time.time()
    while time.time() - start < timeout:
        try:
            proc = psutil.Process(pid)
            for conn in proc.net_connections(kind="tcp"):
                if conn.status == psutil.CONN_LISTEN:
                    return conn.laddr.port
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        time.sleep(0.1)
    raise TimeoutError(f"Process {pid} did not start listening within {timeout}s")


class CollagenServer:
    """A Collagen server process with a Spark Connect session to it. The server
    reads from the catalog and schema in 'options'."""

    def __init__(self, options: dict[str, str | int]) -> None:
        server_args = [f"--{k}={v}" for k, v in {**options, "port": 0}.items()]
        server_path = importlib.resources.files(__package__).joinpath(COLLAGEN_SERVER)
        # The server's output goes to a file, not a pipe: a pipe nobody reads
        # until the server stops fills up and blocks the server mid-request.
        self._output = tempfile.TemporaryFile()
        self._process: subprocess.Popen[bytes] = subprocess.Popen(
            [str(server_path)] + server_args,
            stdout=self._output,
            stderr=subprocess.STDOUT,
        )
        try:
            port = detect_server_port(self._process.pid)
        except TimeoutError as e:
            raise TimeoutError(f"{e}. Output: {self.stop()}") from e
        self.spark: SparkSession = SparkSession.builder.remote(
            f"sc://localhost:{port}"
        ).getOrCreate()

    def stop(self) -> str:
        """Stops the server and returns its output."""
        if hasattr(self, "spark"):
            self.spark.stop()
        self._process.terminate()
        self._process.wait()
        self._output.seek(0)
        output = self._output.read().decode("utf-8")
        self._output.close()
        return output


class CollagenTest(unittest.TestCase):
    """Collagen integration test super class."""

    def options(self) -> dict[str, str | int]:
        """Specify the options supplied to CollagenServer for the test class."""
        return {
            "catalog": "test",
            "schema": "default",
            "runner_id": "local",
        }

    def setUp(self) -> None:
        self._server = CollagenServer(self.options())
        self._spark: SparkSession = self._server.spark

    def tearDown(self) -> None:
        output = self._server.stop()
        if output:
            print(f"\n=== Collagen Server stderr ===\n{output}")

    def plan_json(self, df):
        """Return the Velox physical plan as a parsed JSON dict."""
        import json

        full_output = df._explain_string(extended=True)
        marker = "== Velox Plan JSON =="
        if marker not in full_output:
            self.fail("No Velox plan JSON in explain output")
        json_str = full_output.rsplit(marker, 1)[1].strip()
        return json.loads(json_str)
