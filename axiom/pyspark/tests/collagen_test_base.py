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


class CollagenTest(unittest.TestCase):
    """Collagen integration test super class."""

    def options(self) -> dict[str, str | int]:
        """Specify the options supplied to CollagenServer for the test class."""
        return {
            "catalog": "test",
            "schema": "default",
            "runner_id": "local",
        }

    def setUp(self):
        opts = self.options()
        opts["port"] = 0
        server_args = [f"--{k}={v}" for k, v in opts.items()]

        server_path = importlib.resources.files(__package__).joinpath(COLLAGEN_SERVER)
        # The server's output goes to a file, not a pipe: a pipe nobody reads
        # until tearDown fills up and blocks the server mid-request.
        self._collagen_server_output = tempfile.TemporaryFile()
        self._collagen_server_proc: subprocess.Popen[bytes] = subprocess.Popen(
            [server_path] + server_args,
            stdout=self._collagen_server_output,
            stderr=subprocess.STDOUT,
        )

        try:
            self._collagen_port: int = detect_server_port(
                self._collagen_server_proc.pid
            )
        except TimeoutError as e:
            self._collagen_server_proc.terminate()
            self._collagen_server_proc.wait()
            self.fail(f"{e}. stderr: {self._read_server_output()}")

        self._spark: SparkSession = SparkSession.builder.remote(
            f"sc://localhost:{self._collagen_port}"
        ).getOrCreate()

    def tearDown(self) -> None:
        self._spark.stop()

        self._collagen_server_proc.terminate()
        self._collagen_server_proc.wait()
        output = self._read_server_output()
        if output:
            print(f"\n=== Collagen Server stderr ===\n{output}")

    def _read_server_output(self) -> str:
        self._collagen_server_output.seek(0)
        output = self._collagen_server_output.read().decode("utf-8")
        self._collagen_server_output.close()
        return output

    def plan_json(self, df):
        """Return the Velox physical plan as a parsed JSON dict."""
        import json

        full_output = df._explain_string(extended=True)
        marker = "== Velox Plan JSON =="
        if marker not in full_output:
            self.fail("No Velox plan JSON in explain output")
        json_str = full_output.rsplit(marker, 1)[1].strip()
        return json.loads(json_str)
