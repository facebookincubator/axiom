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


from .collagen_test_base import CollagenTest


class ExplainTest(CollagenTest):
    def test_basic(self) -> None:
        """Sanity check: explain output includes the Velox plan section."""
        df = self._spark.createDataFrame([(1, "a")], ["id", "name"])
        self.assertIn(
            "== Velox Physical Plan (MultiFragmentPlan) ==",
            df._explain_string(extended=True),
        )
