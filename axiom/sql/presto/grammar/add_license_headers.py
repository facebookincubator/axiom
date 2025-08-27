#!/usr/bin/env python3
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
"""
Script to add Apache license headers to generated ANTLR files.
"""

import sys
from pathlib import Path

LICENSE_HEADER = """/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"""


def add_license_header(file_path):
    """Add license header to a file if it doesn't already have one."""
    with open(file_path, "r") as f:
        content = f.read()

    # Check if license header already exists
    if "Copyright (c) Meta Platforms, Inc. and its affiliates." in content:
        print(f"License header already exists in {file_path}")
        return

    # Add license header before the first line
    with open(file_path, "w") as f:
        f.write(LICENSE_HEADER + content)

    print(f"Added license header to {file_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 add_license_headers.py <directory>")
        sys.exit(1)

    directory = Path(sys.argv[1])

    # Process all .h and .cpp files
    for pattern in ["*.h", "*.cpp"]:
        for file_path in directory.glob(pattern):
            if file_path.name.startswith("PrestoSql"):
                add_license_header(file_path)


if __name__ == "__main__":
    main()
