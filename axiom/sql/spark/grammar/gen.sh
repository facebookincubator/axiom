#!/bin/bash
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

# Regenerates the C++ parser from SqlBaseLexer.g4 and SqlBaseParser.g4 and
# copies it next to the grammar. Run from anywhere in fbcode after changing
# the grammar.

set -euo pipefail

GRAMMAR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FBCODE_DIR="$(cd "$GRAMMAR_DIR/../../../.." && pwd)"

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

cd "$FBCODE_DIR"
buck build //axiom/sql/spark/grammar:gen_grammar_rewrite --out "$out/srcs"
python3 axiom/sql/add_license_headers.py "$out/srcs"
cp "$out"/srcs/* "$GRAMMAR_DIR"/
arc f "$GRAMMAR_DIR"/SqlBase*.h "$GRAMMAR_DIR"/SqlBase*.cpp
echo "Regenerated the parser in $GRAMMAR_DIR"
