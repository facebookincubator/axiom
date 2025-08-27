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

set -euo pipefail

# Script to regenerate ANTLR files for Presto SQL grammar
# This script automates the process documented in README.md

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
  echo -e "${BLUE}==>${NC} $1"
}

print_success() {
  echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
  echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
  echo -e "${RED}✗${NC} $1"
}

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GRAMMAR_DIR="$SCRIPT_DIR"

# Find fbcode directory (should be 4 levels up from this script)
FBCODE_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

# Verify we're in the right location
if [[ ! -f "$FBCODE_DIR/.buckconfig" ]] && [[ ! -f "$FBCODE_DIR/BUCK" ]]; then
  print_error "Could not find fbcode directory. Expected it at: $FBCODE_DIR"
  print_error "Please run this script from axiom/sql/presto/grammar/ directory"
  exit 1
fi

if [[ ! -f "$GRAMMAR_DIR/PrestoSql.g4" ]]; then
  print_error "Could not find PrestoSql.g4 grammar file in: $GRAMMAR_DIR"
  print_error "Please run this script from axiom/sql/presto/grammar/ directory"
  exit 1
fi

echo "🔄 Regenerating ANTLR files for Presto SQL grammar"
echo ""
print_step "Working directory: $FBCODE_DIR"
print_step "Grammar directory: $GRAMMAR_DIR"
echo ""

# Step 1: Navigate to fbcode directory
print_step "Step 1: Changing to fbcode directory"
cd "$FBCODE_DIR"
print_success "Now in: $(pwd)"

# Step 2: Build the ANTLR grammar
print_step "Step 2: Building ANTLR grammar with Buck"
if buck build //axiom/sql/presto/grammar:gen_grammar --show-output; then
  print_success "ANTLR grammar built successfully"
else
  print_error "Failed to build ANTLR grammar"
  exit 1
fi

# Step 3: Find the Buck output directory
print_step "Step 3: Finding generated files in Buck output"
BUCK_OUT_DIR=$(find ../buck-out -path "*gen_grammar_rewrite*" -name "srcs" -type d -printf "%T@ %p\n" 2>/dev/null | sort -r -n -k1,1 | head -1 | awk '{print $2}')

if [[ -z $BUCK_OUT_DIR ]]; then
  print_error "Could not find Buck output directory with generated files"
  print_error "Expected to find a directory matching: ../buck-out/*gen_grammar_rewrite*/srcs"
  exit 1
fi

print_success "Found generated files in: $BUCK_OUT_DIR"

# Verify the directory contains the expected files
EXPECTED_FILES=("PrestoSqlLexer.h" "PrestoSqlLexer.cpp" "PrestoSqlParser.h" "PrestoSqlParser.cpp")
for file in "${EXPECTED_FILES[@]}"; do
  if [[ ! -f "$BUCK_OUT_DIR/$file" ]]; then
    print_error "Missing expected file: $BUCK_OUT_DIR/$file"
    exit 1
  fi
done

print_success "All expected files found in Buck output directory"

# Step 4: Apply license headers
print_step "Step 4: Applying license headers to generated files"
if python3 axiom/sql/presto/grammar/add_license_headers.py "$BUCK_OUT_DIR"; then
  print_success "License headers applied successfully"
else
  print_error "Failed to apply license headers"
  exit 1
fi

# Step 5: Copy files to source directory
print_step "Step 5: Copying corrected files to source directory"
if cp "$BUCK_OUT_DIR"/* axiom/sql/presto/grammar/; then
  print_success "Files copied to source directory"
else
  print_error "Failed to copy files to source directory"
  exit 1
fi

# Step 6: Fix coding style with arc lint
print_step "Step 6: Fixing coding style with arc lint"
cd axiom/sql/presto/grammar/
if arc lint -a; then
  print_success "Coding style fixed"
else
  print_warning "arc lint reported some issues (this may be normal)"
fi

# Step 7: Verify the build still works
print_step "Step 7: Verifying the build works with new files"
cd "$FBCODE_DIR"
if buck build //axiom/sql/presto/grammar:grammar; then
  print_success "Grammar target builds successfully"
else
  print_error "Build verification failed - there may be issues with the generated files"
  exit 1
fi

echo ""
echo "🎉 ANTLR file regeneration completed successfully!"
echo ""
print_success "Generated files have been updated in: $GRAMMAR_DIR"
print_step "The following files were regenerated:"

# List the regenerated files
GENERATED_FILES=(
  "PrestoSqlLexer.h" "PrestoSqlLexer.cpp"
  "PrestoSqlParser.h" "PrestoSqlParser.cpp"
  "PrestoSqlBaseListener.h" "PrestoSqlBaseListener.cpp"
  "PrestoSqlListener.h" "PrestoSqlListener.cpp"
  "PrestoSqlBaseVisitor.h" "PrestoSqlBaseVisitor.cpp"
  "PrestoSqlVisitor.h" "PrestoSqlVisitor.cpp"
)

for file in "${GENERATED_FILES[@]}"; do
  if [[ -f "$GRAMMAR_DIR/$file" ]]; then
    echo "  ✓ $file"
  fi
done

echo ""
print_step "Next steps:"
echo "  1. Review the generated files for any unexpected changes"
echo "  2. Run tests to ensure everything still works:"
echo "     buck test //axiom/sql/presto/grammar/..."
echo "  3. Commit the changes if everything looks good"
