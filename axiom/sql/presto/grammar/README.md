<!--
Copyright (c) Meta Platforms, Inc. and its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Presto SQL Parser

The SQL parser here is almost an exact copy of the Presto Java SQL parser but with a few changes to make it work in C++ due to C++ keywords. The original Presto Java grammar is located in [Presto SQL grammar](https://github.com/prestodb/presto/blob/master/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4).

The generated visitor APIs are meant to be implemented in order to create the AST classes representing the user defined ANTLR4 grammar.

## Current Build Setup

The generated ANTLR4 code is currently checked into this directory. The BUCK build system contains an `antlr4_grammar` rule that can regenerate these files from the `PrestoSql.g4` grammar file. The generated files are:

- `PrestoSqlLexer.h/cpp` - Lexical analyzer
- `PrestoSqlParser.h/cpp` - Parser implementation
- `PrestoSqlBaseListener.h/cpp` - Base listener pattern implementation
- `PrestoSqlListener.h/cpp` - Listener interface
- `PrestoSqlBaseVisitor.h/cpp` - Base visitor pattern implementation
- `PrestoSqlVisitor.h/cpp` - Visitor interface

## How to Regenerate ANTLR Files

### Prerequisites

- ANTLR4 C++ runtime (automatically handled by Buck)
- Python 3 (for license header processing)

### Step-by-Step Instructions

1. **Navigate to the fbcode directory:**
   ```bash
   cd /path/to/fbsource/fbcode
   ```

2. **Regenerate the ANTLR files:**
   ```bash
   buck build //axiom/sql/presto/grammar:gen_grammar --show-output
   ```

3. **Find the Buck output directory:**
   ```bash

   BUCK_OUT_DIR=$(find ../buck-out -path "*gen_grammar_rewrite*" -name "srcs" -type d -printf "%T@ %p\n" | sort -r -n -k1,1 | head -1 | awk '{print $2}')
   echo "Generated files are in: $BUCK_OUT_DIR"
   ```

4. **Apply license headers to generated files:**
   ```bash
   python3 axiom/sql/presto/grammar/add_license_headers.py "$BUCK_OUT_DIR"
   ```

5. **Copy the corrected files to source directory:**
   ```bash
   cp "$BUCK_OUT_DIR"/* axiom/sql/presto/grammar/
   ```

5. **Fix coding style:**
   ```bash
   arc lint -a
   ```

### Complete One-Line Command

For convenience, run the provided shell script from fbcode directory:

```bash
./axiom/sql/presto/grammar/gen.sh
```

### Verification

After regeneration, verify the files have been updated:

```bash
# Check that license headers are present
head -20 axiom/sql/presto/grammar/PrestoSqlLexer.h

# Verify the build still works
buck build //axiom/sql/presto/grammar:grammar
```

## ANTLR Grammar Configuration

The BUCK rule is configured with the following settings:

```python
antlr4_grammar(
    name = "gen_grammar",
    srcs = ["PrestoSql.g4"],
    gen_visitor = True,
    grammar = "PrestoSql",
    language = "Cpp",
    package = "axiom::sql::presto",
)
```

This generates:
- **C++ code** (`language = "Cpp"`)
- **Visitor pattern classes** (`gen_visitor = True`)
- **Namespace**: `axiom::sql::presto`

## License Header Preservation

ANTLR4 generation does not preserve license headers by default. The `add_license_headers.py` script automatically adds the required Apache license headers to all generated files.

The script:
- ✅ Adds Apache 2.0 license headers to all `PrestoSql*.h` and `PrestoSql*.cpp` files
- ✅ Checks for existing headers to avoid duplication
- ✅ Uses the same license format as other files in the project

## Grammar File Structure

The main grammar file is `PrestoSql.g4`, which defines:
- **Lexer rules**: Token definitions (keywords, operators, literals)
- **Parser rules**: SQL statement syntax and structure
- **C++ namespace**: Generated code is placed in `axiom::sql::presto::parser::generated` namespace

## Troubleshooting

### Build Failures

If the build fails:

1. **Clean and rebuild:**
   ```bash
   buck clean //axiom/sql/presto/grammar:gen_grammar
   buck build //axiom/sql/presto/grammar:gen_grammar
   ```

2. **Check for grammar syntax errors:**
   ```bash
   # Look for ANTLR parsing errors in the build output
   buck build //axiom/sql/presto/grammar:gen_grammar --verbose 2
   ```

### Missing License Headers

If license headers are missing after regeneration:

```bash
# Re-run the license header script
python3 axiom/sql/presto/grammar/add_license_headers.py axiom/sql/presto/grammar/
```

### File Permission Issues

If you encounter permission errors:

```bash
# Make sure you have write permissions to the grammar directory
chmod u+w axiom/sql/presto/grammar/PrestoSql*.{h,cpp}
```

## Development Workflow

When modifying the grammar:

1. **Edit** `PrestoSql.g4` with your changes
2. **Regenerate** using the commands above
3. **Update** any affected AST builder code that uses the generated classes
4. **Test** by running the grammar tests:
   ```bash
   buck test //axiom/sql/presto/grammar/tests:grammar_test
   ```

## Future Improvements

Eventually, the ANTLR4 generated code may be automatically generated as part of the CMake build process (see https://github.com/antlr/antlr4/tree/dev/runtime/Cpp). This would eliminate the need to check in generated files and simplify the development workflow.

## Files in This Directory

- `PrestoSql.g4` - ANTLR4 grammar definition (source file)
- `BUCK` - Buck build configuration
- `add_license_headers.py` - Script to add Apache license headers
- `PrestoSql*.h/cpp` - Generated ANTLR4 files (regenerated from grammar)
- `tests/` - Grammar parsing tests
