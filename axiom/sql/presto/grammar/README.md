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

## Generated Files

The ANTLR4 generated code is checked into this directory. The CMake build
uses these files directly. Regeneration is only needed when modifying
`PrestoSql.g4`.

- `PrestoSqlLexer.h/cpp` - Lexical analyzer
- `PrestoSqlParser.h/cpp` - Parser implementation
- `PrestoSqlBaseListener.h/cpp` - Base listener pattern implementation
- `PrestoSqlListener.h/cpp` - Listener interface
- `PrestoSqlBaseVisitor.h/cpp` - Base visitor pattern implementation
- `PrestoSqlVisitor.h/cpp` - Visitor interface

## Running Tests

```bash
# Buck
buck test //axiom/sql/presto/grammar/tests:grammar_test

# CMake
ctest --test-dir _build/debug -R axiom_sql_presto_grammar
```

## Regenerating ANTLR Files

Regeneration is only needed when modifying `PrestoSql.g4`.

### Using the ANTLR4 jar

Download the ANTLR4 tool jar (version must match the runtime version used
by the project, currently 4.13.2):

```bash
curl -O https://www.antlr.org/download/antlr-4.13.2-complete.jar
```

Generate C++ files from the grammar (run from the repo root):

```bash
java -jar antlr-4.13.2-complete.jar \
    -Dlanguage=Cpp \
    -visitor \
    -package axiom::sql::presto::parser::generated \
    -o /tmp/antlr-output \
    axiom/sql/presto/grammar/PrestoSql.g4
```

Apply license headers and copy the generated files:

```bash
python3 axiom/sql/add_license_headers.py \
    /tmp/antlr-output/axiom/sql/presto/grammar
cp /tmp/antlr-output/axiom/sql/presto/grammar/PrestoSql*.{h,cpp} \
    axiom/sql/presto/grammar/
```

### Using Buck (Meta-internal)

From the fbcode directory, run the provided script:

```bash
./axiom/sql/presto/grammar/gen.sh
```

Or step by step:

1. **Regenerate the ANTLR files:**
   ```bash
   buck build //axiom/sql/presto/grammar:gen_grammar --show-output
   ```

2. **Find the Buck output directory:**
   ```bash
   BUCK_OUT_DIR=$(find ../buck-out -path "*gen_grammar_rewrite*" -name "srcs" -type d -printf "%T@ %p\n" | sort -r -n -k1,1 | head -1 | awk '{print $2}')
   ```

3. **Apply license headers and copy:**
   ```bash
   python3 axiom/sql/add_license_headers.py "$BUCK_OUT_DIR"
   cp "$BUCK_OUT_DIR"/* axiom/sql/presto/grammar/
   ```

4. **Fix coding style:**
   ```bash
   arc lint -a
   ```

## Grammar File Structure

The main grammar file is `PrestoSql.g4`, which defines:
- **Lexer rules**: Token definitions (keywords, operators, literals)
- **Parser rules**: SQL statement syntax and structure
- **C++ namespace**: Generated code is placed in `axiom::sql::presto::parser::generated` namespace

## Development Workflow

When modifying the grammar:

1. **Edit** `PrestoSql.g4` with your changes
2. **Regenerate** using `gen.sh`
3. **Update** any affected AST builder code that uses the generated classes
4. **Test** by running the grammar tests

## License Header Preservation

ANTLR4 generation does not preserve license headers by default. The `add_license_headers.py` script automatically adds the required Apache license headers to all generated files.

## Future Improvements

Eventually, the ANTLR4 generated code may be automatically generated as part of the CMake build process (see https://github.com/antlr/antlr4/tree/dev/runtime/Cpp). This would eliminate the need to check in generated files and simplify the development workflow.

## Files in This Directory

- `PrestoSql.g4` - ANTLR4 grammar definition (source file)
- `gen.sh` - Script to regenerate ANTLR files (Meta-internal, requires Buck)
- `PrestoSql*.h/cpp` - Generated ANTLR4 files (checked in, regenerated from grammar)
- `tests/` - Grammar parsing tests
