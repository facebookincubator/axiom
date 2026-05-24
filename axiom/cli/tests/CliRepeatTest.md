# Smoke tests for --print_timing and --repeat flags

## --print_timing emits a timing line in non-TTY mode

```scrut
$ $CLI --query "SELECT 1" --print_timing 2>/dev/null | grep -oE '^Query ID: \S+ \| Parsing:'
Query ID: * | Parsing: (glob)
```

## --repeat with --query runs the query multiple times

```scrut
$ $CLI --query "SELECT 1" --repeat 3 --print_timing 2>/dev/null | grep -oE '^Query ID: \S+ \| Parsing:'
Query ID: * | Parsing: (glob)
Query ID: * | Parsing: (glob)
Query ID: * | Parsing: (glob)
```

## --repeat with stdin runs the piped SQL multiple times

```scrut
$ echo "SELECT 1" | $CLI --query '' --repeat 3 --print_timing 2>/dev/null | grep -oE '^Query ID: \S+ \| Parsing:'
Query ID: * | Parsing: (glob)
Query ID: * | Parsing: (glob)
Query ID: * | Parsing: (glob)
```

## --repeat stops on the first failure

```scrut
$ $CLI --query "SELECT * FROM nonexistent_table" --repeat 3 --print_timing 2>&1 >/dev/null | grep -oE '^Query ID: \S+ \| Parsing:'
Query ID: * | Parsing: (glob)
```

## --repeat without --print_timing runs the query N times silently

```scrut
$ $CLI --query "SELECT 1 as a" --repeat 3 2>/dev/null | grep 'rows in 1 batches'
(1 rows in 1 batches)
(1 rows in 1 batches)
(1 rows in 1 batches)
```

## --repeat with multi-statement input fails (CLI exits 0; error to stderr)

```scrut
$ $CLI --query "SELECT 1; SELECT 2" --repeat 2 2>&1 >/dev/null | grep -oE 'Expected a single statement.*'
Expected a single statement. If you want to run multiple statements, use parseMultiple().
```

## --repeat must be at least 1 (CLI exits 1)

```scrut
$ $CLI --query "SELECT 1" --repeat 0 2>&1 1>/dev/null
Error: (0 vs. 1) --repeat must be at least 1
[1]
```
