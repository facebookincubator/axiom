---
oncalls: ['velox']
apply_to_regex: 'fbcode/axiom/.*\.(cpp|h|md)'
---

# Axiom Coding Style

Axiom follows the Velox coding style. See `CODING_STYLE.md` in the Velox repository for the complete guide:
- Meta-internal: `fbcode/velox/public_tld/CODING_STYLE.md`
- OSS: `velox/CODING_STYLE.md`

The rules below are additions or clarifications specific to Axiom.

## Naming Conventions

- **Do not abbreviate** variable, function, or class names. Use full, descriptive names.
- Clarity is more important than brevity.

### Examples

| ❌ Avoid | ✅ Prefer |
|----------|-----------|
| `outputCol` | `outputColumn` |
| `val` | `value` |
| `idx` | `index` |
| `iter` | `it` |
| `agg` | `aggregation` |
| `sel` | `selectivity` |
| `rowCount` | `numRows` |

### Exceptions

- Well-established abbreviations in the domain are acceptable (e.g., `id`, `url`, `sql`, `expr`).
- Loop indices like `i`, `j`, `k` are acceptable for simple loops.
- Iterator variables named `it` are acceptable.
- `numXxx` naming pattern is acceptable and preferred over `xxxCount` or `xxxCnt` (e.g., `numRows`, `numKeys`).
- Type aliases defined elsewhere (e.g., `ExprCP`, `ColumnCP`) should be used as-is.

## Comment Style

- Use `///` (triple-slash) only for **public API documentation** (Doxygen-style comments for public class members, functions, and types in header files).
- Use `//` (double-slash) for **private/internal comments** (implementation details, test code, .cpp files).
- **Start comments with active verbs**, not "This class..." or "This method...".
- **Avoid redundant comments** that simply repeat what the code already says.

### Examples

**Documentation comments - use active verbs:**

| ❌ Avoid | ✅ Prefer |
|----------|-----------|
| `/// This class builds query plans.` | `/// Builds query plans.` |
| `/// This method computes selectivity.` | `/// Computes selectivity.` |
| `/// This function returns the cost.` | `/// Returns the cost.` |

**Avoid redundant comments:**

```cpp
// ❌ Avoid - comment just repeats the code
// Increment the counter.
++counter;

// ❌ Avoid - comment states the obvious
// Return the result.
return result;

// ✅ Prefer - no comment needed when code is self-explanatory
++counter;
return result;

// ✅ Prefer - comment explains WHY, not WHAT
// Use a larger buffer to avoid repeated reallocations for typical queries.
buffer.reserve(1024);
```

**Header file (public API):**
```cpp
/// Computes the selectivity of a filter expression.
/// @param expr The filter expression to evaluate.
/// @return The estimated selectivity between 0 and 1.
Selectivity exprSelectivity(ExprCP expr);
```

**Implementation file (.cpp) or test file:**
```cpp
// Helper to create a Value with min/max bounds for testing.
template <typename T>
Value makeValue(float cardinality, T min, T max) { ... }
```

**Private members in header files:**
```cpp
class Foo {
 public:
  /// Public method documentation.
  void publicMethod();

 private:
  // Private helper - not part of public API.
  void privateHelper();

  // Internal state.
  int count_{0};
};
```

## Rationale

- `///` comments are processed by documentation generators (Doxygen) and should only be used for public-facing API documentation.
- Using `///` for internal/test code creates noise and implies documentation intent where none exists.
