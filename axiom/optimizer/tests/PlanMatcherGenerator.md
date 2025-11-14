# PlanMatcherGenerator

A utility to generate C++ code for constructing `PlanMatcher` trees from `PlanNode` trees.

## Overview

The `PlanMatcherGenerator` takes a `velox::core::PlanNode` tree and generates C++ code that uses `PlanMatcherBuilder` to construct an equivalent `PlanMatcher` tree. This is useful for:

- Creating test assertions from actual query plans
- Documenting expected plan structures
- Quickly generating matcher code instead of writing it manually

## API

### Main Function

```cpp
std::string generatePlanMatcherCode(
    const PlanNodePtr& planNode,
    const std::string& builderVarName = "builder");
```

**Parameters:**
- `planNode`: The root of the PlanNode tree to convert
- `builderVarName`: Optional name for the builder variable (default: "builder")

**Returns:** A string containing C++ code that constructs a PlanMatcher tree

## Usage Example

```cpp
#include "axiom/optimizer/tests/PlanMatcherGenerator.h"

// Create or obtain a PlanNode tree
auto logicalPlan = toLogicalPlan("SELECT name, age * 2 FROM users WHERE age > 18");
auto plan = toSingleNodePlan(logicalPlan);

// Generate the matcher code
std::string matcherCode = facebook::velox::core::generatePlanMatcherCode(plan);

// Print the generated code
std::cout << matcherCode << std::endl;
```

**Output:**
```cpp
auto builder = core::PlanMatcherBuilder()
  .tableScan("users")
  .filter("age > 18")
  .project({"name", "age * 2 AS c1"})
  .build();
```

## Supported Node Types

The generator supports the following PlanNode types:

### Leaf Nodes
- **TableScanNode**: Generates `.tableScan("table_name")`
- **ValuesNode**: Generates `.values()`
- **ExchangeNode**: Generates `.exchange()`
- **MergeExchangeNode**: Generates `.mergeExchange()`

### Unary Operators (Single Input)
- **FilterNode**: Generates `.filter("predicate")`
- **ProjectNode**: Generates `.project({"expr1", "expr2 AS alias"})`
- **ParallelProjectNode**: Generates `.parallelProject({"expr1", "expr2"})`
- **AggregationNode**: Generates aggregation calls based on step:
  - `.singleAggregation({"key1", "key2"}, {"agg1", "agg2"})`
  - `.partialAggregation({"key1"}, {"count(*) AS cnt"})`
  - `.finalAggregation({"key1"}, {"count(a0) AS cnt"})`
- **UnnestNode**: Generates `.unnest({"replicate_cols"}, {"unnest_cols"})`
- **LimitNode**: Generates `.partialLimit(offset, count)` or `.finalLimit(offset, count)`
- **TopNNode**: Generates `.topN(count)`
- **OrderByNode**: Generates `.orderBy({"col1 ASC NULLS FIRST", "col2 DESC"})`
- **LocalPartitionNode**: Generates `.localPartition()`
- **LocalMergeNode**: Generates `.localMerge()`
- **PartitionedOutputNode**: Generates `.partitionedOutput()`
- **TableWriteNode**: Generates `.tableWrite()`

### Binary Operators (Two Inputs)
- **HashJoinNode**: Generates code with a separate matcher for the right side:
  ```cpp
  auto rightMatcher = core::PlanMatcherBuilder()
    .tableScan("right_table")
    .build();

  auto builder = core::PlanMatcherBuilder()
    .tableScan("left_table")
    .hashJoin(rightMatcher, JoinType::kInner)
    .build();
  ```

## Integration

### Adding to Your Test File

1. Include the header:
   ```cpp
   #include "axiom/optimizer/tests/PlanMatcherGenerator.h"
   ```

2. Generate matcher code for your plan:
   ```cpp
   auto plan = ...; // your PlanNode
   auto code = facebook::velox::core::generatePlanMatcherCode(plan);
   std::cout << code << std::endl;
   ```

3. Copy the generated code into your test:
   ```cpp
   TEST_F(MyTest, testQuery) {
     auto plan = toSingleNodePlan(toLogicalPlan("SELECT ..."));

     // Generated code goes here:
     auto matcher = core::PlanMatcherBuilder()
       .tableScan("my_table")
       .filter("condition")
       .build();

     AXIOM_ASSERT_PLAN(plan, matcher);
   }
   ```

## Advanced Usage

### Customizing the Builder Variable Name

```cpp
std::string code = generatePlanMatcherCode(plan, "myMatcher");
// Generates: auto myMatcher = core::PlanMatcherBuilder()...
```

### Handling Complex Joins

For join nodes, the generator creates a separate matcher for the right side:

```cpp
// Input: A join between two table scans
auto plan = ...; // SELECT * FROM a JOIN b ON a.id = b.id

// Generated output:
auto rightMatcher = core::PlanMatcherBuilder()
  .tableScan("b")
  .build();

auto builder = core::PlanMatcherBuilder()
  .tableScan("a")
  .hashJoin(rightMatcher, JoinType::kInner)
  .build();
```

## Implementation Details

The generator uses the following approach:

1. **Post-order Traversal**: Processes the PlanNode tree from leaves to root
2. **Dynamic Type Detection**: Uses `dynamic_cast` to identify node types
3. **Expression Conversion**: Converts `TypedExpr` objects to string representations
4. **Code Generation**: Builds up the PlanMatcherBuilder chain

### Node Type Handling

For each node type, the generator:
- Extracts relevant properties (predicates, expressions, keys, etc.)
- Formats them as C++ string literals or vectors
- Generates the appropriate PlanMatcherBuilder method call

## Limitations

1. **Expression Fidelity**: The generator uses `toString()` on expressions, which may not exactly match the original SQL syntax
2. **Type Information**: Currently doesn't generate detailed type information for table scans
3. **Complex Filters**: Subfield filters in HiveScan nodes are not yet supported
4. **Unknown Node Types**: Generates a comment for unsupported node types

## Future Enhancements

Potential improvements:
- Add support for more specialized matchers (e.g., `hiveScan` with filters)
- Include output type information where relevant
- Better formatting for complex expressions
- Support for window functions and other advanced features

## Files

- `PlanMatcherGenerator.h`: Header file with function declaration
- `PlanMatcherGenerator.cpp`: Implementation
- `PlanMatcherGeneratorTest.cpp`: Unit tests
- `PlanMatcherGeneratorExample.cpp`: Usage examples

## See Also

- `PlanMatcher.h`: PlanMatcher interface and PlanMatcherBuilder
- `PlanMatcher.cpp`: PlanMatcher implementations
- `velox/core/PlanNode.h`: PlanNode definitions
