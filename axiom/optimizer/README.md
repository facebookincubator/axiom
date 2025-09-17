# Terminology

BaseTable
: A reference to a table from a query. There is one of these for each occurrence of the schema table.

DerivedTable
: Represents a SELECT in a FROM clause. This is the basic unit of planning.

Correlation name
: Unique name of a base or derived table in a query graph. Each table is assigned a unique correlation name. Table columns are qualified using table's correlation name: cname.name. This supports plans where multiple tables have columns with same names.

Join Edge
: An edge in a QueryGraph that represents a join between two tables.

Fanout
: Join fanout or one-to-many join refers to a situation where one row in a table joins to multiple rows in another table. This means the number of rows in the joined result set can be greater than the number of rows in the primary (left) table.

Conjunction
: A statement formed by adding two statements with the connector AND. Individual statements are called conjuncts.

Disjunction
: A statement formed by adding two statements with the connector OR. Individual statements are called disjuncts.

Negation
: A statement formed by adding a NOT to a statement.

# Abbreviations

QG
: QueryGraph

DT
: DerivedTable

CName
: Correlation name.

XxxP
: Raw pointer to Xxx: XxxP := Xxx*

XxxCP
: Raw pointer to constant Xxx: XxxCP := const Xxx*. Remember to place 'const' *after* the type: XxxCP const variableName. (const XxxCP doesn't produce the derired effect.)

XxxVector
: Standard vector of raw pointers to Xxx allocated from the arena: XxxVector := `QGVector<XxxP>`.

CPSpan<T>
: A view on an array of const raw pointers `CPSpan<T> := std::span<const T* const>`;

LRFanout
: Left-to-Right Fanout. The average number of right side rows selected for one row on the left.

RLFanout
: Right-to-Left Fanout. The average number of left side rows selected for one row on the right.

# Memory Management

QGAllocator
: STL compatible allocator that manages std:: containers allocated in the QueryGraphContext arena. Use `make<T>(<args>)` to allocate a new object of type T from the arena.

Name
: Pointer to an arena allocated interned copy of a null terminated string. Used for identifiers. Allows comparing strings by comparing pointers. `Name := const char*`. Use `toName(<string>)` to convert an external string.

# Main Workflow

Optimization class from Plan.h is a bridge between 3 main functions:

1. Make query graph from logical plan tree. (LogicalPlanToGraph.cpp and LogicalPlanSubfields.cpp)
2. Do dynamic programming to make PlanSets. (Plan.cpp)
3. Make a Velox plan from the chosen Plan.  (ToVelox.cpp)

There needs to be an optimization-lifetime context, which is Optimization. It has 3 components that could be called ToGraph, PlanSpace and ToVelox. These together make up the context.
